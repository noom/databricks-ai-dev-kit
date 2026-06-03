"""Unit tests for customization.export_query_patch.

No live workspace, server import, or OAuth is needed: path-sandbox logic is
pure, and the streaming writer is exercised with stand-in link objects and a
monkeypatched downloader.
"""

import csv

import pytest

from customization import export_query_patch as ep


# ---------------------------------------------------------------------------
# Path sandboxing
# ---------------------------------------------------------------------------


def test_relative_path_resolves_inside_base(tmp_path):
    dest = ep.resolve_export_path("sub/dir/out.csv", base_dir=tmp_path)
    assert dest == (tmp_path / "sub/dir/out.csv").resolve()
    assert tmp_path.resolve() in dest.parents


def test_absolute_path_inside_base_is_allowed(tmp_path):
    inside = tmp_path / "ok.csv"
    dest = ep.resolve_export_path(str(inside), base_dir=tmp_path)
    assert dest == inside.resolve()


def test_dotdot_traversal_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="outside the export directory"):
        ep.resolve_export_path("../escape.csv", base_dir=tmp_path)


def test_absolute_path_outside_base_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="outside the export directory"):
        ep.resolve_export_path("/etc/passwd", base_dir=tmp_path)


@pytest.mark.parametrize("bad", ["", "   ", ".", "./"])
def test_no_filename_is_rejected(tmp_path, bad):
    with pytest.raises(ValueError):
        ep.resolve_export_path(bad, base_dir=tmp_path)


# ---------------------------------------------------------------------------
# CSV header derivation
# ---------------------------------------------------------------------------


def test_csv_header_line_is_rfc_quoted():
    line = ep._csv_header_line(["a", "b,c", 'd"e'])
    # csv module quotes the field containing a comma and escapes the quote.
    assert line == 'a,"b,c","d""e"\n'


# ---------------------------------------------------------------------------
# Chunk collection + streaming write
# ---------------------------------------------------------------------------


class _FakeLink:
    def __init__(self, chunk_index, payload, next_chunk_index=None, row_offset=0):
        self.chunk_index = chunk_index
        self.next_chunk_index = next_chunk_index
        self.row_offset = row_offset
        self.external_link = f"https://example/{chunk_index}"
        self.http_headers = {}
        self._payload = payload


class _FakeResult:
    def __init__(self, external_links, next_chunk_index=None):
        self.external_links = external_links
        self.next_chunk_index = next_chunk_index


class _FakeClient:
    """Stand-in exposing only get_statement_result_chunk_n for pagination."""

    def __init__(self, chunks_by_index):
        self._chunks = chunks_by_index

        class _SE:
            def get_statement_result_chunk_n(_self, statement_id, chunk_index):
                return self._chunks[chunk_index]

        self.statement_execution = _SE()


def test_iter_external_links_follows_chunk_chain():
    link0 = _FakeLink(0, b"a\n", next_chunk_index=1)
    link1 = _FakeLink(1, b"b\n", next_chunk_index=None)
    first = _FakeResult([link0], next_chunk_index=1)
    second = _FakeResult([link1], next_chunk_index=None)
    client = _FakeClient({1: second})

    links = list(ep._iter_external_links(client, "stmt-1", first))
    assert [link.chunk_index for link in links] == [0, 1]


def test_iter_external_links_orders_within_result():
    link_b = _FakeLink(1, b"b\n", row_offset=10)
    link_a = _FakeLink(0, b"a\n", row_offset=0)
    first = _FakeResult([link_b, link_a], next_chunk_index=None)
    links = list(ep._iter_external_links(_FakeClient({}), "stmt-1", first))
    assert [link.chunk_index for link in links] == [0, 1]


def test_iter_external_links_is_lazy():
    # The next chunk must not be fetched until the previous one is consumed —
    # this is what keeps presigned links fresh during a long download.
    import types

    link0 = _FakeLink(0, b"a\n", next_chunk_index=1)
    link1 = _FakeLink(1, b"b\n", next_chunk_index=None)
    first = _FakeResult([link0], next_chunk_index=1)

    fetched = []

    class _TrackingClient:
        def __init__(self):
            outer = self

            class _SE:
                def get_statement_result_chunk_n(_self, statement_id, chunk_index):
                    fetched.append(chunk_index)
                    return _FakeResult([link1], next_chunk_index=None)

            self.statement_execution = _SE()
            _ = outer

    gen = ep._iter_external_links(_TrackingClient(), "stmt-1", first)
    assert isinstance(gen, types.GeneratorType)
    assert next(gen).chunk_index == 0
    assert fetched == []  # chunk 1 NOT fetched until we ask for the next link
    assert next(gen).chunk_index == 1
    assert fetched == [1]


def test_write_csv_streams_chunk_bytes_verbatim(tmp_path, monkeypatch):
    # Databricks CSV chunks already carry the header in the first chunk; the
    # writer must stream bytes as-is (no synthesized header) to avoid a dupe.
    monkeypatch.setattr(ep, "_download_link", lambda link: link._payload)
    dest = tmp_path / "out.csv"
    links = [
        _FakeLink(0, b"id,name\n1,alice\n2,bob\n"),
        _FakeLink(1, b"3,carol\n"),
    ]
    ep._write_csv(dest, ["id", "name"], links)

    rows = list(csv.reader(dest.read_text().splitlines()))
    assert rows == [
        ["id", "name"],
        ["1", "alice"],
        ["2", "bob"],
        ["3", "carol"],
    ]


def test_write_csv_empty_result_writes_header_only(tmp_path):
    # No external links (empty result) -> header-only CSV from the manifest.
    dest = tmp_path / "empty.csv"
    ep._write_csv(dest, ["id", "name"], [])
    assert dest.read_text() == "id,name\n"


def test_write_csv_failure_preserves_prior_file_and_leaves_no_partial(tmp_path, monkeypatch):
    # A mid-stream chunk failure must not clobber a prior good export, and must
    # leave no partial/temp file behind. (Atomic write: temp + os.replace.)
    dest = tmp_path / "out.csv"
    dest.write_text("PRIOR GOOD DATA\n")

    calls = {"n": 0}

    def flaky(link):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("chunk download failed")
        return link._payload

    monkeypatch.setattr(ep, "_download_link", flaky)
    links = [_FakeLink(0, b"id,name\n1,a\n"), _FakeLink(1, b"2,b\n")]

    with pytest.raises(RuntimeError, match="chunk download failed"):
        ep._write_csv(dest, ["id", "name"], links)

    assert dest.read_text() == "PRIOR GOOD DATA\n"  # untouched
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]  # no .part leftover


def test_write_csv_failure_creates_no_destination(tmp_path, monkeypatch):
    # If dest didn't exist and the write fails, no file should appear at dest.
    dest = tmp_path / "new.csv"
    monkeypatch.setattr(
        ep, "_download_link", lambda link: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(RuntimeError, match="boom"):
        ep._write_csv(dest, ["id"], [_FakeLink(0, b"x")])

    assert not dest.exists()
    assert list(tmp_path.iterdir()) == []  # temp cleaned up


def test_write_csv_refuses_to_clobber_when_overwrite_false(tmp_path, monkeypatch):
    # Even if dest appears after the caller's early check (simulated here by it
    # already existing), the atomic move must not clobber it when overwrite=False.
    monkeypatch.setattr(ep, "_download_link", lambda link: link._payload)
    dest = tmp_path / "out.csv"
    dest.write_text("ORIGINAL\n")

    with pytest.raises(ValueError, match="already exists"):
        ep._write_csv(dest, ["id"], [_FakeLink(0, b"id\n1\n")], overwrite=False)

    assert dest.read_text() == "ORIGINAL\n"  # untouched
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]  # no .part leftover


def test_write_csv_overwrites_when_true(tmp_path, monkeypatch):
    monkeypatch.setattr(ep, "_download_link", lambda link: link._payload)
    dest = tmp_path / "out.csv"
    dest.write_text("OLD\n")

    ep._write_csv(dest, ["id", "name"], [_FakeLink(0, b"id,name\n1,a\n")], overwrite=True)

    assert dest.read_text() == "id,name\n1,a\n"
    assert [p.name for p in tmp_path.iterdir()] == ["out.csv"]


def test_write_csv_refuses_parent_symlink_outside_sandbox(tmp_path, monkeypatch):
    # Simulates a symlink introduced under the base after resolve_export_path ran:
    # base/sub -> external. The re-check must refuse to write and create nothing
    # outside the sandbox.
    monkeypatch.setattr(ep, "_download_link", lambda link: link._payload)
    base = tmp_path / "base"
    base.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    (base / "sub").symlink_to(external, target_is_directory=True)
    dest = base / "sub" / "out.csv"  # resolves to external/out.csv

    with pytest.raises(ValueError, match="outside the export sandbox"):
        ep._write_csv(dest, ["id"], [_FakeLink(0, b"id\n1\n")], overwrite=True, base_dir=base)

    assert list(external.iterdir()) == []  # nothing written outside


def test_write_csv_allows_parent_symlink_inside_sandbox(tmp_path, monkeypatch):
    # A symlink that still resolves *inside* the base is fine (containment, not a
    # blanket symlink ban).
    monkeypatch.setattr(ep, "_download_link", lambda link: link._payload)
    base = tmp_path / "base"
    base.mkdir()
    (base / "real").mkdir()
    (base / "alias").symlink_to(base / "real", target_is_directory=True)
    dest = base / "alias" / "out.csv"  # resolves to base/real/out.csv (inside)

    ep._write_csv(
        dest, ["id", "name"], [_FakeLink(0, b"id,name\n1,a\n")], overwrite=True, base_dir=base
    )

    assert (base / "real" / "out.csv").read_text() == "id,name\n1,a\n"


# ---------------------------------------------------------------------------
# Timeout clamp
# ---------------------------------------------------------------------------


def test_clamp_statement_timeout():
    ceiling = ep.EXPORT_TOOL_TIMEOUT_CEILING
    assert ep._clamp_statement_timeout(ceiling + 5000) == ceiling  # over → clamped
    assert ep._clamp_statement_timeout(120) == 120  # under → unchanged
    assert ep._clamp_statement_timeout(ceiling) == ceiling  # exactly → unchanged


# ---------------------------------------------------------------------------
# Retention sweep
# ---------------------------------------------------------------------------


def _make_aged_file(path, age_days):
    import os
    import time

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x")
    mtime = time.time() - age_days * 86400
    os.utime(path, (mtime, mtime))


def test_sweep_removes_old_keeps_recent(tmp_path):
    old = tmp_path / "old.csv"
    recent = tmp_path / "recent.csv"
    _make_aged_file(old, age_days=10)
    _make_aged_file(recent, age_days=1)

    removed = ep.sweep_old_exports(base_dir=tmp_path, retention_days=7)

    assert removed == 1
    assert not old.exists()
    assert recent.exists()


def test_sweep_disabled_when_retention_not_positive(tmp_path):
    old = tmp_path / "old.csv"
    _make_aged_file(old, age_days=999)
    assert ep.sweep_old_exports(base_dir=tmp_path, retention_days=0) == 0
    assert old.exists()


def test_sweep_does_not_descend_dir_symlink(tmp_path):
    # A directory symlink inside the export dir pointing OUTSIDE must never be
    # followed — files behind it must survive the sweep.
    base = tmp_path / "base"
    base.mkdir()
    external = tmp_path / "external"
    external.mkdir()
    victim = external / "victim.csv"
    _make_aged_file(victim, age_days=999)
    (base / "link").symlink_to(external, target_is_directory=True)

    ep.sweep_old_exports(base_dir=base, retention_days=7)

    assert victim.exists()  # external file must NOT be deleted


def test_sweep_skips_file_symlink_to_external(tmp_path):
    # A file symlink to an external old file must not cause the target's deletion.
    base = tmp_path / "base"
    base.mkdir()
    external = tmp_path / "ext"
    external.mkdir()
    target = external / "target.csv"
    _make_aged_file(target, age_days=999)
    (base / "alias.csv").symlink_to(target)

    ep.sweep_old_exports(base_dir=base, retention_days=7)

    assert target.exists()  # symlink target untouched


def test_sweep_prunes_empty_subdirs(tmp_path):
    nested = tmp_path / "sub" / "deep.csv"
    _make_aged_file(nested, age_days=30)
    ep.sweep_old_exports(base_dir=tmp_path, retention_days=7)
    assert not nested.exists()
    assert not (tmp_path / "sub").exists()  # emptied dir pruned
    assert tmp_path.exists()  # base dir never removed


def test_get_retention_days_default_and_override(monkeypatch):
    monkeypatch.delenv("DATABRICKS_MCP_EXPORT_RETENTION_DAYS", raising=False)
    assert ep.get_export_retention_days() == ep._DEFAULT_RETENTION_DAYS
    monkeypatch.setenv("DATABRICKS_MCP_EXPORT_RETENTION_DAYS", "3")
    assert ep.get_export_retention_days() == 3
    monkeypatch.setenv("DATABRICKS_MCP_EXPORT_RETENTION_DAYS", "garbage")
    assert ep.get_export_retention_days() == ep._DEFAULT_RETENTION_DAYS  # falls back


def test_download_link_applies_http_headers(monkeypatch):
    captured = {}

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return b"data"

    def _fake_urlopen(req):
        captured["headers"] = dict(req.header_items())
        return _Resp()

    monkeypatch.setattr(ep.urllib.request, "urlopen", _fake_urlopen)

    link = _FakeLink(0, b"")
    link.http_headers = {"X-Amz-Token": "abc"}
    out = ep._download_link(link)
    assert out == b"data"
    # urllib title-cases header keys; just confirm the value made it through.
    assert "abc" in captured["headers"].values()
