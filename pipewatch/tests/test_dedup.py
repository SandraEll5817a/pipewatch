"""Tests for pipewatch.dedup."""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipewatch.dedup import (
    _fingerprint,
    DedupEntry,
    is_duplicate,
    load_dedup,
    save_dedup,
)


@pytest.fixture
def dedup_file(tmp_path):
    return tmp_path / "dedup.json"


def test_fingerprint_is_deterministic():
    fp1 = _fingerprint("pipe_a", ["duration", "error_rate"])
    fp2 = _fingerprint("pipe_a", ["error_rate", "duration"])
    assert fp1 == fp2


def test_fingerprint_differs_by_pipeline():
    fp1 = _fingerprint("pipe_a", ["duration"])
    fp2 = _fingerprint("pipe_b", ["duration"])
    assert fp1 != fp2


def test_load_dedup_missing_file(dedup_file):
    result = load_dedup(dedup_file)
    assert result == {}


def test_load_dedup_corrupt_json(dedup_file):
    dedup_file.write_text("not json")
    result = load_dedup(dedup_file)
    assert result == {}


def test_save_and_load_roundtrip(dedup_file):
    now = datetime.now(timezone.utc).isoformat()
    entry = DedupEntry(fingerprint="abc123", pipeline="p", first_seen=now, last_seen=now, count=2)
    save_dedup({"abc123": entry}, dedup_file)
    loaded = load_dedup(dedup_file)
    assert "abc123" in loaded
    assert loaded["abc123"].count == 2
    assert loaded["abc123"].pipeline == "p"


def test_first_alert_not_duplicate(dedup_file):
    result = is_duplicate("pipeline_x", ["duration"], ttl_seconds=3600, path=dedup_file)
    assert result is False


def test_second_alert_within_ttl_is_duplicate(dedup_file):
    is_duplicate("pipeline_x", ["duration"], ttl_seconds=3600, path=dedup_file)
    result = is_duplicate("pipeline_x", ["duration"], ttl_seconds=3600, path=dedup_file)
    assert result is True


def test_alert_after_ttl_not_duplicate(dedup_file):
    fp = _fingerprint("pipeline_x", ["duration"])
    old_time = (datetime.now(timezone.utc) - timedelta(seconds=7200)).isoformat()
    entry = DedupEntry(fingerprint=fp, pipeline="pipeline_x", first_seen=old_time, last_seen=old_time)
    save_dedup({fp: entry}, dedup_file)
    result = is_duplicate("pipeline_x", ["duration"], ttl_seconds=3600, path=dedup_file)
    assert result is False


def test_duplicate_increments_count(dedup_file):
    is_duplicate("pipeline_x", ["error_rate"], ttl_seconds=3600, path=dedup_file)
    is_duplicate("pipeline_x", ["error_rate"], ttl_seconds=3600, path=dedup_file)
    is_duplicate("pipeline_x", ["error_rate"], ttl_seconds=3600, path=dedup_file)
    entries = load_dedup(dedup_file)
    fp = _fingerprint("pipeline_x", ["error_rate"])
    assert entries[fp].count == 3


def test_different_violations_not_duplicate(dedup_file):
    is_duplicate("pipeline_x", ["duration"], ttl_seconds=3600, path=dedup_file)
    result = is_duplicate("pipeline_x", ["error_rate"], ttl_seconds=3600, path=dedup_file)
    assert result is False
