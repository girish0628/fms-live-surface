"""Unit tests for SnippetConversionService (no arcpy required)."""
from __future__ import annotations

import csv
import struct
from pathlib import Path

import pytest

from src.services.snippet_conversion_service import SnippetConversionService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_fake_snp(path: Path, points: list[tuple[float, float, float, float]]) -> None:
    """Write a minimal fake .snp binary file."""
    with open(path, "wb") as f:
        f.write(b"\x00" * 16)  # 16-byte header
        for x, y, z, ts_epoch in points:
            f.write(struct.pack("<ddd", x, y, z))
            f.write(struct.pack("<d", ts_epoch))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_z_adjustment():
    svc = SnippetConversionService(
        site="WB",
        input_folder="",
        output_folder="",
        z_adjustment=3.155,
    )
    pts = [(100.0, 200.0, 10.0, "2025-01-01T00:00:00")]
    adjusted = svc._apply_z_adjustment(pts)
    assert adjusted[0][2] == pytest.approx(13.155)


def test_filter_noise_removes_high_z():
    svc = SnippetConversionService(
        site="WB",
        input_folder="",
        output_folder="",
        max_z=100.0,
    )
    pts = [
        (0.0, 0.0, 50.0, "ts"),
        (1.0, 1.0, 200.0, "ts"),  # should be removed
        (2.0, 2.0, 100.0, "ts"),  # exactly at limit — kept
    ]
    result = svc._filter_noise(pts)
    assert len(result) == 2
    assert all(p[2] <= 100.0 for p in result)


def test_despike_clamps_outliers():
    svc = SnippetConversionService(
        site="WB",
        input_folder="",
        output_folder="",
        despike=True,
    )
    # Most points at Z=50, one spike at Z=200
    pts = [(float(i), 0.0, 50.0, "ts") for i in range(10)]
    pts.append((99.0, 0.0, 200.0, "ts"))
    result = svc._despike(pts)
    # Spike should be clamped to the median (50)
    spike = next(p for p in result if p[0] == 99.0)
    assert spike[2] == pytest.approx(50.0, abs=1.0)


def test_parse_snippet_file_reads_points(tmp_path):
    snp = tmp_path / "test.snp"
    import time

    now = time.time()
    raw_pts = [(400000.0, 7000000.0, 350.0, now), (400001.0, 7000001.0, 351.0, now)]
    _write_fake_snp(snp, raw_pts)

    svc = SnippetConversionService(site="WB", input_folder="", output_folder="")
    parsed = svc._parse_snippet_file(snp)

    assert len(parsed) == 2
    assert parsed[0][0] == pytest.approx(400000.0)
    assert parsed[0][2] == pytest.approx(350.0)


def test_write_csv_creates_file(tmp_path):
    svc = SnippetConversionService(site="WB", input_folder="", output_folder="")
    pts = [(1.0, 2.0, 3.0, "2025-01-01T00:00:00")]
    out = tmp_path / "out.csv"
    svc._write_csv(pts, out)

    assert out.exists()
    rows = list(csv.DictReader(out.open()))
    assert rows[0]["X"] == "1.0"
    assert rows[0]["Z"] == "3.0"


def test_convert_end_to_end(tmp_path):
    """End-to-end test with fake .snp files; arcpy is skipped (no licence)."""
    import time

    landing = tmp_path / "landing"
    landing.mkdir()
    staging = tmp_path / "staging"
    staging.mkdir()

    snp = landing / "sample.snp"
    _write_fake_snp(snp, [(400000.0, 7000000.0, 350.0, time.time())])

    svc = SnippetConversionService(
        site="WB",
        input_folder=str(landing),
        output_folder=str(staging),
        z_adjustment=0.0,
        aoi_feature_class="",  # skip AOI filter
    )
    result = svc.convert()

    assert result["status"] == "SUCCESS"
    assert result["snippet_count"] == 1
    assert result["valid_points"] >= 1
    assert Path(result["csv_path"]).exists()
    assert Path(result["config_path"]).exists()
