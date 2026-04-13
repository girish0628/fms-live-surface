"""Unit tests for OutputHandlerService."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.services.output_handler_service import OutputHandlerService


def _make_dummy_raster(path: Path) -> None:
    path.write_bytes(b"FAKE_TIFF_DATA")


def _make_dummy_shapefile(base: Path) -> None:
    for ext in (".shp", ".dbf", ".shx", ".prj"):
        base.with_suffix(ext).write_bytes(b"FAKE_SHP")


def test_publish_outputs_creates_structure(tmp_path):
    # Prepare dummy source files
    raster = tmp_path / "src_elevation.tif"
    boundary = tmp_path / "src_boundary.shp"
    _make_dummy_raster(raster)
    _make_dummy_shapefile(boundary)

    output_root = tmp_path / "FMS_Output"
    svc = OutputHandlerService(
        site="WB",
        output_root=str(output_root),
        raster_path=str(raster),
        boundary_path=str(boundary),
        processing_metadata={"cell_size": 2, "sourceFiles": {}, "processing": {}},
    )
    result = svc.publish_outputs()

    assert result["status"] == "SUCCESS"
    out_dir = Path(result["output_dir"])

    assert (out_dir / "WB_elevation.tif").exists()
    assert (out_dir / "WB_boundary.shp").exists()
    assert (out_dir / "metadata.json").exists()
    assert (out_dir / "ready.flag").exists()


def test_metadata_json_structure(tmp_path):
    raster = tmp_path / "src.tif"
    boundary = tmp_path / "src.shp"
    _make_dummy_raster(raster)
    _make_dummy_shapefile(boundary)

    output_root = tmp_path / "FMS_Output"
    svc = OutputHandlerService(
        site="ER",
        output_root=str(output_root),
        raster_path=str(raster),
        boundary_path=str(boundary),
        processing_metadata={"cell_size": 2, "sourceFiles": {"snippetCount": 10}, "processing": {}},
    )
    result = svc.publish_outputs()
    metadata = json.loads(Path(result["metadata_path"]).read_text())

    assert metadata["site"] == "ER"
    assert metadata["status"] == "ready_for_publish"
    assert metadata["output"]["format"] == "GeoTIFF"
    assert metadata["output"]["cellSize"] == 2


def test_cleanup_removes_old_folders(tmp_path):
    import time
    from datetime import datetime, timezone, timedelta

    output_root = tmp_path / "FMS_Output"
    site_dir = output_root / "WB"
    site_dir.mkdir(parents=True)

    # Create a folder with an old timestamp name
    old_ts = (datetime.now(tz=timezone.utc) - timedelta(hours=50)).strftime("%Y%m%d_%H%M")
    old_folder = site_dir / old_ts
    old_folder.mkdir()
    (old_folder / "dummy.tif").write_bytes(b"x")

    # Create a recent folder
    new_ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M")
    new_folder = site_dir / new_ts
    new_folder.mkdir()

    raster = tmp_path / "r.tif"
    boundary = tmp_path / "b.shp"
    _make_dummy_raster(raster)
    _make_dummy_shapefile(boundary)

    svc = OutputHandlerService(
        site="WB",
        output_root=str(output_root),
        raster_path=str(raster),
        boundary_path=str(boundary),
        processing_metadata={},
        retention_hours=48,
    )
    removed = svc.cleanup_old_outputs()

    assert removed >= 1
    assert not old_folder.exists()
    assert new_folder.exists()
