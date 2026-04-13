"""
Raster generation service using ArcPy.

Implements the MTD Process Data – Elevation pipeline:
    CSV (MGA50) → 3D Feature Class → TIN → Raster (GeoTIFF)
    + Boundary polygon generation and road-buffer exclusion clipping.

ArcGIS 3D Analyst and Spatial Analyst extensions are required.
"""
from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.core.exceptions import RasterGenerationError
from src.core.logger import get_logger


@dataclass(frozen=True)
class RasterGenerationService:
    """
    Generate an elevation raster and boundary polygon from a point CSV.

    Parameters
    ----------
    site : str
        Mine site code (WB, ER, TG, JB, NM).
    csv_path : str
        Path to the MGA50-projected points CSV (X, Y, Z columns).
    output_dir : str
        Directory where raster and boundary outputs are written.
    scratch_gdb : str
        Path to a scratch File Geodatabase for intermediate datasets.
    spatial_ref_prj : str
        Path to .prj file for the output spatial reference (GDA2020 MGA Zone 50).
    cell_size : float
        Raster cell size in metres (default 2 m per GridSize parameter).
    exclusion_fc : str
        Optional path to road-buffer exclusion polygon feature class
        (MTD_Live_RoadsBuffered).  Leave empty to skip.
    x_field : str
        X coordinate column name in the CSV.
    y_field : str
        Y coordinate column name in the CSV.
    z_field : str
        Z coordinate column name in the CSV.
    """

    site: str
    csv_path: str
    output_dir: str
    scratch_gdb: str
    spatial_ref_prj: str = ""
    cell_size: float = 2.0
    exclusion_fc: str = ""
    x_field: str = "X"
    y_field: str = "Y"
    z_field: str = "Z"

    def generate(self) -> dict[str, Any]:
        """
        Run the full raster generation pipeline.

        Returns
        -------
        dict[str, Any]
            ``status``, ``site``, ``raster_path``, ``boundary_path``,
            ``output_dir``.

        Raises
        ------
        RasterGenerationError
            If any arcpy step fails.
        """
        logger = get_logger(__name__)
        logger.info("=" * 60)
        logger.info("Raster generation started — site: %s", self.site)

        try:
            import arcpy  # noqa: PLC0415
            from arcpy import env  # noqa: PLC0415

            # ------------------------------------------------------------------
            # Scratch GDB — create dynamically if it does not already exist.
            # Track whether we created it so we can clean it up afterwards.
            # ------------------------------------------------------------------
            scratch_gdb_created = False
            if not arcpy.Exists(self.scratch_gdb):
                gdb_parent = str(Path(self.scratch_gdb).parent)
                gdb_name = Path(self.scratch_gdb).name
                arcpy.management.CreateFileGDB(gdb_parent, gdb_name)
                logger.info("Created temporary scratch GDB: %s", self.scratch_gdb)
                scratch_gdb_created = True

            try:
                arcpy.CheckOutExtension("3D")
                arcpy.CheckOutExtension("Spatial")

                env.overwriteOutput = True
                env.workspace = self.scratch_gdb

                sr = arcpy.SpatialReference()
                sr.loadFromString(Path(self.spatial_ref_prj).read_text(encoding="utf-8").strip())
                out_dir = Path(self.output_dir)
                out_dir.mkdir(parents=True, exist_ok=True)

                fc_name = f"{self.site}_pts"
                tin_path = str(Path(self.scratch_gdb).parent / f"{self.site}_tin")
                raw_raster = str(out_dir / f"{self.site}_elevation_raw.tif")
                raster_path = str(out_dir / f"{self.site}_elevation.tif")
                boundary_raw_fc = os.path.join(self.scratch_gdb, f"{self.site}_boundary_raw")
                boundary_fc = os.path.join(self.scratch_gdb, f"{self.site}_boundary")
                boundary_shp = str(out_dir / f"{self.site}_boundary.shp")

                # ----------------------------------------------------------------
                # Step 1: CSV → 3D Feature Class
                # ----------------------------------------------------------------
                logger.info("Step 1: CSV → 3D Feature Class")
                arcpy.management.XYTableToPoint(
                    in_table=self.csv_path,
                    out_feature_class=fc_name,
                    x_field=self.x_field,
                    y_field=self.y_field,
                    z_field=self.z_field,
                    coordinate_system=sr,
                )
                point_count = int(arcpy.management.GetCount(fc_name).getOutput(0))
                logger.info("Created feature class with %d points", point_count)

                # ----------------------------------------------------------------
                # Step 2: 3D Feature Class → TIN
                # ----------------------------------------------------------------
                logger.info("Step 2: 3D Feature Class → TIN")
                arcpy.ddd.CreateTin(
                    out_tin=tin_path,
                    spatial_reference=sr,
                    in_features=f"{fc_name} Shape.Z masspoints",
                )
                logger.info("TIN created: %s", tin_path)

                # ----------------------------------------------------------------
                # Step 3: TIN → Raster (GeoTIFF, cell size = cell_size metres)
                # ----------------------------------------------------------------
                logger.info("Step 3: TIN → Raster (cell size: %.1f m)", self.cell_size)
                arcpy.ddd.TinRaster(
                    in_tin=tin_path,
                    out_raster=raw_raster,
                    data_type="FLOAT",
                    method="LINEAR",
                    sample_distance=f"CELLSIZE {self.cell_size}",
                    z_factor=1.0,
                )
                logger.info("Raw raster written: %s", raw_raster)

                # ----------------------------------------------------------------
                # Step 4: Boundary polygon from point convex hull
                # ----------------------------------------------------------------
                logger.info("Step 4: Boundary polygon (convex hull)")
                arcpy.management.MinimumBoundingGeometry(
                    in_features=fc_name,
                    out_feature_class=boundary_raw_fc,
                    geometry_type="CONVEX_HULL",
                    group_option="ALL",
                )

                # Apply road-buffer exclusion zone if configured
                if self.exclusion_fc and arcpy.Exists(self.exclusion_fc):
                    logger.info("Applying road-buffer exclusion: %s", self.exclusion_fc)
                    arcpy.analysis.Erase(
                        in_features=boundary_raw_fc,
                        erase_features=self.exclusion_fc,
                        out_feature_class=boundary_fc,
                    )
                    effective_boundary = boundary_fc
                else:
                    effective_boundary = boundary_raw_fc

                # ----------------------------------------------------------------
                # Step 5: Clip raster to final boundary
                # ----------------------------------------------------------------
                logger.info("Step 5: Clip raster to boundary")
                desc = arcpy.Describe(effective_boundary)
                extent = desc.extent
                arcpy.management.Clip(
                    in_raster=raw_raster,
                    rectangle=f"{extent.XMin} {extent.YMin} {extent.XMax} {extent.YMax}",
                    out_raster=raster_path,
                    in_template_dataset=effective_boundary,
                    clipping_geometry="ClippingGeometry",
                    maintain_clipping_extent="NO_MAINTAIN_EXTENT",
                )
                logger.info("Clipped raster written: %s", raster_path)

                # ----------------------------------------------------------------
                # Step 6: Export boundary to shapefile in output_dir
                # ----------------------------------------------------------------
                logger.info("Step 6: Export boundary shapefile")
                arcpy.conversion.FeaturesToJSON(
                    in_features=effective_boundary,
                    out_json_file=str(out_dir / f"{self.site}_boundary.geojson"),
                    format_json=True,
                    geoJSON=True,
                )
                arcpy.management.CopyFeatures(
                    in_features=effective_boundary,
                    out_feature_class=boundary_shp,
                )
                logger.info("Boundary shapefile written: %s", boundary_shp)

                # Clean up raw raster
                if Path(raw_raster).exists():
                    arcpy.management.Delete(raw_raster)

                arcpy.CheckInExtension("3D")
                arcpy.CheckInExtension("Spatial")

                result = {
                    "status": "SUCCESS",
                    "site": self.site,
                    "raster_path": raster_path,
                    "boundary_path": boundary_shp,
                    "output_dir": str(out_dir),
                    "point_count": point_count,
                    "cell_size": self.cell_size,
                }
                logger.info("Raster generation complete: %s", result)
                return result

            finally:
                # ------------------------------------------------------------------
                # Scratch GDB cleanup — only if we created it this run.
                # Never raise from here; a leftover GDB is harmless and Jenkins
                # will recreate / overwrite it on the next run.
                # ------------------------------------------------------------------
                if scratch_gdb_created:
                    try:
                        arcpy.management.Delete(self.scratch_gdb)
                        logger.info("Deleted temporary scratch GDB: %s", self.scratch_gdb)
                    except Exception as del_exc:  # noqa: BLE001
                        logger.warning(
                            "Could not delete temporary scratch GDB '%s': %s. "
                            "It was not deleted — it will be removed in the next Jenkins job.",
                            self.scratch_gdb,
                            del_exc,
                        )

        except ImportError as exc:
            raise RasterGenerationError(
                "arcpy is not available. ArcGIS Pro must be installed and licensed."
            ) from exc
        except RasterGenerationError:
            raise
        except Exception as exc:
            logger.error("Raster generation failed", exc_info=True)
            raise RasterGenerationError(
                f"Raster generation failed for site {self.site}"
            ) from exc
