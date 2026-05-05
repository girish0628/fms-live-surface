# -*- coding: utf-8 -*-
"""
Project Manager — ArcGIS Python Toolbox
Publish this .pyt file as a Geoprocessing Service via ArcGIS Server / Portal.

Schema
------
  PROJECTS        — Polygon Feature Class (WGS84). Extent stored as SHAPE geometry.
  PROJECT_LAYERS  — Standalone Table. One row per layer per project.
  Relationship Class PROJECTS_TO_LAYERS enforces ONE-to-MANY integrity.

Tools
-----
  1. CreateSchema    — one-time setup
  2. GetAllProjects  — GET /api/projects
  3. CreateProject   — POST /api/projects
  4. GetProjectById  — GET /api/projects/{id}
  5. UpdateProject   — PUT /api/projects/{id}
  6. DeleteProject   — DELETE /api/projects/{id}
"""

import arcpy
import json
import os
import uuid
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Configuration — update before publishing to ArcGIS Server
# ---------------------------------------------------------------------------
# SDE_CONN: path to an .sde connection file created in ArcGIS Pro
#   (Catalog pane → Databases → New Database Connection → SQL Server)
# Tables will be created as dbo.PROJECTS and dbo.PROJECT_LAYERS in SQL Server.
SDE_CONN    = r"C:\Connections\project_manager.sde"
PROJ_FC     = os.path.join(SDE_CONN, "PROJECTS")
LAYER_TABLE = os.path.join(SDE_CONN, "PROJECT_LAYERS")
REL_CLASS   = os.path.join(SDE_CONN, "PROJECTS_TO_LAYERS")

# Spatial reference for the PROJECTS feature class (WGS84)
SR_WGS84 = arcpy.SpatialReference(4326)

# Field lists — order must match cursor column order exactly
PROJ_FIELDS = [
    "SHAPE@",
    "PROJECT_ID", "NAME", "DESCRIPTION", "WEBMAP_ID", "EXTENT_WKID",
    "BASEMAP_JSON", "GRAPHICS_JSON", "THUMBNAIL",
    "CREATED", "MODIFIED", "CREATED_BY", "MODIFIED_BY",
]
LAYER_FIELDS = ["PROJECT_ID", "LAYER_ID", "TITLE", "VISIBLE", "OPACITY", "SORT_ORDER"]


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _extent_to_polygon(ext, wkid=None):
    """Build a closed Polygon ring from an extent dict. Returns None if extent is missing."""
    if not ext:
        return None
    try:
        xmin, ymin = float(ext["xmin"]), float(ext["ymin"])
        xmax, ymax = float(ext["xmax"]), float(ext["ymax"])
    except (KeyError, TypeError, ValueError):
        return None

    sr = arcpy.SpatialReference(int(wkid)) if wkid else SR_WGS84
    ring = arcpy.Array([
        arcpy.Point(xmin, ymin),
        arcpy.Point(xmin, ymax),
        arcpy.Point(xmax, ymax),
        arcpy.Point(xmax, ymin),
        arcpy.Point(xmin, ymin),   # close the ring
    ])
    polygon = arcpy.Polygon(ring, sr)
    # Always store in WGS84 so the feature class has one consistent SRS
    if sr.factoryCode != 4326:
        polygon = polygon.projectAs(SR_WGS84)
    return polygon


def _shape_to_extent(shape):
    """Extract xmin/ymin/xmax/ymax from a Polygon SHAPE token."""
    if shape is None:
        return {"xmin": None, "ymin": None, "xmax": None, "ymax": None}
    ext = shape.extent
    return {
        "xmin": ext.XMin, "ymin": ext.YMin,
        "xmax": ext.XMax, "ymax": ext.YMax,
    }


# ---------------------------------------------------------------------------
# Shared CRUD helpers
# ---------------------------------------------------------------------------

def _now():
    return datetime.now(timezone.utc)


def _safe_json(s):
    try:
        return json.loads(s) if s else None
    except (json.JSONDecodeError, TypeError):
        return None


def _row_to_dict(row):
    (shape,
     pid, name, desc, wmid, wkid,
     basemap_j, graphics_j, thumb,
     created, modified, created_by, modified_by) = row

    extent_coords = _shape_to_extent(shape)
    return {
        "id":          pid,
        "name":        name,
        "description": desc,
        "webmapId":    wmid,
        "extent": {
            **extent_coords,
            "spatialReference": {"wkid": wkid or 4326},
        },
        "basemap":    _safe_json(basemap_j),
        "graphics":   _safe_json(graphics_j) or [],
        "thumbnail":  thumb,
        "created":    created.isoformat()  if created  else None,
        "modified":   modified.isoformat() if modified else None,
        "createdBy":  created_by,
        "modifiedBy": modified_by,
    }


def _load_layers(project_id):
    safe_id = project_id.replace("'", "''")
    rows = []
    with arcpy.da.SearchCursor(
        LAYER_TABLE, LAYER_FIELDS,
        where_clause=f"PROJECT_ID = '{safe_id}'",
        sql_clause=(None, "ORDER BY SORT_ORDER"),
    ) as cur:
        for r in cur:
            rows.append({
                "id":      r[1],
                "title":   r[2],
                "visible": bool(r[3]),
                "opacity": r[4],
            })
    return rows


def _get_project(project_id):
    safe_id = project_id.replace("'", "''")
    with arcpy.da.SearchCursor(
        PROJ_FC, PROJ_FIELDS,
        where_clause=f"PROJECT_ID = '{safe_id}'",
    ) as cur:
        for row in cur:
            p = _row_to_dict(row)
            p["layers"] = _load_layers(project_id)
            return p
    return None


def _insert_layers(project_id, layers):
    with arcpy.da.InsertCursor(LAYER_TABLE, LAYER_FIELDS) as cur:
        for i, lyr in enumerate(layers):
            cur.insertRow((
                project_id,
                lyr["id"],
                lyr["title"],
                int(bool(lyr.get("visible", True))),
                float(lyr.get("opacity", 1.0)),
                i,
            ))


def _delete_layers(project_id):
    safe_id = project_id.replace("'", "''")
    with arcpy.da.UpdateCursor(
        LAYER_TABLE, ["PROJECT_ID"],
        where_clause=f"PROJECT_ID = '{safe_id}'",
    ) as cur:
        for _ in cur:
            cur.deleteRow()


def _run_in_edit_session(func):
    """Wrap func() in an arcpy.da.Editor session. Returns (ok, error_message)."""
    editor = arcpy.da.Editor(SDE_CONN)
    editor.startEditing(False, True)
    editor.startOperation()
    try:
        func()
        editor.stopOperation()
        editor.stopEditing(True)
        return True, None
    except Exception as exc:
        editor.abortOperation()
        editor.stopEditing(False)
        return False, str(exc)


# ---------------------------------------------------------------------------
# Toolbox registration
# ---------------------------------------------------------------------------

class Toolbox:
    def __init__(self):
        self.label = "Project Manager"
        self.alias = "ProjectManager"
        self.tools = [
            CreateSchema,
            GetAllProjects,
            CreateProject,
            GetProjectById,
            UpdateProject,
            DeleteProject,
        ]


# ---------------------------------------------------------------------------
# Tool 1 — CreateSchema
# ---------------------------------------------------------------------------

class CreateSchema:
    def __init__(self):
        self.label              = "1. Create Schema"
        self.description        = (
            "Creates PROJECTS (Feature Class), PROJECT_LAYERS (Table), "
            "a Relationship Class, and attribute indexes. "
            "Safe to re-run — existing objects are skipped."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        workspace = arcpy.Parameter(
            displayName="Target Workspace (SDE connection or File GDB)",
            name="workspace",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input",
        )
        workspace.value = SDE_CONN

        result_json = arcpy.Parameter(
            displayName="Result",
            name="result_json",
            datatype="GPString",
            parameterType="Derived",
            direction="Output",
        )
        return [workspace, result_json]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        pass

    def updateMessages(self, parameters):
        pass

    def execute(self, parameters, messages):
        ws = parameters[0].valueAsText
        created = []

        def _add(tbl, fname, ftype, length=None, nullable="NULLABLE"):
            kw = {"field_is_nullable": nullable}
            if length:
                kw["field_length"] = length
            arcpy.management.AddField(tbl, fname, ftype, **kw)

        # ── PROJECTS Feature Class ─────────────────────────────────────────
        proj_path = os.path.join(ws, "PROJECTS")
        if not arcpy.Exists(proj_path):
            arcpy.management.CreateFeatureclass(
                ws, "PROJECTS", "POLYGON",
                spatial_reference=arcpy.SpatialReference(4326),
            )
            _add(proj_path, "PROJECT_ID",    "TEXT",   50,   "NON_NULLABLE")
            _add(proj_path, "NAME",          "TEXT",   255,  "NON_NULLABLE")
            _add(proj_path, "DESCRIPTION",   "TEXT",   1000)
            _add(proj_path, "WEBMAP_ID",     "TEXT",   255)
            _add(proj_path, "EXTENT_WKID",   "LONG")
            _add(proj_path, "BASEMAP_JSON",  "TEXT",   4000)
            _add(proj_path, "GRAPHICS_JSON", "TEXT",   4000)
            _add(proj_path, "THUMBNAIL",     "TEXT",   4000)
            _add(proj_path, "CREATED",       "DATE",   None, "NON_NULLABLE")
            _add(proj_path, "MODIFIED",      "DATE",   None, "NON_NULLABLE")
            _add(proj_path, "CREATED_BY",    "TEXT",   255,  "NON_NULLABLE")
            _add(proj_path, "MODIFIED_BY",   "TEXT",   255,  "NON_NULLABLE")

            # Unique index on PROJECT_ID — enforces PK-like uniqueness
            arcpy.management.AddIndex(
                proj_path, ["PROJECT_ID"], "IDX_PROJ_ID",
                unique="UNIQUE", ascending="ASCENDING",
            )
            arcpy.AddMessage("Created: PROJECTS (Feature Class, WGS84) + unique index")
            created.append("PROJECTS")
        else:
            arcpy.AddWarning("PROJECTS already exists — skipped.")

        # ── PROJECT_LAYERS Table ───────────────────────────────────────────
        lyr_path = os.path.join(ws, "PROJECT_LAYERS")
        if not arcpy.Exists(lyr_path):
            arcpy.management.CreateTable(ws, "PROJECT_LAYERS")
            _add(lyr_path, "PROJECT_ID",  "TEXT",   50,  "NON_NULLABLE")
            _add(lyr_path, "LAYER_ID",    "TEXT",   255, "NON_NULLABLE")
            _add(lyr_path, "TITLE",       "TEXT",   255, "NON_NULLABLE")
            _add(lyr_path, "VISIBLE",     "SHORT",  None, "NON_NULLABLE")
            _add(lyr_path, "OPACITY",     "DOUBLE", None, "NON_NULLABLE")
            _add(lyr_path, "SORT_ORDER",  "LONG")

            # Non-unique index on PROJECT_ID — speeds up FK lookups
            arcpy.management.AddIndex(
                lyr_path, ["PROJECT_ID"], "IDX_LAYER_PROJ",
                unique="NON_UNIQUE", ascending="ASCENDING",
            )
            arcpy.AddMessage("Created: PROJECT_LAYERS (Table) + index")
            created.append("PROJECT_LAYERS")
        else:
            arcpy.AddWarning("PROJECT_LAYERS already exists — skipped.")

        # ── Relationship Class ─────────────────────────────────────────────
        rel_path = os.path.join(ws, "PROJECTS_TO_LAYERS")
        if not arcpy.Exists(rel_path):
            arcpy.management.CreateRelationshipClass(
                origin_table=proj_path,
                destination_table=lyr_path,
                out_relationship_class=rel_path,
                relationship_type="SIMPLE",
                forward_label="Has Layers",
                backward_label="Belongs To Project",
                message_direction="FORWARD",          # forward = PROJECTS notifies LAYERS
                cardinality="ONE_TO_MANY",
                attributed="NONE",
                origin_primary_key="PROJECT_ID",
                origin_foreign_key="PROJECT_ID",
            )
            arcpy.AddMessage("Created: Relationship Class PROJECTS_TO_LAYERS")
            created.append("PROJECTS_TO_LAYERS")
        else:
            arcpy.AddWarning("PROJECTS_TO_LAYERS already exists — skipped.")

        arcpy.SetParameterAsText(1, json.dumps({"success": True, "created": created}))


# ---------------------------------------------------------------------------
# Tool 2 — GetAllProjects
# ---------------------------------------------------------------------------

class GetAllProjects:
    def __init__(self):
        self.label              = "2. Get All Projects"
        self.description        = "Returns a JSON array of all saved projects including their layers."
        self.canRunInBackground = False

    def getParameterInfo(self):
        out_json = arcpy.Parameter(
            displayName="Projects JSON",
            name="out_json",
            datatype="GPString",
            parameterType="Derived",
            direction="Output",
        )
        return [out_json]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        pass

    def updateMessages(self, parameters):
        pass

    def execute(self, parameters, messages):
        projects = []
        with arcpy.da.SearchCursor(PROJ_FC, PROJ_FIELDS) as cur:
            for row in cur:
                p = _row_to_dict(row)
                p["layers"] = _load_layers(p["id"])
                projects.append(p)

        arcpy.AddMessage(f"Found {len(projects)} project(s).")
        arcpy.SetParameterAsText(0, json.dumps(projects))


# ---------------------------------------------------------------------------
# Tool 3 — CreateProject
# ---------------------------------------------------------------------------

class CreateProject:
    def __init__(self):
        self.label              = "3. Create Project"
        self.description        = (
            "Inserts a new project. Pass the project as a JSON string. "
            "id, created, modified, createdBy, and modifiedBy are auto-generated."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        project_json = arcpy.Parameter(
            displayName="Project JSON",
            name="project_json",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        current_user = arcpy.Parameter(
            displayName="Current User",
            name="current_user",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        out_json = arcpy.Parameter(
            displayName="Created Project JSON",
            name="out_json",
            datatype="GPString",
            parameterType="Derived",
            direction="Output",
        )
        return [project_json, current_user, out_json]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        pass

    def updateMessages(self, parameters):
        if parameters[0].value:
            try:
                json.loads(parameters[0].valueAsText)
            except (json.JSONDecodeError, TypeError):
                parameters[0].setErrorMessage("Value must be a valid JSON string.")

    def execute(self, parameters, messages):
        body         = json.loads(parameters[0].valueAsText)
        current_user = parameters[1].valueAsText

        pid   = str(uuid.uuid4())
        now   = _now()
        ext   = body.get("extent") or {}
        sr    = ext.get("spatialReference") or {}
        wkid  = sr.get("wkid")
        shape = _extent_to_polygon(ext, wkid)

        def _do_insert():
            with arcpy.da.InsertCursor(PROJ_FC, PROJ_FIELDS) as cur:
                cur.insertRow((
                    shape,
                    pid,
                    body["name"],
                    body.get("description"),
                    body.get("webmapId"),
                    wkid,
                    json.dumps(body["basemap"])  if body.get("basemap")  else None,
                    json.dumps(body["graphics"]) if body.get("graphics") else None,
                    body.get("thumbnail"),
                    now, now,
                    current_user, current_user,
                ))
            _insert_layers(pid, body.get("layers") or [])

        ok, err = _run_in_edit_session(_do_insert)
        if not ok:
            arcpy.AddError(err)
            arcpy.SetParameterAsText(2, json.dumps({"success": False, "error": err}))
            return

        project = _get_project(pid)
        arcpy.AddMessage(f"Created project: {pid}")
        arcpy.SetParameterAsText(2, json.dumps({"success": True, "project": project}))


# ---------------------------------------------------------------------------
# Tool 4 — GetProjectById
# ---------------------------------------------------------------------------

class GetProjectById:
    def __init__(self):
        self.label              = "4. Get Project by ID"
        self.description        = "Loads a single project by UUID including extent, basemap, layers, and graphics."
        self.canRunInBackground = False

    def getParameterInfo(self):
        project_id = arcpy.Parameter(
            displayName="Project ID (UUID)",
            name="project_id",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        out_json = arcpy.Parameter(
            displayName="Project JSON",
            name="out_json",
            datatype="GPString",
            parameterType="Derived",
            direction="Output",
        )
        return [project_id, out_json]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        pass

    def updateMessages(self, parameters):
        pass

    def execute(self, parameters, messages):
        project_id = parameters[0].valueAsText.strip()
        project    = _get_project(project_id)

        if project is None:
            arcpy.AddWarning(f"No project found: {project_id}")
            arcpy.SetParameterAsText(1, json.dumps({"success": False, "error": "Not found"}))
        else:
            arcpy.SetParameterAsText(1, json.dumps({"success": True, "project": project}))


# ---------------------------------------------------------------------------
# Tool 5 — UpdateProject
# ---------------------------------------------------------------------------

class UpdateProject:
    def __init__(self):
        self.label              = "5. Update Project"
        self.description        = (
            "Updates name and/or description of an existing project. "
            "Refreshes modified timestamp and modifiedBy."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        project_id = arcpy.Parameter(
            displayName="Project ID (UUID)",
            name="project_id",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        name = arcpy.Parameter(
            displayName="New Name",
            name="name",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        description = arcpy.Parameter(
            displayName="New Description",
            name="description",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        current_user = arcpy.Parameter(
            displayName="Current User",
            name="current_user",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        out_json = arcpy.Parameter(
            displayName="Updated Project JSON",
            name="out_json",
            datatype="GPString",
            parameterType="Derived",
            direction="Output",
        )
        return [project_id, name, description, current_user, out_json]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        pass

    def updateMessages(self, parameters):
        pass

    def execute(self, parameters, messages):
        project_id   = parameters[0].valueAsText.strip()
        new_name     = parameters[1].valueAsText if parameters[1].value else None
        new_desc     = parameters[2].valueAsText if parameters[2].value else None
        current_user = parameters[3].valueAsText
        now          = _now()
        safe_id      = project_id.replace("'", "''")
        touched      = [False]

        def _do_update():
            with arcpy.da.UpdateCursor(
                PROJ_FC,
                ["NAME", "DESCRIPTION", "MODIFIED", "MODIFIED_BY"],
                where_clause=f"PROJECT_ID = '{safe_id}'",
            ) as cur:
                for row in cur:
                    row[0] = new_name if new_name is not None else row[0]
                    row[1] = new_desc if new_desc is not None else row[1]
                    row[2] = now
                    row[3] = current_user
                    cur.updateRow(row)
                    touched[0] = True

        ok, err = _run_in_edit_session(_do_update)
        if not ok:
            arcpy.AddError(err)
            arcpy.SetParameterAsText(4, json.dumps({"success": False, "error": err}))
            return

        if not touched[0]:
            arcpy.AddWarning(f"No project found: {project_id}")
            arcpy.SetParameterAsText(4, json.dumps({"success": False, "error": "Not found"}))
            return

        project = _get_project(project_id)
        arcpy.AddMessage(f"Updated project: {project_id}")
        arcpy.SetParameterAsText(4, json.dumps({"success": True, "project": project}))


# ---------------------------------------------------------------------------
# Tool 6 — DeleteProject
# ---------------------------------------------------------------------------

class DeleteProject:
    def __init__(self):
        self.label              = "6. Delete Project"
        self.description        = "Deletes a project and all its associated layer rows by UUID."
        self.canRunInBackground = False

    def getParameterInfo(self):
        project_id = arcpy.Parameter(
            displayName="Project ID (UUID)",
            name="project_id",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        out_json = arcpy.Parameter(
            displayName="Result JSON",
            name="out_json",
            datatype="GPString",
            parameterType="Derived",
            direction="Output",
        )
        return [project_id, out_json]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        pass

    def updateMessages(self, parameters):
        pass

    def execute(self, parameters, messages):
        project_id = parameters[0].valueAsText.strip()
        safe_id    = project_id.replace("'", "''")
        counts     = {"projects": 0, "layers": 0}

        def _do_delete():
            with arcpy.da.UpdateCursor(
                PROJ_FC, ["PROJECT_ID"],
                where_clause=f"PROJECT_ID = '{safe_id}'",
            ) as cur:
                for _ in cur:
                    cur.deleteRow()
                    counts["projects"] += 1

            with arcpy.da.UpdateCursor(
                LAYER_TABLE, ["PROJECT_ID"],
                where_clause=f"PROJECT_ID = '{safe_id}'",
            ) as cur:
                for _ in cur:
                    cur.deleteRow()
                    counts["layers"] += 1

        ok, err = _run_in_edit_session(_do_delete)
        if not ok:
            arcpy.AddError(err)
            arcpy.SetParameterAsText(1, json.dumps({"success": False, "error": err}))
            return

        if counts["projects"] == 0:
            arcpy.AddWarning(f"No project found: {project_id}")
            arcpy.SetParameterAsText(1, json.dumps({"success": False, "error": "Not found"}))
        else:
            arcpy.AddMessage(
                f"Deleted project {project_id} ({counts['layers']} layer row(s) removed)."
            )
            arcpy.SetParameterAsText(1, json.dumps({
                "success": True,
                "deleted": {
                    "projectId":        project_id,
                    "layerRowsRemoved": counts["layers"],
                },
            }))
