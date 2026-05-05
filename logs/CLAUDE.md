# Project Manager — Geoprocessing Service

## Overview

Saves and loads web map projects (extent, basemap, layers, graphics, thumbnail) via an
ArcGIS Geoprocessing Service backed by an **ArcGIS Enterprise Geodatabase on Microsoft SQL Server**.

> **Database:** SQL Server (Enterprise SDE)
> **ArcPy version:** ArcGIS Pro
> **Spatial reference:** WGS84 / WKID 4326

## Project Files

| File | Purpose |
|------|---------|
| `project_gp_service.pyt` | Python Toolbox — publish as the GP Service |
| `CLAUDE.md` | This file — schema reference and design decisions |

---

## Design Decisions & Answers

### 1. Can you create real PK / FK constraints via ArcPy or ArcGIS Pro?

**Short answer: PK — partial. FK — use a Relationship Class instead.**

| Mechanism | What it does | How to create |
|-----------|-------------|---------------|
| `OBJECTID` | True auto-increment PK managed internally by SDE. Never modify it. | Automatic |
| Attribute Index (unique) | Enforces uniqueness on `PROJECT_ID` — closest thing to a user-defined PK. | `arcpy.management.AddIndex(..., unique="UNIQUE")` |
| Attribute Rule (constraint) | Arcade expression that rejects duplicate `PROJECT_ID` on insert. Requires ArcGIS Pro 2.1+ / Standard or Advanced licence. | `arcpy.management.AddAttributeRule()` |
| **Relationship Class** | ArcGIS-native FK. Enforces ONE-to-MANY between PROJECTS → PROJECT_LAYERS. Supports cascade behaviour. | `arcpy.management.CreateRelationshipClass()` |
| RDBMS-level FK | True SQL `FOREIGN KEY` constraint in the underlying DB (SQL Server / Oracle / PostgreSQL). ArcGIS does **not** manage these — it may bypass them on its own writes. **Do not add raw DBMS FK constraints on SDE-managed tables.** | SQL DDL (not ArcPy) |

> **Recommendation:** Create a Relationship Class for the PK→FK relationship and a unique Attribute Index on `PROJECT_ID`. That gives ArcGIS-aware referential integrity without conflicting with SDE's internal management.

---

### 2. Is TEXT(65535) safe?

**It depends on the database backend.**

| Backend | What ArcPy TEXT(65535) creates | Safe? |
|---------|-------------------------------|-------|
| File Geodatabase (.gdb) | Native variable-length string up to 2 GB | Yes |
| Enterprise SDE — SQL Server | `nvarchar(max)` (for length > 4000) | Functional, but the column **cannot be indexed** and some GP tools reject it |
| Enterprise SDE — PostgreSQL | `character varying(65535)` | Yes |
| Enterprise SDE — Oracle | `VARCHAR2` max is **4000 bytes** in standard mode — ArcGIS will raise an error | **No** |

**Recommendation used in this project:**

| Field | Length | Reason |
|-------|--------|--------|
| `BASEMAP_JSON` | 4000 | Basemap configs are compact; safe across all backends |
| `GRAPHICS_JSON` | 4000 | If graphics exceed this, store per-graphic in `PROJECT_GRAPHICS` table (see below) |
| `THUMBNAIL` | 4000 | Store only an image URL here; base64 strings are too large for any TEXT field |

> If your graphics or basemap JSON regularly exceeds 4000 characters, use the `PROJECT_CONFIG` BLOB pattern described at the bottom of this file.

---

### 3. Feature Class instead of Table — is it a good idea?

**Yes — preferred.** The `extent` field is spatial data. Storing it as a **Polygon geometry** in a Feature Class unlocks:

- Native spatial queries (`SelectLayerByLocation`, `arcpy.da.SearchCursor` with spatial filter)
- Automatic spatial index for fast bounding-box lookups
- Visual inspection of project extents directly in ArcGIS Pro / Map Viewer
- Removes 4 redundant scalar fields (`EXTENT_XMIN/YMIN/XMAX/YMAX`) — replaced by `SHAPE`
- `EXTENT_WKID` is kept to record the original source coordinate system

`PROJECT_LAYERS` stays as a standalone **Table** — it has no geometry.

---

## Database Schema (Revised)

### Feature Class — PROJECTS  *(Polygon, WGS84 / WKID 4326)*

The extent bounding box is stored as a Polygon in the `SHAPE` field.
All extents are stored in WGS84; `EXTENT_WKID` records the original source WKID.

| # | Field Name     | ArcPy Type | SQL Server Column  | Length | Nullable | Key | Notes |
|---|----------------|------------|--------------------|--------|----------|-----|-------|
| 1 | OBJECTID       | OID        | `int` IDENTITY     | —      | No       | —   | Auto-managed by SDE; never expose in the API |
| 2 | SHAPE          | Polygon    | `geometry`         | —      | Yes      | —   | Extent as bounding-box polygon; spatial index auto-created by SDE |
| 3 | PROJECT_ID     | TEXT       | `nvarchar(50)`     | 50     | No       | PK* | UUID string; unique index enforced |
| 4 | NAME           | TEXT       | `nvarchar(255)`    | 255    | No       | —   | Display name |
| 5 | DESCRIPTION    | TEXT       | `nvarchar(1000)`   | 1000   | Yes      | —   | Optional free-text |
| 6 | WEBMAP_ID      | TEXT       | `nvarchar(255)`    | 255    | Yes      | —   | ArcGIS Online / Portal web map ID |
| 7 | EXTENT_WKID    | LONG       | `int`              | —      | Yes      | —   | Source coordinate system WKID (e.g. 102100, 4326) |
| 8 | BASEMAP_JSON   | TEXT       | `nvarchar(4000)`   | 4000   | Yes      | —   | Basemap config as JSON. 4000 = max for an indexable `nvarchar(n)` |
| 9 | GRAPHICS_JSON  | TEXT       | `nvarchar(4000)`   | 4000   | Yes      | —   | Graphics array as JSON. See PROJECT_CONFIG if data exceeds 4000 chars |
| 10 | THUMBNAIL      | TEXT       | `nvarchar(4000)`   | 4000   | Yes      | —   | Image URL only — never store base64 here |
| 11 | CREATED        | DATE       | `datetime`         | —      | No       | —   | Set once at insert |
| 12 | MODIFIED       | DATE       | `datetime`         | —      | No       | —   | Updated on every PUT |
| 13 | CREATED_BY     | TEXT       | `nvarchar(255)`    | 255    | No       | —   | Creator username |
| 14 | MODIFIED_BY    | TEXT       | `nvarchar(255)`    | 255    | No       | —   | Last editor username |

*`PROJECT_ID` uniqueness is enforced by an Attribute Index, not a DBMS PK constraint.

---

### Table — PROJECT_LAYERS

| # | Field Name   | ArcPy Type | SQL Server Column  | Length | Nullable | Key | Notes |
|---|--------------|------------|--------------------|--------|----------|-----|-------|
| 1 | OBJECTID     | OID        | `int` IDENTITY     | —      | No       | —   | Auto-managed by SDE |
| 2 | PROJECT_ID   | TEXT       | `nvarchar(50)`     | 50     | No       | FK* | → PROJECTS.PROJECT_ID via Relationship Class |
| 3 | LAYER_ID     | TEXT       | `nvarchar(255)`    | 255    | No       | —   | Layer ID from the web map |
| 4 | TITLE        | TEXT       | `nvarchar(255)`    | 255    | No       | —   | Layer display name |
| 5 | VISIBLE      | SHORT      | `smallint`         | —      | No       | —   | 1 = visible, 0 = hidden — SQL Server has no bit type via ArcPy |
| 6 | OPACITY      | DOUBLE     | `float(53)`        | —      | No       | —   | 0.0 (transparent) to 1.0 (opaque) |
| 7 | SORT_ORDER   | LONG       | `int`              | —      | Yes      | —   | Draw order; 0 = bottom layer |

*Referential integrity enforced by Relationship Class `PROJECTS_TO_LAYERS`.

---

### Relationship Class — PROJECTS_TO_LAYERS

| Property | Value |
|----------|-------|
| Type | Simple (no attributed relationship) |
| Cardinality | ONE_TO_MANY |
| Origin table | PROJECTS |
| Origin key | PROJECT_ID |
| Destination table | PROJECT_LAYERS |
| Destination key | PROJECT_ID |
| Forward label | Has Layers |
| Backward label | Belongs To Project |
| Notification | Forward (delete cascades from PROJECTS → LAYERS) |

---

### Attribute Indexes

| Table | Index Name | Field | Unique |
|-------|-----------|-------|--------|
| PROJECTS | IDX_PROJ_ID | PROJECT_ID | Yes |
| PROJECT_LAYERS | IDX_LAYER_PROJ | PROJECT_ID | No |

---

### Entity Relationship

```
PROJECTS (Feature Class)            PROJECT_LAYERS (Table)
─────────────────────────           ──────────────────────────
OBJECTID    (OID)                   OBJECTID    (OID)
SHAPE       (Polygon / WGS84)       PROJECT_ID  (FK → PROJECTS)
PROJECT_ID  (PK unique index) ───<  LAYER_ID
NAME                                TITLE
DESCRIPTION                         VISIBLE
WEBMAP_ID                           OPACITY
EXTENT_WKID                         SORT_ORDER
BASEMAP_JSON
GRAPHICS_JSON
THUMBNAIL
CREATED / MODIFIED
CREATED_BY / MODIFIED_BY

            Relationship Class: PROJECTS_TO_LAYERS
            ONE (PROJECTS.PROJECT_ID) : MANY (PROJECT_LAYERS.PROJECT_ID)
```

---

### Field Type Mapping — REST / JSON → ArcPy → SQL Server

| REST / JSON Type | ArcPy Keyword | SQL Server Column | Indexable | Notes |
|------------------|---------------|------------------|-----------|-------|
| `string` (short) | `"TEXT"` ≤ 4000 | `nvarchar(n)` | Yes | Safe maximum — keep all text fields at or below 4000 |
| `string` (long) | `"TEXT"` > 4000 | `nvarchar(max)` | **No** | Avoid — ArcGIS silently upgrades to `nvarchar(max)`; cannot be indexed or used in some GP operations |
| `number` int | `"LONG"` | `int` | Yes | 32-bit signed (-2 B to 2 B) |
| `number` float | `"DOUBLE"` | `float(53)` | Yes | 64-bit IEEE 754 |
| `boolean` | `"SHORT"` | `smallint` | Yes | No native bool in SDE; write `int()`, read `bool()` |
| `object` / JSON array | `"TEXT"` | `nvarchar(4000)` | Yes | `json.dumps()` on write, `json.loads()` on read |
| ISO datetime string | `"DATE"` | `datetime` | Yes | Millisecond precision; store Python `datetime`, return ISO-8601 string |
| Spatial extent | `"POLYGON"` (SHAPE) | `geometry` | Yes (spatial) | ArcGIS creates spatial index automatically; use `arcpy.Polygon(array, sr)` |
| Binary / large JSON | `"BLOB"` | `varbinary(max)` | No | For PROJECT_CONFIG overflow; store as UTF-8 bytes |

---

## SQL Server Notes

### How ArcGIS SDE stores data in SQL Server

| Topic | Detail |
|-------|--------|
| Schema prefix | By default, ArcGIS creates tables under the `dbo` schema. A `sde` user-schema is used for SDE system tables. Your tables will be `dbo.PROJECTS` and `dbo.PROJECT_LAYERS` in SSMS. |
| OBJECTID | Stored as `int` with SDE-managed sequence (not SQL Server `IDENTITY` directly — SDE uses its own `i<table>` sequence table). |
| SHAPE column | Stored as SQL Server `geometry` type (planar). ArcGIS creates the spatial index and registers it in the `sde_geometry_columns` system table automatically. |
| `nvarchar(4000)` vs `nvarchar(max)` | ArcPy field_length ≤ 4000 → `nvarchar(n)` (specific length, indexable). field_length > 4000 → `nvarchar(max)` (not indexable, not usable in some ArcGIS operations). Always stay ≤ 4000. |
| Dates | Stored as SQL Server `datetime` (millisecond precision). ArcPy returns Python `datetime` objects — always convert to ISO-8601 string before returning to the API caller. |
| Edit sessions | `arcpy.da.Editor` is required for versioned geodatabases. For non-versioned, it is still recommended to ensure consistent multi-table writes. |
| Versioning | If you enable versioning on PROJECTS, SDE creates `adds` and `deletes` delta tables (e.g. `a<n>` / `d<n>`). The Python toolbox works in both versioned and non-versioned mode. |
| Connection file | SDE_CONN in the .pyt file must point to a `.sde` connection file created in ArcGIS Pro (`New Database Connection → SQL Server`). |

### nvarchar(max) — what breaks

If you ever accidentally create a TEXT field with length > 4000, SQL Server stores it as `nvarchar(max)`. The effects:

- `AddIndex()` on that field will **fail silently or raise an error**
- `arcpy.da.SearchCursor` still works for reading
- Some GP tool validators reject `nvarchar(max)` columns as input parameters
- SQL Server cannot include the column in a composite index

**Fix:** Alter the field length in ArcGIS Pro Field Management, or drop and recreate the field.

---

## GP Service Tools

| # | Tool Class | Label | REST Equivalent |
|---|------------|-------|-----------------|
| 1 | `CreateSchema` | 1. Create Schema | — (run once at setup) |
| 2 | `GetAllProjects` | 2. Get All Projects | `GET /api/projects` |
| 3 | `CreateProject` | 3. Create Project | `POST /api/projects` |
| 4 | `GetProjectById` | 4. Get Project by ID | `GET /api/projects/{id}` |
| 5 | `UpdateProject` | 5. Update Project | `PUT /api/projects/{id}` |
| 6 | `DeleteProject` | 6. Delete Project | `DELETE /api/projects/{id}` |

---

## Best Practices

| Rule | Why |
|------|-----|
| Use Feature Class for PROJECTS | Extent is spatial data — enables spatial queries, visual inspection, and spatial indexing |
| Keep JSON fields ≤ 4000 chars | SQL Server `nvarchar(n)` max is 4000 — above that ArcGIS uses `nvarchar(max)` which **cannot be indexed** and breaks some GP operations |
| Never expose OBJECTID in the API | It is an internal SDE implementation detail; use `PROJECT_ID` (UUID) |
| Use Relationship Class for FK | ArcGIS-managed referential integrity; safe with SDE's versioned editing |
| Add unique index on PROJECT_ID | Prevents duplicate project rows; closest to a DBMS PK in ArcGIS |
| Store thumbnail as URL, not base64 | base64 for an image easily exceeds 50 KB — far beyond any TEXT field limit |
| Use `arcpy.da.Editor` for all writes | Required for versioned SDE; ensures PROJECTS and PROJECT_LAYERS stay consistent |
| Delete + re-insert layers on save | Simplest way to keep SORT_ORDER correct without managing individual row diffs |

---

## Optional: PROJECT_CONFIG Table (for large JSON)

If basemap or graphics JSON regularly exceeds 4000 characters, normalise them into a
separate table using a BLOB field instead of a TEXT field.
In SQL Server, `BLOB` maps to `varbinary(max)` — up to 2 GB, no indexing limitation matters here.

| Field | ArcPy Type | SQL Server Column | Notes |
|-------|------------|-------------------|-------|
| OBJECTID | OID | `int` (SDE-managed) | Auto |
| PROJECT_ID | TEXT(50) | `nvarchar(50)` | FK → PROJECTS |
| CONFIG_TYPE | TEXT(50) | `nvarchar(50)` | `"basemap"` or `"graphics"` |
| CONFIG_VALUE | BLOB | `varbinary(max)` | JSON serialised to UTF-8 bytes; no size limit |

```python
# Write — serialise dict to UTF-8 bytes, insert into BLOB field
config_bytes = json.dumps(obj).encode("utf-8")
with arcpy.da.InsertCursor(config_table, ["PROJECT_ID","CONFIG_TYPE","CONFIG_VALUE"]) as cur:
    cur.insertRow((project_id, "basemap", config_bytes))

# Read — BLOB field returns a bytearray in ArcPy
with arcpy.da.SearchCursor(config_table, ["CONFIG_VALUE"],
                           where_clause=f"PROJECT_ID = '{pid}' AND CONFIG_TYPE = 'basemap'") as cur:
    for row in cur:
        obj = json.loads(bytes(row[0]).decode("utf-8"))
```

> `bytearray` is returned by ArcPy for BLOB fields (not `bytes`).
> Wrap with `bytes()` before decoding.
