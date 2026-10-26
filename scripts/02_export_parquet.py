"""Write GeoParquet of the flood inundation polygons that touch Saitama, Chiba, Tokyo or Kanagawa.

Reads the Shapefile zips of the FY2025 edition of A31a under data/raw/<version>/ and keeps every
polygon that intersects the four prefectures (the boundary is N03 of 2026-01-01, used only to
select; it is not republished). The polygons are written as they are in the source, not clipped. One row per
polygon: id, category (planned / maximum / duration / collapse_flow / collapse_erosion), river_code,
river_name, manager_code, manager (the A31a_x01..x04 of the source), class_code (A31a_x05: the
rank of depth or duration, or the kind of the collapse area), river_group (10: 洪水予報河川・水位周知
河川, 20: その他の河川), source_zip, source_member, source_index, geometry (WKB Polygon or MultiPolygon, EPSG:6668).
data/parquet/flood_area.parquet holds all rows; data/parquet/categories/<category>.parquet one
category. Zips that give no polygon in the prefectures are listed in columns.json under unused_zips.

    uv run python scripts/02_export_parquet.py [--version 2026-10-26]
"""

import argparse
import io
import json
import sys
import zipfile
from collections import Counter
from pathlib import Path

import ksj_flood as k
import pyarrow as pa
import pyarrow.parquet as pq
import shapefile
from pyproj import CRS
from shapely import prepare, set_precision  # noqa: F401
from shapely.geometry import box, shape
from shapely.ops import unary_union

ROOT = Path(__file__).parents[1]
SCHEMA = pa.schema(
    [
        ("id", pa.string()),
        ("category", pa.string()),
        ("river_code", pa.string()),
        ("river_name", pa.string()),
        ("manager_code", pa.string()),
        ("manager", pa.string()),
        ("class_code", pa.int8()),
        ("river_group", pa.string()),
        ("source_zip", pa.string()),
        ("source_member", pa.string()),
        ("source_index", pa.int32()),
        ("geometry", pa.binary()),
    ]
)


def prefectures(n03_zips: list[Path]):
    """Union of the four prefectures from the N03 Shapefiles (UTF-8 dbf), as a prepared geometry."""
    geoms, found = [], set()
    for path in n03_zips:
        with zipfile.ZipFile(path) as z:
            stem = next(n[:-4] for n in z.namelist() if n.endswith(".shp"))
            sf = shapefile.Reader(
                shp=io.BytesIO(z.read(stem + ".shp")), shx=io.BytesIO(z.read(stem + ".shx")),
                dbf=io.BytesIO(z.read(stem + ".dbf")), encoding="utf-8")  # fmt: skip
        for sr in sf.iterShapeRecords():
            code = sr.record.as_dict()["N03_007"]
            if code is None or code[:2] not in k.PREFS:
                raise SystemExit(f"{path.name}: a feature outside the four prefectures: {code!r}")
            geoms.append(shape(sr.shape.__geo_interface__))
            found.add(code[:2])
    if found != set(k.PREFS):
        raise SystemExit(f"prefectures missing from N03: {sorted(set(k.PREFS) - found)}")
    u = unary_union(geoms)
    prepare(u)
    return u


def scan(zip_path: Path, area, area_box, emit) -> Counter:
    """Call emit(zip, member, index, attrs, geometry) for each polygon touching the area."""
    stats = Counter()
    with k.open_zip(zip_path) as z:
        for member in k.shp_members(z):
            info = k.parse_member(member)
            stats["members"] += 1
            base = member[:-4]
            shp = z.read(member)
            xmin, ymin, xmax, ymax = k.bbox_of_shp(shp[:100])
            if not box(xmin, ymin, xmax, ymax).intersects(area_box):
                stats["members_outside"] += 1
                continue
            sf = shapefile.Reader(
                shp=io.BytesIO(shp), shx=io.BytesIO(z.read(base + ".shx")),
                dbf=io.BytesIO(z.read(base + ".dbf")), encoding="cp932")  # fmt: skip
            prj = z.read(base + ".prj").decode()
            assert "JGD_2011" in prj or "JGD2011" in prj, prj[:80]
            names = [f[0] for f in sf.fields[1:]]
            if names != k.field_names(info["cat"]):
                raise ValueError(f"{member}: unexpected fields {names}")
            if sf.shapeTypeName != "POLYGON":
                raise ValueError(f"{member}: {sf.shapeTypeName}")
            for i, sr in enumerate(sf.iterShapeRecords()):
                stats["polygons_scanned"] += 1
                x0, y0, x1, y1 = sr.shape.bbox
                if not box(x0, y0, x1, y1).intersects(area_box):
                    continue
                g = shape(sr.shape.__geo_interface__)
                if area.intersects(g):
                    stats["polygons_kept"] += 1
                    emit(zip_path.name, member, i, info, list(sr.record), g)
    return stats


class Sink:
    """Write rows to a Parquet file in batches, so that millions of polygons need not sit in memory."""

    def __init__(self, path: Path, geo: dict, batch: int = 50_000):
        self.writer = pq.ParquetWriter(
            path, SCHEMA.with_metadata({b"geo": json.dumps(geo).encode()}), compression="zstd"
        )
        self.buf: list[dict] = []
        self.batch = batch

    def add(self, row: dict) -> None:
        self.buf.append(row)
        if len(self.buf) >= self.batch:
            self.flush()

    def flush(self) -> None:
        if self.buf:
            self.writer.write_table(pa.Table.from_pylist(self.buf, schema=SCHEMA))
            self.buf = []

    def close(self) -> None:
        self.flush()
        self.writer.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", default=None)
    a = ap.parse_args()
    runs = sorted((ROOT / "data" / "raw").glob("*/MANIFEST.json"))
    raw = runs[-1].parent if a.version is None else ROOT / "data" / "raw" / a.version
    manifest = json.loads((raw / "MANIFEST.json").read_text())
    docs = ROOT / "data" / "docs"
    area = prefectures([docs / Path(u).name for u in k.N03_URLS.values()])
    area_box = box(*area.bounds)
    invalid = Counter()
    geom_types = Counter()
    by_cat = Counter()
    crs = CRS.from_epsg(k.EPSG).to_json_dict()
    geo = {
        "version": "1.1.0", "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Polygon", "MultiPolygon"], "crs": crs}},
    }  # fmt: skip
    par = ROOT / "data" / "parquet"
    (par / "categories").mkdir(parents=True, exist_ok=True)
    sinks = {"all": Sink(par / "flood_area.parquet", geo)}
    for _, (slug, _, _) in k.CATEGORIES.items():
        sinks[slug] = Sink(par / "categories" / f"{slug}.parquet", geo)

    def emit(zname, member, i, info, rec, g):
        code, name, mcode, mname, cls = rec
        _, _, codelist = k.CATEGORIES[info["cat"]]
        if int(cls) not in k.CLASS_CODES[codelist]:
            raise ValueError(f"{member}[{i}]: class code {cls} is not in the code list")
        if g.geom_type not in ("Polygon", "MultiPolygon"):
            raise ValueError(f"{member}[{i}]: {g.geom_type}")
        geom_types[g.geom_type] += 1
        if not g.is_valid:
            invalid[info["cat"]] += 1
        slug = k.CATEGORIES[info["cat"]][0]
        row = {
            "id": f"{info['area']}-{info['group']}-{info['cat']}-{info['river']}-{i:06d}",
            "category": slug, "river_code": code, "river_name": name, "manager_code": mcode,
            "manager": mname, "class_code": int(cls), "river_group": info["group"],
            "source_zip": zname, "source_member": member, "source_index": i, "geometry": g.wkb,
        }  # fmt: skip
        by_cat[slug] += 1
        sinks["all"].add(row)
        sinks[slug].add(row)

    per_zip = {}
    for name, meta in manifest["files"].items():
        if meta["role"] != "candidate":
            continue
        per_zip[name] = dict(scan(raw / name, area, area_box, emit))
        print(name, per_zip[name], flush=True)
    for s_ in sinks.values():
        s_.close()
    rows = sum(by_cat.values())
    if not rows:
        raise SystemExit("no polygon found")
    (par / "columns.json").write_text(
        json.dumps({
            "run": raw.name, "rows": rows, "by_category": dict(by_cat), "by_zip": per_zip,
            "unused_zips": [n for n, v in per_zip.items() if not v.get("polygons_kept")],
            "invalid_geometries": dict(invalid), "geometry_types": dict(geom_types),
            "crs": f"EPSG:{k.EPSG}",
            "classes": {n: {str(c): v for c, v in d.items()} for n, d in k.CLASS_CODES.items()},
            "categories": {c: {"slug": s, "name": n, "class": cl} for c, (s, n, cl) in k.CATEGORIES.items()},
            "river_groups": k.GROUPS,
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")  # fmt: skip
    print(rows, dict(by_cat))
    return 0


if __name__ == "__main__":
    sys.exit(main())
