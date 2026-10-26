"""Write data/README.md (the dataset card), data/LICENSE and data/provenance.yaml.

uv run python scripts/04_card.py
"""

import json
import sys
from collections import Counter, defaultdict
from email.utils import parsedate_to_datetime
from pathlib import Path

import ksj_flood as k
import pyarrow.parquet as pq

ROOT = Path(__file__).parents[1]
REPO = "ksj-a31a-flood-greater-tokyo"

CARD = """---
license: cc-by-4.0
license_name: cc-by-4.0
license_link: https://creativecommons.org/licenses/by/4.0/legalcode.ja
language:
- ja
tags:
- japan
- tokyo
- saitama
- chiba
- kanagawa
- flood
- hazard
- geospatial
- ksj
size_categories:
- 1M<n<10M
pretty_name: Flood inundation areas of Greater Tokyo (国土数値情報 洪水浸水想定区域 A31a, FY2025)
---

# {repo}

The flood inundation polygons (洪水浸水想定区域, river by river) that touch Saitama, Chiba, Tokyo
or Kanagawa (here called Greater Tokyo: prefecture codes 11 to 14), from the Ministry of Land, Infrastructure, Transport and Tourism's 国土数値情報 洪水浸水想定区域
データ（河川単位）A31a, FY2025 edition (令和7年度版): {rows:,} polygons in five categories, each
with the river, the manager of the river and the class (depth rank, duration rank or kind of
collapse area). As GeoParquet, all in one file and one file per category. The sister dataset
`ksj-a31a-flood-tokyo23` is the part that touches the 23 wards of Tokyo.

## Source

- Data page: <{page}> (国土交通省 国土数値情報ダウンロードサイト, 洪水浸水想定区域データ（河川単位）).
  The page says the data are made from the flood inundation area maps that the river managers
  (the Minister, the prefectural governors) provided under 水防法, as polygons per class, per river.
- Files: {used_list}, the Shapefile zips of the source as served (each member is a Shapefile of
  one river and one category; the names inside the zips are cp932). Fetched {fetched}; the server
  states Last-Modified {lm}. They are kept unchanged under `raw/{run}/` with their sha256 in
  `MANIFEST.json`. The page says the FY2025 files were updated in May and in October 2026.
- Which zips: the source cuts the data by manager, not by place, so a prefecture's rivers can flood
  another prefecture. The source's river list (`A31_R7_datalist.xlsx`) names, for each river, the
  1st-level meshes it reaches. All {n_candidates} zips that hold a river reaching a mesh of the four
  prefectures (the meshes 5238 to 5440) were read in full: the four prefectures themselves, their
  neighbours (Ibaraki, Tochigi, Gunma, Yamanashi, Nagano, Shizuoka), and the regional bureaus of
  Kanto (`83`), Hokuriku (`84`) and Chubu (`85`), which manage the national rivers. {unused_text}
  The group is `10` (洪水予報河川・水位周知河川) or `20` (その他の河川).
- Selection: a polygon is kept if it intersects Saitama, Chiba, Tokyo or Kanagawa (the boundaries
  of 国土数値情報 行政区域 N03 of 2026-01-01, <{n03_page}>, prefecture codes 11 to 14). The boundary
  was only used to choose the rows; it is not part of this dataset. The polygons are kept whole,
  not clipped, so they reach beyond the prefectures.

## Files

| Path | Content |
|---|---|
| `parquet/flood_area.parquet` | all {rows:,} polygons |
| `parquet/categories/<category>.parquet` | one category ({category_files}) |
| `parquet/columns.json` | counts, the code lists, what was read |
| `raw/{run}/A31a-25_<area>_<group>_SHP.zip` | the source zips that have polygons in the prefectures |

## Columns

| Column | Content |
|---|---|
| `id` | area, group, category, river code and the order in the source file, such as `13-10-20-8303040044-000123` |
| `category` | `planned` 計画規模, `maximum` 想定最大規模, `duration` 浸水継続時間, `collapse_flow` 家屋倒壊等氾濫想定区域（氾濫流）, `collapse_erosion` 家屋倒壊等氾濫想定区域（河岸浸食） |
| `river_code`, `river_name` | `A31a_x01`, `A31a_x02`: the river that the area belongs to |
| `manager_code`, `manager` | `A31a_x03`, `A31a_x04`: the manager of the data (a prefecture, or a regional bureau) |
| `class_code` | `A31a_x05`: for `planned`, `maximum`: the depth rank; for `duration`: the duration rank; for the two collapse categories: 1 氾濫流, 2 河岸浸食 (below) |
| `river_group` | `10` 洪水予報河川・水位周知河川, `20` その他の河川 (the last two digits of the source's file names) |
| `source_zip`, `source_member`, `source_index` | where the polygon came from: the zip, the `.shp` inside it, and the index in that Shapefile |
| `geometry` | the area as WKB Polygon or MultiPolygon, EPSG:6668 (JGD2011), stated in the GeoParquet metadata as PROJJSON |

Depth ranks (`planned`, `maximum`; the source's water_depth_code): {depth_codes}.
Duration ranks (`duration`): {duration_codes}.
Polygons by class:

{class_rows}

## What was changed

- Only the selection (above) and the columns `id`, `category`, `river_group`, `source_*` were added.
  Coordinates and the attribute values are those of the source (checked against the `.shp` and the
  `.dbf` of the zips, see below). The five attributes of each layer were given the same column
  names for all categories.
- The checks: every polygon of every zip read, the unused ones too, was tested a second time with
  `intersects` against the prefectures without the box pre-filter that the export uses; the set of
  polygons is the same, and the attributes and the coordinates (as WKB) equal the source's.
  Each river code carries a name that the source's river list gives it.

## Counts

By category and by source zip (the zips without polygon in the prefectures are listed above):

| Category | Polygons |
|---|---|
{category_rows}

| Source zip | Polygons kept | Polygons read |
|---|---|---|
{zip_rows}

Rivers (a polygon belongs to one river in one category; the same place is covered by the polygons of
several rivers and several categories, so do not add the polygons up as an area):

| River | Manager | Polygons |
|---|---|---|
{river_rows}

## Known issues

- The polygons are per river. A place on two rivers' floodplains has a polygon from each, with
  its own rank; the file does not merge them into a maximum depth per place. The source's own
  1st-level mesh version (A31b) does that; it is not included here.
- {invalid_text}
- {geom_text}
- The simulation behind the areas leaves out the flooding of tributaries, rain above the assumed
  rain, storm surge and inland water (内水); the page says places outside the areas can flood and
  the real depth can exceed the area's. Tokyo publishes its own 浸水予想区域図 that adds floods of
  rivers without a designated area and inland water; it is not part of this data.
- The page gives the positional accuracy as 25 m (the largest gap between the source's areas and the
  made data) and warns that the classes follow the product specification and can be coloured
  differently from the managers' published maps.
- A river's polygons can lie far from the river. Polygons labelled 浅川 and 大栗川 (managed by the
  Kanto bureau; tributaries of the Tama River) spread over the plain of the lower Tama River, to
  about 35.54 degrees north; they are in the file as the source has them, and this dataset did not
  look into why.
- The layer called `{odd_name}` in some files has the river name `芝川流域` where the river list
  gives `芝川` (`river_name` is as in the source).
- `duration` uses the rank codes 1 to 7 of the code list; `planned`, `maximum` 1 to 6.
- The source cuts the data by manager. Rivers whose managers have not provided data, or whose data
  the source could not publish, are missing.
- The data page of the FY2025 edition says the data were updated in May and October 2026; this is
  the state of {fetched}.

## Licence

The page marks the FY2025 edition (and FY2021 to FY2024) as オープンデータ（CC_BY_4.0） and asks
to follow the site's terms, 国土数値情報ダウンロードサイトコンテンツ利用規約
(<{terms}>), "and the conditions set by each prefecture" (「都道府県毎に定められた利用条件」); the
page does not say what those conditions are, and this dataset did not find them. The `KS-META`
of every zip used here says: 「利用に当たっての制限は、国土情報利用約款を参照してください。」
(the former name of those terms). The terms ask to name the source and to say that the data were
processed, forbid presenting processed data as made by the government, and leave rights of third
parties to the user (the original materials are the managers' flood inundation area maps). This
dataset is offered under CC BY 4.0 on that basis. Credit:

> 出典：「国土数値情報（洪水浸水想定区域データ（河川単位））」（国土交通省）（{page}）を加工して作成
"""


def main() -> int:
    run = sorted((ROOT / "data" / "raw").glob("*/MANIFEST.json"))[-1].parent
    manifest = json.loads((run / "MANIFEST.json").read_text())
    cols = json.loads((ROOT / "data/parquet/columns.json").read_text())
    assert cols["run"] == run.name
    t = pq.read_table(
        ROOT / "data/parquet/flood_area.parquet",
        columns=["category", "river_name", "manager", "class_code"],
    )
    d = t.to_pydict()
    rows = cols["rows"]
    slugs = [v[0] for v in k.CATEGORIES.values()]
    names = {v[0]: v[1] for v in k.CATEGORIES.values()}
    by_cat = Counter(d["category"])
    category_rows = "\n".join(f"| `{s}` {names[s]} | {by_cat[s]:,} |" for s in slugs)
    cand = [n for n, e in manifest["files"].items() if e["role"] == "candidate"]
    unused = cols["unused_zips"]
    used = [n for n in cand if n not in unused]
    unused_text = (
        f"{len(unused)} of them have no polygon in the four prefectures and are not republished "
        "(their sha256 are in `provenance.yaml`): "
        + ", ".join(f"`{n[8:13]}`" for n in unused)
        + "."
        if unused
        else "Every one of them has polygons in the four prefectures."
    )
    zip_rows = "\n".join(
        f"| `{n}` | {cols['by_zip'][n].get('polygons_kept', 0):,} | {cols['by_zip'][n].get('polygons_scanned', 0):,} |"
        for n in cand
    )
    rv = Counter(zip(d["river_name"], d["manager"], strict=True))
    top = sorted(rv.items(), key=lambda x: -x[1])
    river_rows = "\n".join(f"| {r} | {m} | {n:,} |" for (r, m), n in top[:40])
    if len(top) > 40:
        river_rows += f"\n| ({len(top) - 40} more rivers) | | {sum(n for _, n in top[40:]):,} |"
    cls = defaultdict(Counter)
    for c, v in zip(d["category"], d["class_code"], strict=True):
        cls[c][v] += 1
    code_lists = {"planned": k.WATER_DEPTH, "maximum": k.WATER_DEPTH, "duration": k.FLOOD_DURATION,
                  "collapse_flow": k.HAZARDOUS_AREA, "collapse_erosion": k.HAZARDOUS_AREA}  # fmt: skip
    class_rows = (
        "| Category | "
        + " | ".join(f"class {i}" for i in range(1, 8))
        + " |\n|---|"
        + "---|" * 7
        + "\n"
    )
    class_rows += "\n".join(
        f"| `{s}` | "
        + " | ".join(f"{cls[s][i]:,}" if i in code_lists[s] else "-" for i in range(1, 8))
        + " |"
        for s in slugs
    )
    lms = sorted({parsedate_to_datetime(manifest["files"][n]["last_modified"]) for n in cand})
    lm = (
        f"{lms[0]:%Y-%m-%d}"
        if lms[0].date() == lms[-1].date()
        else f"{lms[0]:%Y-%m-%d} to {lms[-1]:%Y-%m-%d} (it differs per file)"
    )
    inv = cols["invalid_geometries"]
    invalid_text = (
        "All polygons are valid in the sense of GEOS."
        if not inv
        else "Polygons that GEOS calls invalid (not repaired, kept as in the source): "
        + ", ".join(f"{k.CATEGORIES[c][0]} {n:,}" for c, n in inv.items())
        + "."
    )
    gt = cols["geometry_types"]
    geom_text = (
        f"Geometry types: Polygon {gt.get('Polygon', 0):,}, MultiPolygon {gt.get('MultiPolygon', 0):,} "
        "(a MultiPolygon is a source polygon with more than one outer ring)."
    )
    card = CARD.format(
        repo=REPO, rows=rows, page=k.PAGE_URL, terms=k.TERMS_URL, n03_page=k.N03_PAGE_URL,
        run=run.name, fetched=run.name, lm=lm, used_list=", ".join(f"`{n}`" for n in used),
        category_files=", ".join(f"`{s}`" for s in slugs), category_rows=category_rows,
        zip_rows=zip_rows, river_rows=river_rows, class_rows=class_rows, invalid_text=invalid_text,
        depth_codes=", ".join(f"{c} {v}" for c, v in k.WATER_DEPTH.items()),
        duration_codes=", ".join(f"{c} {v}" for c, v in k.FLOOD_DURATION.items()),
        odd_name="A31a_x02", geom_text=geom_text, unused_text=unused_text, n_candidates=len(cand),
    )  # fmt: skip
    data = ROOT / "data"
    (data / "README.md").write_text(card, encoding="utf-8")
    (data / "LICENSE").write_text(
        "CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/legalcode)\n\n"
        "The polygons are from 国土数値情報 洪水浸水想定区域データ（河川単位）(A31a, FY2025) of the\n"
        "Ministry of Land, Infrastructure, Transport and Tourism, published under the site's terms\n"
        f"({k.TERMS_URL}), processed as described in README.md.\n"
        f"Credit: 出典：「国土数値情報（洪水浸水想定区域データ（河川単位））」（国土交通省）（{k.PAGE_URL}）を加工して作成\n",
        encoding="utf-8",
    )
    checked = {
        n: {"sha256": manifest["files"][n]["sha256"], "bytes": manifest["files"][n]["bytes"]}
        for n in unused
    }
    (data / "provenance.yaml").write_text(
        f"source: 国土数値情報 洪水浸水想定区域データ（河川単位） (A31a), FY2025 (令和7年度版), 国土交通省\nurl: {k.PAGE_URL}\n"
        f"terms: {k.TERMS_URL}\nrun: {run.name}\nretrieved: {run.name}\nrows: {rows}\n"
        f"used_files: {len(used)}\nread_but_not_republished:\n"
        + "".join(
            f"  - {n}: sha256 {v['sha256']}, {v['bytes']} bytes\n" for n, v in checked.items()
        ),
        encoding="utf-8",
    )
    print("wrote data/README.md, LICENSE, provenance.yaml")
    return 0


if __name__ == "__main__":
    sys.exit(main())
