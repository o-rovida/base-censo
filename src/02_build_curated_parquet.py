"""
02_build_curated_parquet.py
---------------------------
Pipeline principal em Python + DuckDB que lê o ZIP da Base Controlada do Censo 2022
UF por UF (sem descompactar os 11 GB de uma vez), executa:
  1. LEFT JOIN de Pessoas + Domicílios + Família (grão: 1 linha = 1 pessoa)
  2. Remoção de colunas redundantes e flags de imputação (conforme metadata/schema_spec.json)
  3. Decodificação integral de variáveis categóricas e descritores externos para "código - descrição"
  4. Tipagem forte (BIGINT / DOUBLE / VARCHAR) com NULLs nativos
  5. Gravação em Parquet (ZSTD):
       - data/curated/pessoas_obt/uf={UF}/part.parquet
       - data/curated/mortalidade/mortalidade.parquet

Uso:
  python src/02_build_curated_parquet.py              # Processa todas as 27 UFs
  python src/02_build_curated_parquet.py --ufs 11 12  # Processa apenas UFs selecionadas
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import time
import zipfile
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
ZIP_PATH = ROOT / "microdados_censo_amostra_2022_csv_20261124_094334.zip"
SPEC_PATH = ROOT / "metadata" / "schema_spec.json"
OUT_OBT = ROOT / "data" / "curated" / "pessoas_obt"
OUT_MORT = ROOT / "data" / "curated" / "mortalidade" / "mortalidade.parquet"
REPORT_PATH = ROOT / "metadata" / "pipeline_run_report.json"

ALL_UFS = [
    "11", "12", "13", "14", "15", "16", "17",
    "21", "22", "23", "24", "25", "26", "27", "28", "29",
    "31", "32", "33", "35",
    "41", "42", "43",
    "50", "51", "52", "53",
]


def register_lookup_tables(con: duckdb.DuckDBPyConnection, spec: dict[str, Any]) -> dict[str, str]:
    """Registra tabelas de lookup em memória no DuckDB para cada coluna categórica."""
    lookup_names: dict[str, str] = {}
    all_cols = spec["tables"]["pessoas_obt"]["columns"] + spec["tables"]["mortalidade"]["columns"]

    for col in all_cols:
        vmap: dict[str, str] = col.get("value_map") or {}
        if not vmap:
            continue
        code = col["code"]
        if code in lookup_names:
            continue

        int_len = int(col.get("ibge_int") or 0)
        expanded: dict[str, str] = {}
        for raw_k, label in vmap.items():
            k = str(raw_k).strip()
            expanded[k] = label
            # Garante match independente de zero à esquerda (ex.: "1" vs "01")
            if k.isdigit():
                k_int = str(int(k))
                expanded.setdefault(k_int, label)
                if int_len > 0 and len(k_int) <= int_len:
                    expanded.setdefault(k_int.zfill(int_len), label)

        tbl_name = f"_lk_{code.lower()}"
        pa_tbl = pa.table({
            "k": pa.array(list(expanded.keys()), type=pa.string()),
            "v": pa.array(list(expanded.values()), type=pa.string()),
        })
        con.register(tbl_name, pa_tbl)
        lookup_names[code] = tbl_name

    return lookup_names


def build_select_and_joins(
    cols: list[dict[str, Any]],
    lookup_names: dict[str, str],
    alias_by_source: dict[str, str],
) -> tuple[list[str], list[str]]:
    """Monta expressões SELECT e LEFT JOINs de lookup para uma tabela."""
    select_exprs: list[str] = []
    lookup_joins: list[str] = []

    for col in cols:
        code = col["code"]
        src_alias = alias_by_source[col["source_table"]]
        col_name = col["column_name"]
        dtype = col["duckdb_type"]
        raw_expr = f"NULLIF(TRIM({src_alias}.\"{code}\"), '')"

        if code in lookup_names:
            lk_tbl = lookup_names[code]
            lk_alias = f"lk_{code.lower()}"
            lookup_joins.append(f"LEFT JOIN {lk_tbl} AS {lk_alias} ON {raw_expr} = {lk_alias}.k")
            select_exprs.append(f"COALESCE({lk_alias}.v, {raw_expr}) AS \"{col_name}\"")
        elif dtype in ("BIGINT", "DOUBLE"):
            select_exprs.append(f"TRY_CAST({raw_expr} AS {dtype}) AS \"{col_name}\"")
        else:
            select_exprs.append(f"{raw_expr} AS \"{col_name}\"")

    return select_exprs, lookup_joins


def process_uf(
    con: duckdb.DuckDBPyConnection,
    zf: zipfile.ZipFile,
    uf: str,
    spec: dict[str, Any],
    lookup_names: dict[str, str],
    tmp_root: Path,
    mort_tmp_dir: Path,
) -> dict[str, Any]:
    t0 = time.perf_counter()
    uf_tmp = tmp_root / f"uf_{uf}"
    uf_tmp.mkdir(parents=True, exist_ok=True)

    try:
        csv_paths: dict[str, Path] = {}
        for tipo in ("Pessoas", "Domicilios", "Familia", "Mortalidade"):
            member = f"{uf}/{tipo}_{uf}_controlado.csv"
            out_csv = uf_tmp / f"{tipo}_{uf}_controlado.csv"
            with zf.open(member) as src, open(out_csv, "wb") as dst:
                shutil.copyfileobj(src, dst, length=8 * 1024 * 1024)
            csv_paths[tipo] = out_csv

        def csv_scan(path: Path) -> str:
            p = str(path).replace("\\", "/")
            return (
                f"read_csv('{p}', delim=';', header=true, all_varchar=true, "
                f"strict_mode=false, null_padding=true, ignore_errors=false)"
            )

        # 1. Pessoas OBT
        obt_cols = spec["tables"]["pessoas_obt"]["columns"]
        obt_selects, obt_lk_joins = build_select_and_joins(
            obt_cols, lookup_names, {"PESS": "p", "DOMI": "d", "FAMI": "f"}
        )

        uf_out_dir = OUT_OBT / f"uf={uf}"
        uf_out_dir.mkdir(parents=True, exist_ok=True)
        obt_parquet = uf_out_dir / "part.parquet"
        obt_parquet_sql = str(obt_parquet).replace("\\", "/")

        obt_sql = f"""
        COPY (
            SELECT
                {",\n                ".join(obt_selects)}
            FROM {csv_scan(csv_paths['Pessoas'])} AS p
            LEFT JOIN {csv_scan(csv_paths['Domicilios'])} AS d
                ON TRIM(p."P0020") = TRIM(d."D0020")
               AND TRIM(p."P0100") = TRIM(d."D0100")
            LEFT JOIN {csv_scan(csv_paths['Familia'])} AS f
                ON TRIM(p."P0020") = TRIM(f."F0020")
               AND TRIM(p."P0100") = TRIM(f."F0100")
               AND NULLIF(TRIM(p."P0115"), '') = NULLIF(TRIM(f."F0101"), '')
            {"\n            ".join(obt_lk_joins)}
        ) TO '{obt_parquet_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
        """
        con.execute(obt_sql)

        # 2. Mortalidade (separada, sem agregação)
        mort_cols = spec["tables"]["mortalidade"]["columns"]
        mort_selects, mort_lk_joins = build_select_and_joins(
            mort_cols, lookup_names, {"MORT": "m"}
        )
        mort_part = mort_tmp_dir / f"mort_{uf}.parquet"
        mort_part_sql = str(mort_part).replace("\\", "/")

        mort_sql = f"""
        COPY (
            SELECT
                {",\n                ".join(mort_selects)}
            FROM {csv_scan(csv_paths['Mortalidade'])} AS m
            {"\n            ".join(mort_lk_joins)}
        ) TO '{mort_part_sql}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """
        con.execute(mort_sql)

        # Métricas da UF
        obt_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{obt_parquet_sql}')").fetchone()[0]
        mort_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{mort_part_sql}')").fetchone()[0]
        elapsed = time.perf_counter() - t0
        obt_mb = obt_parquet.stat().st_size / (1024 * 1024)
        print(
            f"[UF {uf}] pessoas_obt={obt_rows:>9,} linhas ({obt_mb:6.1f} MB) | "
            f"mortalidade={mort_rows:>6,} linhas | {elapsed:5.1f}s",
            flush=True,
        )
        return {
            "uf": uf,
            "pessoas_obt_rows": int(obt_rows),
            "pessoas_obt_bytes": int(obt_parquet.stat().st_size),
            "mortalidade_rows": int(mort_rows),
            "elapsed_seconds": round(elapsed, 2),
        }
    finally:
        shutil.rmtree(uf_tmp, ignore_errors=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Constrói o dataset curado em Parquet do Censo 2022.")
    parser.add_argument("--ufs", nargs="*", default=ALL_UFS, help="Códigos IBGE das UFs a processar.")
    args = parser.parse_args()

    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    OUT_OBT.mkdir(parents=True, exist_ok=True)
    OUT_MORT.parent.mkdir(parents=True, exist_ok=True)

    t_start = time.perf_counter()
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")
    lookup_names = register_lookup_tables(con, spec)

    uf_reports: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="censo2022_build_") as tmp_str:
        tmp_root = Path(tmp_str)
        mort_tmp_dir = tmp_root / "mort_parts"
        mort_tmp_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            for uf in args.ufs:
                rep = process_uf(con, zf, uf, spec, lookup_names, tmp_root, mort_tmp_dir)
                uf_reports.append(rep)

        # Consolida mortalidade em arquivo Parquet único
        mort_glob = str(mort_tmp_dir / "mort_*.parquet").replace("\\", "/")
        out_mort_sql = str(OUT_MORT).replace("\\", "/")
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{mort_glob}')) "
            f"TO '{out_mort_sql}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        )

    total_elapsed = time.perf_counter() - t_start
    total_obt_rows = sum(r["pessoas_obt_rows"] for r in uf_reports)
    total_obt_bytes = sum(r["pessoas_obt_bytes"] for r in uf_reports)
    total_mort_rows = sum(r["mortalidade_rows"] for r in uf_reports)
    mort_bytes = OUT_MORT.stat().st_size

    summary = {
        "ufs_processed": args.ufs,
        "total_pessoas_obt_rows": total_obt_rows,
        "total_pessoas_obt_gb": round(total_obt_bytes / (1024 ** 3), 3),
        "total_mortalidade_rows": total_mort_rows,
        "total_mortalidade_mb": round(mort_bytes / (1024 ** 2), 2),
        "total_elapsed_seconds": round(total_elapsed, 2),
        "by_uf": uf_reports,
    }
    REPORT_PATH.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("\n=== CONCLUÍDO ===")
    print(f"pessoas_obt : {total_obt_rows:,} linhas | {summary['total_pessoas_obt_gb']:.3f} GiB")
    print(f"mortalidade : {total_mort_rows:,} linhas | {summary['total_mortalidade_mb']:.2f} MiB")
    print(f"Tempo total : {total_elapsed:.1f}s")
    print(f"Relatório   : {REPORT_PATH}")


if __name__ == "__main__":
    main()
