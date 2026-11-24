"""
03_validate_dataset.py
----------------------
Executa validações automatizadas de integridade, grão, pesos amostrais e cobertura
de decodificação sobre o dataset curado em Parquet.
Salva o resultado em metadata/validation_report.json.
"""

from __future__ import annotations

import json
import zipfile
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
ZIP_PATH = ROOT / "microdados_censo_amostra_2022_csv_20261124_094334.zip"
SPEC_PATH = ROOT / "metadata" / "schema_spec.json"
OBT_GLOB = str(ROOT / "data" / "curated" / "pessoas_obt" / "uf=*" / "part.parquet").replace("\\", "/")
MORT_PATH = str(ROOT / "data" / "curated" / "mortalidade" / "mortalidade.parquet").replace("\\", "/")
OUT_REPORT = ROOT / "metadata" / "validation_report.json"


def count_csv_lines_in_zip(zf: zipfile.ZipFile, member: str) -> int:
    """Conta rapidamente as linhas de dados (excluindo cabeçalho) direto do ZIP."""
    count = 0
    with zf.open(member, "r") as f:
        while chunk := f.read(4 * 1024 * 1024):
            count += chunk.count(b"\n")
    return max(count - 1, 0)


def main() -> None:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4;")

    # 1. Validação de contagens por UF contra o ZIP original
    obt_uf_counts = dict(
        con.execute(
            f"SELECT uf, COUNT(*) FROM read_parquet('{OBT_GLOB}', hive_partitioning=true) GROUP BY uf ORDER BY uf"
        ).fetchall()
    )
    mort_total_parquet = con.execute(f"SELECT COUNT(*) FROM read_parquet('{MORT_PATH}')").fetchone()[0]

    uf_checks: list[dict[str, Any]] = []
    total_csv_pessoas = 0
    total_csv_mort = 0
    all_uf_match = True

    with zipfile.ZipFile(ZIP_PATH, "r") as zf:
        for uf in sorted(obt_uf_counts.keys()):
            csv_p = count_csv_lines_in_zip(zf, f"{uf}/Pessoas_{uf}_controlado.csv")
            csv_m = count_csv_lines_in_zip(zf, f"{uf}/Mortalidade_{uf}_controlado.csv")
            total_csv_pessoas += csv_p
            total_csv_mort += csv_m
            match = (obt_uf_counts[uf] == csv_p)
            if not match:
                all_uf_match = False
            uf_checks.append({
                "uf": uf,
                "csv_pessoas": csv_p,
                "parquet_pessoas_obt": int(obt_uf_counts[uf]),
                "match": match,
            })

    mort_match = (mort_total_parquet == total_csv_mort)

    # 2. Validação de unicidade da chave primária em pessoas_obt e mortalidade
    pk_check_obt = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT (uf || '|' || p0100_controle_domicilio || '|' || p0101_ordem_morador)) AS distinct_pk
        FROM read_parquet('{OBT_GLOB}', hive_partitioning=true)
        """
    ).fetchone()
    pk_unique_obt = (pk_check_obt[0] == pk_check_obt[1])

    pk_check_mort = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT (m0020_uf || '|' || m0100_controle_domicilio || '|' || m0101_ordem_obito)) AS distinct_pk
        FROM read_parquet('{MORT_PATH}')
        """
    ).fetchone()
    pk_unique_mort = (pk_check_mort[0] == pk_check_mort[1])

    # 3. Sanidade amostral (população expandida, domicílios expandidos, famílias expandidas, óbitos expandidos)
    pop_expandida = con.execute(
        f"SELECT SUM(p0111_peso_amostral_pessoa) FROM read_parquet('{OBT_GLOB}', hive_partitioning=true)"
    ).fetchone()[0]

    dom_expandido = con.execute(
        f"""
        SELECT COUNT(*) AS dom_amostra, SUM(peso_dom) AS dom_expandidos
        FROM (
            SELECT uf, p0100_controle_domicilio, ANY_VALUE(d0111_peso_amostral_domicilio) AS peso_dom
            FROM read_parquet('{OBT_GLOB}', hive_partitioning=true)
            GROUP BY uf, p0100_controle_domicilio
        )
        """
    ).fetchone()

    fam_expandida = con.execute(
        f"""
        SELECT COUNT(*) AS fam_amostra, SUM(peso_fam) AS fam_expandidas
        FROM (
            SELECT uf, p0100_controle_domicilio, p0115_ordem_familia, ANY_VALUE(f0111_peso_amostral_familia) AS peso_fam
            FROM read_parquet('{OBT_GLOB}', hive_partitioning=true)
            WHERE p0115_ordem_familia IS NOT NULL
            GROUP BY uf, p0100_controle_domicilio, p0115_ordem_familia
        )
        """
    ).fetchone()

    obitos_expandidos = con.execute(
        f"SELECT SUM(m0111_peso_amostral_obito) FROM read_parquet('{MORT_PATH}')"
    ).fetchone()[0]

    # 4. Cobertura de decodificação (verifica valores sem " - " em colunas categóricas)
    unmapped_report: list[dict[str, Any]] = []
    for tname, parquet_expr in [
        ("pessoas_obt", f"read_parquet('{OBT_GLOB}', hive_partitioning=true)"),
        ("mortalidade", f"read_parquet('{MORT_PATH}')"),
    ]:
        cat_cols = [
            c["column_name"]
            for c in spec["tables"][tname]["columns"]
            if c["role"] in ("categorical_internal", "categorical_external")
        ]
        # Verifica em lote quais colunas têm algum valor não-nulo sem " - "
        agg_parts = [
            f"COUNT_IF(\"{cn}\" IS NOT NULL AND NOT CONTAINS(\"{cn}\", ' - ')) AS \"{cn}\""
            for cn in cat_cols
        ]
        res = con.execute(f"SELECT {', '.join(agg_parts)} FROM {parquet_expr}").fetchone()
        for cn, cnt in zip(cat_cols, res):
            if cnt and cnt > 0:
                samples = [
                    r[0]
                    for r in con.execute(
                        f"SELECT DISTINCT \"{cn}\" FROM {parquet_expr} "
                        f"WHERE \"{cn}\" IS NOT NULL AND NOT CONTAINS(\"{cn}\", ' - ') LIMIT 10"
                    ).fetchall()
                ]
                unmapped_report.append({
                    "table": tname,
                    "column": cn,
                    "unmapped_rows": int(cnt),
                    "sample_values": samples,
                })

    report = {
        "ufs_validated": len(obt_uf_counts),
        "all_uf_row_counts_match_csv": all_uf_match,
        "mortalidade_row_count_match_csv": mort_match,
        "total_pessoas_obt_rows": int(pk_check_obt[0]),
        "total_pessoas_csv_rows": int(total_csv_pessoas),
        "total_mortalidade_rows": int(mort_total_parquet),
        "total_mortalidade_csv_rows": int(total_csv_mort),
        "pk_unique_pessoas_obt": pk_unique_obt,
        "pk_unique_mortalidade": pk_unique_mort,
        "sample_expansion_checks": {
            "populacao_expandida_soma_p0111": round(float(pop_expandida), 2),
            "domicilios_amostra_unicos": int(dom_expandido[0]),
            "domicilios_expandidos_soma_d0111": round(float(dom_expandido[1]), 2),
            "familias_amostra_unicas": int(fam_expandida[0]),
            "familias_expandidas_soma_f0111": round(float(fam_expandida[1]), 2),
            "obitos_expandidos_soma_m0111": round(float(obitos_expandidos), 2),
        },
        "unmapped_categorical_values": unmapped_report,
        "uf_checks": uf_checks,
    }
    OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== VALIDAÇÃO DO DATASET CURADO ===")
    print(f"UFs validadas                  : {report['ufs_validated']}")
    print(f"Linhas pessoas_obt == CSV?     : {all_uf_match} ({pk_check_obt[0]:,} vs {total_csv_pessoas:,})")
    print(f"Linhas mortalidade == CSV?     : {mort_match} ({mort_total_parquet:,} vs {total_csv_mort:,})")
    print(f"PK única (pessoas_obt / mort)  : {pk_unique_obt} / {pk_unique_mort}")
    print(f"População expandida (P0111)    : {pop_expandida:,.0f}")
    print(f"Domicílios expandidos (D0111)  : {dom_expandido[1]:,.0f} ({dom_expandido[0]:,} na amostra)")
    print(f"Famílias expandidas (F0111)    : {fam_expandida[1]:,.0f} ({fam_expandida[0]:,} na amostra)")
    print(f"Óbitos expandidos (M0111)      : {obitos_expandidos:,.0f}")
    print(f"Colunas com códigos sem rótulo : {len(unmapped_report)}")
    for item in unmapped_report:
        print(f"  - {item['table']}.{item['column']}: {item['unmapped_rows']:,} linhas | amostras={item['sample_values']}")


if __name__ == "__main__":
    main()
