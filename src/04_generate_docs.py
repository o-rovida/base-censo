"""
04_generate_docs.py
-------------------
Gera a documentação completa em Markdown 100% referenciada nos originais do IBGE:
  1. docs/DICIONARIO_E_MODELAGEM.md
  2. docs/PIPELINE_DADOS.md
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "metadata" / "schema_spec.json"
RUN_PATH = ROOT / "metadata" / "pipeline_run_report.json"
VAL_PATH = ROOT / "metadata" / "validation_report.json"
OUT_DIC = ROOT / "docs" / "DICIONARIO_E_MODELAGEM.md"
OUT_PIPE = ROOT / "docs" / "PIPELINE_DADOS.md"


def md_escape(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ").strip()


def format_col_table(cols: list[dict[str, Any]]) -> str:
    lines = [
        "| # | Coluna Curada (Parquet) | Código IBGE | Origem | Tipo Parquet | Papel | Descrição Oficial (Layout IBGE) |",
        "| :---: | :--- | :---: | :---: | :---: | :--- | :--- |",
    ]
    for i, c in enumerate(cols, 1):
        role_label = {
            "key": "Chave",
            "weight": "Peso amostral",
            "numeric_int": "Numérica (inteiro)",
            "numeric_float": "Numérica (decimal)",
            "categorical_internal": f"Categórica ({len(c['value_map'])} cats.)",
            "categorical_external": f"Descritor externo ({c['external_descriptor']})",
            "code_raw": "Código bruto",
        }.get(c["role"], c["role"])
        lines.append(
            f"| {i} | `{c['column_name']}` | `{c['code']}` | `{c['source_table']}` | "
            f"`{c['duckdb_type']}` | {role_label} | {md_escape(c['ibge_title'])} |"
        )
    return "\n".join(lines)


def generate_dictionary_doc(spec: dict[str, Any], val: dict[str, Any]) -> str:
    obt_cols = spec["tables"]["pessoas_obt"]["columns"]
    mort_cols = spec["tables"]["mortalidade"]["columns"]
    exp = val["sample_expansion_checks"]

    redundant_rows = [
        "| Código Removido | Tabela Origem | Descrição IBGE | Equivalente Mantido na OBT |",
        "| :---: | :---: | :--- | :---: |",
    ]
    for r in spec["removed_redundant"]:
        redundant_rows.append(
            f"| `{r['code']}` | `{r['source_table']}` | {md_escape(r['ibge_title'])} | `{r['kept_equivalent']}` |"
        )

    flags_summary = []
    for tbl, flist in spec["removed_flags"].items():
        codes = ", ".join(f"`{f['code']}`" for f in flist)
        flags_summary.append(f"- **`{tbl}` ({len(flist)} flags):** {codes}")

    return f"""# Dicionário de Dados e Modelagem — Censo Demográfico 2022 (Dataset Curado)

> **Problema de pesquisa:** *"Quais características demográficas e domiciliares estão associadas às diferentes condições de vida da população brasileira?"*  
> **Créditos:** Pipeline e documentação elaborados com auxílio do **Google Antigravity**.  
> **Base de origem:** Microdados da Amostra do Censo Demográfico 2022 — **Acesso Controlado (Nível 2)** ([`microdados_censo_amostra_2022_csv_20261124_094334.zip`](../microdados_censo_amostra_2022_csv_20261124_094334.zip)).  
> **Referência oficial 100% vinculada a:**
> - [`Layout_Microdados_CD2022_Controlado.xlsx`](Layout_e_dicionario/Layout_Microdados_CD2022_Controlado.xlsx)
> - [`Dicionario_de_Variaveis_Microdados_CD2022.pdf`](Layout_e_dicionario/Dicionario_de_Variaveis_Microdados_CD2022.pdf)
> - [`Municipios_BR_Recortes_Territoriais_2022.xlsx`](Divisao_territorial/Municipios_BR_Recortes_Territoriais_2022.xlsx)
> - Bancos de Descritores em [`docs/Bancos_de_descritores/`](Bancos_de_descritores/)
> - Notas Metodológicas em [`docs/Notas_metodologicas/`](Notas_metodologicas/)

---

## 1. Arquitetura, Grão e Chaves das Tabelas

O dataset curado foi modelado em **duas tabelas em formato Parquet (compressão ZSTD)** para equilibrar facilidade analítica, integridade estatística e alta performance:

### Tabela 1 — `data/curated/pessoas_obt/` (One Big Table particionada por `uf=11`..`uf=53`)
* **Grão:** **1 linha = 1 pessoa** recenseada na amostra do Censo 2022 ({val['total_pessoas_obt_rows']:,} linhas no Brasil inteiro).
* **Construção relacional:** `Pessoas` (`PESS`) `LEFT JOIN` `Domicilios` (`DOMI`) por `(UF, Controle)` + `LEFT JOIN` `Familia` (`FAMI`) por `(UF, Controle, Ordem da Família)`.
* **Chave Primária (única verificada = `{val['pk_unique_pessoas_obt']}`):** `(uf, p0100_controle_domicilio, p0101_ordem_morador)`
* **Chave de Domicílio:** `(uf, p0100_controle_domicilio)` — {exp['domicilios_amostra_unicos']:,} domicílios únicos na amostra.
* **Chave de Família:** `(uf, p0100_controle_domicilio, p0115_ordem_familia)` — {exp['familias_amostra_unicas']:,} famílias únicas na amostra (`p0115_ordem_familia` é `NULL` para moradores de domicílios coletivos).
* **Coluna de partição Hive:** `uf` (`'11'` a `'53'`), criada automaticamente pela estrutura de pastas `uf=XX/part.parquet`, além da coluna descritiva `p0020_uf` (`'11 - Rondônia'`, ..., `'53 - Distrito Federal'`).
* **Total de colunas:** **{len(obt_cols)} colunas** (além da coluna virtual de partição `uf`).

### Tabela 2 — `data/curated/mortalidade/mortalidade.parquet` (Separada, sem agregação)
* **Grão:** **1 linha = 1 óbito** de morador ocorrido no domicílio entre janeiro/2019 e julho/2022 ({val['total_mortalidade_rows']:,} linhas no Brasil inteiro).
* **Por que separada?** Um mesmo domicílio pode ter registrado `0, 1, 2 ou mais` óbitos. Juntá-la diretamente à OBT de pessoas causaria produto cartesiano e distorceria contagens e pesos amostrais.
* **Chave Primária (única verificada = `{val['pk_unique_mortalidade']}`):** `(m0020_uf, m0100_controle_domicilio, m0101_ordem_obito)`
* **Chave para cruzamento com Domicílios:** `(SUBSTRING(m0020_uf, 1, 2), m0100_controle_domicilio)` ↔ `(uf, p0100_controle_domicilio)`.
* **Total de colunas:** **{len(mort_cols)} colunas**.

```mermaid
erDiagram
    PESSOAS_OBT {{
        string uf "Particao Hive (11..53)"
        string p0100_controle_domicilio PK "ID Domicilio"
        string p0101_ordem_morador PK "ID Morador"
        string p0115_ordem_familia "ID Familia (NULL em coletivos)"
        double p0111_peso_amostral_pessoa "Peso Pessoa"
        double d0111_peso_amostral_domicilio "Peso Domicilio"
        double f0111_peso_amostral_familia "Peso Familia"
    }}
    MORTALIDADE {{
        string m0020_uf PK "UF (codigo - nome)"
        string m0100_controle_domicilio PK,FK "ID Domicilio"
        string m0101_ordem_obito PK "ID Obito"
        double m0111_peso_amostral_obito "Peso Obito"
    }}
    PESSOAS_OBT }}o--o{{ MORTALIDADE : "mesmo domicilio (UF + Controle)"
```

---

## 2. Regras Metodológicas Obrigatórias (Pesos Amostrais e Deduplicação)

> ⚠️ **Regra de ouro do Censo 2022 (Amostra):** Nunca calcule proporções ou totais populacionais usando `COUNT(*)` simples sem ponderação. Utilize sempre o **peso amostral correspondente à unidade de análise**, conforme a Nota Metodológica [`NM_04_2026_Expansao_Amostra.pdf`](Notas_metodologicas/NM_04_2026_Expansao_Amostra.pdf).

| Unidade de Análise | Tabela | Deduplicação Necessária? | Peso Amostral | Total Expandido Validado (BR) |
| :--- | :--- | :--- | :--- | ---: |
| **Pessoas** (incluindo pessoas por condição do domicílio/família) | `pessoas_obt` | **Não** (já está no grão pessoa) | `p0111_peso_amostral_pessoa` | **{exp['populacao_expandida_soma_p0111']:,.0f}** |
| **Domicílios** | `pessoas_obt` | **Sim:** 1 linha por `(uf, p0100_controle_domicilio)` | `d0111_peso_amostral_domicilio` | **{exp['domicilios_expandidos_soma_d0111']:,.0f}** |
| **Famílias** | `pessoas_obt` | **Sim:** filtrar `p0115_ordem_familia IS NOT NULL` e deduplicar por `(uf, p0100_controle_domicilio, p0115_ordem_familia)` | `f0111_peso_amostral_familia` | **{exp['familias_expandidas_soma_f0111']:,.0f}** |
| **Óbitos** | `mortalidade` | **Não** (já está no grão óbito) | `m0111_peso_amostral_obito` | **{exp['obitos_expandidos_soma_m0111']:,.0f}** |

### Exemplo prático em DuckDB SQL (Deduplicação para Domicílios)
```sql
-- Exemplo: % de DOMICÍLIOS brasileiros por tipo de esgotamento sanitário
WITH domicilios_unicos AS (
    SELECT
        uf,
        p0100_controle_domicilio,
        ANY_VALUE(d0111_peso_amostral_domicilio) AS peso_dom,
        ANY_VALUE(d0250_tipo_esgotamento_sanitario_domicilio) AS esgoto
    FROM read_parquet('data/curated/pessoas_obt/uf=*/part.parquet', hive_partitioning=true)
    GROUP BY uf, p0100_controle_domicilio
)
SELECT
    esgoto,
    ROUND(SUM(peso_dom), 0) AS domicilios_estimados,
    ROUND(100.0 * SUM(peso_dom) / SUM(SUM(peso_dom)) OVER (), 2) AS pct
FROM domicilios_unicos
GROUP BY esgoto
ORDER BY domicilios_estimados DESC;
```

---

## 3. Convenções de Formatação

1. **Nomenclatura das colunas:** `<codigo_ibge_minusculo>_<nome_legivel>` (ex.: `p0160_sexo`, `d0360_rendimento_domiciliar_per_capita`). Permite rastrear imediatamente qualquer coluna até o PDF/Excel oficial do IBGE.
2. **Decodificação em coluna única (`"código - descrição"`):** Todas as variáveis categóricas (internas do Layout e externas dos Bancos de Descritores) foram decodificadas diretamente para o formato `"código - descrição"` (ex.: `"1 - Masculino"`, `"3550308 - São Paulo (SP)"`).
3. **Valores ausentes / não aplicáveis:** Campos em branco nos CSVs originais foram convertidos para `NULL` nativo do Parquet.
4. **Variáveis numéricas e chaves:** Pesos amostrais, rendimentos, idades numéricas, contagens de moradores/cômodos/filhos foram convertidos para `BIGINT` ou `DOUBLE` puros; chaves (`p0100_controle_domicilio`, `p0101_ordem_morador`, `p0115_ordem_familia`, `m0100_controle_domicilio`, `m0101_ordem_obito`) permanecem como `VARCHAR` sem decodificação.

---

## 4. Catálogo Completo de Variáveis — `pessoas_obt` ({len(obt_cols)} colunas)

{format_col_table(obt_cols)}

---

## 5. Catálogo Completo de Variáveis — `mortalidade` ({len(mort_cols)} colunas)

{format_col_table(mort_cols)}

---

## 6. Registro de Colunas Removidas (Auditoria)

### 6.1 Colunas redundantes removidas na unificação da OBT ({len(spec['removed_redundant'])} colunas)
As colunas abaixo possuíam informação idêntica à já presente no registro de `Pessoas` (`PESS`) e foram removidas apenas para eliminar duplicidade:

{chr(10).join(redundant_rows)}

### 6.2 Flags de crítica e imputação removidas ({sum(len(v) for v in spec['removed_flags'].values())} colunas)
As colunas `M*` indicam apenas se o dado original foi imputado pelo algoritmo de crítica do IBGE. Foram removidas do dataset curado para otimizar legibilidade e armazenamento (continuam integralmente preservadas no `.zip` bruto caso seja necessária auditoria de imputação):

{chr(10).join(flags_summary)}
"""


def generate_pipeline_doc(spec: dict[str, Any], run: dict[str, Any], val: dict[str, Any]) -> str:
    uf_lines = [
        "| UF | Linhas `pessoas_obt` | Tamanho Parquet (MiB) | Linhas `mortalidade` | Tempo (s) | Match CSV? |",
        "| :---: | ---: | ---: | ---: | ---: | :---: |",
    ]
    val_map = {u["uf"]: u["match"] for u in val["uf_checks"]}
    for r in run["by_uf"]:
        mb = r["pessoas_obt_bytes"] / (1024 * 1024)
        ok = "✅" if val_map.get(r["uf"]) else "❌"
        uf_lines.append(
            f"| `{r['uf']}` | {r['pessoas_obt_rows']:,} | {mb:.1f} | {r['mortalidade_rows']:,} | {r['elapsed_seconds']:.1f} | {ok} |"
        )

    unmapped = val.get("unmapped_categorical_values") or []
    if unmapped:
        unmapped_md = "\n".join(
            f"- `{u['table']}.{u['column']}`: {u['unmapped_rows']:,} linhas sem rótulo (amostras: `{u['sample_values']}`)"
            for u in unmapped
        )
    else:
        unmapped_md = "Nenhum código categórico ficou sem rótulo (100% de cobertura)."

    return f"""# Documentação do Pipeline de Dados — Censo Demográfico 2022

> **Créditos:** Pipeline e documentação elaborados com auxílio do **Google Antigravity**.

Este documento descreve todo o fluxo de engenharia de dados usado para transformar os **108 arquivos CSV (~11,11 GiB descompactados)** da Base Controlada do Censo 2022 em um **dataset curado, tipado, decodificado e validado em Parquet** e sua camada **Analytics (Gold)** para ICV + ACM.

---

## 1. Estrutura do Projeto

```text
base-censo/
├── pyproject.toml                       # Dependências Python (DuckDB, PyArrow, openpyxl) gerenciadas com uv
├── microdados_censo_amostra_2022_...zip # Base bruta IBGE (Acesso Controlado - intocada)
├── metadata/
│   ├── schema_spec.json                 # Especificação completa gerada automaticamente do Layout + Descritores
│   ├── pipeline_run_report.json         # Telemetria de execução por UF (curated)
│   ├── validation_report.json           # Resultado das auditorias automatizadas (curated)
│   └── analytics_icv_acm_report.json    # Sanity checks, perfil de NULLs e distribuições mort_* (analytics)
├── src/
│   ├── 01_parse_metadata.py             # Etapa 1: extrai Layout + 7 Bancos de Descritores -> schema_spec.json
│   ├── 02_build_curated_parquet.py      # Etapa 2: lê ZIP UF por UF via DuckDB -> grava Parquet ZSTD (curated)
│   ├── 03_validate_dataset.py           # Etapa 3: valida contagens vs CSV, unicidade de PK, pesos e rótulos
│   ├── 04_generate_docs.py              # Etapa 4: gera DICIONARIO_E_MODELAGEM.md e PIPELINE_DADOS.md
│   └── 05_build_analytics_icv_acm.py    # Etapa 5: curated -> analytics (Gold) para ICV + ACM + MODELAGEM_ANALYTICS_ICV_ACM.md
├── data/
│   ├── curated/                         # Camada Silver (imutável após validação)
│   │   ├── pessoas_obt/uf=11..53/part.parquet   # OBT particionada por UF
│   │   └── mortalidade/mortalidade.parquet      # Óbitos (grão original, sem agregação)
│   └── analytics/                       # Camada Gold (pronta para consumo em R / Colab)
│       └── censo2022_icv_acm.parquet            # Dataset enxuto ICV + ACM com mortalidade agregada por domicílio
└── docs/
    ├── DICIONARIO_E_MODELAGEM.md        # Grão, chaves, regras de pesos e 100% das variáveis curadas
    ├── PIPELINE_DADOS.md                # Este documento
    ├── MODELAGEM_ANALYTICS_ICV_ACM.md   # Modelagem e justificativas da camada Analytics (Gold)
    └── (documentação oficial IBGE baixada do FTP)
```

---

## 2. Como Reexecutar o Pipeline do Zero

Pré-requisito: `uv` instalado na máquina e o arquivo `.zip` original na raiz do projeto (ou os Parquets já prontos em `data/curated/` para rodar apenas a Etapa 5).

```powershell
# 1. Criar/sincronizar ambiente virtual isolado (.venv)
uv sync

# 2. Gerar especificação de metadados a partir do Layout e Descritores oficiais
.\\.venv\\Scripts\\python.exe src\\01_parse_metadata.py

# 3. Construir as tabelas Parquet curadas (UF por UF direto do ZIP)
.\\.venv\\Scripts\\python.exe src\\02_build_curated_parquet.py

# 4. Executar validações de integridade e pesos amostrais (curated)
.\\.venv\\Scripts\\python.exe src\\03_validate_dataset.py

# 5. Regerar a documentação Markdown da camada curated
.\\.venv\\Scripts\\python.exe src\\04_generate_docs.py

# 6. Construir a camada Analytics (Gold) para ICV + ACM e gerar docs/MODELAGEM_ANALYTICS_ICV_ACM.md
.\\.venv\\Scripts\\python.exe src\\05_build_analytics_icv_acm.py
```

---

## 3. Decisões Técnicas de Eficiência

* **Processamento em streaming por UF:** Em vez de descompactar 11,11 GiB no disco de uma vez, `02_build_curated_parquet.py` extrai apenas os 4 CSVs de uma UF para um diretório temporário, processa no **DuckDB** e remove os temporários imediatamente antes de passar à próxima UF.
* **Compressão Parquet + ZSTD + Dictionary Encoding:** Os 11,11 GiB de CSVs brutos foram compactados para **{run['total_pessoas_obt_gb']:.3f} GiB** (`pessoas_obt`) + **{run['total_mortalidade_mb']:.2f} MiB** (`mortalidade`), mesmo com todas as variáveis categóricas já decodificadas para texto descritivo.
* **Particionamento Hive por `uf`:** Consultas filtradas por estado (ex.: `WHERE uf = '35'`) lêem apenas a partição correspondente, e o formato colunar Parquet lê apenas as colunas selecionadas no `SELECT`.

---

## 4. Resultados da Execução e Validação

* **Tempo total de construção (27 UFs):** **{run['total_elapsed_seconds']:.1f} segundos**
* **Total `pessoas_obt`:** **{val['total_pessoas_obt_rows']:,} linhas** (CSV original: {val['total_pessoas_csv_rows']:,} — Match 100%: `{val['all_uf_row_counts_match_csv']}`)
* **Total `mortalidade`:** **{val['total_mortalidade_rows']:,} linhas** (CSV original: {val['total_mortalidade_csv_rows']:,} — Match 100%: `{val['mortalidade_row_count_match_csv']}`)
* **Unicidade de Chave Primária:** `pessoas_obt` = `{val['pk_unique_pessoas_obt']}` | `mortalidade` = `{val['pk_unique_mortalidade']}`
* **Cobertura de decodificação:**
{unmapped_md}

### Detalhamento por UF

{chr(10).join(uf_lines)}

---

## 5. Exemplos Prontos de Consulta (Foco: Condições de Vida)

### Exemplo 1 — Rendimento domiciliar per capita médio e acesso a saneamento/internet por Grande Região (Python + DuckDB)
```python
import duckdb

con = duckdb.connect()
df = con.execute(\"\"\"
    SELECT
        p0010_grande_regiao AS regiao,
        ROUND(SUM(p0111_peso_amostral_pessoa), 0) AS pop_estimada,
        ROUND(
            SUM(d0360_rendimento_domiciliar_per_capita * p0111_peso_amostral_pessoa)
            / NULLIF(SUM(CASE WHEN d0360_rendimento_domiciliar_per_capita IS NOT NULL THEN p0111_peso_amostral_pessoa END), 0),
            2
        ) AS renda_dom_pc_media,
        ROUND(
            100.0 * SUM(CASE WHEN d0330_acesso_internet_existencia LIKE '1 -%' THEN p0111_peso_amostral_pessoa ELSE 0 END)
            / SUM(p0111_peso_amostral_pessoa),
            2
        ) AS pct_pop_com_internet
    FROM read_parquet('data/curated/pessoas_obt/uf=*/part.parquet', hive_partitioning=true)
    GROUP BY p0010_grande_regiao
    ORDER BY p0010_grande_regiao
\"\"\").fetchdf()
print(df)
```

### Exemplo 2 — Recorte rápido de um único estado (ex.: São Paulo, `uf = '35'`) cruzando cor/raça e escolaridade
```python
import duckdb

df_sp = duckdb.execute(\"\"\"
    SELECT
        p0210_cor_raca_pessoa AS cor_raca,
        p0760_morador_nivel_instrucao_ensino AS nivel_instrucao,
        ROUND(SUM(p0111_peso_amostral_pessoa), 0) AS pessoas_estimadas
    FROM read_parquet('data/curated/pessoas_obt/uf=*/part.parquet', hive_partitioning=true)
    WHERE uf = '35' AND p0181_idade_anos >= 25
    GROUP BY ALL
    ORDER BY cor_raca, pessoas_estimadas DESC
\"\"\").fetchdf()
```
"""


def main() -> None:
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    run = json.loads(RUN_PATH.read_text(encoding="utf-8"))
    val = json.loads(VAL_PATH.read_text(encoding="utf-8"))

    OUT_DIC.write_text(generate_dictionary_doc(spec, val), encoding="utf-8")
    OUT_PIPE.write_text(generate_pipeline_doc(spec, run, val), encoding="utf-8")
    print(f"Documentação gerada com sucesso:\n - {OUT_DIC}\n - {OUT_PIPE}")


if __name__ == "__main__":
    main()
