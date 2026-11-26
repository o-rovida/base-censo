# Documentação do Pipeline de Dados — Censo Demográfico 2022

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
.\.venv\Scripts\python.exe src\01_parse_metadata.py

# 3. Construir as tabelas Parquet curadas (UF por UF direto do ZIP)
.\.venv\Scripts\python.exe src\02_build_curated_parquet.py

# 4. Executar validações de integridade e pesos amostrais (curated)
.\.venv\Scripts\python.exe src\03_validate_dataset.py

# 5. Regerar a documentação Markdown da camada curated
.\.venv\Scripts\python.exe src\04_generate_docs.py

# 6. Construir a camada Analytics (Gold) para ICV + ACM e gerar docs/MODELAGEM_ANALYTICS_ICV_ACM.md
.\.venv\Scripts\python.exe src\05_build_analytics_icv_acm.py
```

---

## 3. Decisões Técnicas de Eficiência

* **Processamento em streaming por UF:** Em vez de descompactar 11,11 GiB no disco de uma vez, `02_build_curated_parquet.py` extrai apenas os 4 CSVs de uma UF para um diretório temporário, processa no **DuckDB** e remove os temporários imediatamente antes de passar à próxima UF.
* **Compressão Parquet + ZSTD + Dictionary Encoding:** Os 11,11 GiB de CSVs brutos foram compactados para **0.856 GiB** (`pessoas_obt`) + **6.24 MiB** (`mortalidade`), mesmo com todas as variáveis categóricas já decodificadas para texto descritivo.
* **Particionamento Hive por `uf`:** Consultas filtradas por estado (ex.: `WHERE uf = '35'`) lêem apenas a partição correspondente, e o formato colunar Parquet lê apenas as colunas selecionadas no `SELECT`.

---

## 4. Resultados da Execução e Validação

* **Tempo total de construção (27 UFs):** **914.0 segundos**
* **Total `pessoas_obt`:** **21,539,579 linhas** (CSV original: 21,539,579 — Match 100%: `True`)
* **Total `mortalidade`:** **430,965 linhas** (CSV original: 430,965 — Match 100%: `True`)
* **Unicidade de Chave Primária:** `pessoas_obt` = `True` | `mortalidade` = `True`
* **Cobertura de decodificação:**
- `pessoas_obt.f0150_familia_identificacao`: 2 linhas sem rótulo (amostras: `['11']`)
- `pessoas_obt.f0220_familias_conviventes_secundarias_tipologia`: 475 linhas sem rótulo (amostras: `['5']`)

### Detalhamento por UF

| UF | Linhas `pessoas_obt` | Tamanho Parquet (MiB) | Linhas `mortalidade` | Tempo (s) | Match CSV? |
| :---: | ---: | ---: | ---: | ---: | :---: |
| `11` | 170,240 | 7.2 | 2,772 | 5.1 | ❌ |
| `12` | 116,783 | 4.7 | 1,616 | 4.4 | ❌ |
| `13` | 389,199 | 15.0 | 5,749 | 8.4 | ❌ |
| `14` | 85,986 | 3.4 | 1,168 | 4.7 | ❌ |
| `15` | 833,378 | 31.9 | 12,973 | 15.1 | ❌ |
| `16` | 68,341 | 2.8 | 1,126 | 4.4 | ❌ |
| `17` | 274,941 | 11.5 | 4,753 | 6.9 | ❌ |
| `21` | 797,135 | 30.4 | 14,334 | 16.1 | ❌ |
| `22` | 500,931 | 19.4 | 10,279 | 11.6 | ❌ |
| `23` | 883,087 | 34.5 | 17,530 | 17.0 | ❌ |
| `24` | 440,132 | 17.6 | 9,117 | 9.2 | ❌ |
| `25` | 585,741 | 23.0 | 12,457 | 12.0 | ❌ |
| `26` | 912,267 | 36.3 | 18,595 | 16.6 | ❌ |
| `27` | 347,165 | 13.6 | 6,601 | 12.3 | ❌ |
| `28` | 261,720 | 10.7 | 4,915 | 7.4 | ❌ |
| `29` | 1,612,463 | 64.2 | 31,850 | 26.0 | ❌ |
| `31` | 2,582,000 | 106.1 | 53,401 | 79.3 | ❌ |
| `32` | 418,200 | 17.3 | 7,814 | 27.9 | ❌ |
| `33` | 1,129,683 | 45.7 | 26,594 | 52.2 | ❌ |
| `35` | 3,716,773 | 154.0 | 78,485 | 195.1 | ❌ |
| `41` | 1,382,737 | 58.0 | 29,106 | 81.8 | ❌ |
| `42` | 947,947 | 39.9 | 16,780 | 57.9 | ❌ |
| `43` | 1,396,115 | 58.5 | 33,119 | 88.1 | ❌ |
| `50` | 303,624 | 12.7 | 5,663 | 35.2 | ❌ |
| `51` | 445,147 | 18.6 | 7,031 | 37.9 | ❌ |
| `52` | 796,342 | 33.5 | 14,982 | 56.1 | ❌ |
| `53` | 141,502 | 6.1 | 2,155 | 22.9 | ❌ |

---

## 5. Exemplos Prontos de Consulta (Foco: Condições de Vida)

### Exemplo 1 — Rendimento domiciliar per capita médio e acesso a saneamento/internet por Grande Região (Python + DuckDB)
```python
import duckdb

con = duckdb.connect()
df = con.execute("""
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
""").fetchdf()
print(df)
```

### Exemplo 2 — Recorte rápido de um único estado (ex.: São Paulo, `uf = '35'`) cruzando cor/raça e escolaridade
```python
import duckdb

df_sp = duckdb.execute("""
    SELECT
        p0210_cor_raca_pessoa AS cor_raca,
        p0760_morador_nivel_instrucao_ensino AS nivel_instrucao,
        ROUND(SUM(p0111_peso_amostral_pessoa), 0) AS pessoas_estimadas
    FROM read_parquet('data/curated/pessoas_obt/uf=*/part.parquet', hive_partitioning=true)
    WHERE uf = '35' AND p0181_idade_anos >= 25
    GROUP BY ALL
    ORDER BY cor_raca, pessoas_estimadas DESC
""").fetchdf()
```
