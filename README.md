# `base-censo` — Pipeline Curado dos Microdados do Censo Demográfico 2022 (IBGE)

Pipeline reproduzível em **Python + DuckDB + Parquet** para curadoria, padronização, decodificação integral e validação dos **Microdados da Amostra do Censo Demográfico 2022 (Acesso Controlado — Brasil inteiro, 27 UFs)**.

> **Problema de pesquisa:** *"Quais características demográficas e domiciliares estão associadas às diferentes condições de vida da população brasileira?"*  
> **Créditos:** Pipeline e documentação elaborados com auxílio do **Google Antigravity**.

---

## ⚠️ Aviso sobre Dados e Sigilo Estatístico

Este repositório contém **apenas o código do pipeline, a documentação oficial pública do IBGE e os relatórios agregados de metadados/validação**.  
Os microdados brutos (`.zip`) e os arquivos Parquet gerados (`data/curated/`) **não são versionados** (bloqueados via `.gitignore`), em respeito ao **Termo de Compromisso e Confidencialidade do Acesso Controlado (Nível 2)** do IBGE e aos limites de tamanho do GitHub.

Qualquer pesquisador com acesso autorizado ao arquivo oficial `microdados_censo_amostra_2022_csv_*.zip` pode colocar o `.zip` na raiz do projeto e reproduzir integralmente o dataset curado.

---

## Principais Resultados do Dataset (Curated + Analytics)

| Camada / Tabela (Parquet ZSTD) | Grão | Linhas (BR) | Colunas | Tamanho Final |
| :--- | :--- | ---: | :---: | ---: |
| **Curated (Silver)** — `data/curated/pessoas_obt/uf=11..53/part.parquet` | 1 linha = 1 pessoa (`PESS` + `DOMI` + `FAMI`) | 21.539.579 | 172 + `uf` | 0,856 GiB |
| **Curated (Silver)** — `data/curated/mortalidade/mortalidade.parquet` | 1 linha = 1 óbito (separada, sem agregação) | 430.965 | 20 | 6,24 MiB |
| **Analytics (Gold)** — `data/analytics/censo2022_icv_acm.parquet` | 1 linha = 1 pessoa (domicílios particulares permanentes + `mort_*` agregada por domicílio) | 21.437.261 | 46 | 334,7 MiB |

* **Redução de armazenamento:** de **11,11 GiB** em CSVs brutos para **~0,86 GiB** na camada `curated` (~13x menor) e **334,7 MiB** na camada `analytics` (arquivo único pronto para R / Google Colab), com **100% das variáveis categóricas e descritores externos decodificados**.
* **Auditoria de integridade:** contagem 100% idêntica aos 108 CSVs originais na `curated`, igualdade exata antes/depois do `LEFT JOIN` com mortalidade agregada na `analytics`, unicidade de chave primária verificada e população expandida igual a **203.080.756** (total BR) / **202.112.874** (domicílios particulares permanentes ocupados).

---

## Documentação

* 📘 **[Dicionário de Dados e Modelagem (Curated)](docs/DICIONARIO_E_MODELAGEM.md)** — grão, diagrama ER, regras obrigatórias de pesos amostrais e deduplicação para métricas domiciliares/familiares, catálogo completo das 172 + 20 variáveis 100% referenciadas no Layout oficial do IBGE e registro de colunas removidas.
* 🎯 **[Modelagem da Camada Analytics — ICV + ACM (Gold)](docs/MODELAGEM_ANALYTICS_ICV_ACM.md)** — filtro de escopo, justificativa de todas as 46 colunas selecionadas e das 131 colunas descartadas, dicionário com `% NULL` e cardinalidade reais, regras de agregação/categorização de mortalidade (`mort_*`) e exemplos em R + DuckDB.
* ⚙️ **[Documentação do Pipeline de Dados](docs/PIPELINE_DADOS.md)** — arquitetura `raw → curated → analytics`, instruções de execução, métricas por UF e exemplos prontos de consulta em Python + DuckDB.

---

## Como Reproduzir

```powershell
# 1. Sincronizar ambiente virtual isolado (Python + DuckDB + PyArrow + openpyxl)
uv sync

# 2. Extrair metadados e descritores oficiais do IBGE -> metadata/schema_spec.json
.\.venv\Scripts\python.exe src\01_parse_metadata.py

# 3. Construir os Parquets curados (particionado por UF + mortalidade)
.\.venv\Scripts\python.exe src\02_build_curated_parquet.py

# 4. Rodar auditorias automatizadas de integridade, PK e pesos amostrais (curated)
.\.venv\Scripts\python.exe src\03_validate_dataset.py

# 5. Regerar documentação Markdown da camada curated
.\.venv\Scripts\python.exe src\04_generate_docs.py

# 6. Construir a camada Analytics (Gold) para ICV + ACM + docs/MODELAGEM_ANALYTICS_ICV_ACM.md
.\.venv\Scripts\python.exe src\05_build_analytics_icv_acm.py
```
