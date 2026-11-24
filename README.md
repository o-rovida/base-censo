# `base-censo` — Pipeline Curado dos Microdados do Censo Demográfico 2022 (IBGE)

Pipeline reproduzível em **Python + DuckDB + Parquet** para curadoria, padronização, decodificação integral e validação dos **Microdados da Amostra do Censo Demográfico 2022 (Acesso Controlado — Brasil inteiro, 27 UFs)**.

> **Problema de pesquisa:** *"Quais características demográficas e domiciliares estão associadas às diferentes condições de vida da população brasileira?"*

---

## ⚠️ Aviso sobre Dados e Sigilo Estatístico

Este repositório contém **apenas o código do pipeline, a documentação oficial pública do IBGE e os relatórios agregados de metadados/validação**.  
Os microdados brutos (`.zip`) e os arquivos Parquet gerados (`data/curated/`) **não são versionados** (bloqueados via `.gitignore`), em respeito ao **Termo de Compromisso e Confidencialidade do Acesso Controlado (Nível 2)** do IBGE e aos limites de tamanho do GitHub.

Qualquer pesquisador com acesso autorizado ao arquivo oficial `microdados_censo_amostra_2022_csv_*.zip` pode colocar o `.zip` na raiz do projeto e reproduzir integralmente o dataset curado.

---

## Principais Resultados do Dataset Curado

| Tabela Curada (Parquet ZSTD) | Grão | Linhas (BR) | Colunas | Tamanho Final |
| :--- | :--- | ---: | :---: | ---: |
| `data/curated/pessoas_obt/uf=11..53/part.parquet` | 1 linha = 1 pessoa (`PESS` + `DOMI` + `FAMI`) | 21.539.579 | 172 + `uf` | 0,856 GiB |
| `data/curated/mortalidade/mortalidade.parquet` | 1 linha = 1 óbito (separada, sem agregação) | 430.965 | 20 | 6,24 MiB |

* **Redução de armazenamento:** de **11,11 GiB** em CSVs brutos para **~0,86 GiB** em Parquet ZSTD (~13x menor), mesmo com **100% das variáveis categóricas e descritores externos decodificados** no padrão `"código - descrição"` em coluna única.
* **Auditoria de integridade:** contagem 100% idêntica aos 108 CSVs originais, unicidade de chave primária verificada e população expandida (`SUM(p0111_peso_amostral_pessoa)`) igual a **203.080.756** (população oficial do Censo 2022).

---

## Documentação

* 📘 **[Dicionário de Dados e Modelagem](docs/DICIONARIO_E_MODELAGEM.md)** — grão, diagrama ER, regras obrigatórias de pesos amostrais e deduplicação para métricas domiciliares/familiares, catálogo completo das 172 + 20 variáveis 100% referenciadas no Layout oficial do IBGE e registro de colunas removidas.
* ⚙️ **[Documentação do Pipeline de Dados](docs/PIPELINE_DADOS.md)** — arquitetura, instruções de execução, métricas por UF e exemplos prontos de consulta em Python + DuckDB.

---

## Como Reproduzir

```powershell
# 1. Sincronizar ambiente virtual isolado (Python + DuckDB + PyArrow + openpyxl)
uv sync

# 2. Extrair metadados e descritores oficiais do IBGE -> metadata/schema_spec.json
.\.venv\Scripts\python.exe src\01_parse_metadata.py

# 3. Construir os Parquets curados (particionado por UF + mortalidade)
.\.venv\Scripts\python.exe src\02_build_curated_parquet.py

# 4. Rodar auditorias automatizadas de integridade, PK e pesos amostrais
.\.venv\Scripts\python.exe src\03_validate_dataset.py

# 5. Regerar documentação Markdown
.\.venv\Scripts\python.exe src\04_generate_docs.py
```
