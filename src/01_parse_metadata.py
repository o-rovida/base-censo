"""
01_parse_metadata.py
--------------------
Lê o layout oficial do IBGE (Layout_Microdados_CD2022_Controlado.xlsx) e os
Bancos de Descritores / Divisão Territorial em docs/ para gerar uma especificação
auditável (metadata/schema_spec.json) que dirige o pipeline Parquet e a documentação.
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any

import openpyxl

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"
LAYOUT_PATH = DOCS / "Layout_e_dicionario" / "Layout_Microdados_CD2022_Controlado.xlsx"
DESC_DIR = DOCS / "Bancos_de_descritores"
TERR_PATH = DOCS / "Divisao_territorial" / "Municipios_BR_Recortes_Territoriais_2022.xlsx"
OUT_PATH = ROOT / "metadata" / "schema_spec.json"

# Colunas redundantes em DOMI e FAMI que já existem integralmente em PESS
DOMI_REDUNDANT = {
    "D0010": "P0010",
    "D0020": "P0020",
    "D0030": "P0030",
    "D0040": "P0040",
    "D0050": "P0050",
    "D0060": "P0060",
    "D0070": "P0070",
    "D0080": "P0080",
    "D0090": "P0090",
    "D0100": "P0100",
    "D0120": "P0120",
    "D0130": "P0130",
    "D0140": "P0140",
}

FAMI_REDUNDANT = {
    "F0010": "P0010",
    "F0020": "P0020",
    "F0030": "P0030",
    "F0040": "P0040",
    "F0050": "P0050",
    "F0060": "P0060",
    "F0070": "P0070",
    "F0080": "P0080",
    "F0090": "P0090",
    "F0100": "P0100",
    "F0101": "P0115",
    "F0120": "P0120",
    "F0130": "P0130",
    "F0140": "P0140",
}

# Chaves puras (mantidas como VARCHAR sem decodificação para facilitar JOIN/deduplicação)
KEY_VARS = {
    "P0100", "P0101", "P0115",
    "D0100",
    "F0100", "F0101",
    "M0100", "M0101",
}

# Pesos amostrais (mantidos como DOUBLE)
WEIGHT_VARS = {"P0111", "D0111", "F0111", "M0111"}

# Mapeamento de variáveis para descritores externos
EXTERNAL_DESCRIPTOR_MAP = {
    # Geografia de residência (PESS e MORT)
    "P0020": "uf",
    "M0020": "uf",
    "P0030": "mesorregiao",
    "M0030": "mesorregiao",
    "P0040": "microrregiao",
    "M0040": "microrregiao",
    "P0050": "rgint",
    "M0050": "rgint",
    "P0060": "rgi",
    "M0060": "rgi",
    "P0070": "concentracao_urbana",
    "M0070": "concentracao_urbana",
    "P0080": "municipio",
    "M0080": "municipio",
    # UFs, Municípios e Países em migração, estudo e trabalho
    "P0490": "uf",
    "P0500": "municipio",
    "P0510": "pais",
    "P0570": "uf",
    "P0580": "municipio",
    "P0590": "pais",
    "P0610": "uf",
    "P0620": "municipio",
    "P0630": "pais",
    "P0810": "uf",
    "P0820": "municipio",
    "P0830": "pais",
    "P1130": "uf",
    "P1140": "municipio",
    "P1150": "pais",
    # Educação, Trabalho e Religião
    "P0750": "ensino_superior",
    "P0970": "ocupacao",
    "P0980": "atividade",
    "P0410": "religiao_p0410",
    "P0411": "religiao_p0411",
}

# Códigos puros alfanuméricos sem tabela de descritor textual
RAW_CODE_VARS = {"P0090", "M0090", "P1220"}

STOPWORDS = {
    "da", "do", "das", "dos", "de", "e", "em", "no", "na", "nos", "nas",
    "ou", "a", "o", "as", "os", "para", "pela", "pelo", "com", "por", "que",
    "ao", "aos", "se", "um", "uma", "em", "sobre", "entre",
}


def clean_text(s: Any) -> str:
    if s is None:
        return ""
    text = str(s).replace("\xa0", " ").replace("\r\n", "\n").replace("\r", "\n")
    return re.sub(r"[ \t]+", " ", text).strip()


def slugify_title(code: str, title: str) -> str:
    # Remove anotações entre colchetes como "[Para as famílias conviventes secundárias...]"
    t = re.sub(r"\[.*?\]", "", title)
    # Remove sufixos genéricos redundantes
    t = re.sub(r",?\s*(categoria|catogoria|código|codigo|número|numero)\s*\.?$", "", t, flags=re.IGNORECASE)
    # Normaliza acentos
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode("ascii").lower()
    tokens = re.findall(r"[a-z0-9]+", t)
    # Remove stopwords mantendo legibilidade
    filtered = [tok for tok in tokens if tok not in STOPWORDS]
    if not filtered:
        filtered = tokens
    # Limita tamanho para nomes práticos em SQL/Python
    slug = "_".join(filtered[:7])
    # Refinamentos específicos para clareza imediata
    overrides = {
        "P0010": "grande_regiao",
        "P0020": "uf",
        "P0030": "mesorregiao",
        "P0040": "microrregiao",
        "P0050": "regiao_intermediaria",
        "P0060": "regiao_imediata",
        "P0070": "concentracao_urbana",
        "P0080": "municipio",
        "P0090": "area_ponderacao",
        "P0100": "controle_domicilio",
        "P0101": "ordem_morador",
        "P0111": "peso_amostral_pessoa",
        "P0115": "ordem_familia",
        "P0150": "sexo_imputado_publico",
        "P0160": "sexo",
        "P0180": "idade_faixa",
        "P0181": "idade_anos",
        "P0190": "idade_meses",
        "P0380": "idade_ultimo_filho_faixa",
        "P0381": "idade_ultimo_filho_anos",
        "P0410": "religiao_grande_grupo",
        "P0411": "religiao_detalhada",
        "D0111": "peso_amostral_domicilio",
        "D0170": "sexo_responsavel_publico",
        "D0171": "sexo_responsavel",
        "D0180": "idade_responsavel_faixa",
        "D0181": "idade_responsavel_anos",
        "F0111": "peso_amostral_familia",
        "F0180": "idade_responsavel_familia_faixa",
        "F0181": "idade_responsavel_familia_anos",
        "M0010": "grande_regiao",
        "M0020": "uf",
        "M0030": "mesorregiao",
        "M0040": "microrregiao",
        "M0050": "regiao_intermediaria",
        "M0060": "regiao_imediata",
        "M0070": "concentracao_urbana",
        "M0080": "municipio",
        "M0090": "area_ponderacao",
        "M0100": "controle_domicilio",
        "M0101": "ordem_obito",
        "M0111": "peso_amostral_obito",
        "M0150": "ano_obito",
        "M0151": "mes_ano_obito",
        "M0160": "sexo_falecido",
        "M0170": "idade_falecido_faixa",
        "M0171": "idade_falecido_anos",
    }
    if code in overrides:
        slug = overrides[code]
    return f"{code.lower()}_{slug}"


def parse_categories_from_nome(raw_nome: str) -> tuple[str, dict[str, str]]:
    """Extrai (titulo, {codigo: descricao}) da célula NOME do layout oficial."""
    text = clean_text(raw_nome)
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
    if not lines:
        return "", {}
    title = lines[0]
    cats: dict[str, str] = {}

    # Caso especial: múltiplas categorias separadas por linha (padrão majoritário)
    body_lines = lines[1:]
    for ln in body_lines:
        # Ignora linhas explicativas como "Branco", "Não aplicável", "A relação de códigos..."
        if re.match(r"^(branco|a relação|obs|nota|para as)", ln, flags=re.IGNORECASE):
            continue
        # Padrão: "01 - Descrição" ou "1 - Descrição" ou "01- Descrição"
        m = re.match(r"^(\d{1,4})\s*[-–]\s*(.+)$", ln)
        if m:
            code, desc = m.group(1).strip(), m.group(2).strip()
            # Remove eventual "Branco" colado no fim da última categoria
            desc = re.sub(r"\s+Branco\s*$", "", desc, flags=re.IGNORECASE).strip()
            cats[code] = desc
            continue
        # Alguns itens como Grande Região ou UF podem vir concatenados na mesma linha
        inline_matches = list(re.finditer(r"(?:^|\s)(\d{1,2})\s*[-–]\s*([^\d]+?)(?=(?:\s+\d{1,2}\s*[-–])|$)", ln))
        if len(inline_matches) >= 2:
            for im in inline_matches:
                cats[im.group(1).strip()] = im.group(2).strip()
        elif cats:
            # Continuação da linha anterior de uma categoria
            last_key = list(cats.keys())[-1]
            if not re.match(r"^\[", ln):
                cats[last_key] = f"{cats[last_key]} {ln}".strip()

    # Fallback para linhas onde o próprio título traz "1- ... 2- ..." (ex: D0010/D0020)
    if not cats and len(lines) == 1:
        # Tenta separar se houver códigos inline
        pass

    return title, cats


def load_external_descriptors() -> dict[str, dict[str, str]]:
    desc: dict[str, dict[str, str]] = {
        "uf": {},
        "mesorregiao": {},
        "microrregiao": {},
        "rgi": {},
        "rgint": {},
        "concentracao_urbana": {},
        "municipio": {},
        "pais": {},
        "ocupacao": {},
        "atividade": {},
        "ensino_superior": {},
        "religiao_p0410": {},
        "religiao_p0411": {},
    }

    # 1. Divisão territorial (Municípios + Mesorregião + Microrregião + RGI + RGInt + ConcUrbana)
    wb = openpyxl.load_workbook(TERR_PATH, read_only=True, data_only=True)
    ws = wb["recortesCGEOparaCTD"]
    rows = ws.iter_rows(values_only=True)
    header = [clean_text(h) for h in next(rows)]
    idx = {h: i for i, h in enumerate(header)}
    for r in rows:
        if not r or not r[idx["CD_MUN"]]:
            continue
        def add_pair(bucket: str, k_col: str, v_col: str) -> None:
            k = clean_text(r[idx[k_col]])
            v = clean_text(r[idx[v_col]])
            if k and v and k.lower() != "none":
                desc[bucket][k] = v
        add_pair("municipio", "CD_MUN", "NM_MUN")
        add_pair("mesorregiao", "cod_meso", "nome_meso")
        add_pair("microrregiao", "cod_micro", "nome_micro")
        add_pair("rgi", "cod_RGI", "nome_rgi")
        add_pair("rgint", "cod_RGInt", "nome_rgint")
        add_pair("concentracao_urbana", "CodConcUrbana", "NomeConcUrbana")
    wb.close()

    # 2. Codificação Município (complementa e fornece sigla UF)
    uf_names = {
        "11": "Rondônia", "12": "Acre", "13": "Amazonas", "14": "Roraima",
        "15": "Pará", "16": "Amapá", "17": "Tocantins",
        "21": "Maranhão", "22": "Piauí", "23": "Ceará", "24": "Rio Grande do Norte",
        "25": "Paraíba", "26": "Pernambuco", "27": "Alagoas", "28": "Sergipe", "29": "Bahia",
        "31": "Minas Gerais", "32": "Espírito Santo", "33": "Rio de Janeiro", "35": "São Paulo",
        "41": "Paraná", "42": "Santa Catarina", "43": "Rio Grande do Sul",
        "50": "Mato Grosso do Sul", "51": "Mato Grosso", "52": "Goiás", "53": "Distrito Federal",
        "88": "Ignorado", "98": "Ignorado", "99": "Ignorado",
    }
    desc["uf"] = uf_names

    wb = openpyxl.load_workbook(DESC_DIR / "Codificacao_Municipio_CD2022.xlsx", read_only=True, data_only=True)
    ws = wb.active
    for r in ws.iter_rows(values_only=True):
        if r and len(r) >= 4 and r[2] is not None and re.match(r"^\d{7}$", clean_text(r[2])):
            code = clean_text(r[2])
            uf_sigla = clean_text(r[0])
            mun_name = clean_text(r[3])
            if mun_name:
                desc["municipio"][code] = f"{mun_name} ({uf_sigla})" if uf_sigla else mun_name
    wb.close()
    # Códigos especiais comuns em migração/trabalho/estudo
    desc["municipio"].setdefault("9999999", "Município ignorado")
    desc["municipio"].setdefault("8888888", "País estrangeiro")

    # 3. Codificação País
    wb = openpyxl.load_workbook(DESC_DIR / "Codificacao_Pais_CD2022.xlsx", read_only=True, data_only=True)
    ws = wb.active
    for r in ws.iter_rows(values_only=True):
        if r and len(r) >= 2 and r[0] is not None:
            code = clean_text(r[0])
            name = clean_text(r[1])
            if re.match(r"^\d+$", code) and name:
                desc["pais"][code] = name
    wb.close()

    # 4. Codificação Ocupação (COD 2022 - 4 dígitos na coluna 3)
    wb = openpyxl.load_workbook(DESC_DIR / "Codificacao_Ocupacao_CD2022.xlsx", read_only=True, data_only=True)
    ws = wb.active
    for r in ws.iter_rows(values_only=True):
        if not r or len(r) < 5:
            continue
        label = clean_text(r[4])
        if not label:
            continue
        # Família ocupacional (4 dígitos) está na col 3; zero-pad caso o Excel armazene como inteiro
        fam = clean_text(r[3])
        if fam and re.match(r"^\d{1,4}$", fam):
            desc["ocupacao"][fam.zfill(4)] = label
    wb.close()
    desc["ocupacao"].setdefault("0000", "OCUPACOES MALDEFINIDAS")

    # 5. Codificação Atividade (CNAE 2022 - 5 dígitos na col 2; zero-pad para códigos 00000..09999)
    wb = openpyxl.load_workbook(DESC_DIR / "Codificacao_Atividade_CD2022.xlsx", read_only=True, data_only=True)
    ws = wb.active
    for r in ws.iter_rows(values_only=True):
        if not r or len(r) < 4:
            continue
        label = clean_text(r[3])
        classe = clean_text(r[2])
        if classe and re.match(r"^\d{1,5}$", classe) and label:
            desc["atividade"][classe.zfill(5)] = label
    wb.close()

    # 6. Codificação Ensino Superior (4 dígitos na col 2 ou 3)
    wb = openpyxl.load_workbook(DESC_DIR / "Codificacao_Ensino_Superior_CD2022.xlsx", read_only=True, data_only=True)
    ws = wb.active
    for r in ws.iter_rows(values_only=True):
        if not r:
            continue
        for cell in r:
            txt = clean_text(cell)
            m = re.match(r"^(\d{4})\s*[-–]\s*(.+)$", txt)
            if m:
                desc["ensino_superior"][m.group(1)] = m.group(2).strip()
    wb.close()

    # 7. Codificação Religião (abas P0410 e P0411)
    wb = openpyxl.load_workbook(DESC_DIR / "Codificacao_Religiao_CD2022.xlsx", read_only=True, data_only=True)
    for sheet_name, target_key in [
        ("Grupos desagregados (P0410) ", "religiao_p0410"),
        ("Grupos desagregados (P0411)", "religiao_p0411"),
    ]:
        # Localiza aba tolerando espaços
        matched_sheet = next((s for s in wb.sheetnames if s.strip() == sheet_name.strip()), None)
        if not matched_sheet:
            continue
        ws = wb[matched_sheet]
        for r in ws.iter_rows(values_only=True):
            if r and len(r) >= 2 and r[0] is not None and r[1] is not None:
                c = clean_text(r[0])
                lbl = clean_text(r[1])
                if re.match(r"^\d{1,2}$", c) and lbl:
                    desc[target_key][c] = lbl
    wb.close()

    return desc


def parse_layout() -> dict[str, Any]:
    ext = load_external_descriptors()
    wb = openpyxl.load_workbook(LAYOUT_PATH, read_only=True, data_only=True)

    tables_raw: dict[str, list[dict[str, Any]]] = {}
    removed_flags: dict[str, list[dict[str, str]]] = {}

    for sheet_name in ["PESS", "DOMI", "FAMI", "MORT"]:
        ws = wb[sheet_name]
        tables_raw[sheet_name] = []
        removed_flags[sheet_name] = []
        for r in ws.iter_rows(values_only=True):
            if not r or not r[0]:
                continue
            code = clean_text(r[0])
            if re.match(r"^M[DFPM]\d{4}$", code):
                title, _ = parse_categories_from_nome(r[1] if len(r) > 1 else "")
                removed_flags[sheet_name].append({"code": code, "title": title})
                continue
            if not re.match(r"^[DFPM]\d{4}$", code):
                continue

            raw_nome = r[1] if len(r) > 1 else ""
            title, cats = parse_categories_from_nome(raw_nome)
            int_len = int(r[4]) if len(r) > 4 and r[4] not in (None, "") else 0
            dec_len = int(r[5]) if len(r) > 5 and r[5] not in (None, "") else 0
            ibge_type = clean_text(r[6]) if len(r) > 6 else ""

            # Determina papel, tipo DuckDB e dicionário de decodificação
            ext_key = EXTERNAL_DESCRIPTOR_MAP.get(code)
            value_map: dict[str, str] = {}
            source_desc = "Layout_Microdados_CD2022_Controlado.xlsx"

            if code in KEY_VARS:
                role = "key"
                duckdb_type = "VARCHAR"
            elif code in WEIGHT_VARS:
                role = "weight"
                duckdb_type = "DOUBLE"
            elif code in RAW_CODE_VARS:
                role = "code_raw"
                duckdb_type = "VARCHAR"
            elif ext_key:
                role = "categorical_external"
                duckdb_type = "VARCHAR"
                # Combina descritor externo com eventuais categorias extras do layout (ex: 99 - Ignorado)
                combined = dict(ext[ext_key])
                for ck, cv in cats.items():
                    combined.setdefault(ck, cv)
                value_map = {k: f"{k} - {v}" for k, v in combined.items()}
                source_desc = f"Descritor externo ({ext_key}) + Layout"
            elif ibge_type == "N":
                role = "numeric_float" if dec_len > 0 else "numeric_int"
                duckdb_type = "DOUBLE" if dec_len > 0 else "BIGINT"
            elif cats:
                role = "categorical_internal"
                duckdb_type = "VARCHAR"
                value_map = {k: f"{k} - {v}" for k, v in cats.items()}
            else:
                role = "code_raw"
                duckdb_type = "VARCHAR"

            tables_raw[sheet_name].append({
                "code": code,
                "source_table": sheet_name,
                "column_name": slugify_title(code, title),
                "ibge_title": title,
                "ibge_type": ibge_type,
                "ibge_int": int_len,
                "ibge_dec": dec_len,
                "role": role,
                "duckdb_type": duckdb_type,
                "external_descriptor": ext_key,
                "descriptor_source": source_desc,
                "value_map": value_map,
            })
    wb.close()

    # Monta especificação final das 2 tabelas curadas
    pessoas_obt_cols: list[dict[str, Any]] = []
    removed_redundant: list[dict[str, str]] = []

    for col in tables_raw["PESS"]:
        pessoas_obt_cols.append(col)

    for col in tables_raw["DOMI"]:
        if col["code"] in DOMI_REDUNDANT:
            removed_redundant.append({
                "code": col["code"],
                "source_table": "DOMI",
                "ibge_title": col["ibge_title"],
                "kept_equivalent": DOMI_REDUNDANT[col["code"]],
            })
        else:
            pessoas_obt_cols.append(col)

    for col in tables_raw["FAMI"]:
        if col["code"] in FAMI_REDUNDANT:
            removed_redundant.append({
                "code": col["code"],
                "source_table": "FAMI",
                "ibge_title": col["ibge_title"],
                "kept_equivalent": FAMI_REDUNDANT[col["code"]],
            })
        else:
            pessoas_obt_cols.append(col)

    # Garante unicidade absoluta dos nomes de coluna
    for table_name, cols in [("pessoas_obt", pessoas_obt_cols), ("mortalidade", tables_raw["MORT"])]:
        seen: dict[str, int] = {}
        for c in cols:
            nm = c["column_name"]
            if nm in seen:
                seen[nm] += 1
                c["column_name"] = f"{nm}_{seen[nm]}"
            else:
                seen[nm] = 1

    spec = {
        "source_zip": "microdados_censo_amostra_2022_csv_20261124_094334.zip",
        "layout_file": "docs/Layout_e_dicionario/Layout_Microdados_CD2022_Controlado.xlsx",
        "tables": {
            "pessoas_obt": {
                "grain": "1 linha = 1 pessoa recenseada na amostra do Censo Demográfico 2022",
                "primary_key": ["p0020_uf", "p0100_controle_domicilio", "p0101_ordem_morador"],
                "domicilio_key": ["p0020_uf", "p0100_controle_domicilio"],
                "familia_key": ["p0020_uf", "p0100_controle_domicilio", "p0115_ordem_familia"],
                "weights": {
                    "pessoa": "p0111_peso_amostral_pessoa",
                    "domicilio": "d0111_peso_amostral_domicilio",
                    "familia": "f0111_peso_amostral_familia",
                },
                "columns": pessoas_obt_cols,
            },
            "mortalidade": {
                "grain": "1 linha = 1 óbito de morador ocorrido no domicílio (janeiro/2019 a julho/2022)",
                "primary_key": ["m0020_uf", "m0100_controle_domicilio", "m0101_ordem_obito"],
                "domicilio_key": ["m0020_uf", "m0100_controle_domicilio"],
                "weights": {
                    "obito": "m0111_peso_amostral_obito",
                },
                "columns": tables_raw["MORT"],
            },
        },
        "removed_redundant": removed_redundant,
        "removed_flags": removed_flags,
    }
    return spec


def main() -> None:
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    spec = parse_layout()
    OUT_PATH.write_text(json.dumps(spec, ensure_ascii=False, indent=2), encoding="utf-8")

    for tname, tinfo in spec["tables"].items():
        cols = tinfo["columns"]
        by_role: dict[str, int] = {}
        for c in cols:
            by_role[c["role"]] = by_role.get(c["role"], 0) + 1
        print(f"=== {tname}: {len(cols)} colunas | roles={by_role} ===")
        # Alerta se alguma coluna tipo C ficou sem value_map
        missing_c = [c["code"] for c in cols if c["ibge_type"] == "C" and not c["value_map"]]
        if missing_c:
            print(f"  [ALERTA] Variaveis tipo C sem value_map: {missing_c}")

    total_flags = sum(len(v) for v in spec["removed_flags"].values())
    print(f"Removidas: {len(spec['removed_redundant'])} redundantes | {total_flags} flags de imputacao")
    print(f"Salvo em: {OUT_PATH}")


if __name__ == "__main__":
    main()
