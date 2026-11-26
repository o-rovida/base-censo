"""
05_build_analytics_icv_acm.py
-----------------------------
Constrói a camada Analytics (Gold) para análise descritiva ponderada,
Índice de Condição de Vida (ICV) e Análise de Correspondência Múltipla (ACM)
a partir da camada curada (data/curated/), sem alterá-la.

Fluxo:
  1. Filtra data/curated/pessoas_obt/uf=*/part.parquet para domicílios particulares
     permanentes ocupados (mantendo todas as idades).
  2. Agrega data/curated/mortalidade/mortalidade.parquet no nível de domicílio
     (uf, controle_domicilio) ANTES do LEFT JOIN (garantindo ausência de produto cartesiano).
  3. Seleciona, renomeia semanticamente (<prefixo>_<nome>), unifica arranjo familiar
     (f0210 + f0220 com prefixo) e deriva flags determinísticas e categorias mort_*.
  4. Grava data/analytics/censo2022_icv_acm.parquet (arquivo único, ZSTD).
  5. Executa sanity checks automatizados e salva metadata/analytics_icv_acm_report.json.
  6. Gera docs/MODELAGEM_ANALYTICS_ICV_ACM.md com justificativas completas e métricas reais.

Uso:
  .\\.venv\\Scripts\\python.exe src\\05_build_analytics_icv_acm.py
"""

from __future__ import annotations

import json
import tempfile
import time
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "metadata" / "schema_spec.json"
CURATED_OBT_GLOB = str(ROOT / "data" / "curated" / "pessoas_obt" / "uf=*" / "part.parquet").replace("\\", "/")
CURATED_MORT_PATH = str(ROOT / "data" / "curated" / "mortalidade" / "mortalidade.parquet").replace("\\", "/")
OUT_PARQUET = ROOT / "data" / "analytics" / "censo2022_icv_acm.parquet"
OUT_REPORT = ROOT / "metadata" / "analytics_icv_acm_report.json"
OUT_DOC = ROOT / "docs" / "MODELAGEM_ANALYTICS_ICV_ACM.md"

SCOPE_FILTER_VALUE = "01 - Domicílio particular permanente ocupado"

# Catálogo estruturado das 47 colunas da camada Analytics (Gold)
# Cada item: (nome_analytics, bloco, origem_curated, codigo_ibge, tipo_esperado, justificativa)
ANALYTICS_COLUMNS: list[dict[str, str]] = [
    # Chaves (id_)
    {
        "name": "id_domicilio",
        "block": "id_",
        "source": "p0100_controle_domicilio",
        "ibge_code": "P0100",
        "type": "VARCHAR",
        "rationale": "Chave de identificação do domicílio dentro da UF; essencial para deduplicação em análises domiciliares e checagem de integridade.",
    },
    {
        "name": "id_morador",
        "block": "id_",
        "source": "p0101_ordem_morador",
        "ibge_code": "P0101",
        "type": "VARCHAR",
        "rationale": "Ordem do morador no domicílio; compõe a chave primária única (geo_uf, id_domicilio, id_morador).",
    },
    {
        "name": "id_familia",
        "block": "id_",
        "source": "p0115_ordem_familia",
        "ibge_code": "P0115",
        "type": "VARCHAR",
        "rationale": "Ordem da família no domicílio; permite deduplicação no grão família (NULL em domicílios unipessoais ou sem parentesco).",
    },
    # Pesos (peso_)
    {
        "name": "peso_pessoa",
        "block": "peso_",
        "source": "p0111_peso_amostral_pessoa",
        "ibge_code": "P0111",
        "type": "DOUBLE",
        "rationale": "Peso amostral obrigatório para inferência populacional no grão pessoa (Nota Metodológica NM 04/2026).",
    },
    {
        "name": "peso_domicilio",
        "block": "peso_",
        "source": "d0111_peso_amostral_domicilio",
        "ibge_code": "D0111",
        "type": "DOUBLE",
        "rationale": "Peso amostral obrigatório para inferência no grão domicílio (após deduplicação por geo_uf + id_domicilio).",
    },
    {
        "name": "peso_familia",
        "block": "peso_",
        "source": "f0111_peso_amostral_familia",
        "ibge_code": "F0111",
        "type": "DOUBLE",
        "rationale": "Peso amostral obrigatório para inferência no grão família (após deduplicação por geo_uf + id_domicilio + id_familia).",
    },
    # Geografia (geo_)
    {
        "name": "geo_regiao",
        "block": "geo_",
        "source": "p0010_grande_regiao",
        "ibge_code": "P0010",
        "type": "VARCHAR",
        "rationale": "Macrorregião geográfica (5 categorias); captura desigualdades estruturais regionais do Brasil na descritiva e na ACM.",
    },
    {
        "name": "geo_uf",
        "block": "geo_",
        "source": "p0020_uf",
        "ibge_code": "P0020",
        "type": "VARCHAR",
        "rationale": "Unidade da Federação (27 categorias); compõe a chave primária e permite estratificação estadual.",
    },
    # Demográficas (dem_)
    {
        "name": "dem_sexo",
        "block": "dem_",
        "source": "p0160_sexo",
        "ibge_code": "P0160",
        "type": "VARCHAR",
        "rationale": "Sexo biológico/declarado (sem imputação pública de P0150); eixo demográfico básico de estratificação.",
    },
    {
        "name": "dem_faixa_etaria",
        "block": "dem_",
        "source": "p0180_idade_faixa",
        "ibge_code": "P0180",
        "type": "VARCHAR",
        "rationale": "Faixas etárias oficiais do IBGE (18 categorias) como ponto de partida para recategorização etária no notebook.",
    },
    {
        "name": "dem_idade_anos",
        "block": "dem_",
        "source": "p0181_idade_anos",
        "ibge_code": "P0181",
        "type": "BIGINT",
        "rationale": "Idade exata em anos; permite construir faixas customizadas (ex.: ciclos de vida, 14+, 18+, 60+) no notebook sem perda de precisão.",
    },
    {
        "name": "dem_cor_raca",
        "block": "dem_",
        "source": "p0210_cor_raca_pessoa",
        "ibge_code": "P0210",
        "type": "VARCHAR",
        "rationale": "Autodeclaração de cor ou raça; marcador central de desigualdade socioeconômica e de condições de vida no Brasil.",
    },
    {
        "name": "dem_indigena",
        "block": "dem_",
        "source": "p0220_pessoa_indigena",
        "ibge_code": "P0220",
        "type": "VARCHAR",
        "rationale": "Identificação complementar de população indígena (incluindo quem se declara indígena por quesito específico além de cor/raça).",
    },
    {
        "name": "dem_quilombola",
        "block": "dem_",
        "source": "COALESCE(p0260_pessoa_quilombola, '2 - Não (fora de localidade quilombola)')",
        "ibge_code": "P0260",
        "type": "VARCHAR",
        "rationale": "Identificação de população quilombola (novidade do Censo 2022); NULL estrutural (90,3%, setores sem localidade quilombola mapeada) preenchido explicitamente para evitar falsos ausentes.",
    },
    {
        "name": "dem_estado_conjugal",
        "block": "dem_",
        "source": "p0280_convivencia_conjuge_companheiro_pessoa",
        "ibge_code": "P0280",
        "type": "VARCHAR",
        "rationale": "Convivência com cônjuge ou companheiro(a); associa-se à composição de renda e suporte intradomiciliar.",
    },
    {
        "name": "dem_migracao_nascimento",
        "block": "dem_",
        "source": "p0480_local_nascimento_pessoa",
        "ibge_code": "P0480",
        "type": "VARCHAR",
        "rationale": "Indicador sintético de naturalidade/migração (mesmo município, outro município, outro país) com baixa cardinalidade (4 cats.).",
    },
    {
        "name": "dem_deficiencia",
        "block": "dem_",
        "source": "p0470_variavel_indicadora_existencia_deficiencia",
        "ibge_code": "P0470",
        "type": "VARCHAR",
        "rationale": "Indicador consolidado de deficiência (evita redundância das 5 perguntas desagregadas P0420–P0460).",
    },
    {
        "name": "dem_condicao_domicilio",
        "block": "dem_",
        "source": "p0170_condicao_domicilio_pessoa",
        "ibge_code": "P0170",
        "type": "VARCHAR",
        "rationale": "Relação de parentesco/condição no domicílio (responsável, cônjuge, filho, etc.); permite análises de chefia e dependência.",
    },
    {
        "name": "dem_flag_responsavel",
        "block": "dem_",
        "source": "Derivada de p0170_condicao_domicilio_pessoa",
        "ibge_code": "P0170*",
        "type": "BOOLEAN",
        "rationale": "Flag determinística (TRUE quando P0170 = '01 - Pessoa responsável pelo domicílio') para facilitar recortes no nível de responsáveis/domicílios.",
    },
    # Domiciliares (dom_)
    {
        "name": "dom_situacao_urbano_rural",
        "block": "dom_",
        "source": "p0140_situacao_domicilio",
        "ibge_code": "P0140",
        "type": "VARCHAR",
        "rationale": "Situação urbana vs. rural; determinante estrutural do acesso a infraestrutura, saneamento e conectividade.",
    },
    {
        "name": "dom_tipo",
        "block": "dom_",
        "source": "d0200_tipo_especie",
        "ibge_code": "D0200",
        "type": "VARCHAR",
        "rationale": "Tipo do domicílio (casa, apartamento, cômodo, habitação em cortiço, etc.); reflete padrão construtivo e precariedade habitacional.",
    },
    {
        "name": "dom_condicao_ocupacao",
        "block": "dom_",
        "source": "d0190_condicao_ocupacao_domicilio",
        "ibge_code": "D0190",
        "type": "VARCHAR",
        "rationale": "Condição de posse/ocupação (próprio quitado, financiado, alugado, cedido); indicador de segurança habitacional.",
    },
    {
        "name": "dom_num_moradores",
        "block": "dom_",
        "source": "d0150_moradores_domicilio",
        "ibge_code": "D0150",
        "type": "BIGINT",
        "rationale": "Total de moradores no domicílio; permite identificar domicílios unipessoais (d0150 = 1) e tamanho domiciliar.",
    },
    {
        "name": "dom_arranjo_familiar",
        "block": "dom_",
        "source": "COALESCE(f0210, 'Convivente secundária - ' || f0220)",
        "ibge_code": "F0210+F0220",
        "type": "VARCHAR",
        "rationale": "Tipologia do arranjo familiar unificando famílias únicas/principais (F0210) e conviventes secundárias (F0220, 0% de sobreposição) com prefixo sem perda de informação.",
    },
    {
        "name": "dom_num_criancas",
        "block": "dom_",
        "source": "d0160_criancas_domicilio",
        "ibge_code": "D0160",
        "type": "BIGINT",
        "rationale": "Número de crianças no domicílio; proxy direta de razão de dependência infantil e pressão sobre renda domiciliar.",
    },
    {
        "name": "dom_material_paredes",
        "block": "dom_",
        "source": "d0210_material_paredes_domicilio",
        "ibge_code": "D0210",
        "type": "VARCHAR",
        "rationale": "Material predominante das paredes externas; indicador físico clássico de adequação/precariedade da moradia.",
    },
    # Condição de Vida (cv_)
    {
        "name": "cv_renda_per_capita",
        "block": "cv_",
        "source": "d0360_rendimento_domiciliar_per_capita",
        "ibge_code": "D0360",
        "type": "DOUBLE",
        "rationale": "Rendimento domiciliar per capita (em R$); principal dimensão monetária do ICV e referência para faixas de renda no notebook.",
    },
    {
        "name": "cv_condicao_trabalho_14mais",
        "block": "cv_",
        "source": "Unificação hierárquica de p0930 + p0950",
        "ibge_code": "P0930+P0950",
        "type": "VARCHAR",
        "rationale": "Condição de trabalho para 14+ anos ('1 - Ocupada', '2 - Desocupada', '3 - Fora da força de trabalho'; NULL apenas para < 14 anos), eliminando 55,9% de NULL e colinearidade sem perda de informação.",
    },
    {
        "name": "cv_escolaridade",
        "block": "cv_",
        "source": "p0770_morador_nivel_instrucao_ensino_compativel_censo_demografico",
        "ibge_code": "P0770",
        "type": "VARCHAR",
        "rationale": "Nível de instrução harmonizado (5 categorias); ideal para ACM e comparabilidade histórica sem fragmentação excessiva.",
    },
    {
        "name": "cv_anos_estudo",
        "block": "cv_",
        "source": "p0790_anos_estudo_pessoa",
        "ibge_code": "P0790",
        "type": "BIGINT",
        "rationale": "Anos completos de estudo (numérico); insumo contínuo de capital humano para o ICV e estatísticas descritivas.",
    },
    {
        "name": "cv_alfabetizacao",
        "block": "cv_",
        "source": "p0640_alfabetizacao_pessoa",
        "ibge_code": "P0640",
        "type": "VARCHAR",
        "rationale": "Condição de alfabetização (5+ anos); indicador básico de privação educacional.",
    },
    {
        "name": "cv_agua_abastecimento",
        "block": "cv_",
        "source": "d0260_abastecimento_agua_domicilio",
        "ibge_code": "D0260",
        "type": "VARCHAR",
        "rationale": "Forma principal de abastecimento de água; componente essencial da dimensão saneamento do ICV.",
    },
    {
        "name": "cv_agua_canalizada",
        "block": "cv_",
        "source": "d0300_existencia_agua_canalizada_domicilio",
        "ibge_code": "D0300",
        "type": "VARCHAR",
        "rationale": "Existência de água canalizada no domicílio/terreno; complementa a fonte de água com a qualidade do acesso intradomiciliar.",
    },
    {
        "name": "cv_esgoto",
        "block": "cv_",
        "source": "d0250_tipo_esgotamento_sanitario_domicilio",
        "ibge_code": "D0250",
        "type": "VARCHAR",
        "rationale": "Tipo de esgotamento sanitário; marcador crítico de saneamento básico e saúde ambiental.",
    },
    {
        "name": "cv_lixo",
        "block": "cv_",
        "source": "d0310_destino_lixo_domicilio",
        "ibge_code": "D0310",
        "type": "VARCHAR",
        "rationale": "Destino do lixo domiciliar; completa o tripé clássico de saneamento básico (água, esgoto, coleta de lixo).",
    },
    {
        "name": "cv_internet",
        "block": "cv_",
        "source": "d0330_acesso_internet_existencia",
        "ibge_code": "D0330",
        "type": "VARCHAR",
        "rationale": "Existência de acesso à internet no domicílio; indicador contemporâneo de inclusão digital e condições de vida.",
    },
    {
        "name": "cv_maquina_lavar",
        "block": "cv_",
        "source": "d0320_existencia_maquina_lavar_roupa_domicilio",
        "ibge_code": "D0320",
        "type": "VARCHAR",
        "rationale": "Existência de máquina de lavar roupa no domicílio (0% NULL, 62%/38%); único bem durável domiciliar além da internet no Censo 2022, forte discriminador para ICV e ACM.",
    },
    {
        "name": "cv_adensamento",
        "block": "cv_",
        "source": "d0240_numero_moradores_comodo_utilizado_como_dormitorio",
        "ibge_code": "D0240",
        "type": "DOUBLE",
        "rationale": "Razão moradores por dormitório; indicador padrão de adensamento excessivo habitacional (ex.: > 3 moradores/dormitório).",
    },
    {
        "name": "cv_banheiro_exclusivo",
        "block": "cv_",
        "source": "d0270_existencia_banheiro_sanitario_numero_banheiros_uso_exclusivo",
        "ibge_code": "D0270",
        "type": "VARCHAR",
        "rationale": "Existência de banheiro de uso exclusivo e quantidade; captura privação sanitária severa e padrão habitacional.",
    },
    # Mortalidade agregada por domicílio (mort_)
    {
        "name": "mort_qtd_obitos",
        "block": "mort_",
        "source": "Agregação de mortalidade.parquet (COUNT)",
        "ibge_code": "MORT*",
        "type": "BIGINT",
        "rationale": "Total de óbitos de moradores ocorridos no domicílio entre jan/2019 e jul/2022 (0 se sem óbito).",
    },
    {
        "name": "mort_idade_min_obito",
        "block": "mort_",
        "source": "Agregação de m0171_idade_falecido_anos (MIN)",
        "ibge_code": "M0171*",
        "type": "BIGINT",
        "rationale": "Menor idade entre os falecidos do domicílio (NULL se sem óbito ou idade ignorada); permite testar limiares alternativos no notebook.",
    },
    {
        "name": "mort_qtd_obitos_prematuros",
        "block": "mort_",
        "source": "Agregação de m0171_idade_falecido_anos (< 60)",
        "ibge_code": "M0171*",
        "type": "BIGINT",
        "rationale": "Quantidade de óbitos prematuros (< 60 anos) no domicílio (0 se sem óbito).",
    },
    {
        "name": "mort_qtd_obitos_pandemia",
        "block": "mort_",
        "source": "Agregação de m0150_ano_obito (2020–2021)",
        "ibge_code": "M0150*",
        "type": "BIGINT",
        "rationale": "Quantidade de óbitos ocorridos nos anos de pico pandêmico de COVID-19 (2020 e 2021) no domicílio (0 se sem óbito).",
    },
    {
        "name": "mort_ocorrencia_cat",
        "block": "mort_",
        "source": "Derivada de mort_qtd_obitos",
        "ibge_code": "MORT*",
        "type": "VARCHAR",
        "rationale": "Categórica pronta para descritiva/ACM: 'Sem óbito', '1 óbito', '2 ou mais óbitos'.",
    },
    {
        "name": "mort_perfil_etario_cat",
        "block": "mort_",
        "source": "Derivada de m0171_idade_falecido_anos",
        "ibge_code": "M0171*",
        "type": "VARCHAR",
        "rationale": "Perfil etário dos óbitos no domicílio: 'Sem óbito', 'Apenas idoso (60+)', 'Óbito prematuro (< 60 anos)' ou fallback 'Óbito com idade ignorada'.",
    },
    {
        "name": "mort_periodo_cat",
        "block": "mort_",
        "source": "Derivada de m0150_ano_obito",
        "ibge_code": "M0150*",
        "type": "VARCHAR",
        "rationale": "Período dos óbitos no domicílio: 'Sem óbito', 'Apenas fora do pico pandêmico (2019/2022)', 'Inclui período pandêmico (2020–2021)' ou fallback 'Óbito com ano ignorado'.",
    },
]

# Códigos IBGE de pessoas_obt efetivamente utilizados (incluindo P0130 usado no filtro de escopo e F0220 unificado em dom_arranjo_familiar)
USED_OBT_CODES = {
    "P0130",  # usado no filtro de escopo
    "P0100", "P0101", "P0115",
    "P0111", "D0111", "F0111",
    "P0010", "P0020",
    "P0160", "P0180", "P0181", "P0210", "P0220", "P0260", "P0280", "P0480", "P0470", "P0170",
    "P0140", "D0200", "D0190", "D0150", "F0210", "F0220", "D0160", "D0210",
    "D0360", "P0950", "P0930", "P0770", "P0790", "P0640",
    "D0260", "D0300", "D0250", "D0310", "D0320", "D0330", "D0240", "D0270",
}


def classify_discarded_column(col: dict[str, Any]) -> tuple[str, str]:
    """Classifica cada coluna descartada da pessoas_obt em um critério metodológico auditável."""
    code = col["code"]
    # 1. Geografia fina, chaves auxiliares e metadados de coleta
    if code in {"P0030", "P0040", "P0050", "P0060", "P0070", "P0080", "P0090", "P0120", "P0200", "P1210", "P1220", "F0150"}:
        return (
            "1. Geografia fina, chaves auxiliares e metadados de coleta",
            "Recortes subestaduais de alta cardinalidade (até 5.570 municípios) ou metadados operacionais da entrevista (informante, forma de declaração de idade) sem função direta no ICV/ACM nacional.",
        )
    # 2. Redundância e colinearidade com variáveis já selecionadas
    if code in {
        "P0150", "P0190", "P0290",
        "P0420", "P0430", "P0440", "P0450", "P0460",
        "P0760", "P0910", "P0920", "P0960",
        "P1070", "P1080", "P1090", "P1100", "P1110",
        "D0170", "D0171", "D0180", "D0181", "D0220", "D0230", "D0280", "D0290", "D0340", "D0350",
        "D0370", "D0380", "D0390", "D0400", "D0410",
        "F0160", "F0170", "F0180", "F0181", "F0190", "F0200", "F0230", "F0240", "F0250", "F0260", "F0270",
    }:
        return (
            "2. Redundância e colinearidade com variáveis selecionadas",
            "Informação já capturada de forma mais sintética, limpa ou menos colinear pelas colunas escolhidas (ex.: P0150 sexo público vs. P0160; P0420–P0460 deficiências específicas vs. indicador P0470; D0340 ocorrência de óbito vs. bloco agregado mort_*; D0290/D0280 vs. D0260/D0270; atributos do responsável/família já deriváveis de PESS+DOMI).",
        )
    # 3. Alta cardinalidade e/ou NA estrutural (aplicável apenas a subpopulações restritas)
    if code in {
        "P0230", "P0240", "P0250",
        "P0490", "P0500", "P0510", "P0520", "P0530", "P0540", "P0550", "P0560", "P0570", "P0580", "P0590",
        "P0600", "P0610", "P0620", "P0630",
        "P0650", "P0660", "P0670", "P0680", "P0690", "P0700", "P0710", "P0720", "P0730", "P0740", "P0750", "P0780",
        "P0800", "P0810", "P0820", "P0830",
        "P0840", "P0850", "P0860", "P0870", "P0880", "P0890", "P0900", "P0940",
        "P0970", "P0980", "P0990", "P1000", "P1010", "P1020", "P1030", "P1040", "P1050", "P1060",
        "P1120", "P1130", "P1140", "P1150", "P1160", "P1170", "P1180", "P1190",
    }:
        return (
            "3. Alta cardinalidade e/ou NA estrutural (subpopulações específicas)",
            "Quesitos condicionais aplicáveis apenas a frações específicas (migrantes, estudantes atuais, ocupados, indígenas, graduados) ou com dezenas/centenas de categorias (município de destino, CBO, CNAE, curso superior), incompatíveis com a estabilidade fatorial da ACM.",
        )
    # 4. Fora do escopo temático do problema de pesquisa (ICV + condições de vida)
    return (
        "4. Fora do escopo temático da pesquisa (ICV + condições de vida)",
        "Temas específicos não centrais ao recorte de condições de vida e habitação (fecundidade detalhada P0320–P0400, religião P0410–P0411, registro civil infantil P0270, coabitação com pais P0300–P0310, diagnóstico de autismo P1200).",
    )


def build_analytics_sql(out_parquet_sql: str) -> str:
    """Monta a query DuckDB que constrói data/analytics/censo2022_icv_acm.parquet."""
    return f"""
    COPY (
        WITH mort_dom AS (
            SELECT
                SUBSTRING(m0020_uf, 1, 2) AS uf_cod,
                m0100_controle_domicilio AS id_domicilio,
                COUNT(*)::BIGINT AS mort_qtd_obitos,
                MIN(m0171_idade_falecido_anos)::BIGINT AS mort_idade_min_obito,
                COUNT_IF(m0171_idade_falecido_anos < 60)::BIGINT AS mort_qtd_obitos_prematuros,
                COUNT_IF(m0171_idade_falecido_anos >= 60)::BIGINT AS _mort_qtd_obitos_idosos,
                COUNT_IF(m0150_ano_obito IN ('2 - 2020', '3 - 2021'))::BIGINT AS mort_qtd_obitos_pandemia,
                COUNT_IF(m0150_ano_obito IN ('1 - 2019', '4 - 2022'))::BIGINT AS _mort_qtd_obitos_fora_pandemia
            FROM read_parquet('{CURATED_MORT_PATH}')
            GROUP BY 1, 2
        ),
        pessoas_filtradas AS (
            SELECT *
            FROM read_parquet('{CURATED_OBT_GLOB}', hive_partitioning=true)
            WHERE p0130_especie_unidade_visitada = '{SCOPE_FILTER_VALUE}'
        )
        SELECT
            -- Chaves (id_)
            p.p0100_controle_domicilio AS id_domicilio,
            p.p0101_ordem_morador AS id_morador,
            p.p0115_ordem_familia AS id_familia,

            -- Pesos amostrais (peso_)
            p.p0111_peso_amostral_pessoa AS peso_pessoa,
            p.d0111_peso_amostral_domicilio AS peso_domicilio,
            p.f0111_peso_amostral_familia AS peso_familia,

            -- Geografia (geo_)
            p.p0010_grande_regiao AS geo_regiao,
            p.p0020_uf AS geo_uf,

            -- Demográficas (dem_)
            p.p0160_sexo AS dem_sexo,
            p.p0180_idade_faixa AS dem_faixa_etaria,
            p.p0181_idade_anos AS dem_idade_anos,
            p.p0210_cor_raca_pessoa AS dem_cor_raca,
            p.p0220_pessoa_indigena AS dem_indigena,
            COALESCE(p.p0260_pessoa_quilombola, '2 - Não (fora de localidade quilombola)') AS dem_quilombola,
            p.p0280_convivencia_conjuge_companheiro_pessoa AS dem_estado_conjugal,
            p.p0480_local_nascimento_pessoa AS dem_migracao_nascimento,
            p.p0470_variavel_indicadora_existencia_deficiencia AS dem_deficiencia,
            p.p0170_condicao_domicilio_pessoa AS dem_condicao_domicilio,
            COALESCE(p.p0170_condicao_domicilio_pessoa LIKE '01 -%', FALSE) AS dem_flag_responsavel,

            -- Domiciliares (dom_)
            p.p0140_situacao_domicilio AS dom_situacao_urbano_rural,
            p.d0200_tipo_especie AS dom_tipo,
            p.d0190_condicao_ocupacao_domicilio AS dom_condicao_ocupacao,
            p.d0150_moradores_domicilio AS dom_num_moradores,
            COALESCE(
                p.f0210_familia_unicas_conviventes_principais_tipologia,
                CASE
                    WHEN p.f0220_familias_conviventes_secundarias_tipologia IS NOT NULL
                    THEN 'Convivente secundária - ' || p.f0220_familias_conviventes_secundarias_tipologia
                END
            ) AS dom_arranjo_familiar,
            p.d0160_criancas_domicilio AS dom_num_criancas,
            p.d0210_material_paredes_domicilio AS dom_material_paredes,

            -- Condição de Vida (cv_)
            p.d0360_rendimento_domiciliar_per_capita AS cv_renda_per_capita,
            CASE
                WHEN p.p0950_pessoas_14_anos_mais_ocupada LIKE '1 -%' THEN '1 - Ocupada'
                WHEN p.p0950_pessoas_14_anos_mais_ocupada LIKE '0 -%' THEN '2 - Desocupada'
                WHEN p.p0930_pessoa_14_anos_mais_idade_forca_trabalho LIKE '0 -%' THEN '3 - Fora da força de trabalho'
                ELSE NULL
            END AS cv_condicao_trabalho_14mais,
            p.p0770_morador_nivel_instrucao_ensino_compativel_censo_demografico AS cv_escolaridade,
            p.p0790_anos_estudo_pessoa AS cv_anos_estudo,
            p.p0640_alfabetizacao_pessoa AS cv_alfabetizacao,
            p.d0260_abastecimento_agua_domicilio AS cv_agua_abastecimento,
            p.d0300_existencia_agua_canalizada_domicilio AS cv_agua_canalizada,
            p.d0250_tipo_esgotamento_sanitario_domicilio AS cv_esgoto,
            p.d0310_destino_lixo_domicilio AS cv_lixo,
            p.d0330_acesso_internet_existencia AS cv_internet,
            p.d0320_existencia_maquina_lavar_roupa_domicilio AS cv_maquina_lavar,
            p.d0240_numero_moradores_comodo_utilizado_como_dormitorio AS cv_adensamento,
            p.d0270_existencia_banheiro_sanitario_numero_banheiros_uso_exclusivo AS cv_banheiro_exclusivo,

            -- Mortalidade agregada por domicílio (mort_)
            COALESCE(m.mort_qtd_obitos, 0)::BIGINT AS mort_qtd_obitos,
            m.mort_idade_min_obito AS mort_idade_min_obito,
            COALESCE(m.mort_qtd_obitos_prematuros, 0)::BIGINT AS mort_qtd_obitos_prematuros,
            COALESCE(m.mort_qtd_obitos_pandemia, 0)::BIGINT AS mort_qtd_obitos_pandemia,
            CASE
                WHEN COALESCE(m.mort_qtd_obitos, 0) = 0 THEN 'Sem óbito'
                WHEN m.mort_qtd_obitos = 1 THEN '1 óbito'
                ELSE '2 ou mais óbitos'
            END AS mort_ocorrencia_cat,
            CASE
                WHEN COALESCE(m.mort_qtd_obitos, 0) = 0 THEN 'Sem óbito'
                WHEN m.mort_qtd_obitos_prematuros > 0 THEN 'Óbito prematuro (< 60 anos)'
                WHEN m._mort_qtd_obitos_idosos > 0 THEN 'Apenas idoso (60+)'
                ELSE 'Óbito com idade ignorada'
            END AS mort_perfil_etario_cat,
            CASE
                WHEN COALESCE(m.mort_qtd_obitos, 0) = 0 THEN 'Sem óbito'
                WHEN m.mort_qtd_obitos_pandemia > 0 THEN 'Inclui período pandêmico (2020–2021)'
                WHEN m._mort_qtd_obitos_fora_pandemia > 0 THEN 'Apenas fora do pico pandêmico (2019/2022)'
                ELSE 'Óbito com ano ignorado'
            END AS mort_periodo_cat
        FROM pessoas_filtradas AS p
        LEFT JOIN mort_dom AS m
            ON CAST(p.uf AS VARCHAR) = m.uf_cod
           AND p.p0100_controle_domicilio = m.id_domicilio
    ) TO '{out_parquet_sql}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000);
    """


def run_sanity_checks(con: duckdb.DuckDBPyConnection, out_parquet_sql: str, rows_before_join: int) -> dict[str, Any]:
    """Executa auditorias de integridade, expansão amostral, NULLs e distribuições mort_*."""
    # 1. Contagem e unicidade da chave primária
    pk_row = con.execute(
        f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT (geo_uf || '|' || id_domicilio || '|' || id_morador)) AS distinct_pk
        FROM read_parquet('{out_parquet_sql}')
        """
    ).fetchone()
    rows_after_join = int(pk_row[0])
    distinct_pk = int(pk_row[1])

    assert rows_after_join == rows_before_join, (
        f"ERRO CRÍTICO: Contagem divergente após LEFT JOIN com mortalidade! "
        f"Antes={rows_before_join:,} vs Depois={rows_after_join:,}"
    )
    assert rows_after_join == distinct_pk, (
        f"ERRO CRÍTICO: Chave primária (geo_uf, id_domicilio, id_morador) não é única! "
        f"Linhas={rows_after_join:,} vs Distintas={distinct_pk:,}"
    )

    # 2. Expansão amostral (pessoas, domicílios, famílias)
    pop_exp = float(
        con.execute(f"SELECT SUM(peso_pessoa) FROM read_parquet('{out_parquet_sql}')").fetchone()[0]
    )
    dom_row = con.execute(
        f"""
        SELECT COUNT(*) AS dom_amostra, SUM(peso_dom) AS dom_expandidos
        FROM (
            SELECT geo_uf, id_domicilio, ANY_VALUE(peso_domicilio) AS peso_dom
            FROM read_parquet('{out_parquet_sql}')
            GROUP BY geo_uf, id_domicilio
        )
        """
    ).fetchone()
    fam_row = con.execute(
        f"""
        SELECT COUNT(*) AS fam_amostra, SUM(peso_fam) AS fam_expandidas
        FROM (
            SELECT geo_uf, id_domicilio, id_familia, ANY_VALUE(peso_familia) AS peso_fam
            FROM read_parquet('{out_parquet_sql}')
            WHERE id_familia IS NOT NULL
            GROUP BY geo_uf, id_domicilio, id_familia
        )
        """
    ).fetchone()

    # 3. Perfil de cada coluna (% NULL e cardinalidade) em uma única passada
    col_names = [c["name"] for c in ANALYTICS_COLUMNS]
    null_exprs = [f'COUNT_IF("{cn}" IS NULL) AS "null_{cn}"' for cn in col_names]
    card_exprs = [f'COUNT(DISTINCT "{cn}") AS "card_{cn}"' for cn in col_names]

    null_counts = con.execute(f"SELECT {', '.join(null_exprs)} FROM read_parquet('{out_parquet_sql}')").fetchone()
    card_counts = con.execute(f"SELECT {', '.join(card_exprs)} FROM read_parquet('{out_parquet_sql}')").fetchone()

    schema_types = dict(
        con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out_parquet_sql}')").fetchall()[i][:2]
        for i in range(len(col_names))
    )

    column_profiles: list[dict[str, Any]] = []
    for cn, n_null, n_card in zip(col_names, null_counts, card_counts):
        column_profiles.append({
            "column": cn,
            "parquet_type": schema_types.get(cn, ""),
            "null_rows": int(n_null),
            "null_pct": round(100.0 * int(n_null) / rows_after_join, 4),
            "cardinality": int(n_card),
        })

    # 4. Distribuição ponderada das colunas mort_* (grão pessoa e grão domicílio)
    mort_distributions: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for mcol in ("mort_ocorrencia_cat", "mort_perfil_etario_cat", "mort_periodo_cat"):
        p_rows = con.execute(
            f"""
            SELECT
                "{mcol}" AS cat,
                COUNT(*) AS n_amostra,
                SUM(peso_pessoa) AS pop_estimada,
                100.0 * SUM(peso_pessoa) / SUM(SUM(peso_pessoa)) OVER () AS pct_pop
            FROM read_parquet('{out_parquet_sql}')
            GROUP BY 1
            ORDER BY pop_estimada DESC
            """
        ).fetchall()

        d_rows = con.execute(
            f"""
            WITH doms AS (
                SELECT
                    geo_uf,
                    id_domicilio,
                    ANY_VALUE(peso_domicilio) AS peso_dom,
                    ANY_VALUE("{mcol}") AS cat
                FROM read_parquet('{out_parquet_sql}')
                GROUP BY geo_uf, id_domicilio
            )
            SELECT
                cat,
                COUNT(*) AS n_amostra,
                SUM(peso_dom) AS dom_estimados,
                100.0 * SUM(peso_dom) / SUM(SUM(peso_dom)) OVER () AS pct_dom
            FROM doms
            GROUP BY 1
            ORDER BY dom_estimados DESC
            """
        ).fetchall()

        mort_distributions[mcol] = {
            "pessoas": [
                {
                    "category": r[0],
                    "n_amostra": int(r[1]),
                    "pop_estimada": round(float(r[2]), 0),
                    "pct": round(float(r[3]), 4),
                }
                for r in p_rows
            ],
            "domicilios": [
                {
                    "category": r[0],
                    "n_amostra": int(r[1]),
                    "dom_estimados": round(float(r[2]), 0),
                    "pct": round(float(r[3]), 4),
                }
                for r in d_rows
            ],
        }

    # Verifica zero NULLs nas categóricas mort_*
    for mcol in ("mort_ocorrencia_cat", "mort_perfil_etario_cat", "mort_periodo_cat"):
        prof = next(p for p in column_profiles if p["column"] == mcol)
        assert prof["null_rows"] == 0, f"ERRO: {mcol} apresentou {prof['null_rows']} NULLs indevidos!"

    return {
        "rows_before_join": rows_before_join,
        "rows_after_join": rows_after_join,
        "join_row_count_match": rows_before_join == rows_after_join,
        "pk_unique": rows_after_join == distinct_pk,
        "sample_expansion": {
            "populacao_expandida_pessoas": round(pop_exp, 2),
            "domicilios_amostra_unicos": int(dom_row[0]),
            "domicilios_expandidos": round(float(dom_row[1]), 2),
            "familias_amostra_unicas": int(fam_row[0]),
            "familias_expandidas": round(float(fam_row[1]), 2),
        },
        "column_profiles": column_profiles,
        "mort_distributions": mort_distributions,
    }


def generate_analytics_markdown(spec: dict[str, Any], report: dict[str, Any]) -> str:
    """Gera docs/MODELAGEM_ANALYTICS_ICV_ACM.md com justificativas e métricas reais."""
    exp = report["sample_expansion"]
    prof_map = {p["column"]: p for p in report["column_profiles"]}
    scope = report["scope_summary"]

    # Tabela do dicionário das 47 colunas
    dict_lines = [
        "| # | Coluna Analytics | Bloco | Origem (`curated`) | Código IBGE | Tipo | % `NULL` | Cardinalidade | Justificativa de Inclusão |",
        "| :---: | :--- | :---: | :--- | :---: | :---: | ---: | ---: | :--- |",
    ]
    for i, col in enumerate(ANALYTICS_COLUMNS, 1):
        p = prof_map[col["name"]]
        dict_lines.append(
            f"| {i} | `{col['name']}` | `{col['block']}` | `{col['source']}` | `{col['ibge_code']}` | "
            f"`{p['parquet_type']}` | {p['null_pct']:.2f}% | {p['cardinality']:,} | {col['rationale']} |"
        )

    # Agrupamento auditável das colunas descartadas de pessoas_obt
    discarded_groups: dict[str, dict[str, Any]] = {}
    discarded_total = 0
    for col in spec["tables"]["pessoas_obt"]["columns"]:
        if col["code"] in USED_OBT_CODES:
            continue
        discarded_total += 1
        grp_name, grp_desc = classify_discarded_column(col)
        if grp_name not in discarded_groups:
            discarded_groups[grp_name] = {"desc": grp_desc, "cols": []}
        discarded_groups[grp_name]["cols"].append(col)

    discarded_sections: list[str] = []
    for grp_name in sorted(discarded_groups.keys()):
        g = discarded_groups[grp_name]
        codes_str = ", ".join(f"`{c['code']}`" for c in g["cols"])
        discarded_sections.append(
            f"### {grp_name} ({len(g['cols'])} colunas)\n"
            f"{g['desc']}\n\n"
            f"* **Códigos IBGE descartados:** {codes_str}\n"
        )

    # Tabelas de distribuição ponderada de mort_*
    mort_tables: list[str] = []
    mort_titles = {
        "mort_ocorrencia_cat": "Ocorrência de óbitos no domicílio (`mort_ocorrencia_cat`)",
        "mort_perfil_etario_cat": "Perfil etário dos óbitos no domicílio (`mort_perfil_etario_cat`)",
        "mort_periodo_cat": "Período dos óbitos no domicílio (`mort_periodo_cat`)",
    }
    for mcol, title in mort_titles.items():
        p_map = {x["category"]: x for x in report["mort_distributions"][mcol]["pessoas"]}
        d_map = {x["category"]: x for x in report["mort_distributions"][mcol]["domicilios"]}
        cats = [x["category"] for x in report["mort_distributions"][mcol]["domicilios"]]
        lines = [
            f"#### {title}",
            "| Categoria | Domicílios (Amostra) | Domicílios Expandidos | % Domicílios | Pessoas Expandidas | % Pessoas |",
            "| :--- | ---: | ---: | ---: | ---: | ---: |",
        ]
        for cat in cats:
            d = d_map[cat]
            p = p_map[cat]
            lines.append(
                f"| `{cat}` | {d['n_amostra']:,} | {d['dom_estimados']:,.0f} | {d['pct']:.2f}% | "
                f"{p['pop_estimada']:,.0f} | {p['pct']:.2f}% |"
            )
        mort_tables.append("\n".join(lines))

    return f"""# Modelagem da Camada Analytics (Gold) — ICV + ACM (Censo Demográfico 2022)

> **Problema de pesquisa:** *"Quais características demográficas e domiciliares estão associadas às diferentes condições de vida da população brasileira?"*  
> **Créditos:** Pipeline e documentação elaborados com auxílio do **Google Antigravity**.  
> **Artefato gerado:** `data/analytics/censo2022_icv_acm.parquet` (**{report['output_size_mb']:.1f} MiB**, Parquet ZSTD, arquivo único).  
> **Script produtor:** [`src/05_build_analytics_icv_acm.py`](../src/05_build_analytics_icv_acm.py) | **Telemetria/Auditoria:** [`metadata/analytics_icv_acm_report.json`](../metadata/analytics_icv_acm_report.json).

---

## 1. Visão Geral e Arquitetura (`raw` → `curated` → `analytics`)

A arquitetura do projeto segue o padrão em **3 camadas imutáveis**, garantindo rastreabilidade até os arquivos oficiais do IBGE sem alterar a camada curada:

```mermaid
flowchart LR
    RAW["RAW (Bronze) - .zip IBGE 108 CSVs (11,11 GiB)"] -->|"01 + 02 + 03"| CUR["CURATED (Silver) - pessoas_obt + mortalidade (0,86 GiB)"]
    CUR -->|"05_build_analytics_icv_acm.py (DuckDB)"| GOLD["ANALYTICS (Gold) - censo2022_icv_acm.parquet ({report['output_size_mb']:.1f} MiB | {len(ANALYTICS_COLUMNS)} cols)"]
    GOLD -->|"arrow / duckdb"| NB["Notebook R / Colab (Descritiva + ICV + ACM)"]
```

### Fronteira de Responsabilidades (Pipeline vs. Notebook)
* **No pipeline (`05_build_analytics_icv_acm.py`):** filtro de escopo populacional, seleção enxuta de colunas, renomeação semântica padronizada (`<prefixo>_<nome>`), unificações estruturais determinísticas sem perda de informação (`F0210 + F0220`, `P0930 + P0950` e `NULL` estrutural de `P0260`), agregação e categorização de mortalidade por domicílio e validação de integridade.
* **No notebook (R / Google Colab):** recategorizações analíticas para a ACM (ex.: agrupamento de faixas etárias, arranjos familiares, faixas de renda), tratamento/fusão de categorias raras (< 1%), definição de pesos/escores e construção do Índice de Condição de Vida (ICV).

### Resumo Quantitativo e Sanity Checks Validados
| Métrica | Valor Validado |
| :--- | ---: |
| Linhas `pessoas_obt` (camada `curated` total) | {scope['curated_total_rows']:,} |
| Linhas excluídas pelo filtro de escopo (coletivos + improvisados) | {scope['excluded_rows']:,} ({scope['excluded_pct']:.2f}%) |
| **Linhas `censo2022_icv_acm.parquet` (antes == depois do JOIN mortalidade)** | **{report['rows_after_join']:,}** (`match={report['join_row_count_match']}`) |
| Unicidade da chave primária `(geo_uf, id_domicilio, id_morador)` | `{report['pk_unique']}` |
| População expandida (`SUM(peso_pessoa)`) | **{exp['populacao_expandida_pessoas']:,.0f}** |
| Domicílios únicos na amostra / expandidos (`SUM(peso_domicilio)`) | {exp['domicilios_amostra_unicos']:,} / **{exp['domicilios_expandidos']:,.0f}** |
| Famílias únicas na amostra / expandidas (`SUM(peso_familia)`) | {exp['familias_amostra_unicas']:,} / **{exp['familias_expandidas']:,.0f}** |
| Total de colunas | **{len(ANALYTICS_COLUMNS)} colunas** |
| Tamanho do arquivo único (Parquet ZSTD) | **{report['output_size_mb']:.1f} MiB** |
| Tempo total de construção + auditorias | {report['elapsed_seconds']:.1f} s |

---

## 2. Justificativa do Filtro de Escopo (Linhas)

### 2.1 Por que manter apenas `p0130 = '01 - Domicílio particular permanente ocupado'`?
* Os **domicílios coletivos com morador** (`06`, {scope['coletivos_rows']:,} registros na amostra) — como presídios, asilos, quartéis, hotéis e alojamentos — **não possuem questionário de características domiciliares** (esgoto, água, paredes, rendimento domiciliar per capita, condição de ocupação, família), gerando ~100% de `NULL` estrutural nos blocos `dom_` e `cv_`.
* Os **domicílios particulares improvisados ocupados** (`05`, {scope['improvisados_rows']:,} registros na amostra, ~0,07% da população) — como barracas, veículos e imóveis não residenciais — também possuem preenchimento parcial no questionário domiciliar e constituem categoria extremamente rara na ACM.
* Restringir o escopo aos **domicílios particulares permanentes ocupados** ({scope['kept_pct']:.2f}% dos registros; {exp['populacao_expandida_pessoas']:,.0f} habitantes) segue o padrão metodológico do IBGE para indicadores de condições de vida e habitação, eliminando `NULL`s estruturais artificiais.

### 2.2 Por que manter **todas as idades** (sem filtrar 18+ ou responsáveis)?
* O problema de pesquisa investiga as condições de vida da **população brasileira** como um todo — crianças e idosos são grupos centrais na avaliação de vulnerabilidade habitacional, saneamento, adensamento e renda per capita.
* Variáveis específicas de faixas etárias (como `cv_condicao_trabalho_14mais`, aplicável a 14+ anos, ou `cv_alfabetizacao`, 5+ anos) preservam `NULL` nativo fora da faixa elegível, permitindo ao notebook decidir livremente sub-recortes (ex.: `dem_idade_anos >= 18` ou `dem_flag_responsavel = TRUE`) sem precisar reconstruir o pipeline.

---

## 3. Justificativa da Seleção e Exclusão de Colunas

Das **172 colunas** da tabela curada `pessoas_obt`, foram utilizadas **41 colunas de origem** (36 mapeadas 1:1 + `F0210`/`F0220` unificadas em `dom_arranjo_familiar` + `P0930`/`P0950` unificadas em `cv_condicao_trabalho_14mais` + `P0130` aplicada no filtro de escopo, além da flag derivada `dem_flag_responsavel`), somadas às **7 colunas agregadas/categorizadas de `mortalidade.parquet`**, totalizando **{len(ANALYTICS_COLUMNS)} colunas** organizadas em 7 prefixos semânticos (`id_`, `peso_`, `geo_`, `dem_`, `dom_`, `cv_`, `mort_`).

### Tratamentos estruturais determinísticos (sem perda de informação)
1. **`dom_arranjo_familiar` (`F0210 + F0220`):** no layout oficial do IBGE, a tipologia familiar é dividida em duas colunas **100% mutuamente exclusivas** (0 registros com ambas preenchidas): `F0210` (famílias únicas e conviventes principais — 88,4% da população) e `F0220` (famílias conviventes secundárias — 4,4% da população). Unificá-las com o prefixo `"Convivente secundária - "` para `F0220` evita perda de informação e elimina `NULL`s artificiais. Os `NULL`s remanescentes (~6,9%) correspondem estruturalmente a domicílios unipessoais (`dom_num_moradores = 1`, ~6,7%) e domicílios sem laço familiar (~0,5%).
2. **`cv_condicao_trabalho_14mais` (`P0930 + P0950`):** nos microdados, `P0950` (ocupação) só é preenchida para quem está na força de trabalho (`P0930 = 1`), deixando 55,9% de `NULL` quando mantida separada (misturando crianças `< 14` com adultos fora da força). A unificação hierárquica em `'1 - Ocupada'`, `'2 - Desocupada'` e `'3 - Fora da força de trabalho'` preserva 100% da informação, elimina redundância na ACM e restringe o `NULL` estritamente a menores de 14 anos (19,11%).
3. **`dem_quilombola` (`P0260`):** no Censo 2022, o quesito quilombola só foi aplicado em setores com localidades quilombolas mapeadas, resultando em 90,3% de `NULL` estrutural. O preenchimento determinístico com `'2 - Não (fora de localidade quilombola)'` zera os `NULL`s e evita distorções em tabelas descritivas e na ACM.
4. **`cv_maquina_lavar` (`D0320`):** incluída por ser o único bem durável domiciliar disponível no Censo 2022 além da internet (`cv_internet`), com 0% de `NULL` e excelente equilíbrio de classes (~62% Sim / ~38% Não) para o ICV e a ACM.

### Colunas descartadas da `pessoas_obt` ({discarded_total} colunas agrupadas por critério)

{chr(10).join(discarded_sections)}

---

## 4. Dicionário de Dados da Tabela Analytics (`{len(ANALYTICS_COLUMNS)}` colunas)

{chr(10).join(dict_lines)}

---

## 5. Justificativa Metodológica da Agregação e Categorização de Mortalidade (`mort_`)

### 5.1 Por que agregar `mortalidade.parquet` por domicílio ANTES do `LEFT JOIN`?
Na camada curada, `mortalidade.parquet` tem grão **1 linha = 1 óbito** (430.965 óbitos em 412.427 domicílios; 17.551 domicílios registraram 2 ou mais óbitos). Um `LEFT JOIN` direto com `pessoas_obt` multiplicaria as linhas dos moradores desses domicílios (**produto cartesiano**), inflando a população expandida e distorcendo pesos amostrais.

A agregação prévia em CTE por `(SUBSTRING(m0020_uf, 1, 2), m0100_controle_domicilio)` garante relação **N:1 estrita** (`pessoas_obt` → `mort_dom`), verificada automaticamente pelo `assert` de igualdade de linhas antes e depois do join (`{report['rows_before_join']:,} == {report['rows_after_join']:,}`).

### 5.2 Regras de Construção, Fallbacks e Cuidados para a ACM
* **Domicílios sem óbito:** preenchidos explicitamente com `0` nas contagens numéricas e `'Sem óbito'` nas categóricas (0% de `NULL` nas 3 categóricas `mort_*`). Apenas `mort_idade_min_obito` permanece `NULL` quando não houve óbito (ou quando a idade do falecido foi ignorada), pois `0` significaria óbito de menor de 1 ano.
* **Regra de precedência e fallbacks auditados nos dados reais:**
  * `mort_perfil_etario_cat`: se houve ao menos 1 óbito com `m0171_idade_falecido_anos < 60`, classifica-se como `'Óbito prematuro (< 60 anos)'`; senão, se houve óbito com idade `>= 60`, classifica-se como `'Apenas idoso (60+)'`; se todos os óbitos do domicílio tiveram idade `NULL`, recebe o fallback explícito `'Óbito com idade ignorada'`.
  * `mort_periodo_cat`: se houve ao menos 1 óbito em `'2 - 2020'` ou `'3 - 2021'` (pico da pandemia de COVID-19), classifica-se como `'Inclui período pandêmico (2020–2021)'`; senão, se houve óbito em `'1 - 2019'` ou `'4 - 2022'`, classifica-se como `'Apenas fora do pico pandêmico (2019/2022)'`; se todos os óbitos tiveram `'9 - Ignorado'`, recebe o fallback `'Óbito com ano ignorado'`.
* **Orientação importante para a ACM (no notebook):**
  1. **Evitar colinearidade do bloco `"Sem óbito"`:** como ~95% dos domicílios pertencem à categoria `'Sem óbito'`, **não inclua as 3 categóricas `mort_*` simultaneamente como variáveis ativas** na ACM. Escolha **uma** variável temática por modelo (ex.: `mort_perfil_etario_cat`) ou projete as demais como **variáveis suplementares (`quali.sup`)** e as contagens numéricas como `quanti.sup`.
  2. **Categorias raras de fallback:** `'Óbito com idade ignorada'` e `'Óbito com ano ignorado'` representam `< 0,02%` da base; no notebook, filtre-as ou agrupe-as na etapa de tratamento de categorias raras antes de ajustar a ACM.
  3. **Limitação amostral do período:** o Censo 2022 registrou óbitos ocorridos entre **janeiro/2019 e julho/2022** (3,5 anos) **apenas em domicílios que continuaram existindo** na data de referência (óbitos em domicílios unipessoais que se extinguiram não são captados pela pesquisa).

### 5.3 Distribuições Ponderadas Validadas (`mort_*`)

{(chr(10) * 2).join(mort_tables)}

---

## 6. Exemplo de Leitura Rápida em R (Colab) com `duckdb` / `arrow`

```r
library(duckdb)
library(DBI)

con <- dbConnect(duckdb::duckdb())

# Exemplo 1: Distribuição ponderada de mortalidade por Grande Região (grão pessoa)
res_pessoas <- dbGetQuery(con, "
  SELECT
    geo_regiao,
    mort_perfil_etario_cat,
    ROUND(SUM(peso_pessoa), 0) AS pop_estimada,
    ROUND(100.0 * SUM(peso_pessoa) / SUM(SUM(peso_pessoa)) OVER (PARTITION BY geo_regiao), 2) AS pct_regiao
  FROM read_parquet('data/analytics/censo2022_icv_acm.parquet')
  GROUP BY geo_regiao, mort_perfil_etario_cat
  ORDER BY geo_regiao, pop_estimada DESC
")

# Exemplo 2: Métrica no grão DOMICÍLIO (sempre deduplicar por geo_uf + id_domicilio)
res_domicilios <- dbGetQuery(con, "
  WITH dom AS (
    SELECT
      geo_uf,
      id_domicilio,
      ANY_VALUE(peso_domicilio) AS peso_dom,
      ANY_VALUE(cv_esgoto) AS esgoto,
      ANY_VALUE(mort_ocorrencia_cat) AS mort_cat
    FROM read_parquet('data/analytics/censo2022_icv_acm.parquet')
    GROUP BY geo_uf, id_domicilio
  )
  SELECT
    mort_cat,
    ROUND(SUM(peso_dom), 0) AS domicilios_estimados
  FROM dom
  GROUP BY mort_cat
  ORDER BY domicilios_estimados DESC
")
```
"""


def main() -> None:
    t0 = time.perf_counter()
    spec = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    OUT_REPORT.parent.mkdir(parents=True, exist_ok=True)
    OUT_DOC.parent.mkdir(parents=True, exist_ok=True)

    out_parquet_sql = str(OUT_PARQUET).replace("\\", "/")

    with tempfile.TemporaryDirectory(prefix="censo2022_analytics_") as tmp_dir:
        tmp_dir_sql = tmp_dir.replace("\\", "/")
        con = duckdb.connect(database=":memory:")
        con.execute("PRAGMA threads=4;")
        con.execute(f"PRAGMA temp_directory='{tmp_dir_sql}';")

        # 1. Métricas de escopo antes da construção
        scope_rows = con.execute(
            f"""
            SELECT
                p0130_especie_unidade_visitada,
                COUNT(*) AS n
            FROM read_parquet('{CURATED_OBT_GLOB}', hive_partitioning=true)
            GROUP BY 1
            """
        ).fetchall()
        scope_map = {str(r[0]): int(r[1]) for r in scope_rows}
        curated_total_rows = sum(scope_map.values())
        rows_before_join = scope_map.get(SCOPE_FILTER_VALUE, 0)
        assert rows_before_join > 0, f"Nenhum registro encontrado para o filtro: {SCOPE_FILTER_VALUE}"

        coletivos_rows = sum(v for k, v in scope_map.items() if k.startswith("06 -"))
        improvisados_rows = sum(v for k, v in scope_map.items() if k.startswith("05 -"))
        excluded_rows = curated_total_rows - rows_before_join

        print(f"Construindo {OUT_PARQUET} ...", flush=True)
        print(
            f"Escopo: {rows_before_join:,} de {curated_total_rows:,} linhas "
            f"(excluídas: {excluded_rows:,} | coletivos={coletivos_rows:,}, improvisados={improvisados_rows:,})",
            flush=True,
        )

        # 2. Execução do pipeline Analytics (Gold)
        con.execute(build_analytics_sql(out_parquet_sql))
        t_build = time.perf_counter() - t0
        out_bytes = OUT_PARQUET.stat().st_size
        out_mb = out_bytes / (1024 * 1024)
        print(f"Parquet gerado em {t_build:.1f}s ({out_mb:.1f} MiB). Rodando sanity checks...", flush=True)

        # 3. Sanity checks
        checks = run_sanity_checks(con, out_parquet_sql, rows_before_join)
        total_elapsed = time.perf_counter() - t0

        report: dict[str, Any] = {
            "output_file": "data/analytics/censo2022_icv_acm.parquet",
            "output_bytes": int(out_bytes),
            "output_size_mb": round(out_mb, 2),
            "column_count": len(ANALYTICS_COLUMNS),
            "elapsed_seconds": round(total_elapsed, 2),
            "scope_summary": {
                "filter": f"p0130_especie_unidade_visitada = '{SCOPE_FILTER_VALUE}'",
                "curated_total_rows": curated_total_rows,
                "kept_rows": rows_before_join,
                "kept_pct": round(100.0 * rows_before_join / curated_total_rows, 2),
                "excluded_rows": excluded_rows,
                "excluded_pct": round(100.0 * excluded_rows / curated_total_rows, 2),
                "coletivos_rows": coletivos_rows,
                "improvisados_rows": improvisados_rows,
            },
            **checks,
        }

        OUT_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        OUT_DOC.write_text(generate_analytics_markdown(spec, report), encoding="utf-8")

        exp = report["sample_expansion"]
        print("\n=== ANALYTICS (GOLD) CONCLUÍDO COM SUCESSO ===")
        print(f"Arquivo Parquet         : {OUT_PARQUET} ({out_mb:.1f} MiB)")
        print(f"Linhas antes/depois JOIN: {report['rows_before_join']:,} == {report['rows_after_join']:,} (OK)")
        print(f"PK única verificada     : {report['pk_unique']}")
        print(f"População expandida     : {exp['populacao_expandida_pessoas']:,.0f}")
        print(f"Domicílios expandidos   : {exp['domicilios_expandidos']:,.0f} ({exp['domicilios_amostra_unicos']:,} na amostra)")
        print(f"Famílias expandidas     : {exp['familias_expandidas']:,.0f} ({exp['familias_amostra_unicas']:,} na amostra)")
        print(f"Relatório JSON          : {OUT_REPORT}")
        print(f"Documentação Markdown   : {OUT_DOC}")
        print(f"Tempo total             : {total_elapsed:.1f}s")


if __name__ == "__main__":
    main()
