# -*- coding: utf-8 -*-
"""
cda_offline_skf — coleta da plataforma SKF offline (API GraphQL por
shortname: UBU e GERMANO) e carga no Google Sheets.

Etapas (isoladas; a falha de uma não derruba as independentes):
  assets, overall-alarm, lastmeasurement, measurements (D-1, incremental),
  lastcondition, conditions (D-1, incremental), workorders.
"""

from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from cda_utils import (
    Pipeline, anexar_novos, dia_referencia, env_json, env_obrigatoria,
    gravar_aba, http, log, periodo_referencia,
)

# ---------------------------------------------------------------------------
# 1. CONFIGURAÇÃO
# ---------------------------------------------------------------------------

API_KEY = env_obrigatoria("SKF_KEY")
SHORTNAMES = env_json("SKF_SHORTNAMES")   # {"ubu": "...", "germano": "..."}
BASE_URL = env_obrigatoria("SKF_URL").rstrip("/")
HEADERS = {"x-api-key": API_KEY, "Content-Type": "application/json"}
WORKERS = 8

PLANILHAS = {
    "assets": "1W5S8o-sgZch0HpJC052O_TAkFzaz_BVrRC82j4ClsKQ",
    "overall": "1FxNznh_ouoDljB0Zw9pjH3GEEe0J520_AD4lNMzOM04",
    "lastmeasurement": "15TXlKwMk7Q4glMeKVhi5h69sf0dUZnQDFNC68rhqX3Q",
    "measurements": "1UK6AatDxCdqg8NZxgL8ZThyCSNXOh03_AY4jtGaumXc",
    "lastcondition": "1vxJhkW0Zh0kdxU0tUKnT1nUCRLmI4FeJLcjsQylJk9E",
    "conditions": "1-Hkx_2B5HauY71j0RDYXKS_09Ax34J0Dp6wUGRG32q4",
    "workorders": "1dM1sHzskTNjd9Wc8wIQTG7dGWgyekVAZM-_wFYQp4tc",
}

# Tabelas históricas: ao passar deste nº de linhas, o arquivo é arquivado como
# '<nome> revNNN' e a planilha ativa recomeça vazia (mesmo ID/link).
LIMITE_LINHAS = {"measurements": 400_000, "conditions": 300_000}

FIELDS_ASSETS = """
assetId assetName assetDescription functionalLocation parentId parentName
criticality assetStatus assetSegment conditionIndex equipmentType
"""

FIELDS_ALARMS = """
assetId assetName pointId pointName unit alarmMethod publicOrPrivateAlarm
alertHigh alertLow dangerHigh dangerLow
"""

FIELDS_MEASUREMENTS = """
assetId assetName pointId pointName channel unit pointStatus collectedDate
overallValue
"""

FIELDS_CONDITIONS = """
assetId collectDate conditionDate conditionId conditionState inspectionType
trend technique status diagnostic observation author workOrder { id }
"""
# Campos existentes não utilizados: globalValue, globalValueUnity,
# workOrder {orderNumber deadline priority technique scheduledDate ...},
# intervention {date interventionType description isDiagnosticCorrect}

FIELDS_WORKORDERS = """
assetId id deadline priority technique scheduledDate openingDate reWork
cmmsRegister cmms services situation
intervention { date interventionType description isDiagnosticCorrect }
"""
# Campos existentes não utilizados: orderNumber, author

# ---------------------------------------------------------------------------
# 2. CLIENTE DA API
# ---------------------------------------------------------------------------

def consultar(caminho, filtro, campos):
    """POST de uma expressão GraphQL; devolve o JSON da resposta."""
    expression = f"{{ filter{filtro} {{ {campos} }} }}"
    resp = http("POST", f"{BASE_URL}/{caminho}", headers=HEADERS, json={"expression": expression})
    return resp.json()


def _args(**kwargs):
    """Monta '(a: 1, b: "x")' ignorando valores None."""
    partes = []
    for k, v in kwargs.items():
        if v is None:
            continue
        partes.append(f'{k}: "{v}"' if isinstance(v, str) and k != "cursor" else f"{k}: {v}")
    return f"({', '.join(partes)})" if partes else ""


def paginar(caminho, campos, origem, tentativas_completas=2, **filtros):
    """
    Busca todas as páginas (cursor). Se uma página intermediária falhar
    (já aconteceu no overall-alarm), recomeça a paginação do zero uma vez.
    """
    for tentativa in range(1, tentativas_completas + 1):
        dados, cursor = [], None
        try:
            while True:
                # nas páginas seguintes o original enviava só o cursor no lugar
                # do rowsPerPage (os demais filtros continuam)
                filtros_pagina = {
                    k: v for k, v in filtros.items() if not (cursor and k == "rowsPerPage")
                }
                result = consultar(caminho, _args(**filtros_pagina, cursor=cursor), campos)
                dados.extend(result.get("data") or [])
                cursor = result.get("nextCursor")
                if not cursor:
                    break
            break
        except Exception as exc:  # noqa: BLE001
            if tentativa == tentativas_completas:
                raise
            log.warning("[%s/%s] paginação falhou (%s) — recomeçando", caminho, origem, exc)
    for item in dados:
        item["origem"] = origem
    return dados


def por_origem(func):
    """Executa func(origem, shortname) para cada origem. Erro em qualquer uma aborta a etapa."""
    with ThreadPoolExecutor(max_workers=len(SHORTNAMES)) as pool:
        partes = list(pool.map(lambda kv: func(kv[0], kv[1]), SHORTNAMES.items()))
    dados = [item for parte in partes for item in parte]
    df = pd.DataFrame(dados)
    if not df.empty:
        log.info("Registros por origem: %s | total=%d", df.groupby("origem").size().to_dict(), len(df))
    return df


def por_ativo(func, df_assets, tolerancia=0.05):
    """
    Executa func(shortname, origem, asset_id) para cada ativo ativo, em paralelo.
    Falhas isoladas são toleradas até `tolerancia`; acima disso a etapa aborta
    (para não gravar dados incompletos).
    """
    ativos = df_assets[
        df_assets["assetStatus"].notna() & (df_assets["assetStatus"].astype(str) != "None")
    ]
    tarefas = [
        (SHORTNAMES[o], o, int(a))
        for o, a in zip(ativos["origem"], pd.to_numeric(ativos["assetId"], errors="coerce"))
        if pd.notna(a) and o in SHORTNAMES
    ]
    falhas = []

    def seguro(t):
        try:
            return func(*t)
        except Exception as exc:  # noqa: BLE001
            falhas.append(t)
            log.error("Falha asset=%s origem=%s: %s", t[2], t[1], exc)
            return []

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        partes = list(pool.map(seguro, tarefas))

    if tarefas and len(falhas) / len(tarefas) > tolerancia:
        raise RuntimeError(f"{len(falhas)}/{len(tarefas)} ativos falharam — etapa abortada")
    if falhas:
        log.warning("%d/%d ativos falharam (dentro da tolerância)", len(falhas), len(tarefas))

    df = pd.DataFrame([item for parte in partes for item in parte])
    log.info("Total de registros: %d (%d ativos consultados)", len(df), len(tarefas))
    return df


# ---------------------------------------------------------------------------
# 3. TRATAMENTO
# ---------------------------------------------------------------------------

def converter_datas(df, colunas):
    """
    Mesma regra do código original: só converte colunas no formato
    'Thu, 12 Nov 2026 03:00:00 GMT' (vira 'AAAA-MM-DD HH:MM:SS', UTC sem
    timezone). Outros formatos ficam como vieram — mas sem quebrar o script
    (antes, um formato diferente derrubava tudo no .dt.tz_localize).
    """
    for col in colunas:
        if col not in df.columns:
            continue
        if not pd.api.types.is_datetime64_any_dtype(df[col]):
            amostra = df[col].dropna().astype(str).head(5)
            if not any("," in x and "GMT" in x for x in amostra):
                continue
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce", format="mixed")
        if getattr(df[col].dt, "tz", None) is not None:
            df[col] = df[col].dt.tz_convert(None)
    return df


def extrair_dict(df, coluna, prefixo):
    """Expande uma coluna de dicts (pode ter None) em colunas com prefixo."""
    if coluna not in df.columns:
        return df
    dicts = df[coluna].map(lambda x: x if isinstance(x, dict) else {}).tolist()
    expandido = pd.json_normalize(dicts).add_prefix(prefixo)
    expandido.index = df.index
    return df.drop(columns=[coluna]).join(expandido)


# ---------------------------------------------------------------------------
# 4. ETAPAS
# ---------------------------------------------------------------------------

def etapa_assets():
    df = por_origem(lambda o, s: paginar(f"{s}/assets", FIELDS_ASSETS, o, rowsPerPage=300))
    if df.empty:
        raise RuntimeError("Nenhum asset retornado")
    gravar_aba(PLANILHAS["assets"], df, "assets")
    return df


def etapa_overall():
    df = por_origem(lambda o, s: paginar(f"{s}/overall-alarm", FIELDS_ALARMS, o))
    gravar_aba(PLANILHAS["overall"], df, "overall-alarm")
    return df


def _last_measurement(shortname, origem, asset_id):
    dados = consultar(f"{shortname}/lastmeasurement", _args(assetId=asset_id), FIELDS_MEASUREMENTS).get("data") or []
    for item in dados:
        item["origem"] = origem
        item["assetId"] = asset_id
    return dados


def etapa_lastmeasurement(df_assets):
    df = por_ativo(_last_measurement, df_assets)
    df = converter_datas(df, ["collectedDate"])
    gravar_aba(PLANILHAS["lastmeasurement"], df, "lastmeasurement")
    return df


def _periodo():
    ini, fim = periodo_referencia()
    log.info("Período de referência: %s a %s", ini, fim)
    return ini.strftime("%Y-%m-%d 00:00:00"), fim.strftime("%Y-%m-%d 23:59:59")


def etapa_measurements(df_assets):
    inicio, fim = _periodo()

    def buscar(shortname, origem, asset_id):
        dados = paginar(
            f"{shortname}/measurements", FIELDS_MEASUREMENTS, origem,
            assetId=asset_id, collectedDateStart=inicio, collectedDateEnd=fim,
        )
        for item in dados:
            item["assetId"] = asset_id
        return dados

    df = por_ativo(buscar, df_assets)
    if df.empty:
        log.warning("Sem medições no período")
        return df
    df = converter_datas(df, ["collectedDate"])
    if pd.api.types.is_datetime64_any_dtype(df["collectedDate"]):
        df = df.sort_values("collectedDate")
    # A chave agora inclui collectedDate: antes, duas leituras do mesmo ponto
    # com o mesmo valor em dias diferentes eram descartadas como "duplicata".
    anexar_novos(
        PLANILHAS["measurements"], df,
        chaves=["assetId", "pointId", "channel", "collectedDate", "overallValue", "origem"],
        colunas_data=["collectedDate"],
        nome="measurements",
        limite_linhas=LIMITE_LINHAS["measurements"],
    )
    return df


def _last_condition(shortname, origem, asset_id):
    dados = consultar(f"v2/{shortname}/lastcondition", _args(assetId=asset_id), FIELDS_CONDITIONS).get("data") or []
    for item in dados:
        item["origem"] = origem
        item["assetId"] = asset_id
    return dados


def etapa_lastcondition(df_assets):
    df = por_ativo(_last_condition, df_assets)
    if df.empty:
        log.warning("Sem last condition")
        return df
    df = extrair_dict(df, "workOrder", "workOrder")      # -> workOrderid (nome mantido)
    df = converter_datas(df, ["collectDate", "conditionDate"])
    gravar_aba(PLANILHAS["lastcondition"], df, "lastcondition")
    return df


def etapa_conditions(df_assets):
    inicio, fim = _periodo()

    def buscar(shortname, origem, asset_id):
        dados = paginar(
            f"v2/{shortname}/conditions", FIELDS_CONDITIONS, origem,
            assetId=asset_id, collectedDateStart=inicio, collectedDateEnd=fim,
        )
        for item in dados:
            item["assetId"] = asset_id
        return dados

    df = por_ativo(buscar, df_assets)
    if df.empty:
        log.warning("Sem conditions no período")
        return df
    df = extrair_dict(df, "workOrder", "workOrder")      # -> workOrderid (nome do cabeçalho atual)
    df = converter_datas(df, ["collectDate", "conditionDate"])
    if pd.api.types.is_datetime64_any_dtype(df["conditionDate"]):
        df = df.sort_values("conditionDate")
    anexar_novos(
        PLANILHAS["conditions"], df,
        chaves=["assetId", "conditionId", "conditionDate", "origem"],
        colunas_data=["conditionDate"],
        nome="conditions",
        limite_linhas=LIMITE_LINHAS["conditions"],
    )
    return df


def etapa_workorders():
    fim = dia_referencia().strftime("%Y-%m-%d 23:59:59")
    df = por_origem(lambda o, s: paginar(
        f"{s}/workorders", FIELDS_WORKORDERS, o,
        openingDateStart="2020-01-01 00:00:00", openingDateEnd=fim,
    ))
    if df.empty:
        log.warning("Nenhum workorder retornado. Planilha não foi alterada.")
        return df
    df = extrair_dict(df, "intervention", "intervention_")
    df = converter_datas(df, ["scheduledDate", "openingDate", "intervention_date"])
    if pd.api.types.is_datetime64_any_dtype(df["openingDate"]):
        df = df.sort_values("openingDate")
    gravar_aba(PLANILHAS["workorders"], df, "workorders")
    return df


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    p = Pipeline("cda_offline_skf")

    df_assets = p.etapa("assets", etapa_assets)
    p.etapa("overall-alarm", etapa_overall)
    p.etapa("workorders", etapa_workorders)

    dep = ["assets"]
    p.etapa("lastmeasurement", lambda: etapa_lastmeasurement(df_assets), depende_de=dep)
    p.etapa("measurements", lambda: etapa_measurements(df_assets), depende_de=dep)
    p.etapa("lastcondition", lambda: etapa_lastcondition(df_assets), depende_de=dep)
    p.etapa("conditions", lambda: etapa_conditions(df_assets), depende_de=dep)

    p.finalizar()
