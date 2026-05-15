# -*- coding: utf-8 -*-
"""cda_offline_skf.ipynb

# **1. BIBLIOTECAS**
"""

import os
import time
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import get_as_dataframe, set_with_dataframe

"""# **2. DADOS DE ACESSO**

##**2.1. API Key**
"""

API_KEY = os.getenv("SKF_KEY")

"""##**2.2. Shortname**"""

SHORTNAMES = {
  "ubu" : os.getenv("SKF_UBU"),
  "germano" : os.getenv("SKF_GERMANO")
}

"""##**2.3. URL's**"""

BASE_URL = os.getenv("SKF_URL")

"""##**2.4. Header**"""

HEADERS = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json"
}

print(BASE_URL)
print(SHORTNAMES)

"""##**2.5. Fields**

###**2.5.1. Assets**
"""

FIELDS_ASSETS = """
assetId
assetName
assetDescription
functionalLocation
parentId
parentName
criticality
assetStatus
assetSegment
conditionIndex
equipmentType
"""

"""###**2.5.2. Overall alarms**"""

FIELDS_ALARMS = """
assetId
assetName
pointId
pointName
unit
alarmMethod
publicOrPrivateAlarm
alertHigh
alertLow
dangerHigh
dangerLow
"""

"""###**2.5.3. Measurements**"""

FIELDS_MEASUREMENTS = """
assetId
assetName
pointId
pointName
channel
unit
pointStatus
collectedDate
overallValue
"""

"""###**2.5.4. Conditions**"""

FIELDS_CONDITIONS = """
assetId
collectDate
conditionDate
conditionId
conditionState
inspectionType
trend
technique
status
diagnostic
observation
author
workOrder {
    id
}
"""

"""**Campos existentes não utilizados da api**

globalValue

globalValueUnity

workOrder {
   orderNumber
   deadline
   priority
   technique
   scheduledDate
   openingDate
   reWork
   cmmsRegister
   cmms
   services
   situation
   author
}

intervention {
   date
   interventionType
   description
   isDiagnosticCorrect
}

###**2.5.5. Workorders**
"""

FIELDS_WORKORDERS = """
assetId
id
deadline
priority
technique
scheduledDate
openingDate
reWork
cmmsRegister
cmms
services
situation
intervention {
    date
    interventionType
    description
    isDiagnosticCorrect
}
"""

"""**Campos existentes não utilizados da api**

orderNumber

author

##**2.6. Funções**

###**2.6.1. Logger (tempo real)**
"""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

"""###**2.6.2. Log estruturado (memória)**"""

LOGS = []

def add_log(status, endpoint, origem, asset_id=None, tentativa=None, tempo=None, mensagem=None):
    LOGS.append({
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "endpoint": endpoint,
        "status": status,
        "origem": origem,
        "assetId": asset_id,
        "tentativa": tentativa,
        "tempo": tempo,
        "mensagem": mensagem
    })

"""###**2.6.3. Retry com Log**"""

def request_with_retry(url, payload, origem, endpoint, asset_id=None, retries=5, backoff=2):

    for attempt in range(retries):
        start_time = time.time()

        try:
            response = requests.post(
                url,
                headers=HEADERS,
                json=payload,
                timeout=30
            )

            elapsed = round(time.time() - start_time, 2)

            if response.status_code == 200:
                logger.info(f"[{endpoint}] SUCCESS | origem={origem} asset={asset_id} tempo={elapsed}s")
                add_log("SUCCESS", endpoint, origem, asset_id, attempt+1, elapsed)
                return response

            elif response.status_code in [429, 500, 502, 503, 504]:
                wait = backoff ** attempt

                logger.warning(f"[{endpoint}] RETRY {response.status_code} | origem={origem} tentativa={attempt+1}")
                add_log("RETRY", endpoint, origem, asset_id, attempt+1, elapsed, f"HTTP {response.status_code}")

                time.sleep(wait)

            else:
                logger.error(f"[{endpoint}] ERROR | origem={origem} asset={asset_id} erro={response.text}")
                add_log("ERROR", endpoint, origem, asset_id, attempt+1, mensagem=response.text)
                raise Exception(response.text)

        except Exception as e:
            wait = backoff ** attempt

            logger.warning(f"[{endpoint}] RETRY_NETWORK | origem={origem} tentativa={attempt+1}")
            add_log("RETRY_NETWORK", endpoint, origem, asset_id, attempt+1, mensagem=str(e))

            time.sleep(wait)

    logger.critical(f"[{endpoint}] FAIL | origem={origem} asset={asset_id}")
    add_log("FAIL", endpoint, origem, asset_id, mensagem="Falha após retries")

    raise Exception(f"[{origem}] Falhou após {retries} tentativas")

"""###**2.6.2. Conversão para datetime**"""

def convert_dates(df, columns):
    for col in columns:
        if col in df.columns:
            if df[col].dtype == "object":
                sample = df[col].dropna().astype(str).head(5)

                if any("," in x and "GMT" in x for x in sample):
                    df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")
    return df

"""###**2.6.3. Remoção de timezone**"""

def remove_timezone(df, columns):
    for col in columns:
        if col in df.columns:
            df[col] = df[col].dt.tz_localize(None)
    return df

"""##**2.7. Sheets**

###**2.7.1. Autenticação no Sheets**
"""

# Lê a variável de ambiente com o conteúdo do JSON da conta de serviço
service_account_info = json.loads(os.environ["GOOGLE_SERVICE_ACCOUNT"])

# Define os escopos de acesso (Google Sheets)
SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# Cria as credenciais usando o conteúdo do secret
creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)

# Autentica no Google Sheets
gc = gspread.authorize(creds)

"""# **3. REQUISIÇÃO: ASSETS**

##**3.1. Busca com paginação**
"""

def fetch_assets(shortname, origem):

    url = f"{BASE_URL}/{shortname}/assets"
    all_data = []
    cursor = None
    ENDPOINT = "assets"

    while True:

        expression = f"""
        {{
            filter{f"(cursor: {cursor})" if cursor else "(rowsPerPage: 300)"} {{
                {FIELDS_ASSETS}
            }}
        }}
        """

        payload = {"expression": expression}

        response = request_with_retry(url, payload, origem, ENDPOINT)

        result = response.json()

        data = result.get("data", [])
        next_cursor = result.get("nextCursor")

        if not data:
            logger.warning(f"[{ENDPOINT}] EMPTY | origem={origem}")
            add_log("EMPTY", ENDPOINT, origem)

        for item in data:
            item["origem"] = origem

        all_data.extend(data)

        if not next_cursor:
            break

        cursor = next_cursor

    logger.info(f"[{ENDPOINT}] FINAL | origem={origem} total={len(all_data)}")

    return all_data

"""##**3.2. Execução paralela**"""

def fetch_all_assets_parallel():
    results = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(fetch_assets, short, name): name
            for name, short in SHORTNAMES.items()
        }

        for future in as_completed(futures):
            try:
                data = future.result()
                results.extend(data)
            except Exception as e:
                print(f"Erro em {futures[future]}: {e}")

    return results

"""##**3.3. Main**"""

if __name__ == "__main__":
    ativos = fetch_all_assets_parallel()

    df_assets = pd.DataFrame(ativos)

    print("\nResumo:")
    print(df_assets.groupby("origem").size())

    print("\nTotal geral:", len(df_assets))

"""##**3.5. Carga no Sheets**"""

# Nome da planilha
planilha_id = "1W5S8o-sgZch0HpJC052O_TAkFzaz_BVrRC82j4ClsKQ"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados (opcional)
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_assets)

print("Dados enviados com sucesso para o Google Sheets!")

"""#**4. REQUISIÇÃO: OVERALL ALARMS**

##**4.1. Busca com paginação**
"""

def fetch_overall_alarm(shortname, origem):

    url = f"{BASE_URL}/{shortname}/overall-alarm"
    all_data = []
    cursor = None
    ENDPOINT = "overall-alarm"

    while True:
        if cursor:
            expression = f"""
            {{
                filter(cursor: {cursor}) {{
                    {FIELDS_ALARMS}
                }}
            }}
            """
        else:
            expression = f"""
            {{
                filter {{
                    {FIELDS_ALARMS}
                }}
            }}
            """

        payload = {"expression": expression}

        response = request_with_retry(
            url,
            payload,
            origem,
            endpoint=ENDPOINT
        )

        result = response.json()

        data = result.get("data", [])
        next_cursor = result.get("nextCursor")

        if not data:
            logger.warning(f"[{ENDPOINT}] EMPTY | origem={origem}")

            add_log(
                "EMPTY",
                ENDPOINT,
                origem,
                mensagem="Sem dados retornados"
            )

        for item in data:
            item["origem"] = origem

        all_data.extend(data)

        if not next_cursor:
            break

        cursor = next_cursor

    logger.info(f"[{ENDPOINT}] FINAL | origem={origem} total={len(all_data)}")

    add_log(
        "FINAL",
        ENDPOINT,
        origem,
        mensagem=f"Total registros: {len(all_data)}"
    )

    return all_data

"""##**4.2. Execução paralela**"""

def fetch_all_overall_alarm():
    results = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(fetch_overall_alarm, short, name): name
            for name, short in SHORTNAMES.items()
        }

        for future in as_completed(futures):
            origem = futures[future]

            try:
                data = future.result()
                results.extend(data)
            except Exception as e:
                print(f"Erro em {origem}: {e}")

    return results

"""##**4.3. Main**"""

if __name__ == "__main__":
    dados = fetch_all_overall_alarm()

    if not dados:
        print("Nenhum dado retornado da API")
    else:
        df_overall = pd.DataFrame(dados)

        print("\nResumo:")
        print(df_overall.groupby("origem").size())

        print("\nTotal geral:", len(df_overall))

"""##**4.5. Carga no Sheets**"""

# Nome da planilha
planilha_id = "1FxNznh_ouoDljB0Zw9pjH3GEEe0J520_AD4lNMzOM04"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados (opcional)
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_overall)

print("Dados enviados com sucesso para o Google Sheets!")

"""#**5. REQUISIÇÃO: LAST MEASUREMENT**

##**5.1. Busca com paginação**
"""

def fetch_last_measurement_by_asset(shortname, origem, asset_id):

    if pd.isna(asset_id):
        return []

    try:
        asset_id = int(asset_id)
    except:
        return []

    url = f"{BASE_URL}/{shortname}/lastmeasurement"
    ENDPOINT = "lastmeasurement"

    expression = f"""
    {{
        filter(assetId: {asset_id}) {{
            {FIELDS_MEASUREMENTS}
        }}
    }}
    """

    payload = {"expression": expression}

    response = request_with_retry(
        url,
        payload,
        origem,
        endpoint=ENDPOINT,
        asset_id=asset_id
    )

    result = response.json()
    data = result.get("data", [])

    if not data:
        logger.warning(f"[{ENDPOINT}] EMPTY | origem={origem} asset={asset_id}")
        add_log("EMPTY", ENDPOINT, origem, asset_id)
        return []

    for item in data:
        item["origem"] = origem
        item["assetId"] = asset_id

    return data

"""##**5.2. Execução paralela**"""

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_all_last_measurements(df_assets):

    df_assets = df_assets[
        df_assets["assetStatus"].notna() &
        (df_assets["assetStatus"] != "None")
    ]

    results = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                fetch_last_measurement_by_asset,
                row["shortname"],
                row["origem"],
                row["assetId"]
            )
            for _, row in df_assets.iterrows()
        ]

        for future in as_completed(futures):
            try:
                data = future.result()
                if data:
                    results.extend(data)
            except Exception as e:
                logger.error(f"Erro: {e}")

    return results

"""##**5.3. Main**"""

if __name__ == "__main__":

    df_assets = df_assets.dropna(subset=["assetId"])
    df_assets["shortname"] = df_assets["origem"].map(SHORTNAMES)

    dados = fetch_all_last_measurements(df_assets)

    if not dados:
        print("Nenhum dado retornado da API")
    else:
        df_last_measurement = pd.DataFrame(dados)

        print("\nResumo:")
        print(df_last_measurement.groupby("origem").size())

        print("\nTotal geral:", len(df_last_measurement))

"""##**5.4. DataFrame**"""

# conversão para datetime
df_last_measurement = convert_dates(
    df_last_measurement,
    ["collectedDate"]
)

# remoção de timezone
df_lastmeasurement = remove_timezone(
    df_last_measurement,
    ["collectedDate"]
)

"""##**5.5. Carga no Sheets**"""

# Nome da planilha
planilha_id = "15TXlKwMk7Q4glMeKVhi5h69sf0dUZnQDFNC68rhqX3Q"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados (opcional)
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_lastmeasurement)

print("Dados enviados com sucesso para o Google Sheets!")

"""#**6. REQUISIÇÃO: MEASUREMENT BY PERIOD**

##**6.1. Busca com paginação**
"""

def fetch_measurements_by_asset(shortname, origem, asset_id, start_date, end_date):

    if pd.isna(asset_id):
        return []

    try:
        asset_id = int(asset_id)
    except:
        return []

    url = f"{BASE_URL}/{shortname}/measurements"
    all_data = []
    cursor = None
    ENDPOINT = "measurements"

    while True:

        expression = f"""
        {{
            filter(
                assetId: {asset_id},
                collectedDateStart: "{start_date}",
                collectedDateEnd: "{end_date}"
                {f", cursor: {cursor}" if cursor else ""}
            ) {{
                {FIELDS_MEASUREMENTS}
            }}
        }}
        """

        payload = {"expression": expression}

        response = request_with_retry(
            url,
            payload,
            origem,
            endpoint=ENDPOINT,
            asset_id=asset_id
        )

        result = response.json()

        data = result.get("data", [])
        next_cursor = result.get("nextCursor")

        for item in data:
            item["origem"] = origem
            item["assetId"] = asset_id

        all_data.extend(data)

        if not next_cursor:
            break

        cursor = next_cursor

    return all_data

"""##**6.2. Execução paralela**"""

def fetch_all_measurements(df_assets, start_date, end_date):

    df_assets = df_assets[
        df_assets["assetStatus"].notna() &
        (df_assets["assetStatus"] != "None")
    ]

    results = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(
                fetch_measurements_by_asset,
                row["shortname"],
                row["origem"],
                row["assetId"],
                start_date,
                end_date
            )
            for _, row in df_assets.iterrows()
        ]

        for future in as_completed(futures):
            try:
                data = future.result()
                if data:
                    results.extend(data)
            except Exception as e:
                logger.error(f"Erro: {e}")

    return results

"""##**6.3. Main**"""

if __name__ == "__main__":

    # carga manual
    #start_date = "2026-02-21 00:00:00"
    #end_date   = "2026-02-21 23:59:59"

    # carga incremental (D-1)
    ontem = datetime.now(timezone.utc) - timedelta(days=1)

    start_date = ontem.strftime("%Y-%m-%d 00:00:00")
    end_date   = ontem.strftime("%Y-%m-%d 23:59:59")

    df_assets = df_assets.dropna(subset=["assetId"])
    df_assets["shortname"] = df_assets["origem"].map(SHORTNAMES)

    dados = fetch_all_measurements(df_assets, start_date, end_date)

    if not dados:
        print("Nenhum dado retornado da API")
    else:
        df_measurements = pd.DataFrame(dados)

        print("\nResumo:")
        print(df_measurements.groupby("origem").size())

        print("\nTotal geral:", len(df_measurements))

"""##**6.4. DataFrame**"""

# conversão para datetime
df_measurements = convert_dates(
    df_measurements,
    ["collectedDate"]
)

# remoção de timezone
df_measurements = remove_timezone(
    df_measurements,
    ["collectedDate"]
)

# ordenação crescente
df_measurements = df_measurements.sort_values(by="collectedDate")

"""##**6.5. Carga no Sheets**"""

# Nome da planilha e aba
planilha_id = "1UK6AatDxCdqg8NZxgL8ZThyCSNXOh03_AY4jtGaumXc"
nome_da_aba = "Sheet1"

# Abre a planilha e aba
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Lê os dados atuais da aba (já existentes)
df_existente = get_as_dataframe(aba, evaluate_formulas=True).dropna(how="all")

# Garante que colunas estão no mesmo formato e ordem
colunas_chave = ['assetId', 'assetName', 'pointId', 'pointName', 'channel', 'unit', 'pointStatus', 'overallValue', 'origem']
df_existente = df_existente[colunas_chave].dropna()

# Remove duplicados e encontra apenas as linhas novas
df_novos = df_measurements[~df_measurements.isin(df_existente.to_dict(orient='list')).all(axis=1)]

# Se houver novos registros, adiciona abaixo
if not df_novos.empty:
    # Número de linhas já existentes (para inserir a partir da próxima linha vazia)
    ultima_linha = len(df_existente) + 2  # +1 para header, +1 para próxima
    set_with_dataframe(aba, df_novos, row=ultima_linha, col=1, include_column_header=False)
    print(f"{len(df_novos)} novas condições adicionadas à planilha!")
else:
    print("Nenhuma condição nova para inserir")

"""#**7. REQUISIÇÃO: LAST CONDITION**

##**7.1. Busca por ativo**
"""

def fetch_last_condition(shortname, origem, asset_id):

    url = f"{BASE_URL}/v2/{shortname}/lastcondition"
    ENDPOINT = "lastcondition"

    expression = f"""
    {{
        filter(assetId: {asset_id}) {{
            {FIELDS_CONDITIONS}
        }}
    }}
    """

    payload = {"expression": expression}

    response = request_with_retry(
        url,
        payload,
        origem,
        endpoint=ENDPOINT,
        asset_id=asset_id
    )

    data = response.json().get("data", [])

    if not data:
        logger.warning(f"[{ENDPOINT}] EMPTY | origem={origem} asset={asset_id}")
        add_log("EMPTY", ENDPOINT, origem, asset_id)
        return []

    for item in data:
        item["origem"] = origem
        item["assetId"] = asset_id

    return data

"""##**7.2. Execução paralela**"""

from concurrent.futures import ThreadPoolExecutor, as_completed

def fetch_all_last_condition(df_assets):

    df_assets_ativos = df_assets[
        (df_assets["assetStatus"].notna()) &
        (df_assets["assetStatus"] != "None")
    ]

    results = []

    with ThreadPoolExecutor(max_workers=12) as executor:

        futures = [
            executor.submit(
                fetch_last_condition,
                row["shortname"],
                row["origem"],
                row["assetId"]
            )
            for _, row in df_assets_ativos.iterrows()
        ]

        for future in as_completed(futures):
            try:
                data = future.result()
                if data:
                    results.extend(data)
            except Exception as e:
                logger.error(f"Erro: {e}")

    return results

"""##**7.3. Main**"""

if __name__ == "__main__":

    df_assets = df_assets.dropna(subset=["assetId"])
    df_assets["shortname"] = df_assets["origem"].map(SHORTNAMES)

    dados = fetch_all_last_condition(df_assets)

    if not dados:
        print("Nenhum dado retornado da API")
    else:
        df_last_condition = pd.DataFrame(dados)

        if "workOrder" in df_last_condition.columns:
            df_workorder = pd.json_normalize(df_last_condition["workOrder"])

        print("\nResumo:")
        print(df_last_condition.groupby("origem").size())

        print("\nTotal geral:", len(df_last_condition))

"""##**7.4. DataFrame**"""

# separação de dados aninhados
df_last_condition = df_last_condition.join(
    pd.json_normalize(df_last_condition["workOrder"]).add_prefix("workOrder")
    ).drop(columns=["workOrder"]
)

# conversão para datetime
df_last_condition = convert_dates(
    df_last_condition,
    ["collectDate",
    "conditionDate"]
)

# remoção de timezone
df_lastcondition = remove_timezone(
    df_last_condition,
    ["collectDate",
    "conditionDate"]
)

"""##**7.5. Carga no Sheets**"""

# Nome da planilha
planilha_id = "1vxJhkW0Zh0kdxU0tUKnT1nUCRLmI4FeJLcjsQylJk9E"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados (opcional)
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_lastcondition)

print("Dados enviados com sucesso para o Google Sheets!")

"""#**8. REQUISIÇÃO: CONDITION BY PERIOD**

##**8.1. Busca com paginação**
"""

def fetch_conditions_by_asset(shortname, origem, asset_id, start_date, end_date):

    if pd.isna(asset_id):
        return []

    try:
        asset_id = int(asset_id)
    except:
        return []

    url = f"{BASE_URL}/v2/{shortname}/conditions"
    all_data = []
    cursor = None
    ENDPOINT = "conditions"

    while True:
        if cursor:
            expression = f"""
            {{
                filter(
                    assetId: {asset_id},
                    collectedDateStart: "{start_date}",
                    collectedDateEnd: "{end_date}",
                    cursor: {cursor}
                ) {{
                    {FIELDS_CONDITIONS}
                }}
            }}
            """
        else:
            expression = f"""
            {{
                filter(
                    assetId: {asset_id},
                    collectedDateStart: "{start_date}",
                    collectedDateEnd: "{end_date}"
                ) {{
                    {FIELDS_CONDITIONS}
                }}
            }}
            """

        payload = {"expression": expression}

        response = request_with_retry(
            url,
            payload,
            origem,
            endpoint=ENDPOINT
        )

        result = response.json()

        data = result.get("data", [])
        next_cursor = result.get("nextCursor")

        for item in data:
            item["origem"] = origem
            item["assetId"] = asset_id

        all_data.extend(data)

        if not next_cursor:
            break

        cursor = next_cursor

    logger.info(f"[{ENDPOINT}] FINAL | origem={origem} asset={asset_id} total={len(all_data)}")

    add_log(
        "FINAL",
        ENDPOINT,
        origem,
        asset_id,
        mensagem=f"Total registros: {len(all_data)}"
    )

    return all_data

"""##**8.2. Execução paralela**"""

def fetch_all_conditions(df_assets, start_date, end_date):

    df_assets = df_assets[
        df_assets["assetStatus"].notna() &
        (df_assets["assetStatus"] != "None")
    ]

    results = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []

        for _, row in df_assets.iterrows():
            futures.append(
                executor.submit(
                    fetch_conditions_by_asset,
                    row["shortname"],
                    row["origem"],
                    row["assetId"],
                    start_date,
                    end_date
                )
            )

        for future in as_completed(futures):
            try:
                results.extend(future.result())
            except Exception as e:
                print(f"Erro: {e}")

    return results

"""##**8.3. Main**"""

if __name__ == "__main__":

    # carga manual
    #start_date = "2026-02-21 00:00:00"
    #end_date   = "2026-02-21 23:59:59"

    # carga incremental (D-1)
    ontem = datetime.now(timezone.utc) - timedelta(days=1)

    start_date = ontem.strftime("%Y-%m-%d 00:00:00")
    end_date   = ontem.strftime("%Y-%m-%d 23:59:59")

    df_assets = df_assets.dropna(subset=["assetId"])
    df_assets["shortname"] = df_assets["origem"].map(SHORTNAMES)

    dados = fetch_all_conditions(df_assets, start_date, end_date)

    if not dados:
        print("Nenhum dado retornado da API")
    else:
        df_conditions = pd.DataFrame(dados)

        print("\nResumo:")
        print(df_conditions.groupby("origem").size())

        print("\nTotal geral:", len(df_conditions))

"""##**8.4. DataFrame**"""

# separação de dados aninhados
df_conditions = df_conditions.join(
    pd.json_normalize(df_conditions["workOrder"]).add_prefix("workOrder_")
    ).drop(columns=["workOrder"]
)

# conversão para datetime
df_conditions = convert_dates(
    df_conditions,
    ["collectDate",
    "conditionDate"]
)

# remoção de timezone
df_conditions = remove_timezone(
    df_conditions,
    ["collectDate",
    "conditionDate"]
)

# ordenação crescente
df_conditions = df_conditions.sort_values(by="conditionDate")

"""## **8.5. Carga no Sheets**"""

# Nome da planilha e aba
planilha_id = "1-Hkx_2B5HauY71j0RDYXKS_09Ax34J0Dp6wUGRG32q4"
nome_da_aba = "Sheet1"

# Abre a planilha e aba
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Lê os dados atuais da aba (já existentes)
df_existente = get_as_dataframe(aba, evaluate_formulas=True).dropna(how="all")

# Garante que colunas estão no mesmo formato e ordem
colunas_chave = ['assetId', 'collectDate', 'conditionDate', 'conditionId', 'conditionState', 'inspectionType', 'trend', 'technique', 'status', 'diagnostic', 'observation', 'author', 'workOrder', 'origem']
df_existente = df_existente[colunas_chave].dropna()

# Remove duplicados e encontra apenas as linhas novas
df_novos = df_conditions[~df_conditions.isin(df_existente.to_dict(orient='list')).all(axis=1)]

# Se houver novos registros, adiciona abaixo
if not df_novos.empty:
    # Número de linhas já existentes (para inserir a partir da próxima linha vazia)
    ultima_linha = len(df_existente) + 2  # +1 para header, +1 para próxima
    set_with_dataframe(aba, df_novos, row=ultima_linha, col=1, include_column_header=False)
    print(f"{len(df_novos)} novas condições adicionadas à planilha!")
else:
    print("Nenhuma condição nova para inserir")

"""#**9. REQUISIÇÃO: WORKORDER BY PERIOD**

##**9.1. Busca com paginação**
"""

def fetch_workorders(shortname, origem, start_date, end_date):

    url = f"{BASE_URL}/{shortname}/workorders"
    all_data = []
    cursor = None
    ENDPOINT = "workorders"

    while True:

        expression = f"""
        {{
            filter(
                openingDateStart: "{start_date}",
                openingDateEnd: "{end_date}"
                {f", cursor: {cursor}" if cursor else ""}
            ) {{
                {FIELDS_WORKORDERS}
            }}
        }}
        """

        payload = {"expression": expression}

        response = request_with_retry(url, payload, origem, ENDPOINT)

        result = response.json()

        data = result.get("data", [])
        next_cursor = result.get("nextCursor")

        if not data:
            logger.warning(f"[{ENDPOINT}] EMPTY | origem={origem}")
            add_log("EMPTY", ENDPOINT, origem)

        for item in data:
            item["origem"] = origem

        all_data.extend(data)

        if not next_cursor:
            break

        cursor = next_cursor

    logger.info(f"[{ENDPOINT}] FINAL | origem={origem} total={len(all_data)}")

    return all_data

"""##**9.2. Execução paralela**"""

def fetch_all_workorders(start_date, end_date):
    results = []

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(fetch_workorders, short, name, start_date, end_date): name
            for name, short in SHORTNAMES.items()
        }

        for future in as_completed(futures):
            origem = futures[future]

            try:
                results.extend(future.result())
            except Exception as e:
                print(f"Erro em {origem}: {e}")

    return results

"""##**9.3. Main**"""

if __name__ == "__main__":

    # carga completa
    ontem = datetime.now(timezone.utc) - timedelta(days=1)

    start_date = "2020-01-01 00:00:00"
    end_date   = ontem.strftime("%Y-%m-%d 23:59:59")

    dados = fetch_all_workorders(start_date, end_date)

    if not dados:
        print("Nenhum dado retornado da API")
    else:
        df_workorders = pd.DataFrame(dados)

        print("\nResumo:")
        print(df_workorders.groupby("origem").size())

        print("\nTotal geral:", len(df_workorders))

"""##**9.4. DataFrame**"""

# separação de dados aninhados
df_workorders = df_workorders.join(
    pd.json_normalize(df_workorders["intervention"]).add_prefix("intervention_")
    ).drop(columns=["intervention"])

# conversão para datetime
df_workorders = convert_dates(
    df_workorders,
    ["scheduledDate",
    "openingDate",
     "intervention_date"]
)

# remoção de timezone
df_workorders = remove_timezone(
    df_workorders,
    ["scheduledDate",
    "openingDate",
     "intervention_date"]
)

# ordenação crescente
df_workorders = df_workorders.sort_values(by="openingDate")

"""## **9.5. Carga no Sheets**"""

# Nome da planilha
planilha_id = "1dM1sHzskTNjd9Wc8wIQTG7dGWgyekVAZM-_wFYQp4tc"
nome_da_aba = "Sheet1"

# Abre a planilha
planilha = gc.open_by_key(planilha_id)
aba = planilha.worksheet(nome_da_aba)

# Limpa a aba antes de escrever os dados
aba.clear()

# Envia o DataFrame para a aba
set_with_dataframe(aba, df_workorders)

print("Dados enviados com sucesso para o Google Sheets!")

"""#**10. LOG**"""

df_log = pd.DataFrame(LOGS)

output_log = {
    "resumo": df_log["status"].value_counts().to_dict(),
    "erros": df_log[df_log["status"].isin(["ERROR", "FAIL"])].to_dict(orient="records"),
    "total": len(df_log),
    "logs": LOGS
}

with open("log_execucao.json", "w") as f:
    json.dump(output_log, f, indent=4)

print("Log gerado")
