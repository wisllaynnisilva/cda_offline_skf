# -*- coding: utf-8 -*-
"""
cda_utils.py — utilitários comuns de robustez para os pipelines cda_*.

Este arquivo é idêntico em todos os repositórios cda_* (copie as alterações
para os demais quando mexer aqui).

O que ele resolve:
  * HTTP com timeout, retry exponencial e respeito a Retry-After (429/5xx,
    quedas de conexão, leitura incompleta). Erros 4xx falham na hora.
  * Google Sheets com retry (502/503/429 do Google derrubavam o job).
  * Escrita segura: nunca apaga a aba quando o DataFrame veio vazio e não faz
    clear() antes de gravar (se a gravação falhasse a aba ficava vazia).
  * Append incremental com deduplicação por chave normalizada (datas,
    números e textos comparados de forma consistente) e alinhado pelo
    cabeçalho existente na planilha.
  * Execução por etapas isoladas: a falha de uma etapa não derruba as
    outras; no final o job sai com código 1 se algo falhou (GitHub avisa).
"""

import json
import logging
import math
import os
import random
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("cda")

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------


def env_obrigatoria(nome):
    """Lê variável de ambiente e falha com mensagem clara se estiver vazia."""
    valor = os.getenv(nome)
    if not valor:
        raise RuntimeError(f"Variável de ambiente/secret '{nome}' não definida")
    return valor


def env_json(nome):
    return json.loads(env_obrigatoria(nome))


def dia_referencia():
    """
    Dia a ser carregado nas cargas incrementais (padrão: ontem, em UTC).
    Para reprocessar um dia perdido, rode o workflow manualmente informando
    a data (vira a variável DATA_REFERENCIA=AAAA-MM-DD).
    """
    valor = (os.getenv("DATA_REFERENCIA") or "").strip()
    if valor:
        return datetime.strptime(valor, "%Y-%m-%d").date()
    return (datetime.now(timezone.utc) - timedelta(days=1)).date()


def periodo_referencia():
    """
    (início, fim) das cargas incrementais. Padrão: (ontem, ontem).
    Com DATA_INICIO e DATA_FIM (AAAA-MM-DD) reprocessa um período inteiro.
    """
    ini = (os.getenv("DATA_INICIO") or "").strip()
    fim = (os.getenv("DATA_FIM") or "").strip()
    if ini and fim:
        a = datetime.strptime(ini, "%Y-%m-%d").date()
        b = datetime.strptime(fim, "%Y-%m-%d").date()
        if a > b:
            raise ValueError("DATA_INICIO maior que DATA_FIM")
        return a, b
    dia = dia_referencia()
    return dia, dia


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------

TIMEOUT = (15, 180)  # (conexão, leitura) em segundos
STATUS_RETENTAVEIS = {408, 425, 429, 500, 502, 503, 504}
ERROS_REDE = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
)

SESSION = requests.Session()
SESSION.mount(
    "https://", requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
)
SESSION.mount(
    "http://", requests.adapters.HTTPAdapter(pool_connections=20, pool_maxsize=20)
)


class HTTPRetentavel(Exception):
    pass


def _retry_after(resp):
    valor = resp.headers.get("Retry-After") if resp is not None else None
    try:
        return float(valor) if valor else None
    except ValueError:
        return None


class CotaEsgotada(RuntimeError):
    """A API pediu para esperar mais do que vale a pena (limite diário/cota)."""


def http(
    method,
    url,
    *,
    tentativas=6,
    backoff=2.0,
    espera_max=120,
    retry_after_max=300,
    ao_401=None,
    **kwargs,
):
    """
    requests com timeout + retry.

    - 429/5xx/erros de rede: tenta de novo com backoff exponencial
      (respeitando o header Retry-After quando existir).
    - Se a API pedir para esperar mais que `retry_after_max` segundos (sinal
      de cota/limite diário esgotado), desiste na hora: repetir só gastaria
      mais requisições da cota.
    - 401: se `ao_401` for informado, chama-o (para renovar token) e tenta
      de novo uma vez. A função deve devolver os novos headers.
    - Demais 4xx: levanta HTTPError imediatamente (não adianta repetir).
    """
    kwargs.setdefault("timeout", TIMEOUT)
    renovou = False
    ultima_exc = None

    for tentativa in range(1, tentativas + 1):
        resp = None
        try:
            resp = SESSION.request(method, url, **kwargs)

            if resp.status_code == 401 and ao_401 and not renovou:
                log.warning("401 em %s — renovando token", _url_curta(url))
                kwargs["headers"] = ao_401()
                renovou = True
                continue

            if resp.status_code in STATUS_RETENTAVEIS:
                raise HTTPRetentavel(f"HTTP {resp.status_code}")

            resp.raise_for_status()
            return resp

        except (HTTPRetentavel, *ERROS_REDE) as exc:
            ultima_exc = exc
            if tentativa == tentativas:
                break
            espera = min(backoff ** tentativa, espera_max)
            pedido = _retry_after(resp)
            if pedido is not None:
                if pedido > retry_after_max:
                    raise CotaEsgotada(
                        f"{_url_curta(url)}: API pediu espera de {pedido:.0f}s "
                        "(provável limite de requisições esgotado) — não vou insistir"
                    ) from exc
                espera = pedido
            log.warning(
                "%s em %s (tentativa %d/%d) — aguardando %.0fs",
                exc, _url_curta(url), tentativa, tentativas, espera,
            )
            time.sleep(espera)

    raise RuntimeError(
        f"Falha após {tentativas} tentativas em {_url_curta(url)}: {ultima_exc}"
    ) from ultima_exc


def _url_curta(url):
    """Remove querystring para não poluir/expor dados nos logs."""
    return str(url).split("?")[0]


# ---------------------------------------------------------------------------
# Google Sheets
# ---------------------------------------------------------------------------

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

LIMITE_CELULAS = 10_000_000  # limite do Google Sheets por arquivo
_gc = None


def gc():
    global _gc
    if _gc is None:
        info = env_json("GOOGLE_SERVICE_ACCOUNT")
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
        _gc = gspread.authorize(creds)
        _gc.set_timeout(TIMEOUT)
    return _gc


class SheetsIndisponivel(RuntimeError):
    """O Google Sheets continuou fora do ar mesmo após todas as tentativas."""


def sheets_retry(func, *args, tentativas=8, **kwargs):
    """
    Executa uma chamada do gspread com retry em 429/5xx/erros de rede.
    Esperas: 10, 20, 40, 80, 120, 120, 120 s (~8 min no total) + variação
    aleatória, cobrindo as instabilidades curtas do Google (503/502).
    """
    for tentativa in range(1, tentativas + 1):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            codigo = getattr(exc, "code", None)
            # -1 = resposta HTML (ex.: 502 do Google), não JSON
            retentavel = codigo in (-1, 408, 429) or (codigo or 0) >= 500
            if not retentavel:
                raise
            if tentativa == tentativas:
                raise SheetsIndisponivel(f"Google Sheets indisponível: APIError {codigo}") from exc
            erro = f"APIError {codigo}"
        except ERROS_REDE as exc:
            if tentativa == tentativas:
                raise SheetsIndisponivel(f"Google Sheets indisponível: {exc}") from exc
            erro = type(exc).__name__
        espera = min(5 * 2 ** tentativa, 120) + random.uniform(0, 5)
        log.warning(
            "Sheets: %s (tentativa %d/%d) — aguardando %ds",
            erro, tentativa, tentativas, espera,
        )
        time.sleep(espera)


_abas = {}


def abrir_aba(planilha_id, nome_aba="Sheet1"):
    """Abre a aba (com cache: cada planilha é aberta só uma vez por execução)."""
    chave = (planilha_id, nome_aba)
    if chave not in _abas:
        planilha = sheets_retry(gc().open_by_key, planilha_id)
        _abas[chave] = sheets_retry(planilha.worksheet, nome_aba)
    return _abas[chave]


# Gravações que falharam porque o Google ficou fora do ar. Os dados já foram
# coletados (e a cota das APIs já foi gasta), então não são descartados: o
# Pipeline tenta gravá-los de novo no fim da execução.
PENDENTES = []


def com_pendencia(rotulo, func, /, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except SheetsIndisponivel as exc:
        log.error("[%s] %s — gravação adiada para o fim da execução", rotulo, exc)
        PENDENTES.append((rotulo, func, args, kwargs))
        return None


def _checar_limite(df_linhas, df_colunas, nome):
    celulas = (df_linhas + 1) * max(df_colunas, 1)
    if celulas > LIMITE_CELULAS * 0.9:
        log.error(
            "[%s] %s células — perto/acima do limite de 10 milhões do Google "
            "Sheets. Migre esta tabela para um banco de dados.",
            nome, f"{celulas:,}",
        )


def gravar_aba(planilha_id, df, nome="tabela", nome_aba="Sheet1", permitir_vazio=False):
    return com_pendencia(nome, _gravar_aba, planilha_id, df, nome, nome_aba, permitir_vazio)


def _gravar_aba(planilha_id, df, nome, nome_aba, permitir_vazio):
    """
    Substitui o conteúdo da aba pelo DataFrame, de forma segura:
      * se o DataFrame estiver vazio, NÃO mexe na planilha (a menos que
        permitir_vazio=True);
      * redimensiona e sobrescreve em vez de clear() + escrever (se a escrita
        falhasse no meio, a aba ficava vazia).
    """
    if df is None or (df.empty and not permitir_vazio):
        log.warning("[%s] DataFrame vazio — planilha NÃO foi alterada", nome)
        return
    _checar_limite(len(df), len(df.columns), nome)
    aba = abrir_aba(planilha_id, nome_aba)
    sheets_retry(set_with_dataframe, aba, df, resize=True)
    log.info("[%s] %d linhas gravadas no Sheets", nome, len(df))


# ---- normalização para comparação de chaves -------------------------------

_EPOCH_SHEETS = datetime(1899, 12, 30)


def _norm_data(v):
    if v is None or (isinstance(v, float) and math.isnan(v)) or v == "":
        return ""
    try:
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            # data serial do Google Sheets
            ts = pd.Timestamp(_EPOCH_SHEETS + timedelta(days=float(v)))
        else:
            ts = pd.to_datetime(v, utc=True)
            ts = ts.tz_convert(None)
        return ts.round("s").strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError):
        return str(v).strip()


def _norm_valor(v):
    if v is None:
        return ""
    if isinstance(v, float):
        if math.isnan(v):
            return ""
        if v.is_integer():
            return str(int(v))
        return f"{v:.10g}"
    if isinstance(v, (pd.Timestamp, datetime)):
        return _norm_data(v)
    if hasattr(v, "item"):  # numpy scalar
        return _norm_valor(v.item())
    s = str(v).strip()
    if s.lower() in ("nan", "none", "nat"):
        return ""
    # "123.0" / "1.50" vindos como texto
    try:
        f = float(s)
        if math.isfinite(f) and s.lstrip("-").replace(".", "", 1).isdigit():
            return _norm_valor(f)
    except ValueError:
        pass
    return s


def _chave(df, chaves, colunas_data):
    partes = []
    for c in chaves:
        serie = df[c] if c in df.columns else pd.Series([""] * len(df), index=df.index)
        fn = _norm_data if c in colunas_data else _norm_valor
        partes.append(serie.map(fn))
    if not partes:
        return pd.Series([""] * len(df), index=df.index)
    return pd.concat(partes, axis=1).agg("|".join, axis=1)


def _para_celula(v):
    if v is None:
        return ""
    if isinstance(v, float) and math.isnan(v):
        return ""
    if v is pd.NaT:
        return ""
    if isinstance(v, (pd.Timestamp, datetime)):
        if getattr(v, "tzinfo", None) is not None:
            v = v.tz_convert(None) if isinstance(v, pd.Timestamp) else v.replace(tzinfo=None)
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if hasattr(v, "item"):
        return _para_celula(v.item())
    if isinstance(v, (dict, list)):
        return json.dumps(v, ensure_ascii=False)
    return v


def anexar_novos(planilha_id, df, chaves, nome="tabela", **kwargs):
    return com_pendencia(nome, _anexar_novos, planilha_id, df, chaves, nome=nome, **kwargs)


def _anexar_novos(
    planilha_id,
    df,
    chaves,
    nome="tabela",
    nome_aba="Sheet1",
    colunas_data=(),
    lote=5000,
    value_input_option="USER_ENTERED",
    limite_linhas=None,
):
    """
    Acrescenta ao final da aba apenas as linhas cuja chave ainda não existe.

    * limite_linhas: se a aba passar desse número de linhas, o arquivo
      inteiro é arquivado como '<nome> revNNN' e a aba ativa é esvaziada
      (mantendo cabeçalho e o mesmo ID) antes de gravar as linhas novas.

    * Compara as chaves normalizadas (datas -> 'AAAA-MM-DD HH:MM:SS',
      123.0 -> '123', etc.), evitando duplicatas por diferença de tipo.
    * Alinha as colunas pelo cabeçalho que já está na planilha. A estrutura
      da planilha nunca é alterada (colunas novas não são criadas).
    * Usa append (a própria API encontra a última linha).
    * value_input_option="RAW" grava tudo como texto (sem o Sheets
      converter datas/números).
    """
    if df is None or df.empty:
        log.warning("[%s] nada para anexar", nome)
        return 0

    colunas_data = set(colunas_data)
    erro_rotacao = None
    df = df.copy()
    df["_k"] = _chave(df, chaves, colunas_data)
    df = df.drop_duplicates("_k")

    aba = abrir_aba(planilha_id, nome_aba)
    valores = sheets_retry(
        aba.get_all_values, value_render_option="UNFORMATTED_VALUE"
    )

    if not valores or not any(str(c).strip() for c in valores[0]):
        log.info("[%s] aba vazia — gravando tabela inteira", nome)
        sheets_retry(set_with_dataframe, aba, df.drop(columns="_k"), resize=True)
        return len(df)

    largura = max(len(linha) for linha in valores)
    header = [str(c).strip() for c in valores[0]]
    header += [""] * (largura - len(header))

    # A ESTRUTURA DA PLANILHA NUNCA É ALTERADA (o cabeçalho não é reescrito).
    # Colunas do DataFrame que não estão no cabeçalho:
    #  * ocupam, só internamente, as colunas sem título no fim (dados antigos
    #    gravados sem cabeçalho, ex.: 'origem' no trends do online_skf), para
    #    os dados continuarem caindo na mesma posição de sempre;
    #  * as que sobrarem são descartadas com aviso no log.
    extras = [c for c in df.columns if c != "_k" and c not in header]
    if extras:
        vagas = [i for i, h in enumerate(header) if not h]
        mapeadas = []
        for col in list(extras):
            if vagas:
                header[vagas.pop(0)] = col
                mapeadas.append(col)
                extras.remove(col)
        if mapeadas:
            log.info("[%s] colunas sem título usadas para: %s", nome, mapeadas)
        if extras:
            log.warning(
                "[%s] colunas fora do cabeçalho da planilha (NÃO gravadas, "
                "estrutura mantida): %s", nome, extras,
            )

    nomes = [h if h else f"__sem_nome_{i}" for i, h in enumerate(header)]
    corpo = [list(linha) + [""] * (len(nomes) - len(linha)) for linha in valores[1:]]
    existente = pd.DataFrame(corpo, columns=nomes)
    existente = existente.loc[:, ~existente.columns.duplicated()]
    chaves_existentes = set(_chave(existente, chaves, colunas_data))

    novos = df[~df["_k"].isin(chaves_existentes)].drop(columns="_k")
    if novos.empty:
        log.info("[%s] nenhuma linha nova", nome)
        return 0

    # descarta colunas vazias sem título no fim (não precisam ser gravadas)
    while header and not header[-1]:
        header.pop()

    if limite_linhas and len(corpo) + len(novos) > limite_linhas:
        log.info(
            "[%s] %d linhas + %d novas passam do limite de %d — arquivando revisão",
            nome, len(corpo), len(novos), limite_linhas,
        )
        try:
            rotacionar(planilha_id, nome, nome_aba, linhas_esperadas=len(corpo))
            aba = abrir_aba(planilha_id, nome_aba)
            corpo = []
        except Exception as exc:  # noqa: BLE001
            # não arquivou: grava as linhas do dia mesmo assim (sem perder
            # dados) e avisa no fim, marcando a etapa como falha
            log.exception("[%s] arquivamento falhou, aba ativa intacta: %s", nome, exc)
            erro_rotacao = exc

    _checar_limite(len(corpo) + len(novos), len(header), nome)

    novos = novos.reindex(columns=header)
    linhas = [[_para_celula(v) for v in linha] for linha in novos.itertuples(index=False)]

    for i in range(0, len(linhas), lote):
        sheets_retry(
            aba.append_rows,
            linhas[i : i + lote],
            value_input_option=value_input_option,
            table_range="A1",
        )
    log.info("[%s] %d linhas novas anexadas", nome, len(linhas))
    if erro_rotacao is not None:
        raise ErroRevisao(
            f"[{nome}] linhas gravadas, mas o arquivamento (revNNN) falhou: {erro_rotacao}"
        )
    return len(linhas)


# ---------------------------------------------------------------------------
# Revisões (arquivo morto): <nome> rev000, rev001, ...
# ---------------------------------------------------------------------------


class ErroRevisao(RuntimeError):
    pass


def _drive_get(file_id, campos="id,name,parents"):
    r = sheets_retry(
        gc().http_client.request, "get",
        f"https://www.googleapis.com/drive/v3/files/{file_id}",
        params={"fields": campos, "supportsAllDrives": "true"},
    )
    return r.json()


def _url_apps_script():
    """URL do Apps Script de apoio: secret APPS_SCRIPT_URL ou arquivo apps_script_url.txt."""
    url = (os.getenv("APPS_SCRIPT_URL") or "").strip()
    if not url:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), "apps_script_url.txt")
        if os.path.exists(caminho):
            with open(caminho, encoding="utf-8") as f:
                url = f.read().strip()
    return url


def _apps_script(**params):
    """
    Chama o Apps Script de apoio (apps_script/revisoes.gs), que roda na conta
    do dono das planilhas. A autenticação é um token de identidade assinado
    pelo Google para a conta de serviço (nenhum segredo extra é necessário).
    """
    url = _url_apps_script()
    if not url:
        raise ErroRevisao(
            "Cópia indisponível: a conta de serviço não tem espaço no Drive e o Apps Script "
            "de apoio ainda não foi configurado (arquivo apps_script_url.txt)"
        )
    from google.auth.transport.requests import Request
    from google.oauth2 import service_account

    cred = service_account.IDTokenCredentials.from_service_account_info(
        env_json("GOOGLE_SERVICE_ACCOUNT"), target_audience=url
    )
    cred.refresh(Request())
    resp = http("GET", url, params={**params, "id_token": cred.token}, tentativas=4)
    try:
        dados = resp.json()
    except ValueError as exc:
        raise ErroRevisao("Apps Script não devolveu JSON (verifique a implantação/URL)") from exc
    if not dados.get("ok"):
        raise ErroRevisao(f"Apps Script recusou: {dados.get('erro')}")
    return dados


def _email_conta_servico():
    try:
        return env_json("GOOGLE_SERVICE_ACCOUNT").get("client_email", "")
    except Exception:  # noqa: BLE001
        return ""


def copiar_planilha(planilha_id, novo_nome):
    """
    Copia o arquivo inteiro para a mesma pasta. Devolve o ID da cópia.
    Usa o Apps Script de apoio quando configurado (a conta de serviço não tem
    espaço no Drive); sem ele, tenta direto pela API (funciona em Drive
    compartilhado).
    """
    if _url_apps_script():
        return _apps_script(
            acao="copiar", id=planilha_id, nome=novo_nome, leitor=_email_conta_servico()
        )["id"]
    info = _drive_get(planilha_id)
    corpo = {"name": novo_nome}
    if info.get("parents"):
        corpo["parents"] = info["parents"]
    r = sheets_retry(
        gc().http_client.request, "post",
        f"https://www.googleapis.com/drive/v3/files/{planilha_id}/copy",
        params={"supportsAllDrives": "true", "fields": "id,name"},
        json=corpo,
    )
    return r.json()["id"]


def excluir_planilha(planilha_id):
    """Só usado para apagar cópias de teste."""
    if _url_apps_script():
        _apps_script(acao="apagar_teste", id=planilha_id)
        return
    sheets_retry(
        gc().http_client.request, "delete",
        f"https://www.googleapis.com/drive/v3/files/{planilha_id}",
        params={"supportsAllDrives": "true"},
    )


def proximo_nome_revisao(planilha_id):
    info = _drive_get(planilha_id)
    base = info["name"]
    pasta = (info.get("parents") or [None])[0]
    q = f"name contains '{base.replace(chr(39), chr(92) + chr(39))} rev' and trashed = false"
    if pasta:
        q += f" and '{pasta}' in parents"
    r = sheets_retry(
        gc().http_client.request, "get",
        "https://www.googleapis.com/drive/v3/files",
        params={"q": q, "fields": "files(name)", "pageSize": 1000,
                "supportsAllDrives": "true", "includeItemsFromAllDrives": "true"},
    )
    numeros = []
    for f in r.json().get("files", []):
        m = re.fullmatch(re.escape(base) + r" rev(\d{3,})", f["name"])
        if m:
            numeros.append(int(m.group(1)))
    return f"{base} rev{(max(numeros) + 1) if numeros else 0:03d}"


def _contar_linhas(aba):
    """Linhas de dados (sem o cabeçalho), pela última linha preenchida de qualquer coluna."""
    valores = sheets_retry(aba.get_all_values)
    while valores and not any(str(c).strip() for c in valores[-1]):
        valores.pop()
    return max(len(valores) - 1, 0)


def rotacionar(planilha_id, nome, nome_aba="Sheet1", linhas_esperadas=None):
    """
    1. copia o arquivo inteiro como '<nome do arquivo> revNNN';
    2. confere se a cópia tem as mesmas linhas da aba ativa;
    3. só então esvazia a aba ativa (mantém o cabeçalho e o mesmo ID).
    Se qualquer passo falhar, a aba ativa NÃO é alterada.
    """
    aba = abrir_aba(planilha_id, nome_aba)
    originais = _contar_linhas(aba) if linhas_esperadas is None else linhas_esperadas
    nome_rev = proximo_nome_revisao(planilha_id)
    copia_id = copiar_planilha(planilha_id, nome_rev)

    copia = sheets_retry(gc().open_by_key, copia_id)
    aba_copia = sheets_retry(copia.worksheet, nome_aba)
    na_copia = _contar_linhas(aba_copia)
    if na_copia != originais:
        raise ErroRevisao(
            f"[{nome}] cópia '{nome_rev}' tem {na_copia} linhas, original {originais} — "
            "aba ativa NÃO foi esvaziada"
        )
    log.info("[%s] arquivo arquivado como '%s' (%d linhas conferidas)", nome, nome_rev, na_copia)

    sheets_retry(aba.resize, rows=2)
    sheets_retry(aba.batch_clear, ["A2:2"])
    log.info("[%s] aba ativa esvaziada (cabeçalho mantido)", nome)
    return nome_rev, copia_id


# ---------------------------------------------------------------------------
# Execução por etapas
# ---------------------------------------------------------------------------


class Pipeline:
    """
    Executa etapas de forma isolada. Uma etapa que falha é registrada e as
    que dependem dela são puladas; as independentes continuam rodando.
    """

    def __init__(self, nome):
        self.nome = nome
        # ETAPAS=assets,measurements -> roda só essas (execução manual)
        self.somente = {e.strip() for e in (os.getenv("ETAPAS") or "").split(",") if e.strip()}
        self.resultados = {}
        self.falhas = {}
        self.inicio = time.time()

    def etapa(self, nome, func, depende_de=()):
        if self.somente and nome not in self.somente:
            log.info("[%s] não solicitada nesta execução (ETAPAS)", nome)
            return None
        faltando = [d for d in depende_de if d not in self.resultados]
        if faltando:
            log.error("[%s] PULADA — depende de etapa(s) com falha: %s", nome, faltando)
            self.falhas[nome] = f"pulada (dependência: {', '.join(faltando)})"
            return None
        t0 = time.time()
        log.info("===== [%s] início =====", nome)
        try:
            resultado = func()
        except Exception as exc:  # noqa: BLE001 — queremos isolar qualquer falha
            log.exception("[%s] FALHOU: %s", nome, exc)
            self.falhas[nome] = f"{type(exc).__name__}: {exc}"
            return None
        self.resultados[nome] = resultado
        log.info("===== [%s] ok em %.0fs =====", nome, time.time() - t0)
        return resultado

    def _reprocessar_pendentes(self, espera=300):
        if not PENDENTES:
            return
        log.warning(
            "%d gravação(ões) no Sheets pendente(s) — aguardando %ds e tentando de novo",
            len(PENDENTES), espera,
        )
        time.sleep(espera)
        _abas.clear()  # reabre as planilhas do zero
        while PENDENTES:
            nome, func, args, kwargs = PENDENTES.pop(0)
            try:
                func(*args, **kwargs)
                log.info("[%s] gravação pendente concluída", nome)
            except Exception as exc:  # noqa: BLE001
                log.exception("[%s] gravação pendente falhou de novo: %s", nome, exc)
                self.falhas[f"{nome} (gravação no Sheets)"] = f"{type(exc).__name__}: {exc}"

    def finalizar(self):
        self._reprocessar_pendentes()
        dur = time.time() - self.inicio
        log.info("Resumo %s (%.0f min):", self.nome, dur / 60)
        for nome in self.resultados:
            log.info("  OK     %s", nome)
        for nome, erro in self.falhas.items():
            log.error("  FALHA  %s — %s", nome, erro)

        resumo = os.getenv("GITHUB_STEP_SUMMARY")
        if resumo:
            with open(resumo, "a", encoding="utf-8") as f:
                f.write(f"## {self.nome}\n\n| etapa | status |\n|---|---|\n")
                for nome in self.resultados:
                    f.write(f"| {nome} | ✅ |\n")
                for nome, erro in self.falhas.items():
                    erro_md = erro.replace("|", "/").replace("\n", " ")[:300]
                    f.write(f"| {nome} | ❌ {erro_md} |\n")

        if self.falhas:
            sys.exit(1)
