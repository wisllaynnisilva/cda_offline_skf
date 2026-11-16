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


def sheets_retry(func, *args, tentativas=6, **kwargs):
    """Executa uma chamada do gspread com retry em 429/5xx/erros de rede."""
    for tentativa in range(1, tentativas + 1):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as exc:
            codigo = getattr(exc, "code", None)
            # -1 = resposta HTML (ex.: 502 do Google), não JSON
            retentavel = codigo in (-1, 408, 429) or (codigo or 0) >= 500
            if not retentavel or tentativa == tentativas:
                raise
            erro = f"APIError {codigo}"
        except ERROS_REDE as exc:
            if tentativa == tentativas:
                raise
            erro = type(exc).__name__
        espera = min(5 * 2 ** tentativa, 120)
        log.warning(
            "Sheets: %s (tentativa %d/%d) — aguardando %ds",
            erro, tentativa, tentativas, espera,
        )
        time.sleep(espera)


def abrir_aba(planilha_id, nome_aba="Sheet1"):
    planilha = sheets_retry(gc().open_by_key, planilha_id)
    return sheets_retry(planilha.worksheet, nome_aba)


def _checar_limite(df_linhas, df_colunas, nome):
    celulas = (df_linhas + 1) * max(df_colunas, 1)
    if celulas > LIMITE_CELULAS * 0.9:
        log.error(
            "[%s] %s células — perto/acima do limite de 10 milhões do Google "
            "Sheets. Migre esta tabela para um banco de dados.",
            nome, f"{celulas:,}",
        )


def gravar_aba(planilha_id, df, nome="tabela", nome_aba="Sheet1", permitir_vazio=False):
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


def anexar_novos(
    planilha_id,
    df,
    chaves,
    nome="tabela",
    nome_aba="Sheet1",
    colunas_data=(),
    lote=5000,
    value_input_option="USER_ENTERED",
):
    """
    Acrescenta ao final da aba apenas as linhas cuja chave ainda não existe.

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
    return len(linhas)


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
        self.resultados = {}
        self.falhas = {}
        self.inicio = time.time()

    def etapa(self, nome, func, depende_de=()):
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

    def finalizar(self):
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
