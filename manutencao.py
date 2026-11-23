# -*- coding: utf-8 -*-
"""
manutencao.py — auditoria (e, depois, correção) das planilhas do pipeline.

Modo "auditar" (padrão): SOMENTE LEITURA. Para cada planilha listada em
manutencao_tabelas.json mostra tamanho, cabeçalho, linhas desalinhadas,
textos "nan"/"NaT", duplicatas pela chave e o perfil de tipos de cada
coluna. Não imprime valores das células (o log do repositório é público).

Este arquivo é igual em todos os repositórios cda_*; só o
manutencao_tabelas.json muda.
"""

import json
import os
import re
from collections import Counter

import pandas as pd

from cda_utils import (
    LIMITE_CELULAS, _chave, _contar_linhas, abrir_aba, copiar_planilha,
    excluir_planilha, gc, log, sheets_retry,
)

AQUI = os.path.dirname(os.path.abspath(__file__))
TEXTOS_NULOS = {"nan", "NaN", "NaT", "None", "null", "<NA>"}
BLOCO = 50_000  # linhas lidas por chamada


def ler_tudo(aba):
    """Lê a aba inteira em blocos (planilhas grandes estouram uma leitura única)."""
    linhas_grade = aba.row_count
    valores = []
    for ini in range(1, linhas_grade + 1, BLOCO):
        fim = min(ini + BLOCO - 1, linhas_grade)
        parte = sheets_retry(
            aba.get, f"{ini}:{fim}", value_render_option="UNFORMATTED_VALUE"
        )
        valores.extend([list(l) for l in parte])
    while valores and not any(str(c).strip() for c in valores[-1]):
        valores.pop()
    return valores


def tipo(v):
    if v == "" or v is None:
        return "vazio"
    if isinstance(v, bool):
        return "bool"
    if isinstance(v, (int, float)):
        return "número"
    s = str(v).strip()
    if s in TEXTOS_NULOS:
        return "texto-nulo"
    if re.match(r"^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?", s) or "GMT" in s:
        return "data-texto"
    try:
        float(s.replace(",", "."))
        return "número-texto"
    except ValueError:
        return "texto"


def auditar(nome, cfg):
    planilha = sheets_retry(gc().open_by_key, cfg["id"])
    abas = sheets_retry(planilha.worksheets)
    grade_total = sum(a.row_count * a.col_count for a in abas)
    aba = abrir_aba(cfg["id"], cfg.get("aba", "Sheet1"))
    valores = ler_tudo(aba)

    r = {
        "tabela": nome,
        "abas_no_arquivo": [a.title for a in abas],
        "grade_arquivo_celulas": grade_total,
        "uso_do_limite_10M": f"{grade_total / LIMITE_CELULAS:.1%}",
        "linhas_dados": max(len(valores) - 1, 0),
    }
    if not valores:
        r["vazia"] = True
        return r

    header = [str(c).strip() for c in valores[0]]
    corpo = valores[1:]
    larguras = Counter(len(l) for l in corpo)
    r["cabecalho"] = header
    r["colunas_sem_titulo"] = [i for i, h in enumerate(header) if not h]
    r["colunas_repetidas"] = [h for h, n in Counter(header).items() if h and n > 1]
    r["linhas_mais_largas_que_cabecalho"] = sum(n for w, n in larguras.items() if w > len(header))
    r["larguras_das_linhas"] = dict(sorted(larguras.items()))
    r["linhas_totalmente_vazias"] = sum(1 for l in corpo if not any(str(c).strip() for c in l))

    largura = max([len(header)] + list(larguras))
    nomes = header + [""] * (largura - len(header))
    for i, extra in enumerate(cfg.get("colunas_sem_titulo", [])):
        vagas = [j for j, h in enumerate(nomes) if not h]
        if vagas:
            nomes[vagas[0]] = extra
    nomes = [h or f"__col{j}" for j, h in enumerate(nomes)]
    df = pd.DataFrame([l + [""] * (largura - len(l)) for l in corpo], columns=nomes)
    df = df.loc[:, ~df.columns.duplicated()]

    r["textos_nulos_por_coluna"] = {
        c: int(df[c].astype(str).isin(TEXTOS_NULOS).sum())
        for c in df.columns if df[c].astype(str).isin(TEXTOS_NULOS).any()
    }

    # perfil de tipos por coluna (contagens, sem valores)
    perfil = {}
    for c in df.columns:
        cont = Counter(tipo(v) for v in df[c])
        perfil[c] = {k: v for k, v in cont.most_common()}
    r["perfil_tipos"] = perfil

    # valores fora do domínio esperado (ex.: origem)
    for c, permitidos in (cfg.get("dominios") or {}).items():
        if c in df.columns:
            fora = df[c].astype(str).str.strip().str.lower()
            r[f"fora_do_dominio_{c}"] = int((~fora.isin([p.lower() for p in permitidos])).sum())

    # perfil de tipos por "época": ajuda a achar o ponto onde o desalinhamento começou
    if cfg.get("coluna_tempo_esperada") in df.columns and len(df) > 0:
        c = cfg["coluna_tempo_esperada"]
        # data serial do Sheets entre 2000 e 2050, ou texto de data
        bom = df[c].map(
            lambda v: (isinstance(v, (int, float)) and not isinstance(v, bool) and 36526 <= v <= 54789)
            or tipo(v) == "data-texto"
        )
        r["linhas_com_data_valida_em_" + c] = int(bom.sum())
        ruins = (~bom).to_numpy().nonzero()[0]
        if len(ruins):
            r["primeira_linha_ruim"] = int(ruins[0]) + 2
            r["ultima_linha_ruim"] = int(ruins[-1]) + 2

    chaves = cfg.get("chaves") or []
    if chaves:
        faltando = [c for c in chaves if c not in df.columns]
        r["chaves"] = chaves
        if faltando:
            r["chaves_ausentes_no_cabecalho"] = faltando
        k = _chave(df, chaves, set(cfg.get("colunas_data", [])))
        r["duplicatas_pela_chave"] = int(k.duplicated().sum())
    r["duplicatas_linha_inteira"] = int(df.astype(str).duplicated().sum())
    return r


def testar_copia(tabelas):
    """Copia a menor planilha marcada com "teste_copia", confere e apaga a cópia."""
    alvos = [(n, c) for n, c in tabelas.items() if c.get("teste_copia")]
    if not alvos:
        log.info("Nenhuma tabela marcada para teste de cópia neste repositório")
        return
    nome, cfg = alvos[0]
    aba = abrir_aba(cfg["id"], cfg.get("aba", "Sheet1"))
    originais = _contar_linhas(aba)
    try:
        copia_id = copiar_planilha(cfg["id"], f"{nome} TESTE-COPIA-apagar")
    except Exception as exc:  # noqa: BLE001
        print(f"TESTE_COPIA FALHOU ao copiar: {type(exc).__name__}: {str(exc)[:300]}")
        raise SystemExit(1)
    try:
        copia = sheets_retry(gc().open_by_key, copia_id)
        na_copia = _contar_linhas(sheets_retry(copia.worksheet, cfg.get("aba", "Sheet1")))
        print(f"TESTE_COPIA OK: original {originais} linhas, copia {na_copia} linhas")
    finally:
        excluir_planilha(copia_id)
        print("TESTE_COPIA: copia apagada")


def main():
    modo = (os.getenv("MODO") or "auditar").strip()
    with open(os.path.join(AQUI, "manutencao_tabelas.json"), encoding="utf-8") as f:
        tabelas = json.load(f)
    so = [t.strip() for t in (os.getenv("TABELAS") or "").split(",") if t.strip()]

    if modo == "testar_copia":
        testar_copia(tabelas)
        return
    if modo != "auditar":
        raise SystemExit(f"Modo '{modo}' ainda não implementado.")

    resultados = []
    for nome, cfg in tabelas.items():
        if so and nome not in so:
            continue
        try:
            r = auditar(nome, cfg)
        except Exception as exc:  # noqa: BLE001
            log.exception("[%s] falhou: %s", nome, exc)
            r = {"tabela": nome, "erro": f"{type(exc).__name__}: {exc}"}
        resultados.append(r)
        print("AUDITORIA " + json.dumps(r, ensure_ascii=False, default=str), flush=True)

    resumo = os.getenv("GITHUB_STEP_SUMMARY")
    if resumo:
        with open(resumo, "a", encoding="utf-8") as f:
            f.write("## Auditoria das planilhas\n\n| tabela | linhas | uso 10M | dup. chave | desalinhadas | nulos-texto |\n|---|---|---|---|---|---|\n")
            for r in resultados:
                f.write(
                    f"| {r['tabela']} | {r.get('linhas_dados', '-')} | {r.get('uso_do_limite_10M', '-')} | "
                    f"{r.get('duplicatas_pela_chave', '-')} | {r.get('linhas_mais_largas_que_cabecalho', '-')} | "
                    f"{sum((r.get('textos_nulos_por_coluna') or {}).values())} |\n"
                )


if __name__ == "__main__":
    main()
