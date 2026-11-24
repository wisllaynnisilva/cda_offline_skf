# -*- coding: utf-8 -*-
"""
Trava da carga diária: o workflow tem 3 horários agendados por dia (o
agendador do GitHub atrasa e às vezes descarta disparos). Só a PRIMEIRA
carga diária do dia (horário de Brasília) roda; as outras saem sem fazer nada.
Execuções manuais de reprocessamento (com data/etapas) não contam.
Escreve pular=true|false em $GITHUB_OUTPUT.
"""
import json
import os
import urllib.request
from datetime import datetime, timedelta, timezone

BRT = timezone(timedelta(hours=-3))
repo = os.environ["GITHUB_REPOSITORY"]
run_id = int(os.environ["GITHUB_RUN_ID"])
token = os.environ["GH_TOKEN"]

hoje = datetime.now(BRT).date()
inicio = datetime(hoje.year, hoje.month, hoje.day, tzinfo=BRT).astimezone(timezone.utc)
url = (f"https://api.github.com/repos/{repo}/actions/workflows/main.yml/runs"
       f"?per_page=50&created=>={inicio.strftime('%Y-%m-%dT%H:%M:%SZ')}")
req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}",
                                           "Accept": "application/vnd.github+json"})
pular = False
try:
    with urllib.request.urlopen(req, timeout=30) as r:
        runs = json.load(r)["workflow_runs"]
    anteriores = [
        x for x in runs
        if x["id"] != run_id and x["id"] < run_id
        and x.get("display_title") == "carga diária"
        and x.get("conclusion") not in ("cancelled",)
    ]
    pular = bool(anteriores)
    print(f"Cargas diárias anteriores hoje ({hoje}): {len(anteriores)}")
except Exception as exc:  # na dúvida, roda
    print(f"Não consegui consultar execuções anteriores ({exc}); seguindo com a carga")

print("PULAR: a carga de hoje já foi feita" if pular else "Rodando a carga diária")
with open(os.environ["GITHUB_OUTPUT"], "a") as f:
    f.write(f"pular={'true' if pular else 'false'}\n")
