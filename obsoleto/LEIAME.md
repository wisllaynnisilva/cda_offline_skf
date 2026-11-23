# Código original (obsoleto)

Esta pasta guarda a versão do pipeline que rodava **antes** da revisão de
robustez de 23/11/2026, sem nenhuma modificação. Está aqui só como
referência/backup e **não é executada**.

| Arquivo | Origem |
|---|---|
| `cda_offline_skf_original.py` | `cda_offline_skf.py` original |
| `requirements_original.txt` | `requirements.txt` original |
| `main_original.yml` | `.github/workflows/main.yml` original (fora da pasta `.github/workflows`, então o GitHub não o executa) |

Backup completo do repositório nesse estado: branch
[`backup/codigo-original`](../../../tree/backup/codigo-original) (commit `3088075`).

## Como voltar para a versão antiga, se precisar

Pelo GitHub: abra a branch `backup/codigo-original` e crie um pull request
dela para a `main`. Ou pelo terminal:

```bash
git checkout main
git checkout backup/codigo-original -- cda_offline_skf.py requirements.txt .github/workflows/main.yml
git rm -r cda_utils.py
git commit -m "Voltar para o código original"
git push
```
