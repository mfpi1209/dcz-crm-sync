"""Importação do histórico de pagamento de ciclos já fechados.

Sem histórico, todo aluno começa em "Sem histórico" e a previsão só aparece
depois do primeiro ciclo completo. Este módulo lê um CSV exportado do CRM
(histórico do campo "Dia 10") e prepara as linhas de `motor_aluno_ciclo`.

CSV, com cabeçalho, separado por vírgula ou ponto e vírgula:

    contact_id,ciclo,baixa_em
    c123,2026-09,2026-09-08
    c123,2026-10,            (vazio = fechou o ciclo sem baixa)

Regras:
  - só entram ciclos ANTERIORES ao ciclo corrente (o corrente é do motor);
  - linha repetida do mesmo aluno e ciclo: vale a baixa mais antiga;
  - baixa fora do mês do ciclo ou data inválida: a linha é recusada e contada;
  - ciclo importado nunca tem grupo de controle (não houve sorteio).
Quem grava é `scripts/motor_importar_historico.py`, que nunca sobrescreve o que
o motor já registrou.
"""
from __future__ import annotations

import csv
import io
import re
from datetime import date
from typing import Optional

from motor.calendario import ciclo_de, data_do_ciclo

CICLO = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
COLUNAS = ("contact_id", "ciclo", "baixa_em")


def ler_csv(texto: str, *, hoje: date) -> tuple[list[dict], dict[str, int]]:
    """Devolve as linhas prontas para gravar e a contagem do que ficou de fora, por motivo."""
    atual = ciclo_de(hoje)
    fora: dict[str, int] = {}

    def recusar(motivo: str) -> None:
        fora[motivo] = fora.get(motivo, 0) + 1

    amostra = texto[:2000]
    leitor = csv.DictReader(io.StringIO(texto.lstrip("﻿")), delimiter=";" if amostra.count(";") > amostra.count(",") else ",")
    campos = [c.strip().lower() for c in (leitor.fieldnames or [])]
    if any(c not in campos for c in COLUNAS):
        raise ValueError("cabeçalho esperado: " + ",".join(COLUNAS))
    leitor.fieldnames = campos
    por_chave: dict[tuple[str, str], Optional[date]] = {}
    for linha in leitor:
        contato, ciclo, baixa_txt = ((linha.get(c) or "").strip() for c in COLUNAS)
        if not contato:
            recusar("sem contact_id")
            continue
        if not CICLO.match(ciclo):
            recusar("ciclo inválido")
            continue
        if ciclo >= atual:
            recusar("ciclo corrente ou futuro")
            continue
        baixa: Optional[date] = None
        if baixa_txt:
            try:
                baixa = date.fromisoformat(baixa_txt[:10])
            except ValueError:
                recusar("data de baixa inválida")
                continue
            if ciclo_de(baixa) != ciclo:
                recusar("baixa fora do mês do ciclo")
                continue
        chave = (contato, ciclo)
        if chave in por_chave:
            recusar("repetida")
            anterior = por_chave[chave]
            baixa = min(d for d in (anterior, baixa) if d is not None) if (anterior or baixa) else None
        por_chave[chave] = baixa
    linhas = [
        {"contact_id": c, "ciclo": ciclo, "aberto_em": data_do_ciclo(ciclo, 1), "baixa_dia10_em": baixa, "controle": False}
        for (c, ciclo), baixa in sorted(por_chave.items())
    ]
    return linhas, fora


def resumo(linhas: list[dict]) -> dict[str, dict[str, int]]:
    """Por ciclo: alunos e quantos têm baixa."""
    out: dict[str, dict[str, int]] = {}
    for l in linhas:
        r = out.setdefault(l["ciclo"], {"alunos": 0, "com_baixa": 0})
        r["alunos"] += 1
        r["com_baixa"] += l["baixa_dia10_em"] is not None
    return dict(sorted(out.items()))
