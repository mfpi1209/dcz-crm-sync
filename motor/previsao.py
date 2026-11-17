"""Comportamento e previsão: o que o histórico de pagamento diz sobre o ciclo corrente.

  comportamento por estado   de cada estado, quantos pagaram em dia, tarde ou não
                             pagaram no ciclo seguinte (ciclos já fechados do semestre)
  previsão do ciclo          aplica essas taxas aos alunos de hoje: quantos devem
                             pagar em dia, tarde e não pagar, com margem
  ritmo                      quanto já deveria ter sido pago até hoje, pela curva
                             dos ciclos fechados, contra o que foi pago de fato

Sem amostra suficiente (menos de 30 alunos num estado) a taxa do estado não é
usada: entra a taxa geral, e sem ela a previsão fica em branco. Nada aqui muda
decisão do motor; é leitura para quem aprova o plano.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import sqrt
from typing import Iterable, Mapping, Optional

from motor.calendario import ciclo_de, dias_uteis_entre, prazo_efetivo, semestre_de
from motor.modelos import Classe, Estado
from motor.resultados import classes_por_aluno, estado_no_ciclo

AMOSTRA_MIN = 30
DESFECHOS = (Classe.EM_DIA.value, Classe.TARDIO.value, Classe.NAO_PAGOU.value)
CURVA_DE, CURVA_ATE = -10, 15   # dias úteis em relação ao prazo efetivo do dia 10


def _deslocamento(prazo: date, dia: date, feriados: frozenset[date]) -> int:
    """Dias úteis de `dia` em relação ao prazo (negativo = antes)."""
    return dias_uteis_entre(prazo, dia, feriados) if dia >= prazo else -dias_uteis_entre(dia, prazo, feriados)


def _taxas(contagem: Mapping[str, int]) -> Optional[dict]:
    n = sum(contagem.get(d, 0) for d in DESFECHOS)
    if n < AMOSTRA_MIN:
        return None
    return {d: contagem.get(d, 0) / n for d in DESFECHOS}


def comportamento(linhas: Iterable[Mapping], *, hoje: date, folga_du: int = 3, feriados: frozenset[date] = frozenset()) -> dict:
    """Comportamento por estado, curva de pagamento, previsão do ciclo corrente e ritmo."""
    atual = ciclo_de(hoje)
    linhas = [l for l in linhas if semestre_de(l["ciclo"]) == semestre_de(atual) and l["ciclo"] <= atual]
    classes = classes_por_aluno(linhas, hoje=hoje, folga_du=folga_du, feriados=feriados)

    # 1. De cada estado, como o aluno fechou o ciclo seguinte.
    por_estado: dict[str, dict[str, int]] = {e.value: defaultdict(int) for e in Estado}
    geral: dict[str, int] = defaultdict(int)
    curva = [0] * (CURVA_ATE - CURVA_DE + 1)
    base_curva = 0
    fechados = sorted({l["ciclo"] for l in linhas if l["ciclo"] < atual})
    for l in linhas:
        if l["ciclo"] >= atual:
            continue
        classe = classes[l["contact_id"]][l["ciclo"]].value
        por_estado[estado_no_ciclo(classes[l["contact_id"]], l["ciclo"]).value][classe] += 1
        geral[classe] += 1
        base_curva += 1
        baixa = l.get("baixa_dia10_em")
        if baixa is not None:
            d = _deslocamento(prazo_efetivo(l["ciclo"], 10, feriados), baixa, feriados)
            curva[max(CURVA_DE, min(CURVA_ATE, d)) - CURVA_DE] += 1
    acumulada, soma = [], 0
    for qtd in curva:
        soma += qtd
        acumulada.append(round(soma / base_curva, 4) if base_curva else None)

    taxa_geral = _taxas(geral)
    estados = []
    for e in Estado:
        c = por_estado[e.value]
        n = sum(c.get(d, 0) for d in DESFECHOS)
        t = _taxas(c)
        estados.append({
            "estado": e.value, "alunos": n, "amostra_suficiente": t is not None,
            **{d: (round(t[d], 4) if t else None) for d in DESFECHOS},
        })

    # 2. Previsão do ciclo corrente, aluno a aluno, pela taxa do estado dele.
    esperado = {d: 0.0 for d in DESFECHOS}
    variancia = {d: 0.0 for d in DESFECHOS}
    por_estado_atual: dict[str, int] = defaultdict(int)
    sem_taxa = pagos = alunos = 0
    for l in linhas:
        if l["ciclo"] != atual:
            continue
        alunos += 1
        pagos += l.get("baixa_dia10_em") is not None
        estado = estado_no_ciclo(classes[l["contact_id"]], atual).value
        por_estado_atual[estado] += 1
        t = _taxas(por_estado[estado]) or taxa_geral
        if t is None:
            sem_taxa += 1
            continue
        for d in DESFECHOS:
            esperado[d] += t[d]
            variancia[d] += t[d] * (1 - t[d])
    previsao = None
    if alunos and sem_taxa < alunos:
        previsao = {
            d: {"esperado": int(round(esperado[d])), "margem": int(round(1.96 * sqrt(variancia[d])))} for d in DESFECHOS
        }
        previsao["alunos_sem_taxa"] = sem_taxa

    # 3. Ritmo: pela curva dos ciclos fechados, quanto já deveria ter sido pago até hoje.
    ritmo = None
    if alunos and base_curva >= AMOSTRA_MIN:
        d = _deslocamento(prazo_efetivo(atual, 10, feriados), hoje, feriados)
        fracao = 0.0 if d < CURVA_DE else acumulada[min(CURVA_ATE, d) - CURVA_DE]
        ritmo = {
            "dias_uteis_do_prazo": d, "esperado_ate_hoje": int(round(fracao * alunos)), "pagaram_ate_hoje": pagos,
            "diferenca": pagos - int(round(fracao * alunos)),
        }
    return {
        "ciclo": atual, "ciclos_de_base": fechados, "amostra_minima": AMOSTRA_MIN,
        "alunos_no_ciclo": alunos, "pagaram_ate_hoje": pagos, "por_estado_atual": dict(por_estado_atual),
        "por_estado": estados,
        "geral": {d: (round(taxa_geral[d], 4) if taxa_geral else None) for d in DESFECHOS},
        "curva": {"de": CURVA_DE, "ate": CURVA_ATE, "acumulada": acumulada, "alunos": base_curva},
        "previsao": previsao, "ritmo": ritmo,
    }
