"""Resultado do ciclo: quanto o motor mudou o pagamento, medido contra o grupo de controle.

A comparação é por intenção de tratar: de um lado todos os alunos fora do
controle (recebendo ou não mensagem), do outro os 5% sorteados que não recebem
nada. Como o sorteio é por aluno e não olha estado nem histórico, a diferença
entre os dois grupos é o efeito do motor, sem o viés de comparar quem recebeu
com quem não recebeu.

Funções puras: recebem linhas já lidas do livro-razão e devolvem números.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import date
from math import sqrt
from typing import Iterable, Mapping, Optional

from motor.calendario import ciclo_de, semestre_de
from motor.classificador import ROTULOS_DE_EXPERIENCIA
from motor.estados import calcular_estado, classificar_ciclo, corte_dia10, corte_dia25, historico_do_semestre
from motor.modelos import Classe, Estado
from motor.razao import NAO_SAIRAM

AMOSTRA_MIN_CONTROLE = 100   # abaixo disso a diferença é mostrada, mas não é dada como conclusiva
ENTREGUES = ("entregue", "lido")


def classes_por_aluno(linhas: Iterable[Mapping], *, hoje: date, folga_du: int = 3, feriados: frozenset[date] = frozenset()) -> dict[str, dict[str, Classe]]:
    """{aluno: {ciclo: classe}} a partir das linhas de `motor_aluno_ciclo`."""
    atual = ciclo_de(hoje)
    out: dict[str, dict[str, Classe]] = defaultdict(dict)
    for l in linhas:
        out[l["contact_id"]][l["ciclo"]] = classificar_ciclo(
            l["ciclo"], l.get("baixa_dia10_em"), encerrado=l["ciclo"] < atual, folga_du=folga_du, feriados=feriados,
        )
    return out


def estado_no_ciclo(classes: Mapping[str, Classe], ciclo: str) -> Estado:
    """Estado do aluno ao entrar no ciclo, só pelo histórico de pagamento do semestre."""
    return calcular_estado(historico_do_semestre(classes, ciclo))


def _taxa(parte: int, todo: int) -> Optional[float]:
    return round(parte / todo, 4) if todo else None


def comparar(pagou_motor: int, n_motor: int, pagou_controle: int, n_controle: int) -> dict:
    """Diferença entre duas proporções, em pontos percentuais, com intervalo de 95%."""
    if not n_motor or not n_controle:
        return {"diferenca_pp": None, "de_pp": None, "ate_pp": None, "conclusivo": False, "a_mais": None}
    p1, p2 = pagou_motor / n_motor, pagou_controle / n_controle
    erro = 1.96 * sqrt(p1 * (1 - p1) / n_motor + p2 * (1 - p2) / n_controle)
    de, ate = (p1 - p2 - erro) * 100, (p1 - p2 + erro) * 100
    return {
        "diferenca_pp": round((p1 - p2) * 100, 1), "de_pp": round(de, 1), "ate_pp": round(ate, 1),
        "conclusivo": n_controle >= AMOSTRA_MIN_CONTROLE and (de > 0 or ate < 0),
        "a_mais": int(round((p1 - p2) * n_motor)),   # alunos a mais pagando por causa do motor
    }


def _grupo() -> dict:
    return {"alunos": 0, "em_dia": 0, "ate_dia25": 0, "pagaram": 0}


def _fechar(g: dict) -> dict:
    return {**g, "taxa_em_dia": _taxa(g["em_dia"], g["alunos"]), "taxa_ate_dia25": _taxa(g["ate_dia25"], g["alunos"]), "taxa_pagaram": _taxa(g["pagaram"], g["alunos"])}


def resultado_do_ciclo(
    ciclo: str,
    linhas: Iterable[Mapping],
    envios: Iterable[Mapping],
    *,
    hoje: date,
    folga_du: int = 3,
    feriados: frozenset[date] = frozenset(),
) -> dict:
    """Números do ciclo. `linhas`: `motor_aluno_ciclo` do semestre; `envios`: `motor_mensagens` reais do ciclo."""
    linhas = [l for l in linhas if semestre_de(l["ciclo"]) == semestre_de(ciclo) and l["ciclo"] <= ciclo]
    classes = classes_por_aluno(linhas, hoje=hoje, folga_du=folga_du, feriados=feriados)
    c10, c25 = corte_dia10(ciclo, folga_du, feriados), corte_dia25(ciclo, folga_du, feriados)
    grupos = {"motor": _grupo(), "controle": _grupo()}
    por_estado: dict[str, dict[str, dict]] = {e.value: {"motor": _grupo(), "controle": _grupo()} for e in Estado}
    desfecho: dict[str, tuple[bool, bool]] = {}
    do_controle: set[str] = set()

    for l in linhas:
        if l["ciclo"] != ciclo:
            continue
        baixa = l.get("baixa_dia10_em")
        em_dia, ate25 = baixa is not None and baixa <= c10, baixa is not None and baixa <= c25
        desfecho[l["contact_id"]] = (em_dia, ate25)
        nome = "controle" if l.get("controle") else "motor"
        if l.get("controle"):
            do_controle.add(l["contact_id"])
        estado = estado_no_ciclo(classes[l["contact_id"]], ciclo).value
        for g in (grupos[nome], por_estado[estado][nome]):
            g["alunos"] += 1
            g["em_dia"] += em_dia
            g["ate_dia25"] += ate25
            g["pagaram"] += baixa is not None

    por_mensagem: dict[tuple[str, str], dict] = {}
    receberam: set[str] = set()
    rotulos: dict[str, int] = defaultdict(int)
    for e in envios:
        chave = (e["mensagem"], e.get("variante") or "A")
        m = por_mensagem.setdefault(chave, {
            "mensagem": chave[0], "variante": chave[1], "sairam": 0, "entregues": 0, "lidos": 0, "falhas": 0,
            "responderam": 0, "parar_ou_reclamou": 0, "em_dia": 0, "ate_dia25": 0,
        })
        status = e.get("status") or ""
        if status in NAO_SAIRAM or status == "simulado":
            continue
        m["sairam"] += 1
        m["falhas"] += status == "falhou"
        m["entregues"] += status in ENTREGUES
        m["lidos"] += status == "lido"
        if status != "falhou":
            receberam.add(e["contact_id"])
            em_dia, ate25 = desfecho.get(e["contact_id"], (False, False))
            m["em_dia"] += em_dia
            m["ate_dia25"] += ate25
        if e.get("respondeu_em") or e.get("classificacao"):
            m["responderam"] += 1
            rotulo = e.get("classificacao") or "aguardando"
            rotulos[rotulo] += 1
            m["parar_ou_reclamou"] += rotulo in ROTULOS_DE_EXPERIENCIA
    mensagens = []
    for m in sorted(por_mensagem.values(), key=lambda x: (x["mensagem"], x["variante"])):
        alcancados = m["sairam"] - m["falhas"]
        mensagens.append({
            **m, "taxa_entrega": _taxa(m["entregues"], m["sairam"]), "taxa_leitura": _taxa(m["lidos"], m["entregues"]),
            "taxa_resposta": _taxa(m["responderam"], m["entregues"]), "taxa_em_dia": _taxa(m["em_dia"], alcancados),
            "taxa_ate_dia25": _taxa(m["ate_dia25"], alcancados),
            "parar_por_mil": round(1000 * m["parar_ou_reclamou"] / m["entregues"], 1) if m["entregues"] else None,
        })

    gm, gc = grupos["motor"], grupos["controle"]
    estados = []
    for estado, g in por_estado.items():
        if g["motor"]["alunos"] or g["controle"]["alunos"]:
            estados.append({
                "estado": estado, "motor": _fechar(g["motor"]), "controle": _fechar(g["controle"]),
                "em_dia": comparar(g["motor"]["em_dia"], g["motor"]["alunos"], g["controle"]["em_dia"], g["controle"]["alunos"]),
            })
    return {
        "ciclo": ciclo,
        "corte_dia10": c10.isoformat(), "corte_dia25": c25.isoformat(),
        "dia10_fechado": hoje > c10, "dia25_fechado": hoje > c25,   # antes disso os números ainda sobem
        "motor": _fechar(gm), "controle": _fechar(gc),
        "em_dia": comparar(gm["em_dia"], gm["alunos"], gc["em_dia"], gc["alunos"]),
        "ate_dia25": comparar(gm["ate_dia25"], gm["alunos"], gc["ate_dia25"], gc["alunos"]),
        "amostra_minima_controle": AMOSTRA_MIN_CONTROLE,
        "cobertura": {
            "receberam": len(receberam - do_controle),
            "taxa": _taxa(len(receberam - do_controle), gm["alunos"]),
            "controle_que_recebeu": len(receberam & do_controle),   # tem de ser zero; acima disso o controle está contaminado
        },
        "por_estado": estados, "por_mensagem": mensagens,
        "respostas": dict(sorted(rotulos.items(), key=lambda kv: -kv[1])),
    }
