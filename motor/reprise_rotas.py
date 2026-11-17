"""Roteiro das chamadas ao CRM que o motor faria num dia reprisado.

Não chama nada: traduz o resultado de `motor.reprise.reprisar` na sequência de
rotas da API pública do CRM que o envio real usaria (uma campanha por dia sobre
a etiqueta do dia, dosada por pausar e retomar). Serve para ver, com horário,
o que o motor faria no lugar da operação.
"""
from __future__ import annotations

from math import ceil

CAMPANHA = "/api/integrations/campaigns"
CONTROLE_PCT = 0.05
ESCRITA_POR_MIN = 50          # 60 chamadas por minuto, com 10 reservadas a fila, resultado e pausa
EXPEDIENTE = (9, 22)
LEITURAS_DA_FILA_POR_HORA = 120   # uma a cada 30 s


def _min(hm: str) -> int:
    h, m = hm.split(":")
    return int(h) * 60 + int(m)


def _hm(minuto: int) -> str:
    return f"{minuto // 60:02d}:{minuto % 60:02d}"


def chamadas_do_dia(resultado: dict) -> dict:
    """{"chamadas": [...], "resumo": {...}} para a tela. `tipo`: escrita, leitura ou pessoa."""
    lotes, motor = resultado["lotes"], resultado["motor"]
    total = motor["enviados"] + motor["nao_enviados"]
    linhas: list[dict] = []

    def por(minuto: int, metodo: str, rota: str, texto: str, tipo: str = "escrita", vezes: int = 1) -> None:
        linhas.append({"min": minuto, "hora": _hm(minuto), "metodo": metodo, "rota": rota, "descricao": texto, "tipo": tipo, "vezes": vezes})

    por(7 * 60, "GET", "/api/custom-fields?entity=deal", "Descobre os campos Dia 10, Financeira, Situação e Nível.", "leitura")
    por(7 * 60, "GET", "/api/deals", "Carga da base: negócios dos alunos, em páginas de até 1.000. Repetida a cada 30 min no expediente.", "leitura")
    por(7 * 60, "GET", "/api/tags", "Lê as etiquetas: a do dia, as de exclusão e a do controle.", "leitura")
    if not lotes:
        por(_min(resultado["parametros"]["inicio"]), "—", "", "Nenhum lote saiu neste dia: o motor não criou campanha.", "pessoa")
    else:
        inicio = _min(lotes[0]["inicio"])
        n_controle = int(round(total * CONTROLE_PCT))
        duracao = ceil(n_controle / ESCRITA_POR_MIN) if n_controle else 0
        aprovado = max(8 * 60 + 30, inicio - duracao - 10)
        por(aprovado, "—", "", "Uma pessoa aprova o plano do dia na página e informa a etiqueta do dia. É a única ação manual.", "pessoa")
        por(aprovado + 1, "POST", "/api/tags", "Cria a etiqueta do grupo de controle do mês, se ainda não existir.")
        if n_controle:
            por(aprovado + 1, "POST", "/api/contacts/{id}/tags",
                f"Etiqueta o grupo de controle: {n_controle:,} alunos, uma chamada por aluno (cerca de {duracao} min a {ESCRITA_POR_MIN} por minuto).".replace(",", "."),
                vezes=n_controle)
        por(inicio, "POST", CAMPANHA, "Cria a campanha do dia sobre a etiqueta do dia, excluindo \"Sem lembretes de mensalidade\", \"Sem mensagens automáticas\" e o controle.")
        por(inicio, "POST", CAMPANHA + "/{id}/launch", f"Lança a campanha e abre a 1ª janela: {lotes[0]['enviados']:,} envios.".replace(",", "."))
        for k, lote in enumerate(lotes):
            fim = _min(lote["fim"])
            ultimo = k == len(lotes) - 1
            if k:
                por(_min(lote["inicio"]), "POST", CAMPANHA + "/{id}/resume",
                    f"Retoma: {k + 1}ª janela, {lote['enviados']:,} envios (fila em {lote['fila']}, {lote['atendentes']} atendentes).".replace(",", "."))
            if not (ultimo and motor["terminou"]):
                por(fim + 1, "POST", CAMPANHA + "/{id}/pause", f"Pausa: a {k + 1}ª janela cumpriu a cota.")
        for t in resultado["trechos"]:
            if t["estado"] == "pausado":
                por(_min(t["de"]), "POST", CAMPANHA + "/{id}/pause", f"Pausa de proteção: {t['causa']}.")
                por(_min(t["ate"]), "POST", CAMPANHA + "/{id}/resume", "Retoma depois da pausa de proteção.")
        ultimo_envio = _min(motor["ultimo_envio"]) if motor["ultimo_envio"] else inicio
        if motor["terminou"]:
            por(ultimo_envio + 1, "GET", CAMPANHA + "/{id}", "A campanha terminou sozinha: o motor confere o estado final.", "leitura")
        else:
            por(EXPEDIENTE[1] * 60, "POST", CAMPANHA + "/{id}/cancel",
                f"Fim do expediente: cancela a campanha. {motor['nao_enviados']:,} alunos não receberam e voltam no próximo dia de envio.".replace(",", "."))
        por((ultimo_envio + 2) if motor["terminou"] else EXPEDIENTE[1] * 60 + 1, "GET", CAMPANHA + "/{id}/recipients",
            "Lê os destinatários, em páginas, e grava no livro-razão quem recebeu, quem falhou e quem ficou sem envio.", "leitura")
        por(inicio, "GET", CAMPANHA + "/{id}/recipients?status=…", "Durante cada janela: conta entregues, lidos e falhas para medir o lote.", "leitura")
    leituras_fila = (EXPEDIENTE[1] - EXPEDIENTE[0]) * LEITURAS_DA_FILA_POR_HORA
    por(EXPEDIENTE[0] * 60, "GET", "/api/integrations/service-queue?clock=business",
        "Lê a fila a cada 30 s durante o expediente: conversas aguardando e atendentes online. É o que decide liberar, segurar ou pausar.", "leitura", vezes=leituras_fila)
    linhas.sort(key=lambda l: (l["min"], l["tipo"] != "pessoa", l["metodo"] != "POST"))
    escritas = [l for l in linhas if l["tipo"] == "escrita"]
    return {
        "chamadas": linhas,
        "resumo": {
            "escritas": sum(l["vezes"] for l in escritas),
            "escritas_de_campanha": sum(1 for l in escritas if l["rota"].startswith(CAMPANHA)),
            "leituras_da_fila": leituras_fila,
            "acoes_de_pessoa": sum(1 for l in linhas if l["tipo"] == "pessoa" and l["rota"] == "" and "aprova" in l["descricao"]),
        },
    }
