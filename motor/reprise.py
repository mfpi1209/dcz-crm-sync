"""Reprise de uma rodada real: o motor refaz o dia, mais cedo e dosando pela fila.

Nada é enviado e o CRM não é chamado. A entrada é um arquivo de rodada real, sem
identificador de contato, sem texto e sem nome de atendente:

  destinatarios                     na ordem real de envio; por destinatário,
                                    `envio_s` (segundos desde a meia-noite de
                                    Brasília) e `humano_s` (segundos do envio até
                                    a conversa chegar a um atendente; null = não chegou)
  fila_humana_de_fundo_por_minuto   conversas que entraram na fila humana sem
                                    relação com a rodada (1.440 posições)
  atendentes_ativos_por_meia_hora   48 posições

O relógio é simulado, minuto a minuto. A fila é a de fundo mais os alunos da
rodada que chegam a atendente conforme o motor envia; a equipe atende em ordem
de chegada. Quem decide liberar, segurar e pausar é o governador de verdade
(`motor.governador.decidir`), com os limites da tela.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from motor.calendario import BRT
from motor.config import (
    FILA_SOBE_PAUSA_ABSOLUTO,
    FILA_SOBE_SEGURA_ABSOLUTO,
    JANELA_MAIS_CEDO_H,
    LOTE_ABSOLUTO,
    TETO_DIARIO_ABSOLUTO,
    MotorConfig,
)
from motor.governador import decidir, tamanho_do_lote
from motor.modelos import Decisao, Fila, LoteAtivo

MINUTOS = 24 * 60
INICIO_MAIS_TARDE_MIN = 10 * 60   # regra do Marcelo (13/11): a reprise começa às 10h no máximo
CORTE_IA_MAX = 0.40
NOME_DE_RODADA = re.compile(r"^rodada_\d{4}-\d{2}-\d{2}\.json$")


@dataclass(frozen=True)
class Parametros:
    """O que a tela deixa mexer. Os padrões são os da simulação de referência de 13/11."""
    inicio_min: int = 600                           # 10h
    lotes: tuple[int, int, int] = (300, 600, 1200)  # 1º lote, 2º lote, demais
    pela_folga: bool = False                        # tamanho pela folga da equipe (fórmula do motor), com os lotes acima como teto
    fila_sobe_segura: int = 30
    fila_sobe_pausa: int = 60
    base: str = "dia"                               # "dia": nível de antes do 1º lote do dia; "lote": nível de quando o último lote saiu
    intervalo_min: int = 15                         # minutos entre o início de um lote e o do seguinte
    corte_ia: float = 0.0                           # parcela do que foi a atendente que a IA passaria a segurar
    atend_por_hora: float = 7.8                     # primeiros atendimentos por atendente por hora (calibrado em 10/11)
    atendentes_min: int = 5
    velocidade: int = 1                             # mensagens por segundo
    teto_dia: int = 0                               # 0 = a rodada inteira
    semente: int = 7


def _inteiro(v, padrao: int, minimo: int, maximo: int) -> int:
    try:
        return max(minimo, min(maximo, int(float(v))))
    except (TypeError, ValueError):
        return padrao


def _fracao(v, padrao: float, minimo: float, maximo: float) -> float:
    try:
        return max(minimo, min(maximo, float(v)))
    except (TypeError, ValueError):
        return padrao


def ler_parametros(d: Optional[dict]) -> Parametros:
    """Parâmetros vindos da tela, já dentro de limites que mantêm a conta de pé."""
    d = d or {}
    p = Parametros()
    inicio = p.inicio_min
    texto = str(d.get("inicio") or "").strip()
    if re.fullmatch(r"\d{1,2}:\d{2}", texto):
        h, m = (int(x) for x in texto.split(":"))
        inicio = h * 60 + m
    lotes = d.get("lotes") if isinstance(d.get("lotes"), (list, tuple)) else []
    lotes = [_inteiro(v, p.lotes[i], 50, 5000) for i, v in enumerate(list(lotes)[:3])] + list(p.lotes[len(lotes):3])
    return Parametros(
        inicio_min=max(JANELA_MAIS_CEDO_H * 60, min(INICIO_MAIS_TARDE_MIN, inicio)),
        lotes=(lotes[0], lotes[1], lotes[2]),
        pela_folga=bool(d.get("pela_folga")),
        fila_sobe_segura=_inteiro(d.get("fila_sobe_segura"), p.fila_sobe_segura, 1, 500),
        fila_sobe_pausa=_inteiro(d.get("fila_sobe_pausa"), p.fila_sobe_pausa, 1, 1000),
        base="lote" if d.get("base") == "lote" else "dia",
        intervalo_min=_inteiro(d.get("intervalo_min"), p.intervalo_min, 1, 240),
        corte_ia=_fracao(d.get("corte_ia"), p.corte_ia, 0.0, CORTE_IA_MAX),
        atend_por_hora=_fracao(d.get("atend_por_hora"), p.atend_por_hora, 0.5, 30.0),
        atendentes_min=_inteiro(d.get("atendentes_min"), p.atendentes_min, 0, 100),
        velocidade=_inteiro(d.get("velocidade"), p.velocidade, 1, 10),
        teto_dia=_inteiro(d.get("teto_dia"), p.teto_dia, 0, 1_000_000),
        semente=_inteiro(d.get("semente"), p.semente, 1, 2_147_483_646),
    )


def listar_rodadas(pasta: str) -> list[dict]:
    """Arquivos `rodada_AAAA-MM-DD.json` da pasta, com o cabeçalho de cada um."""
    if not pasta or not Path(pasta).is_dir():
        return []
    out = []
    for arq in sorted(Path(pasta).glob("rodada_*.json"), reverse=True):
        if not NOME_DE_RODADA.match(arq.name):
            continue
        try:
            d = json.loads(arq.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        out.append({
            "arquivo": arq.name, "dia": d.get("dia"), "enviados": d.get("enviados"),
            "foram_a_atendente": d.get("foram_a_atendente"), "campanhas": d.get("campanhas") or [],
        })
    return out


def carregar_rodada(pasta: str, arquivo: str) -> dict:
    if not NOME_DE_RODADA.match(arquivo or ""):
        raise ValueError("arquivo de rodada inválido")
    d = json.loads((Path(pasta) / arquivo).read_text(encoding="utf-8"))
    if len(d.get("fila_humana_de_fundo_por_minuto") or []) != MINUTOS or len(d.get("atendentes_ativos_por_meia_hora") or []) != 48:
        raise ValueError("arquivo de rodada fora do formato esperado")
    datetime.strptime(d["dia"], "%Y-%m-%d")
    return d


def _hm(minuto: Optional[float]) -> Optional[str]:
    if minuto is None:
        return None
    return f"{int(minuto) // 60:02d}:{int(minuto) % 60:02d}"


def _quantil(valores: list[float], p: float) -> Optional[int]:
    if not valores:
        return None
    s = sorted(valores)
    return int(round(s[min(len(s) - 1, int(p * len(s)))]))


class _FilaHumana:
    """Fila em ordem de chegada, atendida pela equipe real do dia (mesma conta da simulação de referência)."""

    def __init__(self, fundo: list[int], atendentes: list[int], atend_por_hora: float) -> None:
        self.fundo, self.atendentes, self.mu = fundo, atendentes, atend_por_hora / 60.0
        self.fila: list[tuple[float, int]] = []      # (minuto de chegada, 1 = aluno da rodada)
        self.pendentes: list[float] = []             # chegadas futuras de alunos da rodada
        self.credito = 0.0
        self.esperas: list[float] = []
        self.por_minuto = [0] * MINUTOS

    def online(self, m: int) -> int:
        return self.atendentes[m // 30]

    def avancar(self, m: int) -> int:
        """Chegadas e atendimentos do minuto `m`; devolve quantos ficaram esperando."""
        self.fila += [(float(m), 0)] * self.fundo[m]
        if self.pendentes:
            self.fila += [(a, 1) for a in self.pendentes if a < m + 1]
            self.pendentes = [a for a in self.pendentes if a >= m + 1]
        self.fila.sort(key=lambda c: c[0])
        self.credito = min(self.credito + self.online(m) * self.mu, max(1, self.online(m)))
        while self.credito >= 1 and self.fila:
            chegada, da_rodada = self.fila.pop(0)
            self.credito -= 1
            if da_rodada:
                self.esperas.append(m - chegada)
        self.por_minuto[m] = len(self.fila)
        return len(self.fila)

    def sem_atendimento(self) -> int:
        return sum(1 for _, da_rodada in self.fila if da_rodada)


def _indicadores(fila: _FilaHumana) -> dict:
    """Números do expediente (8h às 22h), para o motor e para o dia real."""
    trecho = fila.por_minuto[8 * 60:22 * 60]
    pico = max(trecho)
    return {
        "pico": pico,
        "hora_do_pico": _hm(8 * 60 + trecho.index(pico)),
        "min_fila_acima_de_30": sum(1 for x in fila.por_minuto if x > 30),
        "espera_p50_min": _quantil(fila.esperas, 0.5),
        "espera_p90_min": _quantil(fila.esperas, 0.9),
        "sem_atendimento_no_fim": fila.sem_atendimento(),
    }


def _config(p: Parametros) -> MotorConfig:
    return replace(
        MotorConfig(),
        fila_sobe_segura=p.fila_sobe_segura, fila_sobe_pausa=p.fila_sobe_pausa,
        fila_base_validade_min=MINUTOS if p.base == "dia" else MotorConfig().fila_base_validade_min,
        vida_min_lote_min=p.intervalo_min, atendentes_min=p.atendentes_min, atend_por_hora=p.atend_por_hora,
        rampa=(p.lotes[0], p.lotes[1]), lote_max=p.lotes[2], lote_min=min(MotorConfig().lote_min, p.lotes[0]),
    )


def _avisos(p: Parametros, cfg: MotorConfig, total: int) -> list[str]:
    """Onde a tela foi além do que o motor de verdade aceita hoje."""
    padrao, avisos = MotorConfig(), []
    if p.fila_sobe_segura > FILA_SOBE_SEGURA_ABSOLUTO or p.fila_sobe_pausa > FILA_SOBE_PAUSA_ABSOLUTO:
        avisos.append(f"No motor de verdade a trava não passa de +{FILA_SOBE_SEGURA_ABSOLUTO} para segurar e +{FILA_SOBE_PAUSA_ABSOLUTO} para pausar (teto em código).")
    if max(p.lotes) > LOTE_ABSOLUTO:
        avisos.append(f"No motor de verdade nenhum lote passa de {LOTE_ABSOLUTO:,} envios (teto em código).".replace(",", "."))
    teto = p.teto_dia or total
    if teto > padrao.teto_diario:
        avisos.append(
            f"Esta reprise envia até {teto:,} no dia. O motor de verdade vem com teto de {padrao.teto_diario:,} por dia "
            f"(máximo em código: {TETO_DIARIO_ABSOLUTO:,}) e divide o público nos 3 dias úteis antes do prazo.".replace(",", ".")
        )
    if p.base == "dia":
        avisos.append("A trava mede a subida desde antes do 1º lote do dia. No código do motor, hoje, ela mede desde o último lote (valendo 2 h): troque em \"Base da trava\" para comparar.")
    if p.intervalo_min != padrao.vida_min_lote_min:
        avisos.append(f"Intervalo entre lotes de {p.intervalo_min} min; o padrão do motor é {padrao.vida_min_lote_min} min.")
    if abs(p.atend_por_hora - padrao.atend_por_hora) > 1e-9 and p.pela_folga:
        avisos.append(f"A fórmula de folga do motor usa {padrao.atend_por_hora:g} atendimentos por atendente por hora; aqui está {p.atend_por_hora:g}.")
    return avisos


def _dia_real(rodada: dict, p: Parametros) -> tuple[dict, _FilaHumana, list[int]]:
    """O dia como aconteceu, passado pelo mesmo modelo de fila (para comparar igual com igual)."""
    fila = _FilaHumana(rodada["fila_humana_de_fundo_por_minuto"], rodada["atendentes_ativos_por_meia_hora"], p.atend_por_hora)
    dest = rodada["destinatarios"]
    fila.pendentes = [(x["envio_s"] + x["humano_s"]) / 60.0 for x in dest if x.get("humano_s") is not None]
    humanos = len(fila.pendentes)
    for m in range(MINUTOS):
        fila.avancar(m)
    envios = [0] * MINUTOS
    for x in dest:
        envios[min(MINUTOS - 1, int(x["envio_s"] // 60))] += 1
    resumo = {
        "enviados": len(dest), "primeiro_envio": _hm(dest[0]["envio_s"] / 60) if dest else None,
        "ultimo_envio": _hm(dest[-1]["envio_s"] / 60) if dest else None, "foram_a_atendente": humanos, **_indicadores(fila),
    }
    return resumo, fila, envios


def _causa(motivo: str) -> str:
    """Causa curta de um trecho segurando ou pausado, para juntar minutos seguidos na tela."""
    if motivo.startswith("fila subiu"):
        return "fila acima do limite de pausa" if "limite de pausa" in motivo else "fila acima do limite de segurar"
    if "minutos de vida" in motivo or "min de vida" in motivo:
        return "intervalo entre lotes"
    if "atendentes online" in motivo:
        return "poucos atendentes"
    return re.sub(r"\s+", " ", re.sub(r"\(.*?\)", "", motivo)).strip()


def reprisar(rodada: dict, p: Parametros) -> dict:
    """Refaz a rodada com o motor. Devolve lotes, fila, minutos segurando e pausado, e a comparação com o dia real."""
    cfg = _config(p)
    dest = rodada["destinatarios"]
    total = min(len(dest), p.teto_dia) if p.teto_dia else len(dest)
    zero = datetime.strptime(rodada["dia"], "%Y-%m-%d").replace(tzinfo=BRT)
    fila = _FilaHumana(rodada["fila_humana_de_fundo_por_minuto"], rodada["atendentes_ativos_por_meia_hora"], p.atend_por_hora)

    # Mesmo gerador da simulação de referência, para o corte da IA cair nos mesmos alunos.
    estado = p.semente
    atraso: list[Optional[float]] = []
    for x in dest[:total]:
        h = x.get("humano_s")
        if h is not None:
            estado = (estado * 16807) % 2147483647
            if estado / 2147483647 < p.corte_ia:
                h = None
        atraso.append(None if h is None else h / 60.0)

    por_minuto_max = 60 * p.velocidade
    enviados = resto = 0
    base_dia: Optional[int] = None
    ativo: Optional[LoteAtivo] = None
    lotes: list[dict] = []
    envios = [0] * MINUTOS
    trechos: list[dict] = []
    segurando = pausado = 0
    fim: Optional[int] = None
    humanos = 0

    def marcar(m: int, estado_txt: str, motivo: str) -> None:
        causa = _causa(motivo)
        if trechos and trechos[-1]["ate_min"] == m and trechos[-1]["estado"] == estado_txt and trechos[-1]["causa"] == causa:
            trechos[-1]["ate_min"] = m + 1
        else:
            trechos.append({"de_min": m, "ate_min": m + 1, "estado": estado_txt, "causa": causa, "motivo": motivo})

    for m in range(MINUTOS):
        esperando = fila.avancar(m)
        if m < p.inicio_min or enviados >= total:
            continue
        agora = zero + timedelta(minutes=m)
        leitura = Fila(aguardando=esperando, maior_espera_s=0, atendentes_online=fila.online(m), lida_em=agora)
        decisao, motivo = decidir(leitura, cfg, agora=agora, lote_ativo=ativo, campanha_em_andamento=resto > 0)
        if "fora da janela" in motivo:
            if m >= cfg.janela_fim_h * 60:
                break      # fim do expediente: a campanha do dia é cancelada e o que falta fica como não enviado
            continue
        if p.base == "dia":
            motivo = motivo.replace("desde o lote anterior", "desde antes do 1º lote").replace("desde o lote", "desde antes do 1º lote")
        if resto > 0:
            if decisao == Decisao.PAUSAR:
                pausado += 1
                marcar(m, "pausado", motivo)
                continue
            n = min(por_minuto_max, resto, total - enviados)
            for j in range(n):
                d = atraso[enviados + j]
                if d is not None:
                    fila.pendentes.append(m + j / por_minuto_max + d)
                    humanos += 1
            enviados += n
            resto -= n
            envios[m] = n
            lotes[-1]["enviados"] += n
            lotes[-1]["fim_min"] = m
            if enviados >= total:
                fim = m
            continue
        if decisao != Decisao.LIBERAR:
            if "sem lote novo hoje" in motivo:
                continue   # o dia acabou para lote novo: o que falta fica como não enviado
            segurando += 1
            marcar(m, "segurando", motivo)
            continue
        teto = p.lotes[min(len(lotes), 2)]
        if base_dia is None:
            base_dia = esperando
        nivel = base_dia if p.base == "dia" else esperando
        tamanho = min(teto, tamanho_do_lote(leitura, cfg, len(lotes), ativo.fila_base if ativo else None)) if p.pela_folga else teto
        tamanho = min(tamanho, total - enviados)
        if tamanho <= 0:
            segurando += 1
            marcar(m, "segurando", "sem folga da equipe para um lote")
            continue
        resto = tamanho
        ativo = LoteAtivo(lote_id=len(lotes) + 1, liberado_em=agora, fila_base=nivel)
        lotes.append({
            "n": len(lotes) + 1, "inicio_min": m, "fim_min": m, "tamanho": tamanho, "enviados": 0,
            "fila": esperando, "atendentes": fila.online(m), "motivo": motivo,
        })

    for lote in lotes:
        lote["inicio"], lote["fim"] = _hm(lote.pop("inicio_min")), _hm(lote.pop("fim_min"))
    for t in trechos:
        t["de"], t["ate"], t["minutos"] = _hm(t["de_min"]), _hm(t["ate_min"]), t["ate_min"] - t.pop("de_min")
        t.pop("ate_min")

    real, fila_real, envios_real = _dia_real(rodada, p)
    horas = []
    for h in range(8, 22):
        faixa = slice(h * 60, (h + 1) * 60)
        horas.append({
            "hora": h, "atendentes": max(rodada["atendentes_ativos_por_meia_hora"][h * 2:h * 2 + 2]),
            "motor_pico": max(fila.por_minuto[faixa]), "real_pico": max(fila_real.por_minuto[faixa]),
            "motor_enviados": sum(envios[faixa]), "real_enviados": sum(envios_real[faixa]),
        })
    motor = {
        "enviados": enviados, "nao_enviados": len(dest) - enviados, "lotes": len(lotes),
        "primeiro_envio": lotes[0]["inicio"] if lotes else None,
        "ultimo_envio": _hm(max((i for i, n in enumerate(envios) if n), default=None)),
        "terminou": fim is not None, "min_segurando": segurando, "min_pausado": pausado,
        "fila_base": base_dia, "foram_a_atendente": humanos, **_indicadores(fila),
    }
    return {
        "dia": rodada["dia"], "campanhas": rodada.get("campanhas") or [], "parametros": {**p.__dict__, "lotes": list(p.lotes), "inicio": _hm(p.inicio_min)},
        "motor": motor, "real": real, "lotes": lotes, "trechos": trechos, "horas": horas,
        "serie": {"de_min": 8 * 60, "motor": fila.por_minuto[8 * 60:22 * 60], "real": fila_real.por_minuto[8 * 60:22 * 60]},
        "avisos": _avisos(p, cfg, total),
    }
