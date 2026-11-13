"""Reprise de rodada: o motor refaz um dia com o governador de verdade, sem enviar nada.

Rodadas de mentira, montadas aqui (nenhum dado real no repositório).
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from motor.reprise import Parametros, carregar_rodada, ler_parametros, listar_rodadas, reprisar


def _rodada(envios: int = 3000, a_cada: int = 40, atraso_s: int = 600, atendentes: int = 8, fundo_por_hora: int = 6, dia: str = "2026-11-10") -> dict:
    """Dia útil: `envios` disparados às 15h (1 por segundo); 1 a cada `a_cada` chega a atendente `atraso_s` depois."""
    dest = [
        {"envio_s": 15 * 3600 + i, "resposta_s": 60 if i % a_cada == 0 else None, "tipo": "texto" if i % a_cada == 0 else None,
         "ia_s": 70 if i % a_cada == 0 else None, "humano_s": atraso_s if i % a_cada == 0 else None}
        for i in range(envios)
    ]
    fundo = [1 if 8 * 60 <= m < 20 * 60 and m % (60 // fundo_por_hora) == 0 else 0 for m in range(1440)]
    equipe = [atendentes if 16 <= k < 44 else 0 for k in range(48)]
    return {
        "dia": dia, "campanhas": ["teste"], "enviados": envios, "foram_a_atendente": sum(1 for d in dest if d["humano_s"] is not None),
        "atendentes_ativos_por_meia_hora": equipe, "fila_humana_de_fundo_por_minuto": fundo, "destinatarios": dest,
    }


class ParametrosTest(unittest.TestCase):
    def test_padrao_e_o_da_simulacao_de_referencia(self) -> None:
        p = ler_parametros({})
        self.assertEqual(p, Parametros())
        self.assertEqual((p.inicio_min, p.lotes, p.fila_sobe_segura, p.fila_sobe_pausa, p.base, p.intervalo_min), (600, (300, 600, 1200), 30, 60, "dia", 15))
        self.assertEqual(p.atend_por_hora, 7.8)

    def test_inicio_fica_entre_9h_e_10h(self) -> None:
        self.assertEqual(ler_parametros({"inicio": "07:00"}).inicio_min, 540)
        self.assertEqual(ler_parametros({"inicio": "09:30"}).inicio_min, 570)
        self.assertEqual(ler_parametros({"inicio": "14:00"}).inicio_min, 600)
        self.assertEqual(ler_parametros({"inicio": "lixo"}).inicio_min, 600)

    def test_valores_da_tela_ficam_em_limites_que_mantem_a_conta(self) -> None:
        p = ler_parametros({"corte_ia": 0.9, "lotes": [10, "x"], "fila_sobe_segura": -4, "atend_por_hora": 0, "velocidade": 99, "base": "qualquer"})
        self.assertEqual(p.corte_ia, 0.40)
        self.assertEqual(p.lotes, (50, 600, 1200))
        self.assertEqual((p.fila_sobe_segura, p.atend_por_hora, p.velocidade, p.base), (1, 0.5, 10, "dia"))


class ReprisarTest(unittest.TestCase):
    def test_dia_folgado_sai_tudo_em_rampa_sem_segurar_pela_fila(self) -> None:
        r = reprisar(_rodada(envios=3000), Parametros())
        m = r["motor"]
        self.assertEqual((m["enviados"], m["nao_enviados"], m["terminou"]), (3000, 0, True))
        self.assertEqual([l["tamanho"] for l in r["lotes"]], [300, 600, 1200, 900])
        self.assertEqual(r["lotes"][0]["inicio"], "10:00")
        self.assertEqual(r["lotes"][1]["inicio"], "10:15")       # 15 min depois do início do anterior
        self.assertEqual(m["min_pausado"], 0)
        self.assertEqual({t["causa"] for t in r["trechos"]}, {"intervalo entre lotes"})
        self.assertEqual(m["foram_a_atendente"], r["real"]["foram_a_atendente"])
        self.assertEqual(r["real"]["primeiro_envio"], "15:00")

    def test_fila_que_sobe_segura_o_lote_seguinte_e_pausa_o_que_esta_saindo(self) -> None:
        # Muita gente indo a atendente, depressa, com equipe curta: a fila passa de +30 e de +60.
        rodada = _rodada(envios=6000, a_cada=4, atraso_s=120, atendentes=5)
        r = reprisar(rodada, Parametros())
        m = r["motor"]
        causas = {(t["estado"], t["causa"]) for t in r["trechos"]}
        self.assertIn(("segurando", "fila acima do limite de segurar"), causas)
        self.assertIn(("pausado", "fila acima do limite de pausa"), causas)
        self.assertGreater(m["min_pausado"], 0)
        self.assertLess(m["pico"], r["real"]["pico"])            # dosando, a fila sobe menos do que no disparo de uma vez
        self.assertEqual(m["enviados"] + m["nao_enviados"], 6000)
        # Trava mais folgada libera mais cedo: menos minutos parado.
        solto = reprisar(rodada, ler_parametros({"fila_sobe_segura": 200, "fila_sobe_pausa": 400}))["motor"]
        self.assertLess(solto["min_segurando"] + solto["min_pausado"], m["min_segurando"] + m["min_pausado"])

    def test_ia_segurando_mais_manda_menos_gente_a_atendente(self) -> None:
        rodada = _rodada(envios=4000, a_cada=10)
        cheio = reprisar(rodada, Parametros())["motor"]["foram_a_atendente"]
        cortado = reprisar(rodada, ler_parametros({"corte_ia": 0.4}))["motor"]["foram_a_atendente"]
        self.assertEqual(cheio, 400)
        self.assertTrue(200 <= cortado <= 280, cortado)

    def test_poucos_atendentes_nao_libera_lote(self) -> None:
        r = reprisar(_rodada(atendentes=4), Parametros())
        self.assertEqual((r["motor"]["lotes"], r["motor"]["enviados"], r["motor"]["nao_enviados"]), (0, 0, 3000))
        self.assertEqual({t["causa"] for t in r["trechos"]}, {"poucos atendentes"})

    def test_nenhum_lote_novo_depois_das_21h_e_teto_do_dia(self) -> None:
        # Intervalo de 4 h entre lotes: 10h, 14h e 18h; o das 22h não existe.
        r = reprisar(_rodada(envios=6000), ler_parametros({"intervalo_min": 240}))
        self.assertEqual([l["inicio"] for l in r["lotes"]], ["10:00", "14:00", "18:00"])
        self.assertEqual(r["motor"]["nao_enviados"], 6000 - 2100)
        self.assertFalse(r["motor"]["terminou"])
        com_teto = reprisar(_rodada(envios=6000), ler_parametros({"teto_dia": 1000}))
        self.assertEqual((com_teto["motor"]["enviados"], com_teto["motor"]["nao_enviados"]), (1000, 5000))

    def test_base_do_ultimo_lote_e_tamanho_pela_folga_usam_as_regras_do_motor(self) -> None:
        r = reprisar(_rodada(envios=3000), ler_parametros({"base": "lote", "pela_folga": True, "intervalo_min": 30}))
        self.assertEqual(r["motor"]["enviados"], 3000)
        self.assertTrue(all(l["tamanho"] <= teto for l, teto in zip(r["lotes"], [300, 600] + [1200] * 20)))
        self.assertEqual(r["lotes"][1]["inicio"], "10:30")
        self.assertFalse(any("1º lote do dia" in a for a in r["avisos"]))

    def test_dia_que_nao_e_util_nao_envia(self) -> None:
        r = reprisar(_rodada(dia="2026-11-15"), Parametros())   # domingo e feriado
        self.assertEqual((r["motor"]["lotes"], r["motor"]["enviados"]), (0, 0))

    def test_saida_nao_leva_destinatarios(self) -> None:
        r = reprisar(_rodada(), Parametros())
        self.assertNotIn("destinatarios", json.dumps(r))
        self.assertEqual(len(r["serie"]["motor"]), 14 * 60)
        self.assertEqual([h["hora"] for h in r["horas"]], list(range(8, 22)))
        self.assertTrue(any("teto de 4.000" in a for a in reprisar(_rodada(envios=5000), Parametros())["avisos"]))


class ArquivosTest(unittest.TestCase):
    def test_lista_e_carrega_so_arquivos_de_rodada(self) -> None:
        with tempfile.TemporaryDirectory() as pasta:
            (Path(pasta) / "rodada_2026-11-10.json").write_text(json.dumps(_rodada(envios=50)), encoding="utf-8")
            (Path(pasta) / "outro.json").write_text("{}", encoding="utf-8")
            (Path(pasta) / "rodada_2026-11-11.json").write_text("isto não é json", encoding="utf-8")
            self.assertEqual([r["arquivo"] for r in listar_rodadas(pasta)], ["rodada_2026-11-10.json"])
            self.assertEqual(carregar_rodada(pasta, "rodada_2026-11-10.json")["enviados"], 50)
            for ruim in ("../segredo.json", "outro.json", "rodada_2026-11-10.json/../x", ""):
                with self.assertRaises(ValueError):
                    carregar_rodada(pasta, ruim)
        self.assertEqual(listar_rodadas(""), [])
        self.assertEqual(listar_rodadas("/pasta/que/nao/existe"), [])


if __name__ == "__main__":
    unittest.main()
