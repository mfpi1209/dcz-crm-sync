"""Resultado do ciclo (motor × controle), comportamento, previsão e verificação dos textos."""
from __future__ import annotations

import unittest
from datetime import date

from motor.modelos import Mensagem
from motor.previsao import comportamento
from motor.resultados import comparar, resultado_do_ciclo
from motor.textos import PROPOSTAS, avaliar, catalogo

HOJE = date(2026, 11, 20)


def _linha(i: int, ciclo: str, baixa, controle: bool = False) -> dict:
    return {"contact_id": f"a{i}", "ciclo": ciclo, "baixa_dia10_em": baixa, "controle": controle}


def _outubro(motor_em_dia: int = 700, motor: int = 1000, controle_em_dia: int = 100, controle: int = 200) -> list[dict]:
    linhas = []
    for i in range(motor):
        linhas.append(_linha(i, "2026-10", date(2026, 10, 9) if i < motor_em_dia else (date(2026, 10, 22) if i < motor_em_dia + 100 else None)))
    for j in range(controle):
        linhas.append(_linha(10_000 + j, "2026-10", date(2026, 10, 9) if j < controle_em_dia else None, controle=True))
    return linhas


class CompararTest(unittest.TestCase):
    def test_diferenca_em_pontos_com_intervalo(self) -> None:
        c = comparar(700, 1000, 100, 200)
        self.assertEqual(c["diferenca_pp"], 20.0)
        self.assertTrue(c["de_pp"] < 20.0 < c["ate_pp"])
        self.assertTrue(c["conclusivo"])
        self.assertEqual(c["a_mais"], 200)

    def test_controle_pequeno_ou_diferenca_dentro_da_margem_nao_e_conclusivo(self) -> None:
        self.assertFalse(comparar(70, 100, 10, 20)["conclusivo"])        # controle abaixo de 100
        self.assertFalse(comparar(510, 1000, 100, 200)["conclusivo"])    # 1 ponto de diferença
        self.assertIsNone(comparar(5, 10, 0, 0)["diferenca_pp"])


class ResultadoDoCicloTest(unittest.TestCase):
    def test_motor_contra_controle_por_intencao_de_tratar(self) -> None:
        r = resultado_do_ciclo("2026-10", _outubro(), [], hoje=HOJE)
        self.assertEqual((r["motor"]["alunos"], r["motor"]["em_dia"], r["motor"]["ate_dia25"]), (1000, 700, 800))
        self.assertEqual((r["controle"]["alunos"], r["controle"]["em_dia"]), (200, 100))
        self.assertEqual(r["motor"]["taxa_em_dia"], 0.7)
        self.assertEqual(r["em_dia"]["diferenca_pp"], 20.0)
        self.assertTrue(r["dia10_fechado"] and r["dia25_fechado"])
        # Prazo efetivo de outubro: 13/10 + 3 dias úteis.
        self.assertEqual(r["corte_dia10"], "2026-10-16")

    def test_baixa_depois_do_corte_nao_e_em_dia(self) -> None:
        linhas = [_linha(1, "2026-10", date(2026, 10, 16)), _linha(2, "2026-10", date(2026, 10, 19)), _linha(3, "2026-10", None)]
        r = resultado_do_ciclo("2026-10", linhas, [], hoje=HOJE)
        self.assertEqual((r["motor"]["em_dia"], r["motor"]["ate_dia25"], r["motor"]["pagaram"]), (1, 2, 2))

    def test_ciclo_em_andamento_avisa_que_ainda_nao_fechou(self) -> None:
        r = resultado_do_ciclo("2026-11", [_linha(1, "2026-11", None)], [], hoje=date(2026, 11, 11))
        self.assertFalse(r["dia10_fechado"])
        self.assertFalse(r["dia25_fechado"])

    def test_estado_vem_do_historico_do_semestre(self) -> None:
        linhas = [
            _linha(1, "2026-08", date(2026, 8, 5)), _linha(1, "2026-09", date(2026, 9, 5)), _linha(1, "2026-10", date(2026, 10, 5)),  # 2 em dia → em_dia
            _linha(2, "2026-09", None), _linha(2, "2026-10", None),                                                                  # 1 sem pagar → primeiro atraso
            _linha(3, "2026-10", date(2026, 10, 5), controle=True),                                                                  # sem histórico, controle
            _linha(4, "2026-06", None), _linha(4, "2026-10", None),                                                                  # semestre anterior não conta
        ]
        r = resultado_do_ciclo("2026-10", linhas, [], hoje=HOJE)
        por = {e["estado"]: e for e in r["por_estado"]}
        self.assertEqual(por["em_dia"]["motor"]["alunos"], 1)
        self.assertEqual(por["primeiro_atraso"]["motor"]["alunos"], 1)
        self.assertEqual((por["sem_historico"]["motor"]["alunos"], por["sem_historico"]["controle"]["alunos"]), (1, 1))

    def test_mensagens_por_variante_cobertura_e_controle_contaminado(self) -> None:
        envios = (
            [{"contact_id": f"a{i}", "mensagem": "lembrete_dia10", "variante": "A", "status": "lido" if i % 2 else "entregue",
              "classificacao": "pediu_parar" if i < 2 else ("ja_paguei" if i < 20 else None), "respondeu_em": None} for i in range(400)]
            + [{"contact_id": f"a{i}", "mensagem": "lembrete_dia10", "variante": "B", "status": "falhou" if i < 410 else "entregue",
                "classificacao": None, "respondeu_em": None} for i in range(400, 600)]
            + [{"contact_id": "a700", "mensagem": "lembrete_dia10", "variante": "B", "status": "nao_enviado", "classificacao": None, "respondeu_em": None}]
            + [{"contact_id": "a10000", "mensagem": "lembrete_dia10", "variante": "A", "status": "entregue", "classificacao": None, "respondeu_em": None}]
        )
        r = resultado_do_ciclo("2026-10", _outubro(), envios, hoje=HOJE)
        a, b = r["por_mensagem"]
        self.assertEqual((a["variante"], a["sairam"], a["entregues"], a["lidos"], a["responderam"]), ("A", 401, 401, 200, 20))
        self.assertEqual(a["parar_por_mil"], round(1000 * 2 / 401, 1))
        self.assertEqual((b["sairam"], b["falhas"], b["entregues"]), (200, 10, 190))      # o não enviado não conta
        self.assertEqual(b["taxa_em_dia"], round(190 / 190, 4))                            # a400..a599 pagaram em dia
        self.assertEqual(r["cobertura"]["receberam"], 400 + 190)
        self.assertEqual(r["cobertura"]["controle_que_recebeu"], 1)
        self.assertEqual(r["respostas"], {"ja_paguei": 18, "pediu_parar": 2})


class ComportamentoTest(unittest.TestCase):
    def _historico(self) -> list[dict]:
        linhas = []
        for i in range(300):
            # Setembro: os 200 primeiros pagam em dia. Outubro: quem pagou em dia repete 90%; quem não pagou, 20%.
            pagou_set = i < 200
            linhas.append(_linha(i, "2026-09", date(2026, 9, 8) if pagou_set else None))
            pagou_out = (i % 10 != 0) if pagou_set else (i % 5 == 0)
            linhas.append(_linha(i, "2026-10", date(2026, 10, 9) if pagou_out else None))
            linhas.append(_linha(i, "2026-11", date(2026, 11, 9) if i < 120 else None))
        return linhas

    def test_taxas_por_estado_e_previsao_do_ciclo(self) -> None:
        c = comportamento(self._historico(), hoje=HOJE)
        self.assertEqual(c["ciclos_de_base"], ["2026-09", "2026-10"])
        por = {e["estado"]: e for e in c["por_estado"]}
        # Outubro visto de setembro: quem vinha de 1 ciclo sem pagar é "primeiro atraso" e paga 20%.
        self.assertEqual(por["primeiro_atraso"]["alunos"], 100)
        self.assertAlmostEqual(por["primeiro_atraso"]["em_dia"], 0.2)
        self.assertTrue(por["sem_historico"]["amostra_suficiente"])
        self.assertFalse(por["silenciado"]["amostra_suficiente"])
        self.assertEqual(c["alunos_no_ciclo"], 300)
        p = c["previsao"]
        self.assertEqual(sum(p[d]["esperado"] for d in ("em_dia", "tardio", "nao_pagou")), 300)
        self.assertTrue(120 <= p["em_dia"]["esperado"] <= 260)
        self.assertGreater(p["em_dia"]["margem"], 0)
        self.assertEqual(c["ritmo"]["pagaram_ate_hoje"], 120)
        self.assertEqual(c["ritmo"]["diferenca"], 120 - c["ritmo"]["esperado_ate_hoje"])

    def test_sem_ciclo_fechado_nao_ha_previsao(self) -> None:
        c = comportamento([_linha(i, "2026-11", None) for i in range(50)], hoje=HOJE)
        self.assertIsNone(c["previsao"])
        self.assertIsNone(c["ritmo"])
        self.assertEqual(c["ciclos_de_base"], [])
        self.assertEqual(c["alunos_no_ciclo"], 50)


class TextosTest(unittest.TestCase):
    def test_propostas_passam_na_propria_verificacao_e_nenhuma_esta_aprovada(self) -> None:
        self.assertEqual({t.mensagem for t in PROPOSTAS}, {m.value for m in Mensagem})
        for t in PROPOSTAS:
            self.assertEqual(t.problemas, (), f"{t.mensagem}/{t.variante}")
            self.assertFalse(t.aprovado)
        self.assertEqual(len(catalogo()), len(PROPOSTAS))

    def test_verificacao_aponta_o_que_derruba_o_texto(self) -> None:
        ruim = avaliar("lembrete_dia10", "{{1}}, PAGUE AGORA!!! ou vai para o Serasa {{3}}", ("Botão que o motor não conhece e é longo",))
        juntos = " | ".join(ruim)
        for trecho in ("variáveis esperadas", "começar nem terminar", "CAIXA ALTA", "pontuação", "ameaça", "caracteres (máximo 25)", "não está entre os que o motor lê"):
            self.assertIn(trecho, juntos)
        self.assertEqual(avaliar("firme", "Oi, {{1}}. Seu nome pode ir para o Serasa se nada for feito.", ()), [])   # só a firme pode
        self.assertIn("variáveis esperadas", avaliar("reconhecimento", "Oi, {{1}}, obrigado. Prazo {{3}} ok.", ())[0])
        self.assertTrue(any("rolar a tela" in p for p in avaliar("recuperacao", "Oi, {{1}}. " + "texto " * 120, ())))
        self.assertEqual(avaliar("lembrete_dia25", "Oi, {{1}}. A de {{2}} vence em {{3}}, e o PIX está na área do aluno.", ("Quero pagar", "Já paguei")), [])


class HistoricoTest(unittest.TestCase):
    def test_le_csv_e_recusa_o_que_nao_serve(self) -> None:
        from motor.historico import ler_csv, resumo

        texto = (
            "\ufeffContact_ID;Ciclo;Baixa_Em\n"
            "c1;2026-09;2026-09-08\n"
            "c1;2026-10;\n"
            "c2;2026-10;2026-10-20T10:00:00\n"
            "c2;2026-10;2026-10-09\n"          # repetida: vale a baixa mais antiga
            "c3;2026-11;2026-11-05\n"          # ciclo corrente: é do motor
            "c4;2026-10;2026-11-02\n"          # baixa fora do mês
            "c5;2026-13;\n"
            ";2026-10;\n"
            "c6;2026-10;ontem\n"
        )
        linhas, fora = ler_csv(texto, hoje=HOJE)
        self.assertEqual([(l["contact_id"], l["ciclo"], l["baixa_dia10_em"]) for l in linhas], [
            ("c1", "2026-09", date(2026, 9, 8)), ("c1", "2026-10", None), ("c2", "2026-10", date(2026, 10, 9)),
        ])
        self.assertTrue(all(l["controle"] is False and l["aberto_em"].day == 1 for l in linhas))
        self.assertEqual(fora, {"repetida": 1, "ciclo corrente ou futuro": 1, "baixa fora do mês do ciclo": 1, "ciclo inválido": 1, "sem contact_id": 1, "data de baixa inválida": 1})
        self.assertEqual(resumo(linhas), {"2026-09": {"alunos": 1, "com_baixa": 1}, "2026-10": {"alunos": 2, "com_baixa": 1}})
        with self.assertRaises(ValueError):
            ler_csv("aluno,mes\nx,y\n", hoje=HOJE)

    def test_historico_importado_alimenta_a_previsao(self) -> None:
        from motor.historico import ler_csv

        csv_txt = "contact_id,ciclo,baixa_em\n" + "".join(
            f"h{i},2026-{m:02d},{'2026-%02d-08' % m if i % 4 else ''}\n" for i in range(200) for m in (9, 10)
        )
        linhas, fora = ler_csv(csv_txt, hoje=HOJE)
        self.assertEqual((len(linhas), fora), (400, {}))
        atuais = [{**_linha(i, "2026-11", None), "contact_id": f"h{i}"} for i in range(200)]
        c = comportamento(linhas + atuais, hoje=HOJE)
        self.assertEqual(c["ciclos_de_base"], ["2026-09", "2026-10"])
        self.assertEqual(c["previsao"]["em_dia"]["esperado"], 150)


if __name__ == "__main__":
    unittest.main()
