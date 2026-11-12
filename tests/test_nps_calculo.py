"""Cálculo do NPS e regras de leitura. Sem banco e sem rede."""

from __future__ import annotations

import unittest

from nps.calculo import (
    N_MIN_COMPARAR,
    N_MIN_PUBLICAR,
    classe,
    diferenca_e_real,
    ler_nota,
    ler_resolvido,
    leitura,
    margem,
    mascarar,
    por_recorte,
    resumir,
)


def _linhas(notas, resolvido="sim", **extra):
    return [{"nota": n, "resolvido": resolvido if n is not None else None, **extra} for n in notas]


class LerRespostaTest(unittest.TestCase):
    def test_nota_aceita_os_formatos_do_formulario(self) -> None:
        self.assertEqual(ler_nota("9"), 9)
        self.assertEqual(ler_nota("10"), 10)
        self.assertEqual(ler_nota("10 - Com certeza"), 10)
        self.assertEqual(ler_nota("nota_0"), 0)
        self.assertEqual(ler_nota(7), 7)
        self.assertEqual(ler_nota(7.0), 7)

    def test_nota_fora_da_escala_ou_ausente_e_none(self) -> None:
        for valor in ("", None, "aguardando", "11", "100", "-", 7.5, 11, True):
            self.assertIsNone(ler_nota(valor), valor)

    def test_resolvido(self) -> None:
        self.assertEqual(ler_resolvido("Sim"), "sim")
        self.assertEqual(ler_resolvido("Em parte"), "parte")
        self.assertEqual(ler_resolvido("Não"), "nao")
        self.assertEqual(ler_resolvido("nao_resolvido"), "nao")
        self.assertEqual(ler_resolvido("Não, sigo com o problema"), "nao")

    def test_resolvido_que_nao_da_para_ler_nao_e_adivinhado(self) -> None:
        for valor in ("", None, "aguardando", "2", "opcao_b", "sinal"):
            self.assertIsNone(ler_resolvido(valor), valor)

    def test_mascara_tira_email_e_numero_longo(self) -> None:
        texto = mascarar("meu rgm 12345678 e email fulano.silva@exemplo.com.br, liguei 3 vezes")
        self.assertNotIn("12345678", texto)
        self.assertNotIn("@", texto)
        self.assertIn("3 vezes", texto)
        self.assertEqual(len(mascarar("x" * 5000)), 1000)


class NpsTest(unittest.TestCase):
    def test_classes(self) -> None:
        self.assertEqual([classe(n) for n in (0, 6, 7, 8, 9, 10)], ["detrator", "detrator", "neutro", "neutro", "promotor", "promotor"])

    def test_nps_e_promotores_menos_detratores(self) -> None:
        r = resumir(_linhas([10] * 50 + [8] * 30 + [3] * 20)).como_dict()
        self.assertEqual(r["respostas"], 100)
        self.assertEqual(r["nps"], 30.0)
        self.assertEqual(r["promotores_pct"], 50.0)
        self.assertEqual(r["detratores_pct"], 20.0)
        self.assertEqual(r["leitura"], "comparavel")

    def test_margem_bate_com_a_conta_a_mao(self) -> None:
        # p = 0,5; d = 0,2; variância = 0,5 + 0,2 − 0,3² = 0,61; EP = √(0,61/100) = 0,0781; × 1,96 = 15,3
        self.assertEqual(margem(50, 20, 100), 15.3)
        self.assertEqual(margem(100, 0, 100), 0.0)
        self.assertIsNone(margem(0, 0, 0))

    def test_sem_resposta_conta_como_envio_e_nao_como_nota(self) -> None:
        r = resumir(_linhas([10] * 30 + [None] * 70)).como_dict()
        self.assertEqual((r["envios"], r["respostas"], r["taxa_resposta"]), (100, 30, 30.0))
        self.assertEqual(r["nps"], 100.0)

    def test_abaixo_do_minimo_nao_mostra_numero(self) -> None:
        r = resumir(_linhas([10] * (N_MIN_PUBLICAR - 1))).como_dict()
        self.assertEqual(r["leitura"], "insuficiente")
        for chave in ("nps", "margem", "promotores_pct", "detratores_pct", "resolvido_sim_pct"):
            self.assertIsNone(r[chave], chave)
        self.assertEqual(r["respostas"], N_MIN_PUBLICAR - 1)

    def test_faixas_de_leitura(self) -> None:
        self.assertEqual(leitura(N_MIN_PUBLICAR), "indicativo")
        self.assertEqual(leitura(N_MIN_COMPARAR - 1), "indicativo")
        self.assertEqual(leitura(N_MIN_COMPARAR), "comparavel")

    def test_resolvido_so_conta_quem_declarou(self) -> None:
        linhas = _linhas([9] * 40, "sim") + _linhas([2] * 10, "nao") + _linhas([5] * 10, None)
        r = resumir(linhas).como_dict()
        self.assertEqual(r["resolvido_respostas"], 50)
        self.assertEqual(r["resolvido_sim_pct"], 80.0)
        self.assertEqual(r["resolvido_nao_pct"], 20.0)

    def test_recortes_ordenados_por_respostas_e_sem_nome_vira_rotulo(self) -> None:
        linhas = _linhas([9] * 40, depto="SAC") + _linhas([4] * 5, depto="Retenção") + _linhas([8] * 2, depto="")
        saida = por_recorte(linhas, "depto", "(sem departamento)")
        self.assertEqual([r["recorte"] for r in saida], ["SAC", "Retenção", "(sem departamento)"])
        self.assertIsNone(saida[1]["nps"])

    def test_diferenca_so_e_afirmada_com_amostra_e_fora_da_margem(self) -> None:
        a = resumir(_linhas([10] * 80 + [3] * 20)).como_dict()      # +60, n=100
        b = resumir(_linhas([10] * 55 + [3] * 45)).como_dict()      # +10, n=100
        c = resumir(_linhas([10] * 75 + [3] * 25)).como_dict()      # +50, n=100
        pequeno = resumir(_linhas([10] * 40)).como_dict()
        self.assertTrue(diferenca_e_real(a, b))
        self.assertFalse(diferenca_e_real(a, c))
        self.assertIsNone(diferenca_e_real(a, pequeno))


if __name__ == "__main__":
    unittest.main()
