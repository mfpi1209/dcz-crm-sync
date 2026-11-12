"""Sorteio do controle, estados do aluno, agenda, seletor e loteador. Sem rede e sem banco."""

from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date

from motor.agenda import acoes_do_dia, datas_de_lembrete, datas_de_recuperacao
from motor.agenda import (
    ACAO_AVISO_CALOURO,
    ACAO_FIRME,
    ACAO_LEMBRETE_DIA10,
    ACAO_LEMBRETE_DIA25,
    ACAO_RECONHECIMENTO,
    ACAO_RECUPERACAO,
    DIAS_LEMBRETE_DIA10,
    DIAS_LEMBRETE_DIA25,
)
from motor.config import MotorConfig
from motor.estados import (
    calcular_estado,
    classificar_ciclo,
    corte_dia10,
    historico_com_ciclo_vencido,
    historico_do_semestre,
)
from motor.loteador import lotear, lotes_que_cabem_na_janela
from motor.modelos import Aluno, Classe, Estado, Frente, Mensagem
from motor.seletor import com_estado, contar_por_estado, no_alvo, selecionar
from motor.sorteio import faixa, no_controle

CFG = MotorConfig(enabled=True, dry_run=True)
E, T, N = Classe.EM_DIA, Classe.TARDIO, Classe.NAO_PAGOU


def _aluno(cid="c1", **kw) -> Aluno:
    base = dict(contact_id=cid, deal_id="d" + cid, dia10_aberto=True)
    base.update(kw)
    return Aluno(**base)


class SorteioTest(unittest.TestCase):
    def test_e_deterministico(self) -> None:
        a = [no_controle(f"contato-{i}", "2026-11", "sal") for i in range(500)]
        b = [no_controle(f"contato-{i}", "2026-11", "sal") for i in range(500)]
        self.assertEqual(a, b)
        # Valor fixo: se a função mudar, o controle de um ciclo aberto mudaria junto.
        self.assertEqual(faixa("contato-1", "2026-11", "sal"), faixa("contato-1", "2026-11", "sal"))
        self.assertEqual(faixa("cmrmbn2lh0uz2nm016beqgbwb", "2026-11", "motor-ativacoes-v1"), 676)

    def test_fica_perto_de_5_por_cento(self) -> None:
        n = 20000
        controle = sum(no_controle(f"contato-{i}", "2026-11", "sal") for i in range(n))
        self.assertGreater(controle / n, 0.045)
        self.assertLess(controle / n, 0.055)

    def test_cada_recorte_do_publico_tem_seu_controle(self) -> None:
        # Recorte arbitrário (ids pares): o controle continua perto de 5%.
        ids = [f"contato-{i}" for i in range(0, 40000, 2)]
        controle = sum(no_controle(i, "2026-11", "sal") for i in ids)
        self.assertGreater(controle / len(ids), 0.043)
        self.assertLess(controle / len(ids), 0.057)

    def test_muda_de_um_ciclo_para_o_outro(self) -> None:
        ids = [f"contato-{i}" for i in range(5000)]
        nov = {i for i in ids if no_controle(i, "2026-11", "sal")}
        dez = {i for i in ids if no_controle(i, "2026-12", "sal")}
        self.assertNotEqual(nov, dez)
        # Sobreposição esperada de 5% × 5%: bem abaixo da metade do grupo.
        self.assertLess(len(nov & dez), len(nov) / 2)

    def test_pct_zero_e_id_vazio_nunca_entram(self) -> None:
        self.assertFalse(no_controle("contato-1", "2026-11", "sal", pct=0))
        self.assertFalse(no_controle("", "2026-11", "sal"))

    def test_pct_cem_pega_todos(self) -> None:
        self.assertTrue(all(no_controle(f"c{i}", "2026-11", "sal", pct=100) for i in range(200)))


class ClasseDoCicloTest(unittest.TestCase):
    def test_corte_do_dia_10_tem_folga_de_dias_uteis(self) -> None:
        # 10/11/2026 é terça; +3 dias úteis = sexta 13/11.
        self.assertEqual(corte_dia10("2026-11", 3), date(2026, 11, 13))
        # 10/10/2026 é sábado; 12/10 é feriado → 13, 14, 15.
        self.assertEqual(corte_dia10("2026-10", 3), date(2026, 10, 15))

    def test_baixa_ate_o_corte_e_em_dia_depois_e_tardio(self) -> None:
        self.assertEqual(classificar_ciclo("2026-11", date(2026, 11, 9), encerrado=False), E)
        self.assertEqual(classificar_ciclo("2026-11", date(2026, 11, 13), encerrado=False), E)
        self.assertEqual(classificar_ciclo("2026-11", date(2026, 11, 16), encerrado=False), T)

    def test_sem_baixa_e_aberto_ate_o_ciclo_fechar(self) -> None:
        self.assertEqual(classificar_ciclo("2026-11", None, encerrado=False), Classe.ABERTO)
        self.assertEqual(classificar_ciclo("2026-11", None, encerrado=True), N)

    def test_historico_recomeca_a_cada_semestre(self) -> None:
        classes = {"2026-05": N, "2026-06": N, "2026-09": E, "2026-10": T, "2026-11": Classe.ABERTO}
        self.assertEqual(historico_do_semestre(classes, "2026-11"), (E, T))
        self.assertEqual(historico_do_semestre(classes, "2026-07"), ())


class EstadosTest(unittest.TestCase):
    def test_sem_historico(self) -> None:
        self.assertEqual(calcular_estado(()), Estado.SEM_HISTORICO)
        self.assertEqual(calcular_estado((E,)), Estado.SEM_HISTORICO)
        self.assertEqual(calcular_estado((N, E)), Estado.SEM_HISTORICO)

    def test_em_dia_com_dois_ciclos_seguidos(self) -> None:
        self.assertEqual(calcular_estado((E, E)), Estado.EM_DIA)
        self.assertEqual(calcular_estado((T, E, E)), Estado.EM_DIA)

    def test_silenciado_com_tres_ciclos_em_dia_ou_a_pedido(self) -> None:
        self.assertEqual(calcular_estado((E, E, E)), Estado.SILENCIADO)
        self.assertEqual(calcular_estado((E, E), pediu_silencio=True), Estado.SILENCIADO)
        self.assertEqual(calcular_estado((), pediu_silencio=True), Estado.SILENCIADO)

    def test_tardio_quando_o_ultimo_ciclo_teve_baixa_depois_do_corte(self) -> None:
        self.assertEqual(calcular_estado((T,)), Estado.TARDIO)
        self.assertEqual(calcular_estado((E, E, T)), Estado.TARDIO)

    def test_primeiro_atraso_quando_fechou_um_ciclo_sem_pagar(self) -> None:
        self.assertEqual(calcular_estado((N,)), Estado.PRIMEIRO_ATRASO)
        self.assertEqual(calcular_estado((E, E, N)), Estado.PRIMEIRO_ATRASO)

    def test_nao_reage_com_dois_ciclos_em_atraso_ou_dois_lembretes_sem_reacao(self) -> None:
        self.assertEqual(calcular_estado((N, N)), Estado.NAO_REAGE)
        self.assertEqual(calcular_estado((T,), lembretes_sem_reacao=2), Estado.NAO_REAGE)
        self.assertEqual(calcular_estado((T,), lembretes_sem_reacao=1), Estado.TARDIO)

    def test_atraso_vence_o_pedido_de_silencio(self) -> None:
        self.assertEqual(calcular_estado((E, N), pediu_silencio=True), Estado.PRIMEIRO_ATRASO)

    def test_ciclo_corrente_em_aberto_depois_do_dia_25_conta_como_atraso(self) -> None:
        kw = dict(ciclo="2026-11", financeira_aberta=True, dia10_aberto=True)
        self.assertEqual(historico_com_ciclo_vencido((E,), hoje=date(2026, 11, 25), **kw), (E,))
        self.assertEqual(historico_com_ciclo_vencido((E,), hoje=date(2026, 11, 26), **kw), (E, N))
        sem_fin = dict(ciclo="2026-11", financeira_aberta=False, dia10_aberto=True)
        self.assertEqual(historico_com_ciclo_vencido((E,), hoje=date(2026, 11, 26), **sem_fin), (E,))

    def test_com_estado_aplica_estado_e_sorteio(self) -> None:
        a = _aluno(historico=(E, E), financeira_aberta=True)
        antes = com_estado(a, ciclo="2026-11", hoje=date(2026, 11, 9), cfg=CFG)
        depois = com_estado(a, ciclo="2026-11", hoje=date(2026, 11, 27), cfg=CFG)
        self.assertEqual(antes.estado, Estado.EM_DIA)
        self.assertEqual(depois.estado, Estado.PRIMEIRO_ATRASO)
        self.assertEqual(antes.controle, no_controle("c1", "2026-11", CFG.controle_sal, CFG.controle_pct))
        self.assertTrue(com_estado(a, ciclo="2026-11", hoje=date(2026, 11, 9), cfg=CFG, controle_gravado=True).controle)


class AgendaTest(unittest.TestCase):
    def test_dia_de_lembrete_em_fim_de_semana_e_antecipado(self) -> None:
        # Novembro/2026: dia 8 é domingo → sexta 6; dia 9 é segunda.
        self.assertEqual(datas_de_lembrete("2026-11", DIAS_LEMBRETE_DIA10), (date(2026, 11, 6), date(2026, 11, 9)))
        # Dia 22 é domingo e 20/11 é feriado → quinta 19; dia 24 é terça.
        self.assertEqual(datas_de_lembrete("2026-11", DIAS_LEMBRETE_DIA25), (date(2026, 11, 19), date(2026, 11, 24)))

    def test_recuperacao_so_em_dia_util_de_26_a_30(self) -> None:
        self.assertEqual(
            datas_de_recuperacao("2026-11"),
            (date(2026, 11, 26), date(2026, 11, 27), date(2026, 11, 30)),
        )
        # Fevereiro: 29 e 30 não existem; sobra 26 (sex) — 27 e 28 são fim de semana.
        self.assertEqual(datas_de_recuperacao("2027-02"), (date(2027, 2, 26),))

    def test_acoes_de_cada_dia(self) -> None:
        def msgs(d):
            return [a.mensagem for a in acoes_do_dia(d)]

        self.assertEqual(
            msgs(date(2026, 12, 8)),
            [Mensagem.AVISO_CALOURO, Mensagem.LEMBRETE_DIA10, Mensagem.RECONHECIMENTO],
        )
        self.assertEqual(msgs(date(2026, 11, 24)), [Mensagem.LEMBRETE_DIA25])
        self.assertEqual(msgs(date(2026, 11, 26)), [Mensagem.RECUPERACAO, Mensagem.FIRME])
        self.assertEqual(msgs(date(2026, 11, 12)), [])
        self.assertEqual(msgs(date(2026, 11, 20)), [])  # feriado
        self.assertEqual(msgs(date(2026, 11, 22)), [])  # domingo


class SeletorTest(unittest.TestCase):
    HOJE = date(2026, 12, 8)

    def test_lembrete_do_dia_10_so_para_tardio_e_sem_historico_veteranos(self) -> None:
        tardio = _aluno("t", estado=Estado.TARDIO, historico=(T,))
        veterano = _aluno("v", estado=Estado.SEM_HISTORICO)
        em_dia = _aluno("e", estado=Estado.EM_DIA, historico=(E, E))
        silenciado = _aluno("s", estado=Estado.SILENCIADO, historico=(E, E, E))
        calouro = _aluno("k", estado=Estado.SEM_HISTORICO, calouro=True)
        pago = _aluno("p", estado=Estado.TARDIO, historico=(T,), dia10_aberto=False)
        sel = selecionar([tardio, veterano, em_dia, silenciado, calouro, pago], ACAO_LEMBRETE_DIA10, hoje=self.HOJE, cfg=CFG)
        self.assertEqual({a.contact_id for a in sel.elegiveis}, {"t", "v"})
        self.assertEqual(sel.fora_do_alvo, 4)

    def test_calouro_na_primeira_mensalidade_recebe_o_aviso_proprio(self) -> None:
        calouro = _aluno("k", estado=Estado.SEM_HISTORICO, calouro=True)
        calouro_2o_mes = _aluno("k2", estado=Estado.SEM_HISTORICO, calouro=True, historico=(E,))
        veterano = _aluno("v", estado=Estado.SEM_HISTORICO)
        self.assertTrue(no_alvo(calouro, ACAO_AVISO_CALOURO))
        self.assertFalse(no_alvo(calouro, ACAO_LEMBRETE_DIA10))
        self.assertFalse(no_alvo(veterano, ACAO_AVISO_CALOURO))
        self.assertFalse(no_alvo(calouro_2o_mes, ACAO_AVISO_CALOURO))
        self.assertTrue(no_alvo(calouro_2o_mes, ACAO_LEMBRETE_DIA10))

    def test_reconhecimento_so_para_em_dia(self) -> None:
        self.assertTrue(no_alvo(_aluno(estado=Estado.EM_DIA), ACAO_RECONHECIMENTO))
        self.assertFalse(no_alvo(_aluno(estado=Estado.SILENCIADO), ACAO_RECONHECIMENTO))

    def test_lembrete_do_dia_25_inclui_silenciado_que_segue_em_aberto(self) -> None:
        self.assertTrue(no_alvo(_aluno(estado=Estado.SILENCIADO), ACAO_LEMBRETE_DIA25))
        self.assertFalse(no_alvo(_aluno(estado=Estado.SILENCIADO, dia10_aberto=False), ACAO_LEMBRETE_DIA25))
        self.assertFalse(no_alvo(_aluno(estado=Estado.NAO_REAGE), ACAO_LEMBRETE_DIA25))

    def test_recuperacao_e_firme_exigem_financeira_em_aberto(self) -> None:
        atraso = _aluno(estado=Estado.PRIMEIRO_ATRASO, financeira_aberta=True)
        nao_reage = _aluno(estado=Estado.NAO_REAGE, financeira_aberta=True)
        self.assertTrue(no_alvo(atraso, ACAO_RECUPERACAO))
        self.assertFalse(no_alvo(replace(atraso, financeira_aberta=False), ACAO_RECUPERACAO))
        self.assertTrue(no_alvo(nao_reage, ACAO_FIRME))
        self.assertFalse(no_alvo(nao_reage, ACAO_RECUPERACAO))

    def test_exclusoes_dos_trilhos_sao_contadas_por_motivo(self) -> None:
        alunos = [
            _aluno("a", estado=Estado.TARDIO),
            _aluno("b", estado=Estado.TARDIO, controle=True),
            _aluno("c", estado=Estado.TARDIO, situacao="Cancelado"),
            _aluno("d", estado=Estado.TARDIO, bloqueio="erro_131026"),
            _aluno("e", estado=Estado.TARDIO, msgs_ciclo=2),
        ]
        sel = selecionar(alunos, ACAO_LEMBRETE_DIA10, hoje=self.HOJE, cfg=CFG)
        self.assertEqual([a.contact_id for a in sel.elegiveis], ["a"])
        self.assertEqual(
            sel.exclusoes,
            {
                "grupo_de_controle": 1,
                "situacao_diferente_de_em_curso": 1,
                "erro_131026": 1,
                "limite_de_mensagens_no_ciclo": 1,
            },
        )

    def test_contagem_por_estado(self) -> None:
        c = contar_por_estado([_aluno(estado=Estado.TARDIO), _aluno(estado=Estado.TARDIO), _aluno(estado=Estado.EM_DIA)])
        self.assertEqual(c["tardio"], 2)
        self.assertEqual(c["em_dia"], 1)
        self.assertEqual(c["nao_reage"], 0)


class LoteadorTest(unittest.TestCase):
    DIA = date(2026, 12, 8)

    def _sel(self, acao, n, prefixo):
        alunos = [_aluno(f"{prefixo}{i}", estado=next(iter(acao.estados))) for i in range(n)]
        if acao is ACAO_AVISO_CALOURO:
            alunos = [replace(a, calouro=True) for a in alunos]
        return selecionar(alunos, acao, hoje=self.DIA, cfg=CFG)

    def test_divide_em_lotes_do_tamanho_pedido(self) -> None:
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 2500, "t")], dia=self.DIA, cfg=CFG, tamanho=1000)
        self.assertEqual([l.tamanho for l in plano.lotes], [1000, 1000, 500])
        self.assertEqual(plano.total, 2500)
        self.assertEqual([l.ordem for l in plano.lotes], [1, 2, 3])

    def test_tamanho_nunca_passa_do_maximo(self) -> None:
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 3000, "t")], dia=self.DIA, cfg=CFG, tamanho=99999)
        self.assertTrue(all(l.tamanho <= CFG.lote_max for l in plano.lotes))
        self.assertEqual(plano.total, 3000)

    def test_resto_pequeno_junta_com_o_lote_anterior(self) -> None:
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 2050, "t")], dia=self.DIA, cfg=CFG, tamanho=1000)
        self.assertEqual([l.tamanho for l in plano.lotes], [1000, 1050])
        ids = [cid for l in plano.lotes for cid in l.contact_ids]
        self.assertEqual(len(set(ids)), 2050)  # ninguém perdido nem repetido ao juntar
        # Se juntar passaria do máximo, o resto fica sozinho.
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 2450, "t")], dia=self.DIA, cfg=CFG, tamanho=1200)
        self.assertEqual([l.tamanho for l in plano.lotes], [1200, 1200, 50])

    def test_respeita_o_teto_diario(self) -> None:
        cfg = replace(CFG, teto_diario=1500)
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 2500, "t")], dia=self.DIA, cfg=cfg, tamanho=1000)
        self.assertEqual(plano.total, 1500)
        self.assertEqual(plano.adiados, 1000)
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 2500, "t")], dia=self.DIA, cfg=cfg, tamanho=1000, enviados_hoje=1400)
        self.assertEqual(plano.total, 100)
        self.assertEqual(plano.adiados, 2400)

    def test_um_lote_tem_uma_unica_mensagem_e_segue_a_prioridade(self) -> None:
        selecoes = [self._sel(ACAO_LEMBRETE_DIA10, 300, "t"), self._sel(ACAO_AVISO_CALOURO, 200, "k")]
        plano = lotear(selecoes, dia=self.DIA, cfg=CFG, tamanho=1000)
        self.assertEqual([(l.mensagem, l.tamanho) for l in plano.lotes], [
            (Mensagem.AVISO_CALOURO, 200),
            (Mensagem.LEMBRETE_DIA10, 300),
        ])
        self.assertEqual({l.frente for l in plano.lotes}, {Frente.DIA10})

    def test_aluno_entra_em_no_maximo_um_lote_por_dia(self) -> None:
        dupla = _aluno("x", estado=Estado.PRIMEIRO_ATRASO, financeira_aberta=True)
        s1 = selecionar([dupla], ACAO_RECUPERACAO, hoje=self.DIA, cfg=CFG)
        s2 = selecionar([replace(dupla, estado=Estado.NAO_REAGE)], ACAO_FIRME, hoje=self.DIA, cfg=CFG)
        plano = lotear([s2, s1], dia=self.DIA, cfg=CFG, tamanho=1000)
        ids = [cid for l in plano.lotes for cid in l.contact_ids]
        self.assertEqual(ids, ["x"])
        self.assertEqual(plano.lotes[0].mensagem, Mensagem.RECUPERACAO)
        self.assertEqual(plano.repetidos, 1)

    def test_ordem_e_estavel_e_nao_segue_o_identificador(self) -> None:
        sel = self._sel(ACAO_LEMBRETE_DIA10, 400, "t")
        a = lotear([sel], dia=self.DIA, cfg=CFG, tamanho=100).lotes
        b = lotear([sel], dia=self.DIA, cfg=CFG, tamanho=100).lotes
        self.assertEqual([l.contact_ids for l in a], [l.contact_ids for l in b])
        self.assertNotEqual(list(a[0].contact_ids), sorted(a[0].contact_ids))

    def test_sem_elegiveis_nao_gera_lote(self) -> None:
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 0, "t")], dia=self.DIA, cfg=CFG, tamanho=1000)
        self.assertEqual(plano.lotes, [])

    def test_max_lotes_adia_o_excedente(self) -> None:
        plano = lotear([self._sel(ACAO_LEMBRETE_DIA10, 3000, "t")], dia=self.DIA, cfg=CFG, tamanho=1000, max_lotes=1)
        self.assertEqual(len(plano.lotes), 1)
        self.assertEqual(plano.adiados, 2000)

    def test_lotes_que_cabem_numa_manha(self) -> None:
        # 5 horas de janela com 30 minutos entre lotes.
        self.assertEqual(lotes_que_cabem_na_janela(CFG), 10)


if __name__ == "__main__":
    unittest.main()
