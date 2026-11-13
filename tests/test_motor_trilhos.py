"""Trilhos e governador do Motor de Ativações. Sem rede e sem banco."""

from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import date, datetime, timedelta

from motor.calendario import BRT, eh_dia_util, feriados_nacionais, somar_dias_uteis
from motor.config import LOTE_ABSOLUTO, TETO_DIARIO_ABSOLUTO, MotorConfig, carregar
from motor.governador import decidir, dentro_da_janela, tamanho_do_lote
from motor.modelos import Aluno, Controle, Decisao, Fila, Frente, Lote, LoteAtivo, Mensagem
from motor.trilhos import modo_efetivo, motivo_exclusao, pausa_de_emergencia, validar_lote

CFG = MotorConfig(enabled=True, dry_run=True)
LIGADO = Controle(ligado=True)
# Terça-feira, 24/11/2026, 10h (dia útil dentro da janela).
AGORA = datetime(2026, 11, 24, 10, 0, tzinfo=BRT)
HOJE = AGORA.date()


def _fila(aguardando=20, espera_s=300, online=8, lida_em=AGORA) -> Fila:
    return Fila(aguardando=aguardando, maior_espera_s=espera_s, atendentes_online=online, lida_em=lida_em)


def _aluno(cid="c1", **kw) -> Aluno:
    base = dict(contact_id=cid, deal_id="d" + cid, dia10_aberto=True, nivel="Graduação")
    base.update(kw)
    return Aluno(**base)


def _excl(aluno: Aluno, frente=Frente.DIA25, mensagem=Mensagem.LEMBRETE_DIA25, hoje=HOJE, cfg=CFG):
    return motivo_exclusao(aluno, frente=frente, mensagem=mensagem, hoje=hoje, cfg=cfg)


class JanelaTest(unittest.TestCase):
    def test_dentro_das_9h_as_14h_em_dia_util(self) -> None:
        self.assertTrue(dentro_da_janela(AGORA.replace(hour=9, minute=0), CFG))
        self.assertTrue(dentro_da_janela(AGORA.replace(hour=13, minute=59), CFG))

    def test_fora_antes_das_9h_e_a_partir_das_14h(self) -> None:
        self.assertFalse(dentro_da_janela(AGORA.replace(hour=8, minute=59), CFG))
        self.assertFalse(dentro_da_janela(AGORA.replace(hour=14, minute=0), CFG))

    def test_fim_de_semana_fica_fora(self) -> None:
        sabado = datetime(2026, 11, 21, 10, 0, tzinfo=BRT)
        self.assertFalse(dentro_da_janela(sabado, CFG))

    def test_feriado_nacional_fica_fora(self) -> None:
        # 20/11/2026 (sexta) é o Dia da Consciência Negra.
        self.assertIn(date(2026, 11, 20), feriados_nacionais(2026))
        self.assertFalse(dentro_da_janela(datetime(2026, 11, 20, 10, 0, tzinfo=BRT), CFG))

    def test_feriado_extra_do_ambiente_fica_fora(self) -> None:
        cfg = replace(CFG, feriados=frozenset({HOJE}))
        self.assertFalse(dentro_da_janela(AGORA, cfg))

    def test_horario_em_utc_e_convertido_para_brasilia(self) -> None:
        from datetime import timezone

        # 16h UTC = 13h em Brasília (dentro); 17h UTC = 14h (fora).
        self.assertTrue(dentro_da_janela(datetime(2026, 11, 24, 16, 0, tzinfo=timezone.utc), CFG))
        self.assertFalse(dentro_da_janela(datetime(2026, 11, 24, 17, 0, tzinfo=timezone.utc), CFG))

    def test_dias_uteis_pulam_fim_de_semana_e_feriado(self) -> None:
        # 19/11 (qui) + 1 dia útil = 23/11 (seg): pula o feriado de 20/11 e o fim de semana.
        self.assertEqual(somar_dias_uteis(date(2026, 11, 19), 1), date(2026, 11, 23))
        self.assertFalse(eh_dia_util(date(2026, 12, 25)))


class ConfigTest(unittest.TestCase):
    def test_sobe_desligado_e_em_simulacao(self) -> None:
        cfg = MotorConfig()
        self.assertFalse(cfg.enabled)
        self.assertTrue(cfg.dry_run)

    def test_ambiente_nao_passa_dos_tetos_absolutos(self) -> None:
        import os
        from unittest import mock

        env = {
            "MOTOR_LOTE_MAX": "999999",
            "MOTOR_TETO_DIARIO": "999999",
            "MOTOR_JANELA_INICIO_H": "0",
            "MOTOR_JANELA_FIM_H": "23",
            "MOTOR_MAX_MSGS_CICLO": "10",
            "MOTOR_CRM_RPM": "5000",
        }
        with mock.patch.dict(os.environ, env, clear=False):
            cfg = carregar()
        self.assertEqual(cfg.lote_max, LOTE_ABSOLUTO)
        self.assertEqual(cfg.teto_diario, TETO_DIARIO_ABSOLUTO)
        self.assertEqual(cfg.janela_inicio_h, 9)
        self.assertEqual(cfg.janela_fim_h, 21)  # o fim é configurável, com teto de 21h em código
        self.assertEqual(cfg.max_msgs_ciclo, 2)
        self.assertEqual(cfg.crm_rpm, 60)

    def test_valor_invalido_cai_no_padrao(self) -> None:
        import os
        from unittest import mock

        with mock.patch.dict(os.environ, {"MOTOR_LOTE_MAX": "abc", "MOTOR_DRY_RUN": ""}, clear=False):
            cfg = carregar()
        self.assertEqual(cfg.lote_max, 1200)
        self.assertTrue(cfg.dry_run)


class ModoTest(unittest.TestCase):
    def test_precisa_das_duas_chaves_para_sair_do_desligado(self) -> None:
        self.assertEqual(modo_efetivo(MotorConfig(enabled=False), LIGADO), "desligado")
        self.assertEqual(modo_efetivo(CFG, Controle(ligado=False)), "desligado")
        self.assertEqual(modo_efetivo(CFG, LIGADO), "simulacao")

    def test_parada_geral_vence_tudo(self) -> None:
        cfg = MotorConfig(enabled=True, dry_run=False)
        self.assertEqual(modo_efetivo(cfg, Controle(ligado=True, parada_geral=True)), "parado")

    def test_real_so_com_dry_run_desligado(self) -> None:
        self.assertEqual(modo_efetivo(MotorConfig(enabled=True, dry_run=False), LIGADO), "real")


class ExclusoesTest(unittest.TestCase):
    def test_aluno_regular_pode_receber(self) -> None:
        self.assertIsNone(_excl(_aluno()))

    def test_grupo_de_controle_nao_recebe(self) -> None:
        self.assertEqual(_excl(_aluno(controle=True)), "grupo_de_controle")

    def test_situacao_diferente_de_em_curso(self) -> None:
        self.assertEqual(_excl(_aluno(situacao="Trancado")), "situacao_diferente_de_em_curso")
        self.assertIsNone(_excl(_aluno(situacao="  EM CURSO ")))

    def test_sem_telefone_erro_131026_e_recusa(self) -> None:
        self.assertEqual(_excl(_aluno(tem_telefone=False)), "sem_telefone")
        self.assertEqual(_excl(_aluno(bloqueio="erro_131026")), "erro_131026")
        self.assertEqual(_excl(_aluno(bloqueio="recusou_mensagens")), "recusou_mensagens")

    def test_no_maximo_duas_mensagens_por_ciclo(self) -> None:
        self.assertIsNone(_excl(_aluno(msgs_ciclo=1)))
        self.assertEqual(_excl(_aluno(msgs_ciclo=2)), "limite_de_mensagens_no_ciclo")

    def test_nunca_duas_campanhas_no_mesmo_dia(self) -> None:
        a = _aluno(msgs_ciclo=1, dias_com_mensagem=frozenset({HOJE}))
        self.assertEqual(_excl(a), "ja_recebeu_hoje")
        ontem = _aluno(msgs_ciclo=1, dias_com_mensagem=frozenset({HOJE - timedelta(days=1)}))
        self.assertIsNone(_excl(ontem))

    def test_uma_mensagem_por_frente_no_ciclo(self) -> None:
        a = _aluno(msgs_ciclo=1, frentes_ciclo=frozenset({Frente.DIA25}))
        self.assertEqual(_excl(a), "ja_recebeu_desta_frente_no_ciclo")
        outra = _aluno(msgs_ciclo=1, frentes_ciclo=frozenset({Frente.DIA10}))
        self.assertIsNone(_excl(outra))

    def test_mensagem_firme_uma_vez_por_ciclo(self) -> None:
        a = _aluno(msgs_ciclo=1, mensagens_ciclo=frozenset({Mensagem.FIRME}))
        self.assertEqual(
            _excl(a, frente=Frente.DIA25, mensagem=Mensagem.FIRME), "ja_recebeu_esta_mensagem_no_ciclo"
        )

    def test_so_entra_graduacao(self) -> None:
        for nivel in ("Graduação", " graduacao ", "GRADUAÇÃO", "Gradua ção"):
            self.assertIsNone(_excl(_aluno(nivel=nivel)), nivel)
        for nivel in ("Pós-Graduação", "", "Pos"):
            self.assertEqual(_excl(_aluno(nivel=nivel)), "fora_do_nivel_do_piloto", nivel)
        self.assertEqual(_excl(_aluno(nivel_misto=True)), "negocio_em_dois_niveis")
        self.assertIsNone(_excl(_aluno(nivel="Pós"), cfg=replace(CFG, nivel_alvo="")))

    def test_quem_reclamou_fica_suspenso_no_ciclo(self) -> None:
        self.assertEqual(_excl(_aluno(suspenso=True)), "reclamou_no_ciclo")
        self.assertEqual(_excl(_aluno(bloqueio="numero_errado")), "numero_errado")

    def test_quem_declarou_pagamento_sai_por_5_dias_uteis(self) -> None:
        # Declarou na terça 17/11; 20/11 é feriado. 5 dias úteis depois = 25/11.
        a = _aluno(declarou_pagamento_em=date(2026, 11, 17))
        self.assertEqual(_excl(a, hoje=date(2026, 11, 24)), "declarou_pagamento")
        self.assertIsNone(_excl(a, hoje=date(2026, 11, 25)))


class ValidarLoteTest(unittest.TestCase):
    def _lote(self, n=3, **kw) -> tuple[Lote, dict]:
        alunos = {f"c{i}": _aluno(f"c{i}") for i in range(n)}
        lote = Lote(ordem=1, frente=Frente.DIA25, mensagem=Mensagem.LEMBRETE_DIA25, contact_ids=tuple(alunos))
        return lote, alunos

    def _validar(self, lote, alunos, **kw):
        base = dict(alunos=alunos, agora=AGORA, cfg=CFG, controle=LIGADO, enviados_hoje=0)
        base.update(kw)
        return [r.codigo for r in validar_lote(lote, **base)]

    def test_lote_regular_passa(self) -> None:
        lote, alunos = self._lote()
        self.assertEqual(self._validar(lote, alunos), [])

    def test_parada_geral_e_desligado_recusam(self) -> None:
        lote, alunos = self._lote()
        self.assertIn("parada_geral", self._validar(lote, alunos, controle=Controle(ligado=True, parada_geral=True)))
        self.assertIn("motor_desligado", self._validar(lote, alunos, controle=Controle(ligado=False)))
        self.assertIn("motor_desligado", self._validar(lote, alunos, cfg=MotorConfig(enabled=False)))

    def test_fora_da_janela_recusa(self) -> None:
        lote, alunos = self._lote()
        self.assertIn("fora_da_janela", self._validar(lote, alunos, agora=AGORA.replace(hour=15)))

    def test_tamanho_maximo_de_lote(self) -> None:
        cfg = replace(CFG, lote_max=2)
        lote, alunos = self._lote(3)
        self.assertIn("lote_acima_do_maximo", self._validar(lote, alunos, cfg=cfg))

    def test_teto_diario(self) -> None:
        cfg = replace(CFG, teto_diario=10)
        lote, alunos = self._lote(3)
        self.assertEqual(self._validar(lote, alunos, cfg=cfg, enviados_hoje=7), [])
        self.assertIn("teto_diario", self._validar(lote, alunos, cfg=cfg, enviados_hoje=8))

    def test_aluno_repetido_ou_desconhecido_recusa(self) -> None:
        lote, alunos = self._lote(2)
        repetido = replace(lote, contact_ids=("c0", "c0"))
        self.assertIn("aluno_repetido_no_lote", self._validar(repetido, alunos))
        estranho = replace(lote, contact_ids=("c0", "zz"))
        self.assertIn("aluno_excluido", self._validar(estranho, alunos))

    def test_aluno_do_controle_dentro_do_lote_recusa_o_lote(self) -> None:
        lote, alunos = self._lote(2)
        alunos["c1"] = replace(alunos["c1"], controle=True)
        recusas = validar_lote(lote, alunos=alunos, agora=AGORA, cfg=CFG, controle=LIGADO, enviados_hoje=0)
        self.assertEqual([str(r) for r in recusas], ["aluno_excluido: grupo_de_controle × 1"])

    def test_em_modo_real_o_plano_precisa_estar_aprovado(self) -> None:
        real = MotorConfig(enabled=True, dry_run=False)
        lote, alunos = self._lote()
        self.assertIn("plano_nao_aprovado", self._validar(lote, alunos, cfg=real))
        self.assertEqual(self._validar(lote, alunos, cfg=real, plano_aprovado=True), [])
        self.assertEqual(self._validar(lote, alunos), [])  # em simulação a aprovação não trava

    def test_depois_do_corte_recusa(self) -> None:
        lote, alunos = self._lote()
        self.assertIn("depois_do_corte", self._validar(lote, alunos, agora=AGORA.replace(hour=13, minute=30)))

    def test_lote_vazio_recusa(self) -> None:
        lote = Lote(ordem=1, frente=Frente.DIA10, mensagem=Mensagem.LEMBRETE_DIA10, contact_ids=())
        self.assertIn("lote_vazio", self._validar(lote, {}))


class GovernadorTest(unittest.TestCase):
    def _d(self, fila, **kw):
        base = dict(agora=AGORA)
        base.update(kw)
        return decidir(fila, CFG, **base)[0]

    def test_libera_com_fila_baixa_e_equipe_suficiente(self) -> None:
        self.assertEqual(self._d(_fila()), Decisao.LIBERAR)

    def test_segura_no_teto_e_com_poucos_atendentes(self) -> None:
        self.assertEqual(self._d(_fila(aguardando=CFG.fila_teto)), Decisao.SEGURAR)
        self.assertEqual(self._d(_fila(aguardando=CFG.fila_critica)), Decisao.SEGURAR)
        self.assertEqual(self._d(_fila(online=CFG.atendentes_min - 1)), Decisao.SEGURAR)

    def test_pausa_acima_do_critico_e_com_sla_estourado(self) -> None:
        self.assertEqual(self._d(_fila(aguardando=CFG.fila_critica + 1)), Decisao.PAUSAR)
        self.assertEqual(self._d(_fila(espera_s=CFG.sla_espera_s + 1)), Decisao.PAUSAR)

    def test_pausa_fora_da_janela_mesmo_com_fila_vazia(self) -> None:
        self.assertEqual(self._d(_fila(aguardando=0), agora=AGORA.replace(hour=14)), Decisao.PAUSAR)

    def test_lote_anterior_precisa_de_30_minutos_de_vida(self) -> None:
        novo = LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(minutes=29))
        velho = LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(minutes=30))
        self.assertEqual(self._d(_fila(), lote_ativo=novo), Decisao.SEGURAR)
        self.assertEqual(self._d(_fila(), lote_ativo=velho), Decisao.LIBERAR)

    def test_falha_de_envio_acima_de_6_por_cento_pausa(self) -> None:
        ruim = LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(hours=1), enviados=1000, falhas=61)
        ok = LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(hours=1), enviados=1000, falhas=60)
        pouco = LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(hours=1), enviados=10, falhas=5)
        self.assertEqual(self._d(_fila(), lote_ativo=ruim), Decisao.PAUSAR)
        self.assertEqual(self._d(_fila(), lote_ativo=ok), Decisao.LIBERAR)
        self.assertEqual(self._d(_fila(), lote_ativo=pouco), Decisao.LIBERAR)

    def test_sem_leitura_da_fila_nunca_libera(self) -> None:
        self.assertEqual(self._d(None), Decisao.SEGURAR)
        velha = _fila(lida_em=AGORA - timedelta(seconds=CFG.fila_validade_s + 1))
        self.assertEqual(self._d(velha), Decisao.SEGURAR)
        self.assertEqual(self._d(None, campanha_em_andamento=True), Decisao.PAUSAR)

    def test_retrato_de_10_11_teria_pausado(self) -> None:
        # 178 conversas aguardando às 17h20 do dia 10/11 com 7 a 13 atendentes.
        self.assertEqual(self._d(_fila(aguardando=178, online=13)), Decisao.PAUSAR)
        # Antes do envio havia cerca de 45 aguardando: libera.
        self.assertEqual(self._d(_fila(aguardando=45, online=10)), Decisao.LIBERAR)

    def test_tamanho_do_lote_pelo_que_a_equipe_absorve(self) -> None:
        # Conta do simulador: folga = 8 × 6 − 1,1 × 28 = 17,2 por hora; em 30 min, 8,6;
        # mais metade do espaço até o teto da fila (60 − 20 = 40 → 20) = 28,6 conversas;
        # 28,6 ÷ 0,025 = 1.144 → arredonda para 1.150.
        fila = _fila(aguardando=20, online=8)
        self.assertEqual(tamanho_do_lote(fila, CFG, lotes_liberados_hoje=2), 1150)
        # Rampa do dia: o 1º lote sai com no máximo 300 e o 2º com no máximo 600.
        self.assertEqual(tamanho_do_lote(fila, CFG, lotes_liberados_hoje=0), 300)
        self.assertEqual(tamanho_do_lote(fila, CFG, lotes_liberados_hoje=1), 600)
        # Nunca passa do máximo, nem com equipe grande.
        self.assertEqual(tamanho_do_lote(_fila(aguardando=0, online=50), CFG, 5), CFG.lote_max)
        # Folga pequena ainda sai no lote mínimo; fila no teto e sem folga não sai nada.
        self.assertEqual(tamanho_do_lote(_fila(aguardando=55, online=5), CFG, 5), CFG.lote_min)
        self.assertEqual(tamanho_do_lote(_fila(aguardando=60, online=5), CFG, 5), 0)
        self.assertEqual(tamanho_do_lote(None, CFG), 0)

    def test_ultimo_lote_sai_uma_hora_antes_do_fim_da_janela(self) -> None:
        def em(h, m, cfg=CFG):
            t = AGORA.replace(hour=h, minute=m)
            return decidir(_fila(lida_em=t), cfg, agora=t)

        self.assertEqual(em(12, 59)[0], Decisao.LIBERAR)
        decisao, motivo = em(13, 0)
        self.assertEqual(decisao, Decisao.SEGURAR)
        self.assertIn("corte", motivo)
        # Com a janela até as 18h (decisão do Marcelo, ainda não tomada), o corte vai para as 17h.
        ate_18 = replace(CFG, janela_fim_h=18)
        self.assertEqual(em(16, 59, ate_18)[0], Decisao.LIBERAR)
        self.assertEqual(em(17, 0, ate_18)[0], Decisao.SEGURAR)

    def test_trava_de_experiencia_segura_e_encerra(self) -> None:
        from motor.governador import trava_de_experiencia
        from motor.modelos import Experiencia

        # 8 por 1.000 entregues com pelo menos 5 casos: segura. 15 por 1.000: encerra o dia.
        self.assertIsNone(trava_de_experiencia(Experiencia(casos=4, entregues=100), CFG))    # 40 por mil, mas só 4 casos
        self.assertIsNone(trava_de_experiencia(Experiencia(casos=7, entregues=1000), CFG))   # 7 por mil
        self.assertEqual(trava_de_experiencia(Experiencia(casos=8, entregues=1000), CFG), "segurar")
        self.assertEqual(trava_de_experiencia(Experiencia(casos=15, entregues=1000), CFG), "encerrar")
        self.assertIsNone(trava_de_experiencia(Experiencia(casos=9, entregues=0), CFG))
        self.assertEqual(self._d(_fila(), experiencia=Experiencia(8, 1000)), Decisao.SEGURAR)
        self.assertEqual(self._d(_fila(), experiencia=Experiencia(15, 1000)), Decisao.PAUSAR)

    def test_limites_da_trava_de_experiencia_so_apertam(self) -> None:
        import os
        from unittest import mock

        env = {"MOTOR_EXP_SEGURA_POR_MIL": "50", "MOTOR_EXP_ENCERRA_POR_MIL": "99", "MOTOR_EXP_MIN_CASOS": "40", "MOTOR_FALHA_ENVIO_MAX": "0.5"}
        with mock.patch.dict(os.environ, env, clear=False):
            cfg = carregar()
        self.assertEqual((cfg.exp_segura_por_mil, cfg.exp_encerra_por_mil, cfg.exp_min_casos, cfg.falha_envio_max), (8.0, 15.0, 5, 0.06))


class PausaDeEmergenciaTest(unittest.TestCase):
    def test_sem_campanha_em_andamento_nao_ha_o_que_pausar(self) -> None:
        self.assertIsNone(
            pausa_de_emergencia(_fila(aguardando=999), CFG, agora=AGORA, campanha_em_andamento=False)
        )

    def test_pausa_pela_fila_pelo_sla_pelo_fim_da_janela_e_as_cegas(self) -> None:
        def p(fila, agora=AGORA):
            return pausa_de_emergencia(fila, CFG, agora=agora, campanha_em_andamento=True)

        self.assertIsNone(p(_fila()))
        self.assertIn("limite crítico", p(_fila(aguardando=CFG.fila_critica + 1)))
        self.assertIn("SLA", p(_fila(espera_s=CFG.sla_espera_s + 60)))
        self.assertIn("janela", p(_fila(), agora=AGORA.replace(hour=14)))
        self.assertIn("sem leitura", p(None))


if __name__ == "__main__":
    unittest.main()
