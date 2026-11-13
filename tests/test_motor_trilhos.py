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

# Expediente curto (até as 14h) na maior parte dos testes; o padrão de 22h tem teste próprio.
CFG = MotorConfig(enabled=True, dry_run=True, janela_fim_h=14)
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

    def test_padrao_e_o_expediente_ate_as_22h(self) -> None:
        padrao = MotorConfig()
        self.assertTrue(dentro_da_janela(AGORA.replace(hour=21, minute=59), padrao))
        self.assertFalse(dentro_da_janela(AGORA.replace(hour=22, minute=0), padrao))
        self.assertFalse(dentro_da_janela(AGORA.replace(hour=8, minute=59), padrao))

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
        self.assertEqual(cfg.janela_fim_h, 22)  # fim do expediente configurável, com teto de 22h em código
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
    """A fila conta pela subida desde o último lote: +30 segura, +60 pausa. A maior espera não entra."""

    def _d(self, fila, **kw):
        base = dict(agora=AGORA)
        base.update(kw)
        return decidir(fila, CFG, **base)[0]

    def _lote(self, base=20, minutos=40, **kw) -> LoteAtivo:
        return LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(minutes=minutos), fila_base=base, **kw)

    def test_libera_com_equipe_suficiente(self) -> None:
        self.assertEqual(self._d(_fila()), Decisao.LIBERAR)
        self.assertEqual(self._d(_fila(online=CFG.atendentes_min - 1)), Decisao.SEGURAR)

    def test_sem_lote_anterior_o_numero_absoluto_da_fila_nao_segura(self) -> None:
        # O awaitingReply do CRM veio em 90 num dia comum (57 conversas paradas havia mais de 1 h).
        self.assertEqual(self._d(_fila(aguardando=90)), Decisao.LIBERAR)
        self.assertEqual(self._d(_fila(aguardando=178)), Decisao.LIBERAR)

    def test_segura_quando_a_fila_sobe_30_desde_o_lote_anterior(self) -> None:
        self.assertEqual(self._d(_fila(aguardando=49), lote_ativo=self._lote(base=20)), Decisao.LIBERAR)
        decisao, motivo = decidir(_fila(aguardando=50), CFG, agora=AGORA, lote_ativo=self._lote(base=20))
        self.assertEqual(decisao, Decisao.SEGURAR)
        self.assertIn("subiu 30", motivo)
        self.assertEqual(self._d(_fila(aguardando=79), lote_ativo=self._lote(base=20)), Decisao.SEGURAR)

    def test_pausa_quando_a_fila_sobe_60_desde_o_lote(self) -> None:
        self.assertEqual(self._d(_fila(aguardando=80), lote_ativo=self._lote(base=20)), Decisao.PAUSAR)
        # 10/11: cerca de 45 aguardando antes do envio e 178 às 17h20.
        self.assertEqual(self._d(_fila(aguardando=178, online=13), lote_ativo=self._lote(base=45)), Decisao.PAUSAR)
        # A fila caiu abaixo do nível do lote: segue normal.
        self.assertEqual(self._d(_fila(aguardando=5), lote_ativo=self._lote(base=45)), Decisao.LIBERAR)

    def test_maior_espera_nao_segura_nem_pausa(self) -> None:
        # Ao vivo, em 13/11, a maior espera veio em 38 horas úteis com overSla ligado.
        fila = _fila(espera_s=38 * 3600)
        self.assertEqual(self._d(fila), Decisao.LIBERAR)
        self.assertEqual(self._d(fila, lote_ativo=self._lote(base=20)), Decisao.LIBERAR)

    def test_nivel_do_lote_vale_por_duas_horas(self) -> None:
        # Três horas depois a subida já não é daquele lote: o próximo mede do nível de agora.
        self.assertEqual(self._d(_fila(aguardando=90), lote_ativo=self._lote(base=20, minutos=119)), Decisao.PAUSAR)
        self.assertEqual(self._d(_fila(aguardando=90), lote_ativo=self._lote(base=20, minutos=181)), Decisao.LIBERAR)

    def test_limites_da_fila_so_apertam(self) -> None:
        import os
        from unittest import mock

        with mock.patch.dict(os.environ, {"MOTOR_FILA_SOBE_SEGURA": "500", "MOTOR_FILA_SOBE_PAUSA": "900"}, clear=False):
            cfg = carregar()
        self.assertEqual((cfg.fila_sobe_segura, cfg.fila_sobe_pausa), (30, 60))
        with mock.patch.dict(os.environ, {"MOTOR_FILA_SOBE_SEGURA": "10", "MOTOR_FILA_SOBE_PAUSA": "5"}, clear=False):
            cfg = carregar()
        self.assertEqual((cfg.fila_sobe_segura, cfg.fila_sobe_pausa), (10, 10))   # a pausa nunca fica abaixo do segurar

    def test_pausa_fora_da_janela_mesmo_com_fila_vazia(self) -> None:
        self.assertEqual(self._d(_fila(aguardando=0), agora=AGORA.replace(hour=14)), Decisao.PAUSAR)

    def test_lote_anterior_precisa_de_30_minutos_de_vida(self) -> None:
        self.assertEqual(self._d(_fila(), lote_ativo=self._lote(base=20, minutos=29)), Decisao.SEGURAR)
        self.assertEqual(self._d(_fila(), lote_ativo=self._lote(base=20, minutos=30)), Decisao.LIBERAR)

    def test_falha_de_envio_acima_de_6_por_cento_pausa(self) -> None:
        self.assertEqual(self._d(_fila(), lote_ativo=self._lote(minutos=60, enviados=1000, falhas=61)), Decisao.PAUSAR)
        self.assertEqual(self._d(_fila(), lote_ativo=self._lote(minutos=60, enviados=1000, falhas=60)), Decisao.LIBERAR)
        self.assertEqual(self._d(_fila(), lote_ativo=self._lote(minutos=60, enviados=10, falhas=5)), Decisao.LIBERAR)

    def test_sem_leitura_da_fila_nunca_libera(self) -> None:
        self.assertEqual(self._d(None), Decisao.SEGURAR)
        velha = _fila(lida_em=AGORA - timedelta(seconds=CFG.fila_validade_s + 1))
        self.assertEqual(self._d(velha), Decisao.SEGURAR)
        self.assertEqual(self._d(None, campanha_em_andamento=True), Decisao.PAUSAR)

    def test_tamanho_do_lote_pelo_que_a_equipe_absorve(self) -> None:
        # Conta do simulador: folga = 8 × 6 − 1,1 × 28 = 17,2 por hora; em 30 min, 8,6;
        # mais metade do espaço que a fila ainda pode subir (30 → 15) = 23,6 conversas;
        # 23,6 ÷ 0,025 = 944 → arredonda para 950.
        fila = _fila(aguardando=20, online=8)
        self.assertEqual(tamanho_do_lote(fila, CFG, lotes_liberados_hoje=2), 950)
        # A fila já subiu 20 desde o lote anterior: sobra espaço de 10 → 8,6 + 5 = 13,6 → 550.
        self.assertEqual(tamanho_do_lote(_fila(aguardando=40, online=8), CFG, 2, fila_base=20), 550)
        # Rampa do dia: o 1º lote sai com no máximo 300 e o 2º com no máximo 600.
        self.assertEqual(tamanho_do_lote(fila, CFG, lotes_liberados_hoje=0), 300)
        self.assertEqual(tamanho_do_lote(fila, CFG, lotes_liberados_hoje=1), 600)
        # Nunca passa do máximo, nem com equipe grande.
        self.assertEqual(tamanho_do_lote(_fila(aguardando=0, online=50), CFG, 5), CFG.lote_max)
        # Folga pequena ainda sai no lote mínimo; sem folga e sem espaço não sai nada.
        self.assertEqual(tamanho_do_lote(_fila(aguardando=48, online=5), CFG, 5, fila_base=20), CFG.lote_min)
        self.assertEqual(tamanho_do_lote(_fila(aguardando=50, online=5), CFG, 5, fila_base=20), 0)
        self.assertEqual(tamanho_do_lote(None, CFG), 0)

    def test_ultimo_lote_sai_uma_hora_antes_do_fim_do_expediente(self) -> None:
        def em(h, m, cfg):
            t = AGORA.replace(hour=h, minute=m)
            return decidir(_fila(lida_em=t), cfg, agora=t)

        # Padrão (decisão do Marcelo, 13/11): atendimento até as 22h, último lote até as 21h.
        padrao = MotorConfig(enabled=True, dry_run=True)
        self.assertEqual((padrao.janela_inicio_h, padrao.janela_fim_h, padrao.corte_min), (9, 22, 60))
        self.assertEqual(em(20, 59, padrao)[0], Decisao.LIBERAR)
        decisao, motivo = em(21, 0, padrao)
        self.assertEqual(decisao, Decisao.SEGURAR)
        self.assertIn("corte", motivo)
        self.assertEqual(em(21, 59, padrao)[0], Decisao.SEGURAR)
        self.assertEqual(em(22, 0, padrao)[0], Decisao.PAUSAR)           # fim do expediente
        # Corte menor não empurra o último lote para depois das 21h (teto em código).
        self.assertEqual(em(21, 10, replace(padrao, corte_min=30))[0], Decisao.SEGURAR)
        # Expediente mais curto configurado: o corte acompanha.
        self.assertEqual(em(12, 59, CFG)[0], Decisao.LIBERAR)             # CFG dos testes: até as 14h
        self.assertEqual(em(13, 0, CFG)[0], Decisao.SEGURAR)
        self.assertEqual(em(16, 59, replace(CFG, janela_fim_h=18))[0], Decisao.LIBERAR)
        self.assertEqual(em(17, 0, replace(CFG, janela_fim_h=18))[0], Decisao.SEGURAR)

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
    LOTE = LoteAtivo(lote_id=1, liberado_em=AGORA - timedelta(minutes=10), fila_base=20)

    def test_sem_campanha_em_andamento_nao_ha_o_que_pausar(self) -> None:
        self.assertIsNone(pausa_de_emergencia(_fila(aguardando=999), CFG, agora=AGORA, campanha_em_andamento=False, lote_ativo=self.LOTE))

    def test_pausa_pela_subida_da_fila_pelo_fim_do_expediente_e_as_cegas(self) -> None:
        def p(fila, agora=AGORA):
            return pausa_de_emergencia(fila, CFG, agora=agora, campanha_em_andamento=True, lote_ativo=self.LOTE)

        self.assertIsNone(p(_fila()))
        self.assertIsNone(p(_fila(aguardando=79)))                 # +59: ainda não pausa
        self.assertIn("fila subiu 60", p(_fila(aguardando=80)))
        self.assertIsNone(p(_fila(espera_s=40 * 3600)))            # maior espera não pausa
        self.assertIn("janela", p(_fila(), agora=AGORA.replace(hour=14)))
        self.assertIn("sem leitura", p(None))


if __name__ == "__main__":
    unittest.main()
