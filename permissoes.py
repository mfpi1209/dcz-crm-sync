"""Permissão por página conferida no servidor (Fase 1 da modernização).

Até aqui a tabela `user_permissions` só escondia itens do menu: qualquer usuário
logado chamava direto as APIs de escrita (preços, agendamentos, upload de
matriculados...). `exige_pagina` faz o servidor conferir a mesma tabela, com a
mesma regra que `routes/bwipo_precos.py` já usava:

- admin (papel `admin`) e o login de emergência (`user_id` 0) passam sempre;
- os demais passam se tiverem QUALQUER uma das páginas indicadas.

Cada rota recebe as páginas cujas telas a chamam (mapeado no front em 25/11/2026),
para nenhum usuário perder algo que já usa pela tela.
"""
from __future__ import annotations

from functools import wraps

from flask import jsonify, session

from db import get_conn

SEM_PERMISSAO = "Sem permissão para esta ação."


def tem_alguma_pagina(*paginas: str) -> bool:
    if not session.get("authenticated"):
        return False
    if (session.get("role") or "") == "admin" or session.get("user_id", 0) == 0:
        return True
    uid = session.get("user_id")
    if not uid or not paginas:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM user_permissions WHERE user_id = %s AND page = ANY(%s) LIMIT 1",
                (uid, list(paginas)),
            )
            if cur.fetchone() is not None:
                return True
            return "dashboard" in paginas and _dashboard_pelo_menu(cur, uid)
    finally:
        conn.close()


def _dashboard_pelo_menu(cur, uid) -> bool:
    """`dashboard` segue a regra do menu (app.inject_nav_perms / nav_can), não só a tabela.

    O menu mostra a home para quem não é comercial nem suporte comercial, sem olhar
    `user_permissions` (em 26/11, só 7 de 40 usuários tinham a página gravada).
    """
    from helpers import is_suporte_comercial_categoria, is_suporte_comercial_login

    cur.execute("SELECT categoria, username FROM app_users WHERE id = %s", (uid,))
    row = cur.fetchone()
    if not row:
        return False
    categoria, username = row
    if (categoria or "").strip().lower() == "comercial":
        return False
    if is_suporte_comercial_categoria(categoria) or is_suporte_comercial_login(username):
        return False
    return True


def exige_pagina(*paginas: str):
    """Decorator: 403 em JSON se o usuário não tiver nenhuma das páginas. Sem páginas = só admin."""
    def deco(view):
        @wraps(view)
        def wrapper(*args, **kwargs):
            if not tem_alguma_pagina(*paginas):
                return jsonify({"ok": False, "error": SEM_PERMISSAO}), 403
            return view(*args, **kwargs)
        wrapper.paginas_exigidas = paginas
        return wrapper
    return deco


def so_admin(view):
    return exige_pagina()(view)


# ── Rotas que ainda não checavam permissão (aprovado em 26/11/2026) ──────
# Aplicado no app.py por `aplicar_mapa(app)`, depois de registrar os blueprints,
# para não espalhar decorators por 26 arquivos. Páginas = telas que chamam a rota
# (levantado no front em 26/11). Tupla vazia = só admin (diagnóstico e manutenção
# que nenhuma tela chama). Rota /api/ nova: @exige_pagina(...) na própria rota ou
# uma linha aqui; tests/test_permissao_rotas.py falha se ficar sem nenhum dos dois.

# Livres de propósito: sem login (/health, /login), saída, a casca HTML (o menu já
# filtra as telas), o rastreio de uso e os avisos (popup para todos os usuários).
ROTAS_SEM_PAGINA = frozenset({
    "/health",
    "/login",
    "/logout",
    "/",
    # Casca do front novo (Fase 4): as telas chamam /api/ com a mesma permissão de sempre.
    "/app",
    "/app/",
    "/app/<path:caminho>",
    "/api/track-page-view",
    "/api/avisos",
    "/api/avisos/nao-lidos",
    "/api/avisos/<int:aviso_id>/lido",
    "/api/avisos/marcar-todos-lidos",
})

# Sem página de propósito: cada um só lê/grava o que é seu (escopo pelo user_id da
# sessão dentro da rota). Exigir página aqui tiraria o acesso do próprio dono.
ROTAS_DO_PROPRIO_USUARIO = frozenset({
    "/api/minha-performance/minhas-matriculas",  # matrículas que o consultor lança para si
    "/api/solicitacoes_ti/meus",                 # chamados abertos pela própria pessoa
})

PAGINAS_POR_ENDPOINT: dict[str, tuple[str, ...]] = {
    # academico_interacoes
    'academico_interacoes_bp.atender': ('academico_interacoes',),
    'academico_interacoes_bp.atender_preview': ('academico_interacoes',),
    'academico_interacoes_bp.list_claims': ('academico_interacoes',),
    'academico_interacoes_bp.registrar_ligacao': ('academico_interacoes',),
    'academico_interacoes_bp.resumo': ('academico_interacoes',),
    # ativacoes
    'ativacoes.proxy_ativacoes': ('ativacoes',),
    # bwipo_comercial
    'bwipo_bp.api_bwipo_painel': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_agente_carteira': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_conflitos': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_conflitos_resolver': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_contatos': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_deals': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_diagnostico': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_duplicatas': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_extras': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_mini_sync': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_rgm': ('bwipo_comercial',),
    'bwipo_bp.api_bwipo_painel_sem_data': ('bwipo_comercial',),
    # bwipo_sync
    'bwipo_bp.api_bwipo_deal_refresh': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_depara_link': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_depara_search': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_funnel': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_recent_changes': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_status': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_sync': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_sync_cancel': ('bwipo_sync',),
    'bwipo_bp.api_bwipo_task': ('bwipo_sync',),
    # captacao
    'captacao_bp.captacao_lead': ('captacao',),
    # comercial_rgm, bwipo_comercial
    'comercial_rgm.crgm_agente_detalhe': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_atividade_kommo': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_ciclo_atual': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_ciclos_create': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_ciclos_delete': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_ciclos_list': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_ciclos_update': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_conflitos': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_conflitos_resolver': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_conflitos_resolver_delete': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_congelar': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_data_agentes': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_data_grids': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_data_kpis': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_delete_meta': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_duplicatas': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_filters': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_interacao_leads': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_matriculas_sem_data': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_metas_categorias': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_rgm_atribuicao': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_snapshot_info': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_sync_users': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_turmas_create': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_turmas_delete': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_turmas_list': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_turmas_stats': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_update_meta': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_update_metas_batch': ('comercial_rgm', 'bwipo_comercial'),
    'comercial_rgm.crgm_upload': ('comercial_rgm', 'bwipo_comercial'),
    # comercial_rgm, bwipo_comercial, dist_consultor
    'comercial_rgm.crgm_kommo_sync_lead': ('comercial_rgm', 'bwipo_comercial', 'dist_consultor'),
    # comercial_rgm, bwipo_comercial, minha_performance
    'comercial_rgm.crgm_data': ('comercial_rgm', 'bwipo_comercial', 'minha_performance'),
    'comercial_rgm.crgm_get_metas': ('comercial_rgm', 'bwipo_comercial', 'minha_performance'),
    'comercial_rgm.crgm_save_metas': ('comercial_rgm', 'bwipo_comercial', 'minha_performance'),
    # config
    'config_bp.api_ciclos_list': ('config',),
    'config_bp.api_turmas_list': ('config',),
    # dashboard
    'dashboard.api_dashboard_ciclos': ('dashboard',),
    'dashboard.api_dashboard_funnel_yesterday': ('dashboard',),
    'dashboard.api_dashboard_inbound_transfers': ('dashboard',),
    'dashboard.api_dashboard_students': ('dashboard',),
    'kommo_bp.api_kommo_funnel_status': ('dashboard',),
    'kommo_bp.api_kommo_new_leads_today': ('dashboard',),
    'kommo_bp.api_kommo_yesterday_summary': ('dashboard',),
    'supervisor_dashboard.api_dashboard_supervisor': ('dashboard',),
    'upload_bp.api_lista_alunos_historico': ('dashboard',),
    'upload_bp.api_lista_alunos_latest': ('dashboard',),
    # dashboard, kommo_sync
    'kommo_bp.api_kommo_funnel_live': ('dashboard', 'kommo_sync'),
    # dashboard, rematricula
    'dashboard.api_dashboard': ('dashboard', 'rematricula'),
    'dashboard.api_dashboard_ciclos_distinct': ('dashboard', 'rematricula'),
    'dashboard.api_dashboard_timeline': ('dashboard', 'rematricula'),
    # dist_consultor, comercial_rgm
    'comercial_rgm.dist_consultor_filters': ('dist_consultor', 'comercial_rgm'),
    'comercial_rgm.dist_consultor_total_kommo': ('dist_consultor', 'comercial_rgm'),
    # distribuicao, macro_email, dashboard
    'config_bp.api_distribuicao_get': ('distribuicao', 'macro_email', 'dashboard'),
    # inadimplencia
    'inadimplencia.api_inadimplencia_atual': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_comparar': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_comparar_periodo': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_competencias': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_delete_snapshot': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_evolucao': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_evolucao_por_mes': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_list': ('inadimplencia',),
    'inadimplencia.api_inadimplencia_reincidencia': ('inadimplencia',),
    # inscricao
    'inscricao_bp.api_inscricao': ('inscricao',),
    'inscricao_bp.api_inscricao_afiliados': ('inscricao',),
    # intelligence
    'engagement.api_comm_evaluate': ('intelligence',),
    'engagement.api_comm_log_list': ('intelligence',),
    'engagement.api_comm_queue': ('intelligence',),
    'engagement.api_comm_rules_create': ('intelligence',),
    'engagement.api_comm_rules_delete': ('intelligence',),
    'engagement.api_comm_rules_list': ('intelligence',),
    'engagement.api_comm_rules_update': ('intelligence',),
    'engagement.api_engagement_export_sem_ava': ('intelligence',),
    'engagement.api_engagement_recalculate': ('intelligence',),
    'engagement.api_engagement_scores': ('intelligence',),
    'engagement.api_engagement_student': ('intelligence',),
    'engagement.api_engagement_timeline': ('intelligence',),
    'upload_bp.api_snapshots_compare': ('intelligence',),
    'upload_bp.api_snapshots_crossref': ('intelligence',),
    'upload_bp.api_snapshots_crossref_export': ('intelligence',),
    'upload_bp.api_snapshots_timeline': ('intelligence',),
    # kommo_dispatcher
    'kommo_dispatcher.proxy_stats': ('kommo_dispatcher',),
    # kommo_sync
    'kommo_bp.api_kommo_leads_by_stage': ('kommo_sync',),
    'kommo_bp.api_kommo_pipelines': ('kommo_sync',),
    'kommo_bp.api_kommo_recent_changes': ('kommo_sync',),
    'kommo_bp.api_kommo_status': ('kommo_sync',),
    'kommo_bp.api_kommo_sync_cancel': ('kommo_sync',),
    # kommo_sync, comercial_rgm
    'kommo_bp.api_kommo_sync': ('kommo_sync', 'comercial_rgm'),
    'kommo_bp.api_kommo_task': ('kommo_sync', 'comercial_rgm'),
    # leads_parados
    'leads_parados_bp.api_distribuir_leads': ('leads_parados',),
    'leads_parados_bp.api_leads_parados': ('leads_parados',),
    # logs, config
    'logs_bp.api_logs_download': ('logs', 'config'),
    'logs_bp.api_logs_list': ('logs', 'config'),
    'logs_bp.api_logs_view': ('logs', 'config'),
    # macro_email
    'config_bp.api_me_consultores': ('macro_email',),
    # match_merge
    'kommo_merge.api_merge_job_status': ('match_merge',),
    'kommo_merge.api_merge_preview': ('match_merge',),
    'match_merge.mm_clear_uploads': ('match_merge',),
    'match_merge.mm_data_corte': ('match_merge',),
    'match_merge.mm_exec_status': ('match_merge',),
    'match_merge.mm_execute_unificar_lote': ('match_merge',),
    'match_merge.mm_logs': ('match_merge',),
    'match_merge.mm_preview': ('match_merge',),
    'match_merge.mm_preview_export': ('match_merge',),
    'match_merge.mm_status': ('match_merge',),
    'match_merge.mm_unif_status': ('match_merge',),
    'match_merge.mm_upload': ('match_merge',),
    'match_merge.mm_upload_info': ('match_merge',),
    # meta-campaigns
    'dashboard.api_google_campaigns': ('meta-campaigns',),
    'dashboard.api_meta_campaigns': ('meta-campaigns',),
    'dashboard.api_sem_campanha_leads': ('meta-campaigns',),
    # minha_performance
    'minha_performance.api_ajustes_agent_create': ('minha_performance',),
    'minha_performance.api_minha_campanhas': ('minha_performance',),
    'minha_performance.api_mp_sync_bwipo_prefill': ('minha_performance',),
    'minha_performance.api_mp_sync_lead_prefill': ('minha_performance',),
    # motor_ativacoes
    'motor_reprise_bp.api_motor_reprise': ('motor_ativacoes',),
    'motor_reprise_bp.api_motor_reprise_rodadas': ('motor_ativacoes',),
    'motor_resultados_bp.api_motor_resultados': ('motor_ativacoes',),
    # recadastros
    'dashboard.api_recadastros': ('recadastros',),
    # schedule, config
    'config_bp.api_schedules_list': ('schedule', 'config'),
    # search
    'crm.api_search': ('search',),
    'crm.api_search_xl': ('search',),
    'upload_bp.api_xl_snapshots': ('search',),
    # sync
    'crm.api_sync': ('sync',),
    'crm.api_sync_logs': ('sync',),
    'crm.api_sync_state': ('sync',),
    'crm.api_sync_status': ('sync',),
    'crm.api_sync_stop': ('sync',),
    # update, distribuicao, intelligence
    'upload_bp.api_upload_info': ('update', 'distribuicao', 'intelligence'),
    # só admin
    'bwipo_bp.api_bwipo_connection_test': (),
    'comercial_rgm.crgm_cache_clear': (),
    'comercial_rgm.crgm_diagnostics': (),
    'comercial_rgm.crgm_populate': (),
    'crm.api_debug_address': (),
    'dashboard.api_meta_webhook': (),
    'engagement.api_comm_callback': (),
    'engagement.api_comm_dispatch_manual': (),
    'kommo_bp.api_diag_kommo': (),
    'kommo_bp.api_diag_kommo_probe': (),
    'kommo_bp.api_kommo_connection_test': (),
    'kommo_bp.api_kommo_reconcile_aceites': (),
    'kommo_bp.api_sync_responsible_history': (),
    'kommo_merge.api_merge_session_status': (),
}


def aplicar_mapa(app) -> list[str]:
    """Embrulha cada endpoint do mapa com `exige_pagina`. Devolve os que não existem no app."""
    faltando = []
    for endpoint, paginas in PAGINAS_POR_ENDPOINT.items():
        view = app.view_functions.get(endpoint)
        if view is None:
            faltando.append(endpoint)
            continue
        if hasattr(view, "paginas_exigidas"):  # já embrulhado (ou checado na própria rota)
            continue
        app.view_functions[endpoint] = exige_pagina(*paginas)(view)
    return faltando
