"""Quem roda cada job agendado: o site (Gunicorn) ou o worker (Modernização, Fase 3).

`AGENDADOR` escolhe o papel deste processo:
- `tudo` (padrão): todos os jobs, como antes da Fase 3;
- `site`: só os jobs que aquecem cache na memória do próprio site;
- `worker`: os demais (gravam no banco ou chamam APIs). É o que `worker.py` usa;
- `nenhum`: nenhum job (ex.: scripts e testes).

Os jobs que gravam no banco ou chamam APIs rodam com a trava do `trava_jobs`: se
outro processo já está rodando o mesmo job, esta execução é pulada. Os que aquecem
cache na memória (`JOBS_POR_PROCESSO`) rodam sem trava, um por processo do site.

Ordem para ligar em produção (EasyPanel):
1. site com `AGENDADOR=site`;
2. serviço novo com a mesma imagem e o comando `python worker.py`.
Com o site em `tudo` e o worker ligado, cada job rodaria nos dois (a trava evita
só que rodem ao mesmo tempo).
"""
from __future__ import annotations

import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler

from trava_jobs import com_trava

logger = logging.getLogger(__name__)

PAPEIS = ("tudo", "site", "worker", "nenhum")

# Jobs que escrevem em memória que as rotas do site leem: precisam rodar dentro do site.
JOBS_DO_SITE = frozenset({
    "funnel_cache_warm",        # kommo_sync._funnel_cache (funil da home)
    "funnel_cache_warm_boot",
    "mp_bwipo_periodo_warm",    # minha_performance._bwipo_periodo_cache
    "mv_leads_rgm_site",        # leads_rgm._ultimo_ok: o site passa a usar a cópia indexada
    "painel_cache_vigia",       # vence o cache do painel quando o worker remonta a base
    "alunos_home_warm",         # dashboard._ALUNOS_CACHE (rota de alunos da home)
})
# Gravam no banco, chamam APIs ou o n8n: vão para o worker, sempre com a trava.
# Lista só para conferência (tests/test_fase3_agendador.py): job fora das duas listas
# vai para o worker, mas o teste falha até alguém decidir de que lado ele fica.
JOBS_DO_WORKER = frozenset({
    "bwipo_owner_recente",           # Bwipo: dono dos leads novos
    "bwipo_painel_base",             # tabela bwipo_painel_base (o site vê pelo painel_cache_vigia)
    "bwipo_contact_ads",             # origem dos contatos (Meta/Google) → bwipo_contact_ads
    "kommo_leads_dia",               # tabela kommo_leads_dia
    "mv_leads_rgm",                  # cria/refaz a cópia indexada (o site confere com mv_leads_rgm_site)
    "dist_comercial_schedule_check", # aplica turnos agendados (n8n)
    "blog_scheduled_publish",        # publica posts agendados (Supabase)
    "conversao_backfill",
    "conversao_backfill_boot",
    "engagement_daily",              # scores e fila de comunicação (n8n)
    "siaa_avisos_incidentes",        # avisos de incidente SIAA (última execução em painel_job_status)
    "aceite_reconcile",              # Kommo (só com KOMMO_API_JOBS=1)
    "responsible_history_daily",     # Kommo (só com KOMMO_API_JOBS=1)
})
# Agendamentos da tela Config (/api/schedules): a rota mexe no agendador do próprio site.
FUNCOES_DO_SITE = frozenset({"_run_scheduled_sync"})

# Valem para a memória de cada processo: cada worker do Gunicorn precisa rodar o seu.
# Ficam SEM a trava do banco (com ela, só um processo aqueceria o cache).
JOBS_POR_PROCESSO = frozenset({
    "funnel_cache_warm",
    "funnel_cache_warm_boot",
    "mp_bwipo_periodo_warm",
    "mv_leads_rgm_site",
    "painel_cache_vigia",
    "alunos_home_warm",
})


def papel() -> str:
    p = os.getenv("AGENDADOR", "tudo").strip().lower()
    if p not in PAPEIS:
        logger.warning("AGENDADOR=%r desconhecido; usando 'tudo'", p)
        return "tudo"
    return p


def lado_do_job(job_id: str | None, func) -> str:
    nome = getattr(func, "__name__", "")
    if (job_id or "") in JOBS_DO_SITE or nome in FUNCOES_DO_SITE:
        return "site"
    return "worker"


def precisa_trava(job_id: str | None, func) -> bool:
    """Trava só o que grava no banco ou chama API; cache por processo não."""
    if (job_id or "") in JOBS_POR_PROCESSO:
        return False
    return getattr(func, "__name__", "") not in FUNCOES_DO_SITE


def roda_aqui(job_id: str | None, func, p: str | None = None) -> bool:
    p = p or papel()
    if p == "tudo":
        return True
    if p == "nenhum":
        return False
    return lado_do_job(job_id, func) == p


class Agendador(BackgroundScheduler):
    """BackgroundScheduler que filtra os jobs pelo papel e põe a trava em cada um."""

    def __init__(self, *a, papel_do_processo: str | None = None, **k):
        super().__init__(*a, **k)
        self.papel = papel_do_processo or papel()
        self.ignorados: list[str] = []

    def add_job(self, func, *args, **kwargs):
        job_id = kwargs.get("id")
        if not roda_aqui(job_id, func, self.papel):
            self.ignorados.append(job_id or getattr(func, "__name__", "?"))
            logger.info("[agendador] %s não roda neste processo (papel=%s)", job_id, self.papel)
            return None
        if callable(func) and precisa_trava(job_id, func):
            func = com_trava(job_id or getattr(func, "__name__", "job"), func)
        return super().add_job(func, *args, **kwargs)
