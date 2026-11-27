"""Regra do menu: quais telas o usuário logado vê (Fase 4, etapa 1, 27/11/2026).

Antes ficava dentro do `app.inject_nav_perms` e só existia na hora de montar o
HTML com Jinja. Aqui ela vale para os dois lados:

- o Jinja (`app.inject_nav_perms`) continua recebendo `nav_can`, `nav_is_admin`...;
- o `/api/me` devolve o mesmo resultado no bloco `nav`, para o front novo montar o menu.

A regra em si não mudou. Quem decide se a API responde é `permissoes.exige_pagina`;
isto aqui só decide o que aparece no menu.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from flask import session

from db import get_conn
from helpers import ALL_PAGES
from helpers import can_access_subir_blog

# Páginas pessoais — sempre visíveis (exceto regras específicas, ex: comercial sem dashboard)
# Chamados NÃO entram aqui: `solicitacoes_ti` depende da categoria
# (CHAMADOS_ABRIR_CATEGORIAS), `meus_chamados_ti` da categoria ou de já ter
# chamado, e `chamados_ti` (fila) da allowlist — tudo reconciliado em
# _ensure_chamados_ti_page.
SEMPRE = ("avisos", "profile")
# Páginas restritas a admin — nunca visíveis para outros perfis, mesmo com permissão explícita.
SO_ADMIN = frozenset({"siaa_consulta", "siaa_sessao", "match_inadimplentes", "materias_alunos", "tracker_tarefas", "conexao_siaa"})
# Conjunto completo conhecido pelo front (PAGES no utils.js + páginas pessoais)
CONHECIDAS = sorted(set(ALL_PAGES) | set(SEMPRE) | {"dashboard"})

# Grupos do menu lateral: o grupo aparece se o usuário vê pelo menos uma tela dele.
# Eram `{% set _show_... %}` no _sidebar.html; os casos especiais estão em `secoes()`.
GRUPOS: dict[str, tuple[str, ...]] = {
    "academico": (
        "ativacoes", "motor_ativacoes", "distribuicao", "intelligence", "inadimplencia",
        "feedback", "macro_email", "meus_atendimentos", "rematricula",
        "disparador_whatsapp", "academico_interacoes",
    ),
    "ferramentas": (
        "comparar_cursos", "recomendacao_cursos", "localizacao_polos", "info_cursos",
        "leads_inscricao", "cadastro_leads", "subir_blog",
    ),
    "comercial": (
        "dist_consultor", "comercial_rgm", "bwipo_comercial", "dist_comercial", "inscricao",
        "inscricao_automatica", "recadastros", "comercial_dashboard", "auditoria_comercial",
        "atualizar_preco", "precos_bwipo", "leads_parados", "repasse", "captacao", "clicks",
        "leads_promotores", "premiacao_admin",
    ),
    "vocacional": ("vocacional",),
    "meta": ("meta-campaigns",),
    "ti": (
        "ia_comercial", "page_views", "match_inadimplentes", "materias_alunos",
        "tracker_tarefas", "conexao_siaa",
    ),
    "chamados": ("solicitacoes_ti", "meus_chamados_ti", "chamados_ti"),
    "sistema": (
        "pipeline", "update", "ajustes_matricula", "kommo_sync", "bwipo_sync", "match_merge",
        "config", "schedule", "kommo_dispatcher", "logs",
    ),
    # Subgrupos dentro de Sistema.
    "sistema_crm": ("pipeline", "update", "ajustes_matricula", "kommo_sync", "bwipo_sync", "match_merge"),
    "sistema_geral": ("config", "schedule", "kommo_dispatcher", "logs"),
    "premiacoes_internas": ("premiacoes_internas", "aprovacao_premiacoes"),
}


def carregar_usuario() -> tuple[str, set, str]:
    """Retorna (role, set(pages), categoria) do usuário logado, com fallback seguro."""
    if not session.get("authenticated"):
        return "", set(), ""
    role = (session.get("role") or "").strip()
    uid = session.get("user_id", 0)
    if role == "admin" or uid == 0:
        return (role or "admin"), set(ALL_PAGES), ""
    pages = set()
    categoria = ""
    try:
        conn = get_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT categoria FROM app_users WHERE id = %s", (uid,))
            r = cur.fetchone()
            if r and r[0]:
                categoria = r[0]
            cur.execute("SELECT page FROM user_permissions WHERE user_id = %s", (uid,))
            pages = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception:
        pages = set()
    # Promove sub-permissoes "disparador_whatsapp_*" pra master "disparador_whatsapp"
    # — o sidebar/nav_can testa a master pra decidir se mostra o link no menu
    # do Acadêmico. Ter qualquer sub equivale a ter acesso ao modulo.
    if any(p.startswith("disparador_whatsapp_") for p in pages):
        pages.add("disparador_whatsapp")
    # Inscrição - Automática: quem administra também opera (o menu testa a master).
    if "inscricao_automatica_admin" in pages:
        pages.add("inscricao_automatica")
    # Fila de chamados é uma página só, escopada por departamento no backend.
    # Quem tem a fila de Marketing (`chamados_marketing`) precisa abrir a
    # mesma página `chamados_ti` — lá só verá os chamados de Marketing.
    if "chamados_marketing" in pages:
        pages.add("chamados_ti")
    return role, pages, categoria


@dataclass
class Menu:
    role: str
    pages: set
    categoria: str
    username: str
    is_admin: bool = field(init=False)
    is_comercial: bool = field(init=False)
    is_suporte_comercial: bool = field(init=False)
    pode_chamado: bool = field(init=False)

    def __post_init__(self):
        from helpers import (
            is_suporte_comercial_categoria,
            is_suporte_comercial_login,
            pode_abrir_chamado,
        )

        cat = (self.categoria or "").strip().lower()
        self.is_admin = self.role == "admin"
        self.is_comercial = (not self.is_admin) and cat == "comercial"
        self.is_suporte_comercial = (not self.is_admin) and (
            is_suporte_comercial_categoria(self.categoria)
            or is_suporte_comercial_login(self.username)
        )
        self.pode_chamado = pode_abrir_chamado(self.role, self.categoria)

    @property
    def perf_home(self) -> bool:
        return self.is_comercial or self.is_suporte_comercial

    def pode(self, page: str) -> bool:
        """A tela aparece no menu? (era o `nav_can` do Jinja)"""
        if page == "subir_blog":
            return can_access_subir_blog(self.role, self.username, self.pages)
        if self.is_admin:
            return True
        if page in SO_ADMIN:
            return False
        # Abrir chamado segue a categoria (mesma regra do gate no POST).
        if page == "solicitacoes_ti":
            return self.pode_chamado
        # Quem perdeu o direito de abrir continua acompanhando o histórico.
        if page == "meus_chamados_ti":
            return self.pode_chamado or ("meus_chamados_ti" in self.pages)
        if self.perf_home and page == "dashboard":
            return False
        if page in SEMPRE:
            return True
        if (not self.perf_home) and page == "dashboard":
            return True
        return page in self.pages

    @property
    def pagina_inicial(self) -> str:
        cat = (self.categoria or "").strip().lower()
        academico_simples = (
            (not self.is_admin)
            and cat in ("acadêmico", "academico")
            and ("meus_atendimentos" in self.pages)
        )
        if self.perf_home:
            return "minha_performance"
        if academico_simples:
            return "meus_atendimentos"
        return "dashboard"

    def paginas_permitidas(self) -> list[str]:
        return sorted(p for p in CONHECIDAS if self.pode(p))

    def secoes(self) -> dict[str, bool]:
        """Quais grupos do menu lateral aparecem."""
        algum = {nome: any(self.pode(p) for p in paginas) for nome, paginas in GRUPOS.items()}
        # Minha Performance fica no grupo Comercial só para quem não a tem como home.
        minha_perf_no_comercial = (not self.is_comercial) and self.pode("minha_performance")
        algum["minha_perf_no_comercial"] = minha_perf_no_comercial
        algum["comercial"] = algum["comercial"] or minha_perf_no_comercial
        # Acadêmico: admin vê o grupo mesmo sem nenhuma tela marcada.
        algum["academico"] = algum["academico"] or self.is_admin
        # TI: só admin.
        algum["ti"] = self.is_admin and algum["ti"]
        return algum

    def para_api(self) -> dict:
        """Bloco `nav` do /api/me: o que o front novo precisa para montar o menu."""
        return {
            "is_admin": self.is_admin,
            "is_comercial": self.is_comercial,
            "is_suporte_comercial": self.is_suporte_comercial,
            "pagina_inicial": self.pagina_inicial,
            "paginas": self.paginas_permitidas(),
            "secoes": self.secoes(),
        }


def menu_do_usuario() -> Menu:
    role, pages, categoria = carregar_usuario()
    return Menu(role, pages, categoria, (session.get("username") or "").strip())
