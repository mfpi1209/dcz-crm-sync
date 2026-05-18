# AGENTS.md — dcz-crm-sync

Este arquivo registra decisões técnicas tomadas em conjunto com agentes Opus, para que execuções futuras (qualquer modelo) sigam o que já foi acordado sem refazer trade-offs.

## Decisões técnicas

### 2026-05-13 — Redesign visual com flag de rollback (Fase 1)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Adotar redesign visual padronizado (referência: "Executive Architect Dashboard") em fases, com coexistência v1/v2 controlada por cookie (`dcz_ui`) e default global `v1`. Opt-in via `?ui=v2`.
- **Fase 1 (escopo desta entrega):** sidebar reorganizada (`_sidebar_v2.html`) e `Meus Atendimentos` refinado (`_meus_atendimentos_v2.html`). Sem fusão de páginas — só renomeação/reorganização visual, preservando 100% dos `nav_can()` e IDs/data hooks usados pelo JS.
- **Fase 2 (planejada, não nesta entrega):** consolidar 3 abas de Distribuição (`dist_consultor`, `dist_comercial`, `comercial_dashboard`) em página única com tabs internos.
- **Fase 3 (planejada):** migrar demais abas para o novo padrão, deprecar `_v1`.
- **Como reverter:**
  1. Por usuário: abrir qualquer URL com `?ui=v1` (reseta cookie).
  2. Default global: já é `v1`; nenhum usuário vê a v2 sem opt-in.
  3. Reverter código: `git revert <commit>` da Fase 1 — só toca em `app.py` (+25 linhas), `index.html` (2 includes condicionais), e adiciona 3 arquivos novos.
- **Alternativas descartadas:**
  - Branch separada de redesign: dificulta deploy paralelo e testes lado a lado.
  - Refactor in-place dos partials antigos: viola "preservar tudo e fácil desfazer".
  - Feature flag global por variável de ambiente: menos granular que cookie por usuário.

### 2026-05-13 — Redesign Fase 1.1 (visual agressivo, mockup-fiel)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** A v2 entregue na Fase 1 ficou visualmente sutil demais (usuário não percebeu diferença). Refazer `_sidebar_v2.html` e `_meus_atendimentos_v2.html` aplicando o visual mockup-fiel: sidebar branca com items mixed-case e barra lateral ativa, KPIs com ícone-chip + número 2.5rem Manrope-black em primary, hero card lateral em gradient azul, banner amber "Visual Novo · Beta" fixo no topo de todas as páginas em modo v2.
- **Não alterado:** flag `dcz_ui`, context processors em `app.py`, includes condicionais em `index.html`, partials v1.
- **Compatibilidade:** todos os IDs/data hooks do `meus_atendimentos.js` foram preservados; classes legacy (`.ma-pill*`, `.ma-tab.is-active`) mantidas e estilizadas no `<style>` interno da v2.
- **Alternativas descartadas:** Opção A (só banner) e B (médio) — usuário escolheu C para diferenciação visual clara.

### 2026-05-15 — Aprovação de ajuste credita venda (conflito RGM)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Ao aprovar solicitação em `matricula_ajustes`, fazer upsert em `comercial_rgm_conflito_resolucao` com `resolved_by = 'ajuste_aprovado'`, usando RGM normalizado + `kommo_user_id` do agente que abriu o chamado — mesma mecânica de **Vendas em Conflito** (`_apply_conflito_overrides_to_agent_rgms` em Minha Performance e ranking).
- **Requisitos:** RGM válido e `kommo_user_id` do agente; sem RGM a aprovação grava status mas retorna `aviso` na API.
- **Não altera:** responsável do lead no Kommo (só override de contagem); rejeitar não remove override existente.
- **Alternativas descartadas:** só marcar ticket sem efeito (comportamento anterior); PATCH automático no Kommo na aprovação (escopo maior, risco de efeito colateral no CRM).

### 2026-05-14 — Remoção da UI v2 (beta)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Descarte completo do redesign beta: removidos cookie `dcz_ui`, query `?ui=v1|v2`, handlers em `app.py`, banner "Visual Novo · Beta" e includes condicionais em `index.html`; arquivos `templates/partials/_sidebar_v2.html` e `_meus_atendimentos_v2.html` apagados. Interface única = partials v1 (`_sidebar.html`, `_meus_atendimentos.html`).
- **Motivo:** pedido de produto — não manter coexistência v1/v2.
- **Nota:** As entradas 2026-05-13 acima ficam como histórico; Fases 2/3 do redesign não estão em curso.

### Convenções derivadas

- Toda decisão estrutural tomada por Opus deve ser registrada neste arquivo na seção "Decisões técnicas" antes de delegar a implementação.
