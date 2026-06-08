# AGENTS.md — dcz-crm-sync

Este arquivo registra decisões técnicas tomadas em conjunto com agentes Opus, para que execuções futuras (qualquer modelo) sigam o que já foi acordado sem refazer trade-offs.

## Decisões técnicas

### 2026-06-08 — Preflight do disparador escalável: dedupe por pessoa + métrica de threshold corrigida (tool_whatsapp_alunos)
- **Modelo usado:** Opus 4.7 (principal) decidiu e implementou diretamente.
- **Decisão:** Refatorar `buildLeadsLookupIndex` em `tool_whatsapp_alunos/server/services/datacrazyClient.js` pra (a) aceitar API nova `{contacts: [{email, phone}]}` com vínculo por pessoa, (b) trocar a métrica do threshold de soma (`emails + phones`) por `Math.max(emails, phones)` ou `personList.length`, (c) subir defaults pra `THRESHOLD=250` e `CONCURRENCY=10`, (d) executar 2 passadas no atalho — 1ª por telefone (1 chamada por pessoa), 2ª por email só pra quem ficou faltando. Callers `runDatacrazyActivationBatch` e `previewDatacrazyMatches` atualizados pra passar `contacts`. Retrocompat com formato antigo `{emails, phones}` preservada.
- **Problema corrigido:** disparos de 100 leads travavam minutos em "Buscando alunos no DataCrazy" porque o threshold default `100` era comparado a `200` (cada pessoa = email + telefone = 2 termos). Caía no loop de paginação completa (500 páginas × 400ms). Em escala (4k–10k) ficaria ingerenciável.
- **Quick fix sem rebuild** (env vars no Easypanel): `DATACRAZY_DIRECT_SEARCH_THRESHOLD=300` + `DATACRAZY_DIRECT_SEARCH_CONCURRENCY=15`.
- **Impacto:** 100 leads passa de minutos pra ~5–10s; 250 leads ~15–25s via atalho; >250 cai no caminho de paginação (~40s pra base de 50k com early_stop).
- **Onda 2 — pendente:** cache persistente `cpf → datacrazy_lead_id` em tabela própria, populado por cron noturno varrendo a base do CRM. Pra 10k leads cairia de ~11min pra ~100ms quando todos em cache. Spec a escrever quando houver demanda concreta recorrente >300 leads.
- **Alternativas descartadas:**
  - **Só env vars (sem commit):** destravaria o lote atual mas mantém 2× chamadas/pessoa, não escala pra 4k+.
  - **Cache persistente agora:** escopo maior (migration + repo + cron + invalidação), Onda 1 sozinha resolve o caso atual.
  - **Dedupe no caller mantendo formato antigo:** quebra extensibilidade, qualquer caller futuro reescreveria a lógica.
- **Detalhe completo da decisão:** `tool_whatsapp_alunos/AGENTS.md` (entrada 08/06/2026 do mesmo dia).

### 2026-06-08 — UNIQUE de activation_responses passa a incluir o dia (corrige 027)
- **Modelo usado:** Opus 4.7 (principal).
- **Decisão:** Migration `028_activation_responses_unique_per_day.sql` em `tool_whatsapp_alunos` (commit `26e4d69`). Relaxa o UNIQUE de `(external_id, category)` para `(external_id, category, (received_at at time zone 'UTC')::date)`. Aplicada em produção.
- **Problema corrigido:** o `external_id` do DataCrazy/WhatsApp identifica a **conversa persistente** entre o número da escola e o número do aluno, não a mensagem individual (mesma conversa reaparece em campanhas futuras com o mesmo id). O UNIQUE da 027 bloqueava a 2ª resposta legítima quando a mesma pessoa, dois meses depois, recebia novo disparo da mesma categoria e respondia.
- **Cast usado:** `(received_at at time zone 'UTC')::date`. `date_trunc('day', ...)` e cast direto `timestamptz::date` são STABLE, não IMMUTABLE, e o Postgres recusa em índice. Forçar UTC primeiro torna a expressão IMMUTABLE.
- **N8n correspondente** (FORA deste repo): o `NOT EXISTS` da query INSERT também precisa de filtro temporal `AND ar.received_at >= now() - interval '24 hours'`, senão a query continua pulando o INSERT por achar resposta antiga.
- **Alternativas descartadas:**
  - **Granularidade por hora** — granular demais, webhooks atrasados poderiam quebrar idempotência.
  - **Granularidade por mês/semana** — não cobre cenário de pessoa que recebe campanha 2 semanas depois e responde.
  - **Adicionar `dispatch_id` em `activation_responses` e usar como dimensão do UNIQUE** — mais correto semanticamente, mas exige migração maior + alteração no n8n pra resolver o id do disparo + backfill. Trade-off não justifica.
- **Trade-off conhecido:** se a mesma pessoa responder 2x em dias diferentes pra disparos do mesmo dia, ainda contabiliza só a 1ª (idempotência por dia continua valendo). Operacionalmente OK — não é cenário que distorce métrica.

### 2026-06-08 — Resposta única do WhatsApp pode contabilizar em N categorias (UNIQUE por (external_id, category))
- **Modelo usado:** Opus 4.7 (principal).
- **Decisão:** Relaxar o constraint `UNIQUE (external_id) WHERE external_id IS NOT NULL` em `activation_responses` para `UNIQUE (external_id, category) WHERE external_id IS NOT NULL`. Migration `027_activation_responses_unique_per_category.sql` em `tool_whatsapp_alunos` (commit `7280b0f`). Aplicada no DB de produção (`31.97.91.47/disparos`) via `npm run migrate` local apontando pro mesmo host.
- **Problema corrigido:** quando uma pessoa tinha 2+ disparos pendentes (ex: CAA + DOC) e respondia uma única vez, o UNIQUE em `external_id` bloqueava a 2ª inserção. A resposta caía só na 1ª categoria retornada pelo SELECT do n8n, e o painel de Conversão da outra categoria mostrava 0 respostas.
- **Outras mudanças necessárias** (no n8n, FORA deste repo): a query INSERT precisa de 2 ajustes:
  1. Adicionar `AND d.created_at >= now() - interval '72 hours'` na cláusula do `activation_dispatch_events` (mesma janela `staleHours` que `findRespondedMasterKeys` usa pra correlacionar resposta↔disparo).
  2. Mudar `NOT EXISTS (... ar.external_id = $1)` pra `NOT EXISTS (... ar.external_id = $1 AND ar.category = COALESCE(d.category, 'financeiro'))`.
- **Alternativas descartadas:**
  - **Manter UNIQUE em `external_id` e atribuir resposta só ao disparo mais recente:** funcionaria pra contar a resposta em UMA categoria correta, mas perde o sinal nas outras (consultor não vê que a pessoa "está engajada" em DOC quando ela respondeu ao CAA). O time precisa do sinal em **todas** as categorias com disparo pendente.
  - **Inferência por janela temporal com atribuição única:** complexidade alta no n8n + queries do tool. Trade-off não justifica vs duplicar respostas por categoria.
- **Trade-off conhecido:** o KPI "Responderam" pode inflar quando o mesmo lead tem N disparos pendentes (1 pessoa = N respostas, uma por categoria). Operacionalmente correto, métrica que precisa ser lida como "respostas por categoria" não "pessoas únicas globais".
- **Backfill:** NÃO foi feito. Respostas antigas continuam atribuídas só à 1ª categoria. Efeito vale só pra disparos/respostas a partir do deploy. Se um dia precisar retroagir, escrever script que, pra cada `(external_id, category)` faltante, duplique a resposta existente com a categoria do dispatch_event correspondente.

### 2026-06-08 — Progresso real % no overlay da ativação via job em memória + polling
- **Modelo usado:** Opus 4.7 (principal) — decisão; implementação delegada ao Executor (Sonnet 4.6) em `tool_whatsapp_alunos` commit `ed44afc`.
- **Decisão:** Implementar barra de progresso real (com %) no `LoadingOverlay` da ação "Buscar e ativar" usando **job em memória + polling do frontend a cada 2s**. Backend cria `jobId`, processa em background com `runDatacrazyActivationBatch(category, opts, callbacks)` chamando `onProgress` a cada chunk. Frontend usa `setInterval` no `ActivationListActions.tsx` pra atualizar a UI; após 3 erros de rede consecutivos, mostra "Conexão perdida…" e para o polling.
- **Endpoints novos** (em `tool_whatsapp_alunos/server/routes/activation.js`):
  - `POST /api/activation/:category/run-datacrazy-batch?async=1` — retorna `202 { jobId, status: 'running' }` e dispara processamento em background (fire-and-forget envolto em try/catch).
  - `GET /api/activation/jobs/:jobId/progress` — devolve o estado atual do job.
  - Modo síncrono (sem `?async`) continua funcionando idêntico (retrocompat total).
- **Registry**: `server/services/activationJobsRegistry.js` (novo) — singleton `Map<jobId, entry>` com cleanup automático a cada 5 min (jobs finalizados há >1h ou iniciados há >6h são removidos). `setInterval.unref()` pra não bloquear shutdown do node.
- **Alternativas descartadas:**
  - **SSE (Server-Sent Events)** — push em tempo real, mas exige adaptar Express pra streaming e o proxy do Easypanel pode cortar conexão longa. Polling 2s é suficiente pra UX (latência aceitável).
  - **Persistir em tabela `activation_jobs`** — sobreviveria a restart e funcionaria com múltiplas instâncias, mas overkill pra 1 instância no Easypanel e disparos curtos (1–3 min). Migration nova + escrita constante no DB sem ganho prático.
- **Limites conhecidos:** se o servidor reiniciar durante um disparo, o frontend perde o ponteiro do job (mas o disparo em si pode ter completado). Não suporta cancelamento (escopo: só mostrar progresso). Multi-instância não funciona (jobs ficam na memória de um worker só).
- **UX:**
  - Enquanto `total === 0` (backend ainda fazendo preflight/buildLeadsLookupIndex), barra mostra estado indeterminado com `animate-pulse`.
  - Conforme `processed` avança, barra anima até 100% com transition suave.
  - Linha embaixo: `X% · processed de total`; linha pequena de stats: `N enviados · M não encontrados · K falhas`.
  - Minimizar continua funcionando; polling segue rodando por trás.

### 2026-06-03 — "Meu Painel" no tool_whatsapp_alunos: marcação manual por consultor + KPIs
- **Modelo usado:** Opus 4.7 (principal) — implementação direta (usuário pediu "não delegue, faça tudo você").
- **Decisão:** Criar uma nova **aba "Meu Painel" dentro do `tool_whatsapp_alunos`** (não no dcz). A aba mostra os leads do consultor logado (atribuídos via webhook do n8n em `activation_responses.consultor_responsavel_nome`) cruzados com `caa_protocols` e `activation_manual_outcomes`, e permite marcar o desfecho manualmente (revertido | confirmado | sem_contato | outro).
- **Identidade do consultor:** o dcz já injeta `?consultor=<username>&consultor_nome=<full_name>&role=<role>` no `src` do iframe (em `templates/partials/_disparador_whatsapp.html`). A página lê desses query params na primeira carga e **persiste em `localStorage` (`dw_consultor_identity_v1`)** — react-router perde a query em navegação interna, então sem persistência a aba "Meu Painel" ficaria sem identidade após o usuário clicar em outra aba e voltar.
- **Matching consultor ↔ banco:** `consultor_responsavel_nome` é gravado pelo webhook do n8n em formato livre (ex.: "Wesley Guerreiro"). O dcz manda o `full_name` derivado do `username` (ex.: "wesley.guerreiro" → "Wesley Guerreiro"). O backend usa `ILIKE %nome%` em vez de igualdade exata pra tolerar variações (acento, caixa, sobrenome composto).
- **Admin:** quem entra com `role=admin` ganha botão "Ver todos (admin)" que troca o filtro de consultor pra `*` (sem filtro). Por padrão admin entra em modo "ver tudo".
- **Endpoints novos** (em `server/routes/activation.js`, reusam `requireApiKey` quando configurada):
  - `GET /api/activation/meu-painel/list` — lista leads cruzados com CAA + última marcação.
  - `GET /api/activation/meu-painel/stats` — KPIs agregados (atribuído, marcado, revertido, confirmado, sem_contato, outro, taxa_reversao).
  - `POST /api/activation/meu-painel/outcomes` — grava marcação (wraps `manualOutcomesRepository.insertOutcome` que já existia desde a migration `018_activation_manual_outcomes.sql`).
- **Não criei tabela nova:** `activation_manual_outcomes` já tem todos os campos necessários (outcome enum, motivo, notes, consultor_nome, proof_path, occurred_at). Upload de prova ficou pra v2 (escopo dobraria).
- **Schemas reutilizados:**
  - `activation_responses` (migration `015` + coluna `consultor_responsavel_nome` da `026`): leads que responderam ao disparador.
  - `caa_protocols` (migration `014`): status CAA (open / won_reverted / lost_confirmed / lost_canceled / unknown).
  - `activation_manual_outcomes` (migration `018`): grava marcação manual, com unique `(category, master_key)` via `proof_path` opcional.
- **Alternativas descartadas:**
  - **Página no dcz puxando do tool via proxy** — daria pra fazer mas exigiria duplicar UI de KPIs em vanilla JS e proxy de 3 endpoints novos. Mais lento e menos coeso (o `tool_whatsapp_alunos` já tem todo o stack de KPIs/tabelas em React).
  - **Tabela nova `consultant_panel_actions`** — `activation_manual_outcomes` já cobre 100% do caso de uso. Criar tabela paralela quebraria o sync existente CRM→outcomes (cron `crmDesfechoSyncService`).
  - **Filtrar por `username` em vez de nome** — `activation_responses` só guarda nome (não tem coluna `username`). Adicionar coluna exigiria backfill + alteração no webhook do n8n. ILIKE no nome resolve o 80% sem nada disso.
- **Build validado:** `npm run build` no tool passou (+3 módulos, JS 1.276 → 1.313 MB, gzip 330 kB).
- **Commit:** `b93209c` em `Mikyxx1234/tool_whatsapp_alunos` — Easypanel auto-deploya.
- **URL final:** `https://banco-disparador-whatsapp.6tqx2r.easypanel.host/meu-painel?consultor=<u>&consultor_nome=<nome>&role=<role>&theme=dark`. Dentro do iframe do dcz, a query é passada automaticamente pelo `_iframe_url` em `_disparador_whatsapp.html`.

### 2026-06-03 — Disparador WhatsApp: dark mode integrado ao iframe (continuação da revisão acima)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Implementar **dark mode real no app externo `tool_whatsapp_alunos`** para o iframe parecer parte nativa do CRM (sem mais "card branco flutuando no fundo escuro"). Estratégia em três camadas:
  1. **`tailwind.config.js`** (no repo `tool_whatsapp_alunos`): `darkMode: 'class'` + paleta `dcz.*` espelhando os tokens do `_head.html` (`--bg-main #0b1623`, `--bg-card #122033`, `--bg-elevated #1a2942`, `--text-primary #e6edf6`, `--border rgba(76,112,154,0.30)`).
  2. **`index.html`** do app externo: script inline antes do bundle Vite que aplica `.dark` no `<html>` baseado em: (a) `?theme=dark` na URL, (b) `localStorage.tw_theme`, (c) auto-detect de iframe (`window.self !== window.top` → dark default), (d) `prefers-color-scheme` como fallback. Evita flash branco.
  3. **`src/index.css`** do app externo: camada `.dark .bg-* / .text-* / .border-* / .ring-* / .divide-* / inputs` que sobrescreve todas as classes neutras do Tailwind (gray/slate/zinc) usadas pelos ~30 componentes do app. Cores de status (green/red/yellow/blue/purple/indigo) ganham variantes dark com transparência. Verde de marca WhatsApp (#25D366) e bolhas do `WhatsAppPreview` (#efeae2/#d9fdd3) preservados.
- **Iframe no dcz:** `templates/partials/_disparador_whatsapp.html` aponta para `{{ whatsapp_tool_base }}/?theme=dark` (redundância com auto-detect) e usa `background: #0b1623` no `<iframe>` pra evitar flash branco. Header do CRM foi reduzido a um breadcrumb mínimo ("Acadêmico · Disparador WhatsApp") + botão recarregar — eliminando duplicação com o header do app externo.
- **Por que essa estratégia (camada global) vs editar cada componente:**
  - Cobre os ~30 componentes / 7 páginas sem ter que adicionar `dark:` em ~600 ocorrências.
  - Mudanças concentradas em 3 arquivos (auditáveis em um diff curto).
  - O app externo continua funcionando em light mode sem param.
- **Alternativas descartadas:**
  - **`filter: invert + hue-rotate` no `<iframe>`** (hack CSS no CRM) — instantâneo, mas distorce verde-marca e ícones coloridos.
  - **Adicionar `dark:` em cada `bg-white`/`text-gray-*`/etc. dos 30 componentes** — limpo no fim, mas ~600 edições + risco de quebrar variantes não-cobertas. A camada global atinge o mesmo efeito sem isso.
  - **Deixar o app sempre dark (sem toggle)** — quebra quem acessa o host externo diretamente esperando o tema claro.
- **Pendente (usuário):** deploy no Easypanel. Como o Easypanel está conectado a `github.com/Mikyxx1234/tool_whatsapp_alunos`, basta commit + push das mudanças em `tailwind.config.js`, `index.html`, `src/index.css` que ele rebuilda automaticamente via Dockerfile (`npm ci && npm run build`).
- **Build local validado:** `npm run build` passou (CSS final `dist/assets/index-B3cQqxfc.css` 47.7 kB / 8.4 kB gzip, +8 kB vs antes — overhead do dark layer).

### 2026-06-03 — Disparador WhatsApp: virou iframe do app completo (revisa decisão anterior do mesmo dia)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Substituir o wrapper vanilla (que cobria só a aba "Disparador") por **iframe full-height do app externo `tool_whatsapp_alunos` inteiro** dentro da página Flask `disparador_whatsapp`. A página Flask continua existindo e continua protegida por `nav_can('disparador_whatsapp')`, mas seu conteúdo passou a ser `<iframe src="{{ whatsapp_tool_base }}">` ocupando `calc(100vh - 170px)`. URL base injetada via novo `context_processor` `inject_whatsapp_tool_base` em `app.py`, lendo `WHATSAPP_TOOL_BASE_URL` com default Easypanel de produção. Header mínimo manteve título + botão "Recarregar" + "Abrir em nova aba".
- **Por que mudou (no mesmo dia):** o wrapper vanilla cobria 1 das 7 telas do app externo (Disparador, Alunos, Calendário, Bases, Relatórios, Conversão, Regras). Usuário pediu cobertura completa após ver a v1. Reimplementar as 6 abas restantes em vanilla custaria semanas (≈130 KB de TSX original + ~35 componentes React + 31 tabelas no banco do app externo). Como o app já roda 100% funcional no Easypanel sem auth próprio, iframe entrega tudo agora.
- **Hierarquia herdada:** preservada na **entrada** — só usuário com permissão `disparador_whatsapp` (ou admin) carrega o partial e vê o iframe. Uma vez dentro do iframe, as 7 abas internas não passam mais por permissão granular do `dcz-crm-sync` (essa granularidade não existe no app externo hoje).
- **Caveats aceitos:**
  - URL do `dcz-crm-sync` não muda enquanto se navega entre as 7 abas (todas vivem dentro do iframe; back/forward do browser ignora navegação interna).
  - Se o app externo ganhar auth próprio no futuro, precisa replanejar (passar identidade do dcz pra dentro do iframe via postMessage ou token de sessão).
  - Scroll em duas camadas (sidebar do dcz fora + scroll interno do iframe).
- **Alternativas descartadas (nesta revisão):**
  - **Manter wrapper vanilla + portar as outras 6 abas** — semanas de trabalho, refaz React→vanilla e Node→Flask sem ganho funcional (era a opção B do INTEGRATION.md, "desaconselhada").
  - **Plano A do INTEGRATION.md** (proxy Easypanel `/whatsapp/*` + middleware `dczAuth`) — solução definitiva sem caveats, mas custa dias e exige acesso ao Easypanel. Reabrir se o app externo ganhar auth próprio ou se granularidade por aba virar requisito.
  - **Página nova separada com iframe + manter wrapper vanilla atual** — usuário escolheu não duplicar; só uma página "Disparador WhatsApp" que agora aponta pro app completo.
- **Mantido por compatibilidade (não removido):**
  - `routes/disparador_whatsapp.py` continua registrado em `app.py`. Não é mais usado pelo iframe (que conversa direto com o host externo), mas fica disponível caso a abordagem proxy/vanilla precise ser retomada.
  - `static/js/disparador_whatsapp.js` foi simplificado pra só `loadDisparadorWhatsapp()` (init noop) + `dwReloadIframe()` (botão recarregar).
- **Variáveis no `.env`:** `WHATSAPP_TOOL_BASE_URL` (a que importa para o iframe), `WHATSAPP_TOOL_API_KEY` e `WHATSAPP_TOOL_TIMEOUT_S` continuam servindo só ao proxy em standby.

### 2026-06-03 — Disparador WhatsApp como wrapper Flask (aba Acadêmico) — REVISADA pela entrada acima
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Integrar a ferramenta externa `tool_whatsapp_alunos` (React+Node, hospedada em `banco-disparador-whatsapp.6tqx2r.easypanel.host`) como **página vanilla "Disparador WhatsApp" no grupo Acadêmico** do `dcz-crm-sync`, seguindo o mesmo padrão do `leads_inscricao`: template Jinja + JS vanilla + proxy Flask. Proxy fica em `routes/disparador_whatsapp.py`, expõe `/api/disparador_whatsapp/templates` e `/api/disparador_whatsapp/send-message`, valida permissão server-side (`user_permissions.page = 'disparador_whatsapp'` ou admin), e encaminha para o app externo injetando `WHATSAPP_TOOL_API_KEY` (opcional) como `x-api-key` + `Authorization: Bearer`. Hierarquia herdada nativamente: usuário só vê a página se `nav_can('disparador_whatsapp')` for true, e mesmo se chamar a API direto via curl, o blueprint barra com 403 sem a permissão. Escopo inicial: **só a tela do disparador** (upload CSV → escolhe template aprovado → loop client-side de envio com intervalo configurável e cancelamento). As outras 6 telas do app externo (Bases, Jornadas, CAA Funnel/Daily, Conversão, Consultores, Reports) ficam fora — quem precisar delas usa o host direto.
- **Alternativas descartadas:**
  - **Plano A do INTEGRATION.md** (lado a lado via proxy Easypanel `/whatsapp/*` + middleware `dczAuth` com `X-User-Id`): cobriria todas as 7 telas mas exige acesso ao Easypanel, criação de schema `whatsapp_app` no Postgres compartilhado, middleware novo no Node e dias de trabalho. Adiado — reabrir quando precisar das outras telas.
  - **Iframe** do host externo dentro de uma página Flask: não passa sessão, app externo hoje não tem auth, sujeito a problemas de cookie cross-origin.
  - **Link externo** no sidebar (target=_blank): zero refactor, mas não herda a hierarquia de permissões — qualquer um com a URL acessa.
  - **Port completo Node → Flask + React → Jinja**: opção B do INTEGRATION.md, "trabalho enorme, perde o stack já testado". Desaconselhado pelo próprio autor.
- **Não inclui (por design):** persistência de campanha no banco do `dcz-crm-sync` (envios usam `POST /api/send-message` do host externo, individuais; histórico fica no app externo). Nenhuma tabela nova no Postgres do `dcz-crm-sync`.
- **Variáveis novas no `.env`:** `WHATSAPP_TOOL_BASE_URL` (default Easypanel de produção), `WHATSAPP_TOOL_API_KEY` (opcional), `WHATSAPP_TOOL_TIMEOUT_S` (default 30).

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

### 2026-05-18 — PIX diário por equipe (faixas por matrículas)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** PIX diário amarrado à **equipe** (`premiacao_grupo`: Alta Performance, Impulso, etc.). Agentes entram no grupo na seção Grupos; faixas em `premiacao_pix_faixa` (`min_matriculas` → `valor`, flag `apenas_sabado`). Cálculo: maior faixa atingida no dia (ex. 13 mat na Alta → R$ 150 da faixa de 12).
- **API admin:** `GET/POST /api/premiacao/campanhas/<id>/pix-equipe`. Presets no front para nomes com "alta"/"impulso" alinhados à planilha de premiação.
- **Legado:** `premiacao_pix_nivel_membro` e meta por dia da semana permanecem no código mas não são a UI principal.
- **Alternativas descartadas:** níveis abstratos 1/2/3 sem vínculo com equipe; meta fixa + bônus por dia da semana (não reflete tabela PIX Dia da planilha).

### 2026-05-19 — PIX Suporte: fila de aceite + calendário fechados ganhos
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Abas e **Campanha** no topo. **PIX do Dia** (Suporte): **fila de aceite** atual no Kommo (todos consultores). **Calendário** e **Matrículas por Dia**: mesma série — `data_matricula` da base `comercial_rgm`, situação **EM CURSO** (`comercial_periodo_vendas_resumo`), alinhado ao gráfico (ex. 106 no dia 18).
- **Implementação:** `_all_consultants_aceites_fila`; heatmap usa `mat_by_date` do Comercial, não `closed_at` do Kommo.
- **Alternativas descartadas:** fechados ganhos por `closed_at` (divergia do gráfico 106 vs calendário 102); aceites só do time Suporte.

### 2026-05-19 — Home Suporte Comercial (4 logins, mesmo painel)
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** Os logins `felipe.nolasco@cruzeiroead.com.br`, `jessica.castro@eduit.com.br`, `suporte@eduit.com.br` e `thais.martins@cruzeiroead.com.br` recebem categoria **Suporte Comercial**, permissões sem `dashboard`, página inicial **`minha_performance`** e painel **Equipe Suporte Comercial** (`suporte_equipe=1`). Bootstrap idempotente em `_ensure_suporte_comercial_users()` no startup.
- **Alternativas descartadas:** manter Dashboard Acadêmico como home (confundia com o painel de premiação); depender só de ajuste manual no Config sem lista fixa de logins.

### 2026-05-19 — Painel Suporte: matrículas = Comercial inteiro
- **Modelo usado:** Opus 4.7 (principal)
- **Decisão:** No painel **Equipe Suporte Comercial** (Minha Performance), KPI **Matrículas no Período** e barra de meta usam **`vendas` bruto** (mesmo card do Dashboard Comercial); gráfico diário usa **`evolucao` EM CURSO**. Função compartilhada `comercial_periodo_vendas_resumo()` em `comercial_rgm.py`; o front também chama `GET /api/comercial-rgm/data` ao abrir o painel (fallback se período da campanha divergir do filtro do Comercial). **PIX diário** continua por **aceites** do time Suporte.
- **Alternativas descartadas:** somar só os 4 usuários Suporte; contar só EM CURSO no KPI (card Comercial é bruto); confiar só em import interno sem bater com a API do Comercial.

### Convenções derivadas

- Toda decisão estrutural tomada por Opus deve ser registrada neste arquivo na seção "Decisões técnicas" antes de delegar a implementação.
