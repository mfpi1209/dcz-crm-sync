# AGENTS.md — dcz-crm-sync

Este arquivo registra decisões técnicas tomadas em conjunto com agentes Opus, para que execuções futuras (qualquer modelo) sigam o que já foi acordado sem refazer trade-offs.

## Decisões técnicas

### 2026-07-07 — Editar consultores no Dashboard Comercial (renomear / ocultar / excluir → Admin Sistema)
- **Modelo usado:** Opus 4.8 (principal). Decisão aprovada pelo usuário (escopo dashboard-only; capacidades renomear + ocultar + excluir).
- **Problema/pedido:** botão para gerenciar os consultores (agentes) no header do Dashboard Comercial. Ao **excluir** um consultor, os **leads/matrículas dele devem passar a contar para o "Admin Sistema"** (kommo_user_id **8261837**, `adm@eduit.com.br`, já com ~220k leads).
- **Restrição arquitetural central:** o sistema é **espelho do Kommo**. `leads.responsible_user_id` é puxado do Kommo pelo sync (Sync Kommo + delta a cada 10 min). Reatribuir **só no banco local seria desfeito** no próximo sync; reatribuir **no Kommo via API** é pesado (ex.: Kamilly ~24k leads = milhares de PATCH) e mexe na produção, e o Kommo **não permite deletar usuário via API**. Por isso o usuário optou por **override no nível do dashboard** (mesma filosofia de `comercial_rgm_conflito_resolucao`), que **não briga com o sync** e é reversível.
- **Decisão:** nova tabela **`comercial_consultor_ajuste`** (banco principal `dcz_sync`): `kommo_user_id PK`, `display_name` (renomear só na exibição), `hidden BOOL` (some do ranking/filtro AGENTE), `reassign_to INT` (excluído → destino da reatribuição = Admin Sistema), `updated_at/by`. Criada no ensure-schema de `routes/comercial_rgm.py` (junto de conflito/outlier).
- **Helpers** (`routes/comercial_rgm.py`): `_admin_sistema_uid()` (resolve por `name ILIKE 'admin sistema'` em `kommo_sync.users`, fallback 8261837), `_load_consultor_ajustes()` (cacheado por request via `flask.g`), `_consultor_reassign_map()`, `_consultor_hidden_uids()`, `_apply_reassign_to_rgm_map()`.
- **Pontos de integração (reatribuição = remap de `rgm_to_uid` + merge de `crm_stats`):**
  - Ranking `_build_agent_ranking_completa_vw`: remap após conflito override; merge das stats de CRM do excluído no destino; drop de ocultos/excluídos de `uids_real`.
  - `crgm_agente_detalhe` e `_crgm_kommo_lookup_rgms` (fora do padrão/evasão): remap + renomeação em `uid_to_nome`.
  - `_fetch_kommo_user_names`: sobrepõe `display_name` (renomeação propaga em todo lugar que resolve nome).
  - `/api/comercial-rgm/filters`: renomeia e remove ocultos/excluídos do dropdown AGENTE.
  - `routes/minha_performance.py` `_get_rgm_to_uid_map`: novo `_load_consultor_reassign()` aplica o remap → Minha Performance fica consistente com o ranking do Comercial (leads do excluído aparecem no Admin Sistema).
- **Endpoints (admin-only, `session.role == 'admin'`):** `GET /api/comercial-rgm/consultores` (lista de `kommo_sync.users` + contagem de leads + ajuste), `POST /api/comercial-rgm/consultores/<uid>` (upsert estado completo `{display_name, hidden, excluir}`; `excluir=true` seta `reassign_to=admin` e `hidden=true`; bloqueia excluir o próprio Admin Sistema), `DELETE /api/comercial-rgm/consultores/<uid>` (restaura ao padrão).
- **UI:** botão "Editar consultores" no header (após "Sync Agentes", `_comercial_rgm.html`); modal em `templates/index.html` (`crgm-consultores-modal`, z-[9999]) com busca, input de nome inline, toggle ocultar, excluir/restaurar; JS em `static/js/comercial_rgm.js` (`crgmAbrirConsultores` etc.). Ao fechar com alterações, recarrega filtros + ranking.
- **Efeito colateral aceito:** "ocultar" (sem reatribuir) tira o consultor da lista mas mantém a atribuição dos leads a ele — os números dele somem da visão, sem irem para ninguém. "Excluir" é o caminho para transferir a contagem ao Admin Sistema. Ambos são reversíveis e não tocam o Kommo.
- **Alternativas descartadas:** reatribuir no Kommo via API (pesado, irreversível, mexe na produção, e não deleta usuário); reatribuir só em `kommo_sync.leads` (desfeito pelo sync); reaproveitar `comercial_rgm_conflito_resolucao` (é por RGM, não por consultor — não cobre renomear/ocultar nem reatribuição em massa por agente).

### 2026-07-07 — Upload matriculados: dedup de PÓS com múltiplos ciclos (mantém o mais antigo)
- **Modelo usado:** Opus 4.8 (principal). Implementação direta após validação nos dados.
- **Problema:** no relatório "Relação de matriculados por polo", alunos de **pós-graduação** de um ciclo anterior (ex.: `2026/1`) reaparecem também com o ciclo do período atual (ex.: `2026/2`) — **mesmo RGM, 2 linhas, ciclos diferentes, ambas EM CURSO**. Como a view `comercial_rgm_atual` filtra `ciclo = ciclo_atual` (2026/2), esses alunos entram na base do ciclo corrente com **RGM de prefixo antigo** (< 49 dominante) e poluem o painel "Fora do padrão RGM" do Dashboard Comercial (68 casos observados; 59/60 vinham dessa duplicação).
- **Decisão:** dedup na **ingestão** (`_persist_snapshot_entries` em `routes/upload.py`, só para `tipo='matriculados'`, via `_dedup_pos_matriculados_por_ciclo`). Quando o mesmo RGM **de pós** aparece com **>1 ciclo distinto**, mantém **apenas 1 linha — a do ciclo mais antigo e (desempate) data de matrícula mais antiga** — e descarta as demais. Só atua em pós (detecção nivel/negocio/curso, mesma regra da view) e só quando há ciclos diferentes; grad e linhas de ciclo único não são tocadas.
- **Validação (snapshot 182):** 1.526 grupos pós multi-ciclo; **100% das linhas mantidas ficam EM CURSO** (0 casos de risco em que a linha mantida teria situação pior que a removida); 0 RGM pós com >1 ciclo após o dedup. Remove ~1.526 linhas duplicadas do relatório (o painel só exibia o subconjunto EM CURSO+contável, por isso mostrava só 68).
- **Escopo/efeito:** vale para **novos uploads** (o usuário re-sobe o arquivo). Esses alunos passam a contar em 2026/1 (ciclo real) e somem da base 2026/2 → limpam o painel "Fora do padrão". Impacto colateral: Dashboard Acadêmico passa a contá-los em 2026/1 (correto, são alunos do ciclo anterior).
- **Alternativas descartadas:** dedup no read/view (não casa com o fluxo "re-subir arquivo" pedido; espalha a regra); manter a linha por situação/"mais vivo" em vez de ciclo (o dado mostrou que ambas as linhas são EM CURSO, então ciclo-primário é seguro); restringir a prefixo antigo (menos geral que a regra pedida e deixaria casos de fora).

### 2026-06-18 — Match/Merge: campo Origem só é preenchido em leads NOVO
- **Modelo usado:** Opus 4.8 (principal). Implementação direta (bugfix pontual).
- **Problema:** no `executar_acoes` (`match_merge_lib.py`), o `update_fields_map` incluía `"Origem": "origem"` e todo lead montado no pipeline carrega `origem="SIAA"` fixo no `base`. Como o `_build_custom_fields` envia qualquer campo com valor, **toda ação `ATUALIZAR` (e `RESTAURAR`) sobrescrevia a origem original do lead (Indicação, Site, Tronco, etc.) com "SIAA"** no PATCH. `MATRICULADO` já não tocava (o `_mat_map` não tem Origem).
- **Decisão:** remover `"Origem": "origem"` do `update_fields_map` (usado por ATUALIZAR/RESTAURAR) e criar `novo_fields_map = {**update_fields_map, "Origem": "origem"}`, usado **apenas** nos blocos NOVO (NOVO normal e `novo_matriculado`). Regra final: **Origem=SIAA só nos NOVO**; ATUALIZAR/MATRICULADO/RESTAURAR não tocam na origem (preservam o valor original; vazio continua vazio, pois `_build_custom_fields` só envia campos com valor).
- **Correção de dados (retroativa):** restaurados **56 leads** cuja Origem real foi sobrescrita para "SIAA" entre 14/05/2026 e 18/06/2026, usando o `value_before` do histórico de eventos do Kommo (`custom_field_31764_value_changed`) — valor exato anterior, 100% fiel. Só entraram leads com valor anterior real (não-vazio, ≠ SIAA) e que ainda estavam como "SIAA". Leads com origem anteriormente vazia NÃO foram tocados (não dá pra distinguir overwrite indevido de NOVO legítimo). Plano/auditoria em `origem_restore_plan.csv`.
- **Alternativas descartadas:** limpar em massa todos os `vazio→SIAA` (erra os NOVO legítimos, onde SIAA é correto); manter Origem no `update_fields_map` e tratar caso a caso (não escala, continua sobrescrevendo).

### 2026-06-17 — Polos: nomes canônicos + ranking estilo Comercial no Dashboard Acadêmico
- **Modelo usado:** Opus 4.7 (principal).
- **Decisão:** `helpers.normalize_polo_display()` centraliza mapeamento de variantes (`POLO SP_*`, `CEB POLO`, parênteses, `_JD CRISTINA`, etc.) para 13 nomes canônicos em Title Case (ex.: `Barra Funda`, `Taboão da Serra_Centro`). Usado na agregação `by_polo` do Dashboard Acadêmico (`routes/dashboard.py`) e no ranking/filtro do Comercial (`routes/comercial_rgm.py`). Filtro por polo no Comercial compara nome canônico pós-fetch (não mais igualdade SQL no raw). UI Acadêmico/Supervisor Acadêmico passa a tabela ranked com barra gradiente cyan→blue (mesmo padrão de `Matrículas por Polo` no Comercial).
- **Merge explícito:** variantes CEB + POLO de Taboão Centro somam em `Taboão da Serra_Centro`; Campinas ignora sufixo `_JD CRISTINA`.
- **Alternativas descartadas:** whitelist SQL com ILIKE por polo (frágil); manter `_normalize_polo` antigo só strip CEB (não unifica SP_/parênteses); dropdown com nomes raw (UX inconsistente entre telas).

### 2026-06-11 — Premiação: metas + R$/matrícula por equipe (Alta Performance vs Impulso)
- **Modelo usado:** Opus 4.7 (principal) decidiu; Executor (Sonnet 4.6) implementará.
- **Problema:** a campanha tem alvos únicos (`def_meta_intermediaria/def_meta/def_supermeta`) e R$ por faixa únicos (`premiacao_tier_bonus`) que valem para todos. O time precisa diferenciar Alta Performance (alvo maior, R$/mat possivelmente maior) de Impulso (alvo menor) na MESMA campanha.
- **Decisão:** criar tabela `premiacao_grupo_meta(campanha_id, grupo_id, meta_intermediaria, meta, supermeta, valor_base, valor_intermediaria, valor_meta, valor_supermeta)` com PK composta `(campanha_id, grupo_id)`. Todos os campos NULLABLE — quando `NULL` cai no fallback. UI no MESMO modal "Editar Campanha" lista as equipes da campanha e mostra inputs por equipe.
- **Precedência (para 1 agente):**
  1. **Meta individual** em `premiacao_campanha_meta` (já existe — override por agente)
  2. **Meta da equipe** em `premiacao_grupo_meta` (NOVO — via `premiacao_grupo_membro.kommo_user_id`)
  3. **Default da campanha** (`def_meta_*` em `premiacao_campanha`)
  4. Fallback final: `comercial_metas` (já existe)
- **Mesma precedência para R$/matrícula:** equipe (NOVO via `premiacao_grupo_meta.valor_*`) → campanha (`premiacao_tier_bonus`).
- **Schema delta** (adicionar em `db.py` no bloco de migrations):
  ```sql
  CREATE TABLE IF NOT EXISTS premiacao_grupo_meta (
    campanha_id INTEGER NOT NULL REFERENCES premiacao_campanha(id) ON DELETE CASCADE,
    grupo_id INTEGER NOT NULL REFERENCES premiacao_grupo(id) ON DELETE CASCADE,
    meta_intermediaria NUMERIC,
    meta NUMERIC,
    supermeta NUMERIC,
    valor_base NUMERIC,
    valor_intermediaria NUMERIC,
    valor_meta NUMERIC,
    valor_supermeta NUMERIC,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (campanha_id, grupo_id)
  );
  ```
- **Endpoints novos** (em `routes/minha_performance.py`, padrão dos existentes — `_is_admin()` gate):
  - `GET /api/premiacao/campanhas/<cid>/metas-grupo` → `{ ok: true, grupos: [{ grupo_id, grupo_nome, meta_intermediaria, meta, supermeta, valor_base, valor_intermediaria, valor_meta, valor_supermeta }] }` listando TODAS as equipes da campanha (left join: equipe sem linha em `premiacao_grupo_meta` vem com `null` em todos os campos numéricos).
  - `POST /api/premiacao/campanhas/<cid>/metas-grupo` → `body: { grupos: [{ grupo_id, meta_intermediaria, meta, supermeta, valor_base, valor_intermediaria, valor_meta, valor_supermeta }] }`. UPSERT por `(campanha_id, grupo_id)`. Campos numéricos vazios/`null` viram `NULL` no DB (= "usar fallback").
- **Helper backend a CRIAR** (em `routes/minha_performance.py`):
  - `_get_agent_grupo_id(kommo_uid, campanha_id) -> Optional[int]` — lookup em `premiacao_grupo_membro JOIN premiacao_grupo`. Pode aproveitar a função similar que já existe no PIX por equipe.
  - `_get_grupo_meta_e_bonuses(campanha_id, grupo_id) -> dict | None` — retorna `{ meta_intermediaria, meta, supermeta, tiers: { base, intermediaria, meta, supermeta } }` ou None.
- **Pontos a alterar (com cuidado para não regredir):**
  - `_get_agent_metas(kommo_uid, dt_ini, dt_fim)` → adicionar passo 2 (lookup equipe via campanha ativa no período). Compatibilidade: se equipe não tiver `meta_*` definidos, segue pro fallback atual.
  - `_get_tier_bonuses(campanha_id)` → criar variante `_get_tier_bonuses_for_agent(campanha_id, kommo_uid)` que primeiro tenta `premiacao_grupo_meta.valor_*` da equipe do agente, fallback pra `premiacao_tier_bonus` da campanha. Trocar chamadas no fluxo de Minha Performance pelo novo helper (chamadas existentes que NÃO têm `kommo_uid` no escopo continuam usando `_get_tier_bonuses` puro — relevante pra dashboards agregados que não são por agente).
- **UI** (`templates/partials/_premiacao_admin.html` no modal `pa-edit-modal`):
  - Adicionar nova seção após "R$ por matrícula por faixa" e antes de "Recebimentos":
    ```
    Por equipe (sobrescreve o padrão acima)
    ┌─ Alta Performance ─────────────────┐
    │ Metas (mat): [Inter] [Meta] [Super]│
    │ R$/mat:      [Base] [Inter] [Meta] [Super]│
    └────────────────────────────────────┘
    ┌─ Impulso ──────────────────────────┐
    │ ...                                │
    └────────────────────────────────────┘
    ```
  - Quando a campanha não tem equipes, mostrar mensagem: "Crie equipes em 'Grupos de Agentes' abaixo para definir metas por equipe."
  - Render dinâmico — JS busca equipes via `/api/premiacao/campanhas/<cid>/grupos` (já existe) ao abrir o modal.
  - Salvar: `paSaveEditCampanha()` envia tudo do modal (campanha + tiers + recebimento) e DEPOIS chama `POST /api/premiacao/campanhas/<cid>/metas-grupo` se houver equipes editadas.
- **Alternativas descartadas:**
  - **Reaproveitar "Metas por Agente"** (preencher em lote todos do grupo): funcionaria pra metas, mas não cobre R$/mat por equipe (tabela `premiacao_tier_bonus` não tem dimensão de agente). Mais arrastado pro admin (precisa preencher N linhas em vez de 1 por equipe). E não diferencia "agente sem override individual mas com override de equipe" de "agente totalmente default".
  - **Colunas extras `*_alta`/`*_impulso` em `premiacao_campanha`:** quebra com qualquer 3ª equipe futura; viola schema normalizado.
  - **JSON `metas_por_equipe` em `premiacao_campanha`:** flexível mas perde tipagem, validação no DB e índices; obriga app-layer a fazer normalização.
- **Compatibilidade:** campanhas existentes sem nenhum override em `premiacao_grupo_meta` se comportam idêntico ao hoje (tudo cai no fallback `def_meta_*`/`premiacao_tier_bonus`). Nenhum agente fica órfão (passos 3-4 do fallback continuam intactos).
- **Correção lateral:** ANTES desta implementação, `_get_agent_metas` em `routes/minha_performance.py` ignorava `def_meta_*` da campanha — pulava direto do override individual (`premiacao_campanha_meta`) para `comercial_metas`. Isso fazia Minha Performance divergir do Dashboard Comercial (que SEMPRE leu `def_meta_*` como fallback — ver `routes/comercial_rgm.py` linhas ~3216-3255). Agora `_get_agent_metas` aplica `def_meta_*` como passo 3 — Minha Performance fica alinhado com Dashboard Comercial. **Impacto:** agentes sem linha em `comercial_metas` mas com campanha ativa preenchida (cenário comum) ANTES viam meta=0 em Minha Performance, AGORA verão os alvos `def_meta_*` da campanha. Tratamos como correção, não regressão.
- **Não inclui (escopo futuro):** sobrescrita por equipe das **regras de recebimento** (`premiacao_recebimento_regra`) e do **PIX diário** (`premiacao_pix_faixa` JÁ é por equipe, separado). Se virar requisito, ampliar a tabela `premiacao_grupo_meta` ou criar tabelas paralelas.

### 2026-06-10 — Dashboard Acadêmico lê `ciclo` direto do snapshot + filtro de ciclo independente de período
- **Modelo usado:** Opus 4.7 (principal).
- **Decisão:** No `routes/dashboard.py`, refatorar `_MAT_CTE` para incluir uma coluna `ciclo` lida diretamente de `r.data->>'ciclo'` (validada por regex `^\d{4}/\d$` para descartar lixo de cabeçalho/rodapé do XLSX). `_STUDENT_METRICS_QUERY` agora filtra por `m.ciclo = %(f_ciclo)s` em vez de converter ciclo para intervalo `dt_inicio/dt_fim` via tabela `ciclos`. Nova rota `GET /api/dashboard/ciclos-distinct` retorna ciclos distintos presentes no snapshot (com contagem). Front (`static/js/dashboard.js` `populateCicloFilter`/`applyCicloFilter` + `static/js/dashboard_supervisor_academico.js` `loadDashboardSupervisorAcademico`) passa a usar essa rota e persiste a escolha em `localStorage` (`dash_ciclo_v1` / `dsa_ciclo_v1`). Template `_dashboard_supervisor_academico.html` ganha `<select id="dsa-ciclo">` ao lado do `dsa-nivel`. O `applyCicloFilter` deixa de mexer em `students-from`/`students-to` — ciclo e período passam a ser filtros independentes.
- **Problema corrigido:** o snapshot de matriculados (`xl_rows`) já trazia a coluna `ciclo` preenchida ("2026/1", "2026/2", etc.), mas o `_MAT_CTE` ignorava esse campo e inferia o ciclo via `LEFT JOIN LATERAL ciclos` usando `data_matricula BETWEEN dt_inicio AND dt_fim`. A tabela `ciclos` (acadêmica, separada de `ciclos_comercial`) só tinha 2026.1 cadastrado, então **1.810 matrículas de 2026/2 caíam como "(sem ciclo)"** na quebra `by_ciclo`. Os KPIs totais (EM CURSO 26.869, etc.) estavam corretos — o bug atingia só a quebra por ciclo e o filtro de ciclo, que dependiam do JOIN.
- **Validação local:** rodando `_STUDENT_METRICS_QUERY` com `f_ciclo=NULL` obtém o mesmo total de 30.580 e o mesmo `by_situacao` (EM CURSO 26.869, etc.) que o painel já mostrava. Filtro por `2026/2` retorna 1.810 (1.774 EM CURSO + 36 CANCELADO). Filtro por `2026/1` retorna 28.768. Lixo do XLSX ("Filtros aplicados…" e "Total", 2 linhas) é descartado pelo regex.
- **Não tocado:** `_CICLO_COMPARE_QUERY` (Master Panel "Ciclos") continua usando JOIN com `ciclos` — é tela diferente, fora do escopo desta correção. `/api/ciclos` (config) também segue lendo da tabela `ciclos` — usado pela tela de Config de Ciclos, não pelo dropdown do Dashboard.
- **Alternativas descartadas:**
 - **Cadastrar 2026/2 na tabela `ciclos` acadêmica via Config** — funcionaria pra esta virada, mas continua frágil (toda virada de ciclo exige cadastro manual; cadastro errado de data — como aconteceu em `ciclos_comercial` id=5 com `dt_inicio=2026-05-14` e `dt_fim=2025-12-15` — quebra a inferência sem aviso).
 - **Migrar Dashboard Acadêmico para usar `ciclos_comercial`** — unifica fonte (bom), mas acopla o Dashboard Acadêmico ao schema do Comercial. Ler a coluna direto do snapshot é auto-suficiente e simétrico ao que o Comercial já faz internamente.
- **Bug lateral identificado, não corrigido aqui:** `ciclos_comercial` id=5 tem `dt_inicio=2026-05-14`, `dt_fim=2025-12-15` (fim ANTES do início). Afeta o Dashboard Comercial. Anotar pra corrigir em UI separada.

### 2026-06-10 — Conversão: RESPONDIDOS atribuídos ao dia do disparo, não ao dia da resposta (tool_whatsapp_alunos)
- **Modelo usado:** Opus 4.7 (principal). Implementação direta.
- **Decisão:** Na aba Conversão (`activationConversionService.js`), o KPI **RESPONDIDOS** (e derivados: clickers, messages, opt_outs, TAXA, top buttons, recent rows, kpis_by_ciclo) passam a ser atribuídos ao **dia do disparo correspondente** (`d.created_at`), não ao dia da resposta (`r.received_at`). REVERTIDOS continuam atribuídos pela data de marcação manual (`occurred_at`).
- **Problema:** "Financeiro: 16 respondidos hoje" estava contando respostas recebidas hoje referentes a disparos de **ontem**. A TAXA do dia ficava inflada artificialmente (numerador de hoje, denominador de dias anteriores) e não refletia performance real do disparo.
- **Regra de correlação resposta↔disparo:** já existia via `buildValidResponseExists` ("dispatch sent na mesma master_key/category, antes da resposta, dentro de `staleHours` (default 72h)"). A mudança é apenas adicionar `AND d.created_at >= $sinceIso [AND d.created_at < $untilIso]` no EXISTS — corta respostas cujos dispatches estão fora do período pedido.
- **Implementação:**
  - `buildValidResponseExists(rAlias, staleIdx, dispSinceIdx?, dispUntilIdx?)`: ganha 2 params opcionais de índice de `sinceIso`/`untilIso` no array de params. Quando passados, adiciona o filtro no EXISTS.
  - Em cada chamada (KPIs, byCategory, topButtons, recentRows, total_recent, kpis_by_ciclo), passar os índices de `$sinceIso` e (quando houver) `$untilIso`.
  - **Manter** filtros `r.received_at >= $sinceIso` (lower bound — resposta nasce DEPOIS do dispatch, então respeitar lower bound mantém perf sem cortar nada válido).
  - **Relaxar** upper bound do `r.received_at`: filtros `< untilIso` viram `< untilIso + (staleHours * interval '1 hour')` pra cobrir respostas tardias dentro da janela.
- **Trade-off conhecido:** TAXA do dia atual fica **incompleta até janela `staleHours` (72h) fechar**. Hoje 12h o dia mostra 5%, amanhã pode subir pra 15% conforme respostas tardias chegam. Operacionalmente correto — TAXA reflete performance do disparo, não atividade do dia.
- **Não toca:** REVERTIDOS (`activation_manual_outcomes` continua agregando por `occurred_at`); aba Meu Painel (continua mostrando respostas pela `received_at` — semântica diferente, é registro do que chegou, não conversão de disparo).
- **Alternativas descartadas:**
  - **JOIN explícito** em vez de EXISTS: query mais limpa mas pode duplicar contagens quando mesma resposta tem 2 dispatches válidos. EXISTS + DISTINCT mantém semântica unique.
  - **Atribuir revertido ao dia do disparo** (consistência total): user preferiu manter no dia da marcação ("é mais intuitivo pro consultor: o que marquei hoje aparece em hoje").

### 2026-06-10 — Meu Painel: Supervisor Acadêmico vê tudo igual admin (tool_whatsapp_alunos + dcz-crm-sync)
- **Modelo usado:** Opus 4.7 (principal). Implementação direta (escopo pequeno, 5 arquivos em 2 repos).
- **Decisão:** Ampliar o "ver tudo" do Meu Painel no `tool_whatsapp_alunos` pra incluir usuários com `categoria = 'Supervisor Acadêmico'` no dcz, com **a mesma capacidade plena** que `role=admin` (ver todos os leads + reatribuir consultor manualmente). Padrão espelha o de `routes/meus_atendimentos.py` no dcz, que já tem helper `_ma_is_admin_or_supervisor_academico()`.
- **Granularidade:** só "Supervisor Acadêmico". Outros perfis de supervisão (Supervisor Comercial, etc.) NÃO ganham acesso pleno ao Meu Painel — domínios diferentes. Reabrir se surgir necessidade.
- **Mecânica:**
  - **dcz** (`templates/partials/_disparador_whatsapp.html`): URL do iframe ganha `&categoria=<session.categoria | urlencode>` (já manda `role`; agora manda também `categoria`).
  - **tool backend** (`server/routes/activation.js`): `resolveConsultor` passa a aceitar `categoria` normalizada (lowercase, sem acento) ∈ `{'supervisor acadêmico', 'supervisor academico'}` como equivalente a `role=admin`. `assign-consultor` espelha (libera reatribuição pra supervisor acadêmico). Mantém compat: ausência de categoria continua válido (admin via role só).
  - **tool frontend** (`src/services/meuPainelApi.ts`): `Identity` ganha campo `categoria`. `readConsultorIdentity()` lê de `?categoria=` (e persiste em `localStorage`).
  - **tool frontend** (`src/pages/MeuPainelPage.tsx`): `isAdmin` vira `isAdminOrSupervisor`. UI (label "modo admin" / "Ver todos (admin)") mantém texto "admin" pra não confundir o supervisor — operacional é igual.
- **Alternativas descartadas:**
  - **Forçar `role=admin` no dcz quando categoria é Supervisor Acadêmico** (sem mudar o tool): mais rápido mas confunde semântica de role no tool (admin no tool deixa de bater com role real no dcz). Recusada.
  - **Whitelist de categorias no .env do tool**: mais flexível, mas overkill pra 1 categoria. Reabrir se virar 3+.
- **Compat:** sessões abertas (que ainda não recarregaram o iframe) continuam funcionando — quando sem `categoria` na URL, comportamento volta ao anterior (só admin via role tem o poder).

### 2026-06-09 — Congelar ciclos: arquivar 2026/1 das operações (tool_whatsapp_alunos)
- **Modelo usado:** Opus 4.7 (principal) decidiu; Executor (Sonnet 4.6) implementará.
- **Decisão:** Adicionar capacidade de **arquivar um ciclo inteiro** (ex: "2026/1") em vez de congelar snapshot por snapshot. Tabela nova `frozen_cycles(ciclo PK, frozen_at, frozen_by, reason)`. Quando um ciclo é congelado, o disparador, relatórios (Conversão, CAA Daily/Funil), comparações e dropdowns de ciclo no UI **excluem 100%** os leads desse ciclo. Histórico em `activation_dispatch_events` continua intacto pra auditoria.
- **Problema:** Na virada de ciclo (ex: 2026/1 → 2026/2), o operador precisa de um gesto explícito pra "fechar" o ciclo antigo: parar de disparar pra leads de 2026/1, parar de incluí-los em relatórios. Hoje o sistema usa só o "snapshot mais recente"; quando sobe a base nova, o ciclo antigo some das queries mas continua misturado se o snapshot de matriculados ainda tiver alunos de ambos os ciclos (cenário típico da janela de transição).
- **Granularidade escolhida:** **por ciclo, não por base/snapshot**. Faz mais sentido conceitualmente porque o ciclo é a unidade real ("2026/1 acabou pra TODAS as bases ao mesmo tempo"). Se algum dia precisar congelar uma base específica de um ciclo (ex: só financeiro de 2026/1), a gente reabre essa decisão.
- **Sem escape hatch `?include_frozen=1`** — decisão explícita do usuário. Ciclo arquivado some 100% de qualquer view operacional. Histórico só via consultas SQL diretas em `activation_dispatch_events`/`activation_responses` (que continuam intactas — flag de freeze não as toca).
- **Reativação:** botão "Reativar ciclo" disponível, deleta a linha de `frozen_cycles` e tudo volta a aparecer. Útil se congelar por engano ou se decidir rodar uma campanha tardia.
- **UI:** novo card "Ciclos" no topo da página Bases, lista ciclos disponíveis (deriva via `getAvailableCiclos`) com status (ativo / arquivado em DD/MM/AAAA por USER). Botão "🔒 Congelar ciclo X" → modal com motivo opcional → confirma. Snapshots em si continuam visíveis na lista de baixo, sem mudança no comportamento de upload/delete deles.
- **Motivo opcional ao congelar** — usuário escolheu. Não bloqueia o gesto.
- **Componentes tocados:**
  - Migration nova `030_frozen_cycles.sql` (tabela única).
  - `server/repositories/frozenCyclesRepository.js` (novo): `listFrozen`, `freezeCycle`, `unfreezeCycle`, `getFrozenSet`.
  - `server/services/cicloResolverService.js`: nova `getActiveCiclos()` (filtra os frozen).
  - `server/services/activationService.js`: roster filtra OUT leads de ciclos frozen (via `cicloMap` + `frozenSet`).
  - `server/services/activationConversionService.js`, `caaFunnelService.js`, `caaProtocolsService.js` (se usa ciclo), `repositories/reportRepository.js`: filtram out ciclos frozen dos `kpis_by_ciclo` / `counts_by_ciclo`.
  - `server/routes/cycles.js` (novo) ou subrota em `reports.js`: `GET /api/cycles` (lista todos com status), `POST /api/cycles/:ciclo/freeze` (body: `{ reason? }`), `DELETE /api/cycles/:ciclo/freeze`.
  - `src/services/cyclesApi.ts` (novo): client.
  - `src/pages/BasesPage.tsx`: card novo "Ciclos" no topo + modal de freeze.
- **Alternativas descartadas:**
  - **Flag por snapshot (nível A original)**: só protegeria contra apagar; não muda comportamento operacional, que era exatamente o que o usuário queria.
  - **Flag por base+ciclo (granularidade dupla)**: complexidade extra sem caso de uso real. Reabrir quando surgir.
  - **Refactor ciclo-aware no nível de snapshot (nível C, ~2 dias)**: matar mosca com canhão — o `cicloResolverService` já abstrai isso. Adicionar flag por ciclo aproveita 100% da infra existente.
- **Trabalho estimado:** ~4-5h (Executor).

### 2026-06-09 — Seleção multi-página no Disparador via dropdown combo (tool_whatsapp_alunos)
- **Modelo usado:** Opus 4.7 (principal) decidiu UX; Executor (Sonnet 4.6) implementou.
- **Decisão:** Adicionar dropdown "Mais ▾" ao lado do checkbox do header em `ActivationRosterTable` (tool_whatsapp_alunos), com 4 opções: "Página atual (100)", "Próximas 5 páginas (~500)", "Próximas 10 páginas (~1.000)", "Todos filtrados (N)". Página atual e próximas N **adicionam** à seleção; "Todos filtrados" **substitui** a seleção. Linha de status acima da tabela mostra contagem + botão "Desmarcar todos" + estado de loading durante operações bulk.
- **Backend novo:** `GET /api/activation/:category/roster/keys` — mesmos query params do `/roster` (stage, ciclo, bb_subgrupo, responseFilter), retorna apenas `master_keys[]` (payload pequeno, ~80kB pra 2k chaves vs ~600kB do roster cheio). Service novo `getActivationRosterKeys` reusa cache `buildRosterRowsCached`.
- **Problema:** Disparos manuais de 2k+ leads exigiam avançar dezenas de páginas marcando 100 por vez. Inviável operacionalmente.
- **Alternativas descartadas:**
  - **Só "Todos filtrados" (sem dropdown)**: simples mas tudo-ou-nada; usuário às vezes quer só 200/500.
  - **Só "Próximas N páginas"**: cobre parcial mas pra "tudo" exigiria N requests sequenciais (40 pra 4k leads).
  - **Aumentar `PAGE_SIZE` temporariamente** (ex: "mostrar 500/página"): tabela fica gigante, UX ruim.
- **Onde:** `tool_whatsapp_alunos` — `server/services/activationService.js` (função `getActivationRosterKeys`), `server/routes/activation.js` (rota nova ANTES de `/roster`), `src/services/activationApi.ts` (método `rosterKeys` + tipo), `src/components/ActivationPanel.tsx` (callbacks `addSelectionMany`/`replaceSelection`), `src/components/ActivationRosterTable.tsx` (dropdown + handler + linha de status).
- **Commit:** `7b042bb` em `Mikyxx1234/tool_whatsapp_alunos`. Easypanel auto-deploya.
- **Detalhe completo:** `tool_whatsapp_alunos/AGENTS.md` (entrada 09/06/2026).

### 2026-06-08 — Onda 2: cache persistente Postgres cpf→datacrazy_lead_id (tool_whatsapp_alunos)
- **Modelo usado:** Opus 4.7 (principal) decidiu/escreveu a spec; Executor (Sonnet 4.6) implementou. Opus revisou diff antes do commit.
- **Decisão:** Adicionar migration `029_datacrazy_lead_cache.sql` no `tool_whatsapp_alunos` com tabela `datacrazy_lead_cache(cpf PK, datacrazy_lead_id, email_norm, phone_norm, nome, raw_lead, source, last_synced_at, last_seen_at)` + `datacrazy_lead_cache_sync_log` pra auditoria. Cache populado por cron noturno (`startDatacrazyCacheSyncCron`, default 03:00 UTC) + hits oportunistas dentro do próprio `buildLeadsLookupIndex`. Endpoints novos `POST /api/maintenance/sync-datacrazy-cache` e `POST /api/maintenance/invalidate-datacrazy-cache` (ambos protegidos por `requireApiKey`).
- **Integração com `buildLeadsLookupIndex`**: FASE 0 (lookup no cache antes de bater na API) + FASE 2 (upsert oportunista fire-and-forget de leads resolvidos via API). Retorno ganhou `cache_hits` e `cache_stale_skipped`.
- **Callers** (`runDatacrazyActivationBatch` e `previewDatacrazyMatches` em `activationService.js`): `contacts` passa a incluir `cpf: item.cpf` (já existia no roster, sem mudança de schema).
- **Env vars novas no `.env.example`:** `DATACRAZY_CACHE_ENABLED=1`, `DATACRAZY_CACHE_SYNC_HOUR_UTC=3`, `DATACRAZY_CACHE_SYNC_MAX_PAGES=2000`, `DATACRAZY_CACHE_TTL_DAYS=7`.
- **Impacto:** 10k leads cai de ~17min (Onda 1) pra ~3–5s com cache quente; cold start mantém Onda 1 sem regressão.
- **Aplicação:** migration aplicada manualmente pelo usuário via `npm run migrate` apontando pra produção; Easypanel rebuilda no push pro `main`.
- **Detalhe completo:** `tool_whatsapp_alunos/AGENTS.md` (entrada 08/06/2026 — Onda 2).
- **Alternativas descartadas (Redis, cache só em memória, sync sob demanda, webhook do DataCrazy):** detalhe completo no AGENTS.md do tool.

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

### 2026-06-01 — Reconcile aceites: GET individual em leads "stale" (fim do false-deleted)
- **Modelo usado:** Opus 4.7 (principal)
- **Problema:** `reconcile_aceite_leads()` em `routes/kommo_sync.py` (rodando a cada 10 min via scheduler) marcava como `is_deleted=TRUE` qualquer lead que estivesse em "Aceite" no DB local mas **não retornasse no filtro de Aceite** da API do Kommo. Não distinguia "lead deletado de fato" de "lead que mudou de etapa naturalmente" (Ganho, Em Atendimento, etc). Sintoma: matriculados movidos pra "Venda ganha" no Kommo eram marcados como deletados no DB local, sumindo do dashboard e impedindo o pipeline de match_merge de gerar ações `MATRICULADO`.
- **Escala do impacto:** 197 leads acumulados em `Aceite + is_deleted=TRUE` em 01/06 (49 marcados no próprio dia, 98 em 29/05); destes, 163 estavam vivos no Kommo (a maioria já em "Venda ganha") e 34 realmente deletados.
- **Decisão:** No bloco `stale_ids = db_aceite_ids - api_lead_ids`, em vez de UPDATE em lote para `is_deleted=TRUE`, fazer **GET individual** `/api/v4/leads/{id}` em cada stale:
  - HTTP 200 → atualiza `status_id`, `pipeline_id`, `responsible_user_id`, `updated_at` reais e mantém `is_deleted=FALSE`.
  - HTTP 404/204 → marca `is_deleted=TRUE` (deletado de fato).
  - Outros erros → conta em `stale_check_errors`, não muda o lead.
- **Salvaguardas:** sleep 0.05s entre requests; limite `STALE_BATCH_LIMIT=500` por execução (acima disso, pula stale check e loga warning — anomalia provável).
- **Custo:** +1 request por lead "stale" por ciclo (típico ~5–50). Em condições normais, ciclo de 10 min absorve sem rate-limit.
- **Fix imediato aplicado:** script `_resync_falso_deletados.py` rodado em 01/06 11:31 → 163 revividos, 34 confirmados deletados.
- **Alternativas descartadas:**
  - *Aumentar `KOMMO_DELTA_LOOKBACK_DAYS` (1→7):* não resolve, o bug é do reconcile, não do delta.
  - *Desligar reconcile:* perde detecção de novos leads em Aceite.
  - *Marcar deletado só após N ciclos seguidos sem retorno:* atrasa o detect real e mantém o bug.
  - *Bulk GET (`filter[id][]`):* mais eficiente porém adiciona complexidade; mantém GET único por simplicidade enquanto o volume é baixo.

### 2026-06-09 — Match/Merge: criar lead para matriculado órfão (fim do filtro silencioso)
- **Modelo usado:** Opus 4.7 (principal)
- **Problema:** No `match_merge_lib.gerar_acoes()`, a regra de geração de `NOVO` filtrava por `data_inscr >= data_corte_novo` (D-2 default). Quando a pessoa se inscreveu antes de D-2 e matriculou depois, o pipeline a descartava silenciosamente (contado em `n_data_filtrada`). O bloco `MATRICULADO D-1` só atua sobre leads existentes — não cria. Resultado: matrículas ficavam sem lead correspondente no Kommo, somem do dashboard e do funil. Em janela de 30 dias foram identificados **16 órfãos** (15 corrigidos via script manual em 09/06; 1 era falso positivo por sync delay).
- **Decisão (Opção C: A + B combinadas):**
  - **A — Inscrito Matriculado ignora a janela:** No loop principal de `inscritos_match["detalhes"]`, quando `lead_id is None` e `siaa_situacao == "MATRICULADO"`, força a criação de `NOVO` com `novo_matriculado=True` (já cria direto em Venda ganha) independente de `data_inscr`. Enriquece com RGM/data_matricula/email_ad lookup em `mm_matriculados` por CPF.
  - **B — Matriculado órfão cria NOVO direto a partir de `mm_matriculados`:** Após o bloco MATRICULADO D-1, varre `mm_matriculados` (janela 60d, polos UNICID/CSED, tipos NOVA MATRICULA/RECOMPRA/RETORNO). Para cada matriculado **sem nenhum match no Kommo** (CPF/email/email_ad/tel/RGM nos índices já carregados) e que ainda não está em outra ação, gera `NOVO` com `novo_matriculado=True`. Captura o caso de matrícula direta no SIAA sem inscrição prévia online.
- **Não altera:** filtro `is_recente_novo` para inscritos NÃO matriculados — preserva o comportamento de não trazer histórico antigo de inscrições que nunca viraram nada. Guard `search_lead_by_cpf` em `executar_acoes` (linhas 3573-3588) continua atuando como anti-duplicata final em ambos os caminhos.
- **Cobertura prevista:** 100% dos matriculados de polos UNICID/CSED com NOVA MATRICULA/RECOMPRA/RETORNO terão lead no Kommo automaticamente (com data_inscr antiga, sem inscrição online ou inscritos com status terminal "Matriculado").
- **Alternativas descartadas:**
  - *Opção D (remover janela `is_recente_novo` de vez):* trazia milhares de inscritos antigos do histórico SIAA sem matrícula como leads novos — bagunça funil, sobrecarrega consultores.
  - *Estender `data_corte_novo` para 60d global:* mesmo efeito da D, só que diferido.
  - *Tratar manualmente sempre:* já provamos que não escala (16 casos em 30d, tendência crescente).

### 2026-06-10 — Match/Merge: MATRICULADO fallback inscrito Aceite + RGM (sem mm_matriculados)
- **Modelo usado:** Opus 4.7 (principal)
- **Problema:** Inscrito com `siaa_situacao=Matriculado` no relatório de candidatos, lead existente em Aceite com RGM preenchido no Kommo (consultor preencheu na matrícula), mas **sem linha** em `mm_matriculados`. Pipeline pulava ATUALIZAR (SIAA=Matriculado) e não gerava MATRICULADO (só lê matriculados). Caso Camila Estevam (L21315921, RGM 49212028).
- **Decisão:** No loop principal de `inscritos_match`, quando `lead_id` existe, SIAA=Matriculado, `lead_status_id == ACEITE (48566207)` e RGM preenchido no lead (`lead_custom_field_values`), gerar `MATRICULADO` com `match_tipo=inscrito_aceite_rgm`. Enriquece com `cpf_to_mat_data` se disponível. Anti-duplicidade: se já existe lead em Ganho (142) com o **mesmo RGM**, pula (mesma regra do bloco D-1).
- **Não altera:** duplicatas (Daniela 2 Aceite + 1 Ganho) — anti-dup continua bloqueando os Aceite quando Ganho já tem o RGM. UNIFICAR continua manual para Aceite vs Aceite.
- **Alternativas descartadas:** remover skip de ATUALIZAR para Matriculado (moveria para Aprovado em vez de Ganho); estender janela mm_matriculados global (não resolve RGM só no Kommo).

### Convenções derivadas

- Toda decisão estrutural tomada por Opus deve ser registrada neste arquivo na seção "Decisões técnicas" antes de delegar a implementação.
