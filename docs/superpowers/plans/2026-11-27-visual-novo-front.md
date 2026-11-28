# Visual novo do front (`/app/`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Trocar a casca crua do front novo por um menu lateral agrupado com Fixadas (guardadas no banco), barra de cima, busca Ctrl K, e refazer Uso do Dashboard no visual novo.

**Architecture:** Servidor: `navegacao.Menu.visivel` + tabela `nav_fixadas` + blueprint `routes/fixadas.py` (`GET/PUT /api/me/fixadas`). Front: `menu.ts` passa a montar também Fixadas/Mais usadas e a saber o grupo de cada tela; componentes novos `MenuLateral`, `BarraTopo`, `BuscaTelas`; `Estrutura` organiza layout desktop/celular. A barra de cima mostra título, grupo, estrela, sino e tema; ações da tela (Atualizar) ficam na própria tela.

**Tech Stack:** Flask + psycopg2 (pytest); React 18 + TypeScript + TanStack Query 5 + React Router 6 + Tailwind 3 (vitest + Testing Library). Ícones: Material Symbols (já carregados).

**Spec:** `docs/superpowers/specs/2026-11-27-visual-novo-front-design.md`

## Global Constraints

- Visibilidade só pelo servidor: Fixadas/Mais usadas = `Menu.visivel(p)` (= `pode(p)` e, se `p` está em algum `GRUPOS`, `secoes()[grupo]` verdadeiro).
- Banco: só `CREATE TABLE IF NOT EXISTS nav_fixadas (...)` como no spec; nada apagado/alterado fora das linhas do próprio usuário no PUT.
- Máximo de 8 fixadas; Mais usadas = top 3 dos últimos 30 dias (fuso America/Sao_Paulo), desempate pelo nome da página.
- Rotas `/api/me/fixadas` em `permissoes.ROTAS_DO_PROPRIO_USUARIO`.
- Login de emergência (`user_id` 0): GET listas vazias; PUT 400 "Este login não guarda telas fixadas."
- Sem dependência npm nova; versões exatas mantidas.
- Ícones por grupo: comercial `trending_up`, academico `school`, premiacoes_internas `emoji_events`, ferramentas `construction`, vocacional `explore`, meta `campaign`, chamados `support_agent`, ti `memory`, sistema `settings`.
- Estado aberto/fechado dos grupos: `localStorage` chave `eduit-menu-grupos` (sempre com try/catch).
- Avisos: `GET /api/avisos/nao-lidos` → `count`, a cada 2 min; clique leva a `/#avisos`.
- Textos em português, frase curta, sentença normal.
- Windows: rodar npm em `%TEMP%\fr` (caminho longo quebra o esbuild) e copiar `package-lock.json`/`dist` de volta — ver AGENTS.md.

## Review Focus

- Permissão tirada depois de fixar: a fixada some do GET e do menu (teste no Task 1: `test_get_some_fixada_sem_permissao`).
- Não admin com `page_views` na tabela de permissões: não pode fixar nem aparecer em Mais usadas (Task 1: `test_put_recusa_tela_de_grupo_oculto`, `test_mais_usadas_respeita_visivel`).
- Falha do `/api/me/fixadas` (500 ou rede): menu continua inteiro, sem seção Fixadas (Task 2: `"sem fixadas quando a API falha"`).
- Busca só com teclado: Ctrl K abre, setas, Enter, Esc devolve o foco (Task 4: `"teclado de ponta a ponta"`).
- Celular: escolher uma tela fecha a gaveta (Task 3: `"gaveta fecha ao escolher"`).

---

### Task 1: Fixadas no servidor

**Files:**
- Modify: `navegacao.py` (método `Menu.visivel`)
- Modify: `db.py` (`_ensure_nav_fixadas_table()` ao lado de `_ensure_page_views_table`), `app.py` (chamar no boot junto de `_ensure_page_views_table()`; registrar blueprint)
- Create: `routes/fixadas.py` (blueprint `fixadas_bp`)
- Modify: `permissoes.py` (`ROTAS_DO_PROPRIO_USUARIO` += `"/api/me/fixadas"`)
- Test: `tests/test_fixadas.py`, `tests/test_navegacao.py` (visivel)

**Interfaces:**
- Produces: `Menu.visivel(page: str) -> bool`; `GET /api/me/fixadas` → `{"fixadas": list[str], "mais_usadas": list[str]}`; `PUT /api/me/fixadas` body `{"paginas": list[str]}` → 200 `{"fixadas": list[str]}` ou 400 `{"error": str}`; constante `MAX_FIXADAS = 8` em `routes/fixadas.py`.

- [ ] **Step 1: Testes que falham**
  - `test_navegacao.py::test_visivel_esconde_tela_de_grupo_oculto`: `Menu("viewer", {"page_views","repasse"}, "Supervisor Comercial", "f@x")` → `visivel("page_views") is False`, `visivel("repasse") is True`, `visivel("dashboard") is True`; admin → `visivel("page_views") is True`.
  - `test_fixadas.py` (cliente Flask com `fixadas_bp` + sessão; banco falso no padrão de `tests/test_navegacao.py` capturando SQL e parâmetros):
    - `test_get_devolve_fixadas_em_ordem`
    - `test_get_some_fixada_sem_permissao` (banco devolve `["repasse","logs"]`, usuário sem `logs` → `["repasse"]`)
    - `test_get_mais_usadas_top3_30_dias` (SQL contém `30 days` e `America/Sao_Paulo`, `LIMIT` não confiado: filtra e corta em 3 no Python)
    - `test_mais_usadas_respeita_visivel` (`page_views` vindo do banco some para não admin)
    - `test_put_substitui_lista` (DELETE do usuário + INSERT com `ordem` 0..n-1, commit)
    - `test_put_valida`: parametrizado — não lista → 400; repetida → 400; 9 itens → 400; `"xyz"` → 400; tela sem permissão → 400
    - `test_put_recusa_tela_de_grupo_oculto` (não admin, `page_views`) → 400
    - `test_usuario_zero` (GET `{"fixadas":[],"mais_usadas":[]}`; PUT 400 com a mensagem do spec)
    - `test_rota_e_do_proprio_usuario` (`"/api/me/fixadas" in permissoes.ROTAS_DO_PROPRIO_USUARIO`)
    - `test_tabela_criada_no_boot` (texto de `app.py` chama `_ensure_nav_fixadas_table()`; `db.py` contém `CREATE TABLE IF NOT EXISTS nav_fixadas` e `ON DELETE CASCADE`)

- [ ] **Step 2:** `"$TEMP/painel-venv/Scripts/python.exe" -m pytest tests/test_fixadas.py tests/test_navegacao.py -q` → falham (módulo inexistente).

- [ ] **Step 3: Implementar.** `visivel`: `pode(p)` e, para cada grupo de `GRUPOS` que contém `p`, exigir `secoes()[grupo]`. Rotas usam `navegacao.menu_do_usuario()`; conexão por `db.get_conn()`; mais usadas: `SELECT page, COUNT(*) FROM page_views WHERE user_id=%s AND ts >= NOW() - INTERVAL '30 days' GROUP BY page ORDER BY COUNT(*) DESC, page`, filtrar `visivel` e `CONHECIDAS`, cortar em 3. Erro de banco no GET → 200 com listas vazias e `logger.warning` (menu não quebra).

- [ ] **Step 4:** mesmos testes + `tests/test_permissao_rotas.py` → PASS.

- [ ] **Step 5: Commit** `feat(fixadas): telas fixadas por usuário no banco`.

### Task 2: Dados do menu no front (fixadas, grupo da tela, ícones)

**Files:**
- Modify: `frontend/src/menu.ts`
- Create: `frontend/src/fixadas.ts` (hook)
- Test: `frontend/src/menu.test.tsx`, `frontend/src/fixadas.test.tsx`

**Interfaces:**
- Consumes: API do Task 1.
- Produces:
  - `GRUPOS_DO_MENU` ganha `icone: string` (valores do Global Constraints).
  - `montarMenu(nav, telasNovas, fixadas?: {fixadas: string[]; mais_usadas: string[]} | null): Menu` com `Menu = { fixadas: { titulo: "Fixadas" | "Mais usadas"; itens: ItemMenu[] } | null; avulsas; grupos }`; itens de Fixadas só se estiverem entre os itens visíveis do menu (avulsas ∪ grupos); itens continuam também no seu lugar normal.
  - `grupoDaTela(menu: Menu, pagina: string): GrupoMenu | null`.
  - `useFixadas(): { dados: {fixadas: string[]; mais_usadas: string[]} | null; alternar(pagina: string): void; fixada(pagina: string): boolean }` — query key `["fixadas"]`; `alternar` faz PUT com lista nova (adiciona no fim / remove), atualização otimista e rollback no erro; erro do GET → `dados = null`.

- [ ] **Step 1: Testes que falham** em `menu.test.tsx`: `"fixadas primeiro, com título Fixadas"`, `"sem fixadas usa Mais usadas"`, `"fixada invisível não aparece"`, `"sem fixadas quando a API falha"` (`montarMenu(nav, set, null).fixadas === null`), `"grupoDaTela acha o grupo"`, `"todo grupo tem ícone"`. Em `fixadas.test.tsx`: `"alternar adiciona no fim e envia PUT"`, `"alternar remove"`, `"erro no PUT volta a lista"`, `"limite de 8 não envia"` (9ª chamada não faz PUT).
- [ ] **Step 2:** rodar vitest no `%TEMP%\fr` → falham.
- [ ] **Step 3:** implementar.
- [ ] **Step 4:** `npm run typecheck` e `npm test` → PASS.
- [ ] **Step 5: Commit** `feat(front): dados do menu com fixadas e grupos com ícone`.

### Task 3: Casca nova (menu lateral, barra de cima, celular)

**Files:**
- Modify: `frontend/src/componentes/MenuLateral.tsx`, `frontend/src/componentes/Estrutura.tsx`, `frontend/src/estilo.css`
- Create: `frontend/src/componentes/BarraTopo.tsx`, `frontend/src/avisos.ts` (`useAvisosNaoLidos(): number`)
- Test: `frontend/src/componentes/casca.test.tsx`

**Interfaces:**
- Consumes: `montarMenu`, `grupoDaTela`, `useFixadas` (Task 2); `TITULOS`.
- Produces: `<BarraTopo titulo grupo? pagina? />` (estrela só quando `pagina` é tela migrada); `Estrutura` passa a abrir a busca (Task 4 encaixa `onBuscar`).

- [ ] **Step 1: Testes que falham** (`casca.test.tsx`, fetch falso para `/api/me`, `/api/me/fixadas`, `/api/avisos/nao-lidos`):
  - `"grupos abrem e fecham e lembram"` (clicar no grupo alterna `aria-expanded`; `localStorage["eduit-menu-grupos"]` atualizado; nova renderização respeita)
  - `"grupo da tela atual começa aberto"` (rota `/page_views`, admin → grupo TI `aria-expanded="true"`)
  - `"item ativo marcado"` (`aria-current="page"` no Uso do Dashboard)
  - `"estrela fixa a tela"` (clique envia PUT com a página)
  - `"sino mostra contagem"` (`count: 3` → texto "3"; `count: 0` → sem bolinha) e link `/#avisos`
  - `"tema alterna e grava eduit-theme"`
  - `"gaveta fecha ao escolher"` (largura de celular simulada via botão Menu: abrir, clicar num item novo, `aria-expanded="false"`)
  - `"rodapé com usuário e sair"` (link `/logout`)
- [ ] **Step 2:** vitest → falham.
- [ ] **Step 3:** implementar no visual do spec (seções Fixadas, Geral, grupos com ícone e seta; item ativo com barra lateral `var(--primary)`; largura 248px; gaveta no celular).
- [ ] **Step 4:** typecheck + vitest → PASS.
- [ ] **Step 5: Commit** `feat(front): casca nova com menu agrupado, fixadas e barra de cima`.

### Task 4: Busca de telas (Ctrl K)

**Files:**
- Create: `frontend/src/componentes/BuscaTelas.tsx`, `frontend/src/busca.ts` (`filtrarTelas(itens: ItemMenu[], texto: string): ItemMenu[]` — sem acento/maiúscula, começa-com antes de contém)
- Modify: `frontend/src/componentes/Estrutura.tsx` (atalho global Ctrl/Cmd+K, botão "Buscar tela" no menu)
- Test: `frontend/src/busca.test.ts`, `frontend/src/componentes/BuscaTelas.test.tsx`

**Interfaces:**
- Consumes: itens de `montarMenu` (avulsas + grupos, sem repetir).
- Produces: `<BuscaTelas aberta itens onFechar />`.

- [ ] **Step 1: Testes que falham:** `busca.test.ts`: `"ignora acento"` (`"inscricao"` acha "Inscrições"), `"começa com vem antes"`, `"vazio lista tudo"`. `BuscaTelas.test.tsx`: `"teclado de ponta a ponta"` (Ctrl+K abre `role="dialog"`, digita, ArrowDown, Enter → tela nova navega para `/page_views`; tela antiga chama `navegador.irPara("/#repasse")`), `"Esc fecha e devolve o foco"`, `"só telas visíveis"`.
- [ ] **Step 2–4:** vitest falha → implementar → passa.
- [ ] **Step 5: Commit** `feat(front): busca de telas com Ctrl K`.

### Task 5: Uso do Dashboard no visual novo

**Files:**
- Modify: `frontend/src/paginas/UsoDoDashboard.tsx`, `frontend/src/App.tsx` (rota de tela renderiza `BarraTopo` com título/grupo/página)
- Test: `frontend/src/paginas/UsoDoDashboard.test.tsx`

**Interfaces:**
- Consumes: `BarraTopo` (Task 3); `TITULOS`.
- Denominador dos cartões = total do relatório: `d.por_pagina.length + d.nao_acessadas.length` (com os dados reais de 27/11: 58 + 20 = 78). Cartões: "58 de 78 telas" e "20 sem uso".
- Produces: nada novo.

- [ ] **Step 1: Ajustar/criar testes:** `"barras proporcionais"` (maior linha `style.width === "100%"`, linha com metade dos acessos `"50%"`), `"cartão mostra contexto"` (dados de teste com 2 acessadas e 2 sem acesso → texto `"2 de 4 telas"`), `"pílulas de período"` (`aria-pressed`), testes antigos continuam (atualizar seletores de título: título agora vem da barra de cima).
- [ ] **Step 2–4:** falha → implementar → passa.
- [ ] **Step 5: Commit** `feat(front): Uso do Dashboard no visual novo`.

### Task 6: Conferência e entrega

- [ ] **Step 1:** suíte Python completa (`pytest tests -q`) e front (`typecheck`, `test`, `build` em `%TEMP%\fr`; copiar `dist`/`package-lock.json` de volta se mudarem).
- [ ] **Step 2:** painel local (`servir_local.py`): desktop e celular, claro e escuro, Ctrl K, fixar/desafixar e recarregar, grupo TI aberto em `/app/page_views`, item ativo, sino. Screenshots para revisão.
- [ ] **Step 3:** AGENTS.md: entrada no topo (decisão, tabela nova, rotas, regra `visivel`, onde ficam os componentes); commitar spec e plano.
- [ ] **Step 4:** PR com o que muda / não muda / como foi conferido; esperar CI (`front`, `imagem-pr`, pytest). Avisar que o #159 pode ser publicado depois deste.
