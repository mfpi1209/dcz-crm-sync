# AGENTS.md — dcz-crm-sync

Este arquivo registra decisões técnicas tomadas em conjunto com agentes Opus, para que execuções futuras (qualquer modelo) sigam o que já foi acordado sem refazer trade-offs.

## Decisões técnicas

### 2026-11-28 — Fase 4: Dashboard (home) no front novo (`/app/dashboard`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** migrar a home de quem não é comercial (`_dashboard.html` + `dashboard.js`) para o front novo, no visual novo, com os mesmos números. As rotas de `routes/dashboard.py`, `kommo_sync.py` e `upload.py` não mudam.
- **Tela nova (`frontend/src/paginas/Dashboard.tsx` + `paginas/dashboard/`):**
  - **Quem vê o quê (pela categoria do `/api/me`, como o `loadDashboard`):** supervisor comercial e supervisor acadêmico veem um aviso com link para `/#dashboard` (painel deles não migrado) e a tela não busca nada da home; acadêmico vai direto para Meus Atendimentos se tiver a página, senão vê os 4 atalhos; os demais, o Dashboard Acadêmico. As rotas são todas da página `dashboard`, sem modo admin.
  - **Funil de vendas:** `funnel-live` + `new-leads-today` ao abrir; ao terminar o funil, `yesterday-summary` (ou `funnel-yesterday`). KPIs (novos hoje, ganho ontem com leads e tendência, total, aceites com delta, conversão), etapas em destaque, visualização em barras com Perdidos. Funil "aquecendo": pergunta de novo sem forçar em 4 s e depois a cada 5 s, até 30 vezes (como `_dashPollFunnelWarm`); limite de 2 min por pedido; aviso de demora aos 8 s. "Atualizar" força (`?force=1`) só funil, leads de hoje e ontem. Não há atualização periódica (a antiga também não tinha).
  - **Fonte de dados** (`/api/dashboard`), **matrículas por tipo** (Total, Novos = calouros+regresso+recompra com sub-cartões, Rematrículas/Veteranos, "!" com a regra), **situação** (cartões clicáveis, select no cabeçalho, transferências recebidas sob demanda), **saúde financeira** (só com `has_data`; cartões levam à Inadimplência), **polo** (nomes canônicos por `nomePolo`, cópia de `normalizePoloDisplay`), **nível**, **turma/ciclo**, **evolução** (chart.js, agregado/detalhado, clique no mês abre os dias) e **comparação de ciclos**.
  - **Filtros:** ciclo (salvo em `localStorage["dash_ciclo_v2"]`; vale se ainda existe, senão o mais recente), nível e situação aplicam na hora, como na antiga; Limpar volta ao ciclo mais recente e zera o resto.
  - **Servidor:** `/api/dashboard/students` é chamada uma vez por combinação de filtros (abertura = 1 chamada). Os blocos buscam um de cada vez, como o `_stuRefreshFiltered`: alunos → evolução → saúde → ciclos. Nenhuma consulta da tela tenta de novo sozinha. Todos os pedidos usam o `signal` do TanStack Query.
  - **chart.js 4.4.7** (versão exata do painel antigo), registrado só com `LineController`, `LineElement`, `PointElement`, `CategoryScale`, `LinearScale` e `Tooltip`. Cores das séries e da grade vêm das variáveis `--grafico-*` de `estilo.css`, lidas na hora; o gráfico redesenha ao trocar o tema. Novas variáveis: `--positivo`, `--negativo`, `--aviso`, `--destaque`, `--fundo-positivo`, `--funil-<etapa>`.
- **Diferenças de propósito:**
  - período (De/Até) só vale no "Aplicar" (a antiga buscava a cada data mexida, refazendo a rota pesada);
  - trocar qualquer filtro fecha o mês aberto no gráfico (a antiga ficava na granularidade diária sem o mês, um estado quebrado);
  - os ciclos buscam de novo quando o nível muda por qualquer caminho, inclusive o clique em Graduação/Pós (a antiga só no `applyDashboardFilters` e mostrava ciclos do nível anterior); com o mesmo nível não buscam de novo; a saúde financeira não busca de novo quando só o período muda (a rota não usa período);
  - `new-leads-today` vai uma vez na abertura (a antiga chamava duas);
  - erros do servidor aparecem na tela (fonte, alunos, evolução, saúde, ciclos, transferências); a antiga deixava em branco ou só no console;
  - sem a animação de contagem dos números e sem o selo de filtros do cabeçalho (os selects já mostram; a barra "Filtrando por" e a legenda do gráfico continuam); conversão com vírgula (`25,0%`).
- **Não reproduzido:** os painéis de supervisor comercial e acadêmico (`_dashboard_supervisor*.html`, `dashboard_supervisor*.js`): ficam no painel atual, com link na tela nova.
- **Menu antigo:** o item Dashboard do `_sidebar.html` continua na tela antiga até a aprovação; `tests/test_front_novo.py` ganhou `test_dashboard_migrado_mas_menu_antigo_ainda_na_tela_antiga`.
- **Testes:** `paginas/Dashboard.test.tsx` (43, chart.js falso que guarda a configuração do gráfico). `menu.test.tsx` usava `dashboard` como exemplo de tela não migrada; passou a usar Avisos.

### 2026-11-28 — Fase 4: Repasse no front novo (`/app/repasse`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** migrar a tela Repasse para o front novo, no visual novo, com os mesmos números e o mesmo comportamento da antiga (`_repasse.html` + `repasse.js`). As APIs de `routes/repasse.py` não mudam.
- **Tela nova (`frontend/src/paginas/Repasse.tsx` + `paginas/repasse/`):**
  - filtros numa linha (ciclo, turma dependente do ciclo, tipo com "Mensalidade" por padrão, taxa), Limpar, Atualizar e **Buscar** à direita;
  - **Buscar explícito:** os selects mexem num rascunho; só o Buscar aplica (a rota de agentes é pesada, sem cache, e produção tem 1 worker com 4 threads). A primeira busca é automática. Buscar e Atualizar buscam de novo só agentes, planilha e matriculados (como o `repLoad`), nunca filtros nem taxa; os pedidos usam o `signal` do TanStack Query para cancelar o que ficou velho;
  - **Limpar** (como o `repReset`): filtros de cima ao padrão e aplicados, busca de consultor limpa, e planilha e matriculados recriados (busca, página, pagamento, mês, dia, nível e tipo "NOVA MATRICULA");
  - KPIs (Recebido e Consultores só admin), cartões de consultor no lugar do carrossel (iniciais, selo "1", alunos, barra, recebido só admin, repasse) com busca no cliente; clique abre o detalhe (RGM, tipo, turma, ciclo, valor recebido só admin, repasse) com busca;
  - admin: planilha e matriculados paginados (50) com busca de 300 ms, filtros próprios dos matriculados e links CSV com os mesmos parâmetros;
  - consultor: abre direto no próprio detalhe, banner "Seus dados", sem planilha, matriculados e CSV, taxa só leitura.
- **Diferenças de propósito:**
  - a primeira busca é automática (a antiga esperava o Buscar); depois, só o Buscar aplica, e ele fecha o detalhe, como o `repLoad`;
  - cartões em grade no lugar do carrossel (sem as setas);
  - taxa inválida mostra aviso na tela (não `alert`) e os números ficam com a última taxa válida; "Taxa salva." aparece ao lado do campo (não no toast);
  - matriculados: `ciclo_mat` só vai depois de escolhido (antes, o servidor usa o mais recente e a tela o mostra); mês ou nível que some das opções sai do filtro;
  - CSV por link: se o servidor falhar, aparece a resposta dele (a antiga dava `alert`);
  - erro do servidor aparece na tela e dentro da planilha, dos matriculados e do detalhe.
- **Taxa:** o repasse continua calculado no navegador (valor × taxa), e muda na hora em que se digita; "Salvar taxa" (ou Enter) faz `PUT {taxa}` só para admin.
- **Menu antigo:** o item Repasse do `_sidebar.html` **continua** na tela antiga até o usuário aprovar. `tests/test_front_novo.py` passou a checar o link novo só para `TELAS_APROVADAS` (hoje `page_views`); ao aprovar uma tela, ela entra nessa lista no mesmo PR que troca o link.
- **Testes:** `paginas/Repasse.test.tsx` (27). Os testes do menu, da busca e da casca usavam Repasse como exemplo de tela antiga; passaram a usar Meus Atendimentos, e o menu ganhou um teste de que Repasse abre em `/app/repasse`.

### 2026-11-28 — Fase 4: menu do painel atual abre a tela nova de Uso do Dashboard
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** o usuário comparou as duas telas em produção e aprovou trocar o link. Publicado depois do visual novo (entrada abaixo), para ninguém cair na casca antiga.
- **Mudança:** no `_sidebar.html`, o item "Uso do Dashboard" (TI) agora é `href="/app/page_views"`, sem o `onclick="navigate(...)"`. A tela antiga continua no código e abre por `/#page_views`, para quem tiver o link salvo.
- **Regra para as próximas telas:**
  1. a tela entra em `TELAS_NOVAS` (`frontend/src/telas.tsx`) e é publicada;
  2. o usuário compara em produção e aprova;
  3. o item do `_sidebar.html` passa a `href="/app/<tela>"`.
  A tela antiga (partial, JS, `include` no `index.html`) só sai na Etapa 5. `tests/test_front_novo.py::test_menu_antigo_leva_as_telas_migradas_ao_front_novo` falha se uma tela de `TELAS_NOVAS` ficar com o link antigo no menu.
- **Não muda:** a permissão (o item só aparece para quem tem `page_views`; a API continua só admin) e o registro de uso (a tela nova registra a abertura).

### 2026-11-27 — Fase 4: visual novo do front (`/app/`) — menu agrupado, fixadas no banco, busca Ctrl K
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** o usuário achou feia a casca da etapa 2 e quis um visual novo, moderno, com as cores do eduit e as funções atuais. Entre 3 propostas, escolheu a **C** (menu lateral agrupado, com fixadas) mais a busca Ctrl K e as barras de proporção da A. Desenho: `docs/superpowers/specs/2026-11-27-visual-novo-front-design.md`; plano: `docs/superpowers/plans/2026-11-27-visual-novo-front.md`.
- **Menu (`frontend/src/componentes/MenuLateral.tsx`):**
  - estrutura: marca, botão "Buscar tela" (Ctrl K), Fixadas ou Mais usadas, Geral, grupos com ícone que abrem e fecham, usuário e Sair no rodapé;
  - o estado aberto/fechado fica em `localStorage["eduit-menu-grupos"]`, e o grupo da tela atual sempre abre;
  - no celular vira gaveta (`h-dvh`), aberta pelo botão "Menu" da barra de cima, que é uma barra só.
- **Barra de cima (`BarraTopo.tsx`):** grupo, título da tela, estrela para fixar (só em tela migrada e visível no menu), sino com os não lidos (`/api/avisos/nao-lidos`, a cada 2 min; o clique leva a `/#avisos`) e tema. A tela não repete o título; as ações (Atualizar) ficam na própria tela.
- **Fixadas:**
  - **Banco:** tabela `nav_fixadas` (`user_id` → `app_users` ON DELETE CASCADE, `page`, `ordem`; PK `(user_id, page)`), criada em `db._ensure_nav_fixadas_table()` no boot. Só criação.
  - **API (`routes/fixadas.py`, rotas do próprio usuário):**
    - `GET /api/me/fixadas` devolve `{fixadas, mais_usadas}`. Mais usadas = top 3 de `page_views` em 30 dias.
    - `PUT /api/me/fixadas` com `{paginas}` substitui a lista. São no máximo 8, sem repetir, todas conhecidas e visíveis; senão, 400.
    - Login 0: GET devolve listas vazias e PUT dá 400.
    - Erro de banco no GET devolve 200 com listas vazias, para o menu nunca quebrar.
  - **Regra de visibilidade:** `navegacao.Menu.visivel(p)` = `pode(p)`, e o grupo de `p` está visível em `secoes()`. Fixadas e Mais usadas seguem essa regra; o front ainda cruza com os itens do próprio menu. Assim, ninguém fixa nem vê tela que o menu esconde (ex.: TI para não admin), e uma permissão tirada faz a fixada sumir sozinha.
  - **Front (`frontend/src/fixadas.ts`):** atualização otimista. As mutações rodam em série (`scope`) e, no fim, a lista é relida do servidor. Um erro volta a lista e mostra aviso discreto (`role="status"`). Resposta fora do formato vira `null`, e a seção some sem derrubar o menu.
- **Busca (`BuscaTelas.tsx`, `busca.ts`):** Ctrl/Cmd+K em qualquer lugar. Ignora acento e maiúscula, e "começa com" vem antes de "contém". Lista só as telas do menu. Usa teclado (setas, Enter, Esc, Tab preso no diálogo) e devolve o foco ao fechar. Tela migrada abre em `/app/<tela>`, as demais em `/#<tela>`.
- **Uso do Dashboard no visual novo:** períodos em pílulas e cartões "Acessos", "Telas usadas: N de M telas" (M = linhas das duas tabelas) e "Telas sem uso". A tabela ganha uma barra proporcional aos acessos, em que a maior linha vale 100%.
- **Estilo:** rótulos em sentença normal (`.rotulo` sem caixa alta), com os tokens de cor de `estilo.css` para os temas claro e escuro. Ícones Material Symbols por grupo: comercial `trending_up`, acadêmico `school`, premiações `emoji_events`, ferramentas `construction`, vocacional `explore`, meta `campaign`, chamados `support_agent`, TI `memory`, sistema `settings`.
- **Não muda:** o painel atual em `/`, as permissões das APIs e o registro de uso.
- **Processo:** construído tarefa a tarefa por agentes, cada uma com revisão própria, e depois uma revisão do ramo inteiro com uma rodada de correções (barra única no celular, aviso de erro, `h-dvh`, PUTs em série, validação da resposta, estrela pela regra `visivel`, Esc com busca e gaveta, rótulos).
- **Testes:**
  - `tests/test_fixadas.py`, mais `visivel` em `tests/test_navegacao.py`.
  - No front: `fixadas.test.tsx`, `componentes/casca.test.tsx`, `busca.test.ts`, `componentes/BuscaTelas.test.tsx`, `paginas/UsoDoDashboard.test.tsx` e `menu.test.tsx` (86 testes no vitest).

### 2026-11-27 — Modernização, Fase 4, etapa 3: primeira tela no front novo (Uso do Dashboard, `page_views`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** tela piloto da migração. A escolhida é só de leitura e só de admin, então ninguém da operação é afetado.
- **Tela nova (`frontend/src/paginas/UsoDoDashboard.tsx`):** mesma API (`/api/page-views/stats`), mesmos filtros (Hoje, 7, 15 e 30 dias, com 30 como padrão, e datas livres com "Aplicar"), os mesmos 3 indicadores e as mesmas duas tabelas da tela antiga (`_page_views.html` + `page_views.js`).
  - **Melhorias:** erro do servidor aparece na tela (a antiga ficava em branco); "Atualizando…" enquanto busca.
  - **Não muda:** os números, a API, a permissão (só admin, conferida no servidor).
- **Registro de uso:** toda tela do front novo registra a abertura em `/api/track-page-view` (`frontend/src/rastreio.ts`, `App.Tela`), igual ao `trackPageView` do `utils.js`. Sem isso, as telas migradas sumiriam deste mesmo relatório. É um registro por abertura, conferido no navegador.
- **Onde abre:**
  - No menu do front novo (`/app/`), "Uso do Dashboard" já leva a `/app/page_views`.
  - No painel atual, o item continua abrindo a tela antiga. A troca do link antigo entra num PR separado, depois de a tela nova ser aprovada.
- **Conferido em 27/11:** com a mesma resposta da API entregue às duas telas no painel local, indicadores, tabelas, selos e datas saíram iguais, linha por linha. Em produção, a API dos 30 dias trouxe 2076 acessos, 57 páginas com acesso e 21 sem acesso; as duas telas leem essa mesma resposta.
- **Testes:**
  - **Tela** (`UsoDoDashboard.test.tsx`): 30 dias no início, indicadores, preset, datas só com "Aplicar", Atualizar, período vazio, 403, erro 500, registro de uso, quem não tem a página não abre nem chama a API, link do menu.
  - **Também:** `rastreio.test.ts` e `datas.test.ts`. São 32 testes no front.

### 2026-11-27 — Modernização, Fase 4, etapa 2: esqueleto do front novo (React + TypeScript) em `/app/`
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** segunda etapa do plano de separar o front do back (ver a entrada da etapa 1, logo abaixo).
- **Onde fica:** `frontend/`, com Vite 5, React 18, TypeScript, React Router 6 (modos v7 ligados), TanStack Query 5 e Tailwind 3. As versões são exatas no `package.json`, com `package-lock.json`.
- **Como é servido (`routes/front_novo.py`):** o próprio Flask serve o build em `/app/`, no mesmo endereço do painel. Login, cookie de sessão e permissões são os mesmos, e não há CORS.
  - `/app/assets/*`: cache de 1 ano (nome com hash).
  - Qualquer outro `/app/...`: o `index.html`, sem cache, porque as rotas são do React Router.
  - Sem login: vai para o login, inclusive o JS e o CSS.
  - Sem build: 503 explicando.
  - As rotas `/app` estão em `permissoes.ROTAS_SEM_PAGINA` como casca, igual ao `/`. Os dados continuam vindo das mesmas `/api/` com a mesma permissão.
- **Build:**
  - **Dockerfile:** uma etapa `node:22-slim` faz `npm ci` e `npm run build`; a imagem final continua `python:3.14-slim` e só copia o `frontend/dist`. Node não vai para produção.
  - **CI:** o job `front` roda tipos, testes (vitest) e build, e o deploy depende dele, como do pytest. No PR, o job `imagem-pr` monta a imagem inteira sem publicar.
  - **Versão do Node:** o teste exige a mesma no CI e no Dockerfile.
  - `frontend/node_modules` e `frontend/dist` ficam fora do Git.
- **Menu (`frontend/src/menu.ts`):**
  - **Quem vê o quê:** vem do `/api/me` (bloco `nav`, que agora inclui `grupos`, com as telas de cada grupo na ordem). O front só decide a ordem dos grupos, o nome e o destino.
  - **Destino:** tela registrada em `TELAS_NOVAS` (`frontend/src/telas.tsx`) abre em `/app/<tela>`; as demais abrem no painel atual (`/#<tela>`).
  - **Grupo escondido:** as telas dele também somem do menu, como no menu atual.
  - **Nomes:** `frontend/src/titulos.ts` é cópia de `PAGE_TITLES` (`static/js/utils.js`), e um teste exige que os dois sejam iguais.
- **Visual:** os mesmos tokens de cor e as mesmas fontes do `_head.html` (`frontend/src/estilo.css`, `tailwind.config.js`). O tema claro/escuro usa a mesma chave `eduit-theme`, então segue o que a pessoa escolheu no painel.
- **Como migrar uma tela (etapas 3 e 4):**
  1. criar o componente em `frontend/src/paginas/`;
  2. registrar em `TELAS_NOVAS` com o nome da página (`helpers.ALL_PAGES`);
  3. a rota só abre para quem tem a página no `/api/me`.
  O painel antigo continua no ar até a tela nova ser aprovada.
- **Dev local:**
  - `npm run dev` em `frontend/` sobe o Vite na 5173, com proxy de `/api`, `/login` e `/static` para o Flask na 5001.
  - **Windows:** a pasta do projeto dentro do Claude-Data passa do limite de 260 caracteres de caminho, e o `npm install` falha no `esbuild`. Para testar, rodar o npm numa pasta curta (ex.: `%TEMP%r`) e trazer `package-lock.json` e `dist/` de volta. CI e Docker rodam em Linux e não têm o problema.
- **Não muda:** o painel atual em `/`, os templates, o `static/` e as APIs. Ninguém tem link para `/app/` ainda.
- **Testes:**
  - `frontend/src/*.test.ts(x)`: 14 testes.
    - **Cliente da API:** 401 vai para o login, 403 vira aviso de permissão, erro sem JSON, cabeçalhos.
    - **Menu:** consultor comercial, Minha Performance no Comercial, grupo escondido, subpermissão, tela migrada.
    - **App:** menu pelo `/api/me`, tela inexistente, falha do `/api/me`.
  - `tests/test_front_novo.py`:
    - **Rotas do Flask:** index sem cache, rotas do React, assets imutáveis, 404, não sai da pasta, sem login, sem build.
    - **Menu:** nomes iguais ao painel atual, toda tela do menu com nome, grupos do front que existem no servidor.
    - **Build:** Dockerfile e versão do Node.

### 2026-11-27 — Modernização, Fase 4 (separar front e back), etapa 1: regra do menu no servidor e bloco `nav` no `/api/me`
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** separar o front do back sem reescrever. O plano aprovado tem cinco etapas:
  1. a regra do menu sai do Jinja e passa a ser servida pela API;
  2. esqueleto React + TypeScript (Vite) em `frontend/`, servido pelo próprio Flask em `/app/`, no mesmo endereço (login, cookie e permissões iguais, sem CORS);
  3. uma tela piloto só de leitura;
  4. as demais telas, uma por PR, com a antiga no ar até a nova ser aprovada;
  5. a virada: o front novo passa a abrir em `/` e os templates saem.
- **Ponto de partida:** a permissão de cada `/api/` já é conferida no servidor desde 26/11 (`permissoes.py`, com teste). O menu só decide o que aparece; não protege nada. Por isso o front separado não abre acesso novo.
- **`navegacao.py` (novo):** `carregar_usuario()` (era `app._nav_load_user_data`) e `Menu` com:
  - `pode(page)`, que era o `nav_can`;
  - `pagina_inicial`, `paginas_permitidas()` e `para_api()`;
  - `secoes()`, com os grupos do menu lateral que antes eram 12 `{% set _show_... %}` no `_sidebar.html`.
- **O Jinja continua igual:** `app.inject_nav_perms` entrega os mesmos nomes (`nav_can`, `nav_is_admin`, `nav_initial_page`, `nav_allowed_pages`...) a partir de `Menu`, mais `nav_secoes`. O `_sidebar.html` lê os grupos de `nav_secoes.<grupo>`. Os nomes antigos do `app.py` (`_NAV_ALWAYS`, `_NAV_ADMIN_ONLY`, `_NAV_KNOWN_PAGES`, `_nav_load_user_data`) apontam para o módulo.
- **`/api/me`:** ganhou o bloco `nav` (`is_admin`, `is_comercial`, `is_suporte_comercial`, `pagina_inicial`, `paginas`, `secoes`). Os campos antigos ficam exatamente como estavam, porque 7 telas antigas os leem.
- **Regra para mexer no menu:** grupo ou tela nova do menu entra em `navegacao.GRUPOS` / `helpers.ALL_PAGES`, não em expressão no template. Assim o menu antigo e o front novo não divergem.
- **Não muda:** quem vê qual tela, a tela inicial de cada perfil e as permissões das APIs. Conferido em 27/11: o HTML de `/` saiu idêntico byte a byte entre a `master` e a versão nova (admin local).
- **Testes:** `tests/test_navegacao.py` compara com uma cópia congelada do código anterior numa grade de 972 perfis (papel × categoria × login × conjunto de páginas). A comparação cobre cada tela, a tela inicial, a lista permitida e os 12 grupos, avaliando as expressões antigas do template com o próprio Jinja. Também cobre a promoção de subpáginas, o banco fora do ar e o `/api/me` (bloco `nav`, campos antigos, admin de emergência, 401 sem login).

### 2026-11-27 — Cache da rota de alunos da home (`/api/dashboard/students`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** atacar o que mais pesa no painel, medido antes de subir mais processos.
- **Medição em produção (27/11, só leitura, admin):**
  - **Rápidas:** quase todas as rotas das telas principais respondem em 15–300 ms.
  - **Minha Performance:** `insights` ~1 s.
  - **`/api/dashboard/students?ciclo=2027/1`:** 28–39 s em **toda** chamada. Duas ao mesmo tempo levaram 27 s e 55 s; na rodada com tudo junto, o `insights` foi de 1 s para 21 s.
  - **Conclusão:** a home chama essa rota sempre; é a maior parte do "painel pesado".
- **Cache (`routes/dashboard.py`):**
  - **Chave:** `(snapshot_id do último upload de Matriculados, linhas do snapshot, filtros from/to/nivel/situacao/ciclo/tipo/polo)`. A rota só lê o último upload e os sumidos, que já têm cache pela mesma chave. A contagem de linhas pega um upload ainda gravando no mesmo id.
  - **Como:** a conta original virou `_api_dashboard_students_sem_cache`, sem nenhuma mudança na lógica. A rota confere a versão, devolve do cache ou calcula. Mesma chave ao mesmo tempo: só uma conta, as outras esperam o resultado. Erro não entra no cache. Upload novo: as chaves da versão anterior saem. Até 32 chaves. Cabeçalho `X-Cache: hit|miss` para medir.
  - **Aquecimento:** job `alunos_home_warm` (site, por processo, 2 min) recalcula os filtros pedidos nas últimas 24 h quando o upload muda, até 8 por rodada. A primeira pessoa depois de um upload não espera os ~30 s. Depois de reiniciar o processo, a primeira chamada de cada filtro ainda paga a conta.
- **Por que não subir 2 processos agora:** o levantamento de 27/11 achou 6 telas com tarefa e progresso na memória do processo, que quebrariam com 2 processos. São elas: sync Kommo (`kommo_sync._tasks`), sync Bwipo (`bwipo_sync._tasks`), merge de leads (`kommo_merge_route._merge_jobs`), Match & Merge (`match_merge._running/_result/...`), preços de Pós (`bwipo_precos._POS_JOBS`) e Matérias dos alunos (`materias_alunos._job`). Elas precisam guardar o estado no banco antes de `WEB_WORKERS>1`.
- **Não muda:** a resposta, os filtros, a permissão da rota e os números.
- **Testes:** `tests/test_cache_alunos_home.py`: hit/miss, filtro diferente, upload novo ou gravando, erro fora do cache, uma conta só com 5 pedidos juntos, parâmetros e aquecimento.

### 2026-11-27 — Upload Comercial: 2 matrículas no mesmo dia, cada RGM no seu negócio

Caso Giulia Bonfante (#117659 Marketing / #76837 Publicidade): no MATRICULADO D-1 (`gerar_acoes`, `match_merge_lib.py`) os dois negócios abertos do CPF empatavam na fase e o escolhido saía da ordem de um `set` (muda a cada processo). As duas matrículas iam para o mesmo negócio, a segunda sobrescrevia RGM e curso da primeira, e o outro ficava em Aceite.

- A escolha do negócio agora segue esta ordem: primeiro o que já tem **o mesmo RGM**, depois a fase mais quente, com desempate fixo pelo id.
- Fica de fora o negócio que já recebeu outro RGM na mesma execução, ou que tem o RGM de outra matrícula ativa do mesmo CPF.
- Se não sobrar negócio livre, nada é gravado e sai um aviso no log (antes, sobrescrevia).
- No enriquecimento, a matrícula pega a inscrição **do mesmo curso** (`_insc_do_curso`), não a primeira do CPF.

### 2026-11-27 — Modernização, Fase 3: jobs agendados fora do site (worker) e trava no Postgres
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** tirar os jobs do APScheduler de dentro do Gunicorn e garantir que nenhum rode em dobro. É o pré-requisito para subir mais workers do site.
- **Papel do processo (`agendador.py`, env `AGENDADOR`):**
  - `tudo` (padrão): todos os jobs, como antes. Sem mexer no EasyPanel, nada muda.
  - `site`: só os jobs que escrevem em memória lida pelas rotas.
  - `worker`: os que gravam no banco ou chamam APIs. `worker.py` força esse papel.
  - `nenhum`: nenhum job.
  - **Como filtra:** `Agendador` (subclasse do `BackgroundScheduler`) filtra no `add_job`. Nenhum `register_*` mudou.
- **Quem fica onde (levantamento de 27/11):**
  - **Site (`JOBS_DO_SITE`):**
    - `funnel_cache_warm` (+`_boot`): `kommo_sync._funnel_cache`;
    - `mp_bwipo_periodo_warm`: `minha_performance._bwipo_periodo_cache`, que itera os períodos que as rotas pediram;
    - os agendamentos da tela Config (`_run_scheduled_sync`, hoje só loga; as rotas `/api/schedules` mexem no agendador do próprio site);
    - os dois jobs novos `mv_leads_rgm_site` e `painel_cache_vigia`;
    - o aquecimento `warm_academic_sumidos_cache` (só roda em `tudo`/`site`).
  - **Worker (`JOBS_DO_WORKER`):** `bwipo_owner_recente`, `bwipo_painel_base`, `bwipo_contact_ads`, `kommo_leads_dia`, `mv_leads_rgm`, `dist_comercial_schedule_check`, `blog_scheduled_publish`, `conversao_backfill` (+`_boot`), `engagement_daily`, `siaa_avisos_incidentes`, `aceite_reconcile` e `responsible_history_daily` (os dois últimos só com `KOMMO_API_JOBS=1`).
  - **SIAA:** a última execução vai para `painel_job_status` (`trava_jobs.gravar_status`), e a Conexão SIAA lê de lá (`siaa_avisos.ultimo_status`; em `tudo`, cai para a memória se a tabela ainda não existir).
  - **Job novo:** precisa entrar numa das duas listas. O `tests/test_fase3_agendador.py` lê todos os `id=` do código e falha se algum ficar sem lado.
- **Trava (`trava_jobs.py`):**
  - **Jobs que gravam:** rodam embrulhados em `pg_try_advisory_lock(<hash do id>)`, numa conexão própria fora do pool (o `RESET ALL` do pool não solta lock de sessão). Se outro processo está rodando o mesmo job, a execução é pulada.
  - **O que cobre:** dois processos rodando o mesmo job **ao mesmo tempo** (deploy com o container velho e o novo de pé, site em `tudo` com o worker ligado). Vale a partir do segundo deploy desta mudança; no primeiro, o container velho ainda não pega trava.
  - **O que não cobre:** dois processos com o mesmo job agendado rodam um depois do outro (cada um no seu horário). Por isso todo job que grava fica só no worker, que é um processo só, e nenhum job que grava fica no site (que pode ter vários workers do Gunicorn).
  - **Sem banco:** roda sem trava (`TRAVA_JOBS_ESTRITA=1` pula em vez disso). `TRAVA_JOBS=0` desliga.
  - **Cache por processo (`JOBS_POR_PROCESSO`):** roda sem trava. Cada processo do site aquece o seu; com trava, só um aqueceria.
  - **`trava_global(nome)`:** trava de trecho entre processos, com espera de até N s. Ela substitui o `threading.Lock` onde site e worker fazem a mesma coisa:
    - `rebuild_painel_base`: a "forte" (upload, tombamento) espera até 5 min e, se desistir, tenta de novo em 60 s; a "suave" (job) só tenta. O `ensure_painel_base` (painel aberto com a base vazia) também pega a trava;
    - `_apply_turno` da Distribuição Comercial: o agendado (worker) e o manual (site) ficam em fila (até 2 min), como o `_apply_lock` fazia. Se o agendado não conseguir a trava, devolve `ocupado` e **não** marca o dia como feito: o job do minuto seguinte tenta de novo.
    - A chave do trecho é diferente da do job: as duas sessões se bloqueariam.
- **O que o site precisa saber do worker:**
  - **Base do painel:** o `painel_cache_vigia` (1 min) lê `bwipo_painel_meta.built_at`, que só muda quando alguma linha mudou, e vence os caches do painel e da Minha Performance. Quem montou no próprio processo marca o valor como visto e não vence duas vezes.
  - **Cópia `mv_leads_rgm`:** o `_ultimo_ok` vale por processo. Quem cria ou refaz grava `refeita_em=<epoch>` no comentário da materialized view (com savepoint: se falhar, o refresh vale igual). O `mv_leads_rgm_site` (2 min, `leads_rgm.confirmar_copia`) confere em `pg_matviews` que a cópia existe e populada, e usa a hora do comentário como `_ultimo_ok`. No modo periódico, o `pronta()` continua vencendo se o worker parar de refazer. Se a cópia faltar, ele cria.
- **Ressalva conhecida:** `aceite_reconcile`/`responsible_history_daily` usam o `_kommo_api_bg_lock` em memória, que deixa de serializar com as threads de aquecimento do site. Hoje os dois estão desligados (`KOMMO_API_JOBS=0`).
- **Ligar em produção (EasyPanel), nesta ordem:**
  1. Site com `AGENDADOR=site`.
  2. Serviço novo, com a mesma imagem (Fonte: Imagem Docker com o mesmo usuário/token do GHCR do painel) e as mesmas variáveis, mais `AGENDADOR=worker`, sem domínio. O EasyPanel desta conta não deixa trocar o comando pela tela, por isso o `iniciar.sh` (CMD da imagem) roda `python worker.py` quando `AGENDADOR=worker` e o Gunicorn nos outros casos.
  - **Por que nessa ordem:** com o site em `tudo` e o worker ligado, cada job rodaria nos dois processos (a trava só impede que seja ao mesmo tempo).
  - **Horário:** evitar fazer isso perto das 08:00 (`engagement_daily`).
  - **Conferir:** o log do worker mostra "worker no ar com N jobs".
  3. Copiar o webhook de deploy do serviço worker para o segredo `PAINEL_WORKER_DEPLOY_HOOK` do repositório (GitHub → Settings → Secrets → Actions). O workflow dispara os dois webhooks depois do build. Sem o segredo, só avisa, e o worker fica no código antigo.
- **Depois, num passo separado:** subir os workers do Gunicorn (`--workers 2`). A SECRET_KEY fixa já está no ar. Cada worker roda os jobs de cache do site, de propósito; nenhum deles grava no banco.
- **Não muda:** nenhum job mudou de horário nem de lógica. Os botões de sync/upload continuam no site. Sem `AGENDADOR`, o comportamento é o de antes.
- **Testes:** `tests/test_fase3_agendador.py`: papéis, trava, classificação de todos os jobs, vigia, cópia `leads_rgm`, e trava real entre duas sessões no job `testes-pg`.

### 2026-11-27 — Modernização, Fase 2: reaproveitamento de conexões, versões fixas e tabela de Usuários
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** terminar a Fase 2 com o pool de conexões e as versões fixas, e corrigir a coluna de ações da tabela de Usuários.
- **Pool de conexões (`db_pool.py`):**
  - **Antes:** cada `get_conn()` abria uma conexão nova (TCP + autenticação) e o `conn.close()` a derrubava.
  - **Agora:** `conectar(**dsn)` devolve um proxy da conexão. O `close()` devolve a conexão a uma pilha por DSN e o próximo `conectar()` a reaproveita. Nenhuma das ~450 chamadas a `conn.close()` mudou.
  - **Sem limite de conexões abertas:** com a pilha vazia, abre uma nova, como antes. Só as ociosas têm teto (`DB_POOL_MAX_OCIOSAS`, padrão 8 por DSN). Não há fila nem `PoolError` dentro de request.
  - **Ao devolver:** `rollback()`, `reset()` (RESET ALL) e sessão de volta ao padrão (`autocommit` False, sem readonly). Conexão fechada ou com erro no reset é descartada.
  - **Ao pegar:** ociosa há mais de `DB_POOL_PING_S` (30 s) recebe `SELECT 1`. Com mais de `DB_POOL_VIDA_S` (30 min) de vida, é trocada. Conexão de outro processo (fork) nunca é usada.
  - **Esquecida sem `close()`:** o proxy fecha a conexão de vez quando o GC o recolhe, como antes.
  - **Onde entrou:** `db.get_conn`, `get_disparos_conn`, `get_me_conn`, `get_google_ads_conn` e os `_pg`/`_pg_kommo`/`_get_conn` de `auth`, `avisos`, `page_views`, `comercial_rgm`, `minha_performance`, `repasse`, `premiacoes_internas`, `bwipo_sync`, `kommo_sync`, `supervisor_dashboard`, `disparador_whatsapp` e `services/bwipo_owner`.
  - **Fora de propósito:** scripts, `match_merge_lib` (tabelas temporárias, jobs longos), `kommo_merge`, `materias_alunos`, `conversao_backfill`.
  - **Desligar sem deploy:** `DB_POOL=0` no EasyPanel faz `conectar()` virar `psycopg2.connect()` puro.
  - **Medir:** `/api/debug` (só admin) mostra `db_pool` (abertas, reusadas, descartadas, ociosas, média em ms para abrir). O log traz o mesmo resumo a cada 10 min (`DB_POOL_LOG_S`).
- **CI:** novo job `testes-pg` com um Postgres descartável (service container `postgres:17`, senha que só vale dentro do job). Ele roda `tests/test_db_pool.py` contra um banco de verdade: transação aberta, SET de sessão, autocommit/readonly, transação abortada e sessão morta no servidor. O `build-and-deploy` passa a depender de `[testes, testes-pg]`. O job `testes` continua sem Postgres e sem segredo.
- **Versões fixas (`requirements.txt`):** `==` nas dependências diretas e na pilha do Flask (o Werkzeug faz o hash das senhas). São as versões que estavam instaladas em 25/11. Atualizar = trocar o número num PR; o CI testa antes do deploy.
- **Tabela de Usuários:** a coluna de ações tinha 8% da largura e não cabiam os três botões (Entrar como, Editar, Excluir). Os botões vazavam sobre "Criado em" e a lixeira ficava cortada. Agora a coluna tem 120 px fixos e a do usuário fica com o resto.
- **Não muda:** as consultas, as regras, as permissões, o login e o número de workers do Gunicorn (isso é a Fase 3).
- **Testes:** `tests/test_db_pool.py` (17 com conexões falsas, sempre; 5 com Postgres, só com `POOL_TEST_DSN`) e `tests/test_ci_workflow.py` (job novo).

### 2026-11-26 — Performance de Campanhas: Meta e Sem Campanha passam a vir do Bwipo (01/10/2026 em diante)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** a tela Meta Ads · Performance mostrava "Status: ERROR" e zero. O Raphael pediu para ler Meta e Sem Campanha do Bwipo, com backfill desde 01/10.
- **Diagnóstico (26/11):**
  - **Supabase parado:** `campanhas_meta` (lida pelo n8n `count_leads_meta`) e `sem_campanha` param em 01/10/2026. Eram alimentadas pelo Kommo.
  - **Erro na tela:** o n8n responde 200 com corpo vazio e a rota quebrava no `response.json()`.
  - **Google segue vivo:** `campanhas_google` continua recebendo leads dos formulários do site.
- **Onde está a origem no Bwipo:** no **contato**, e só em `GET /api/contacts/{id}`. A listagem e o contato embutido no negócio não trazem esses campos. São eles: `adUtmSource/Medium/Campaign/Content/Term`, `adCtwaClid`, `adSourceId/Type`, `adHeadline`, `adResolved*` e também `gclid`, `fbclid`, `referrer`. O `adUtmCampaign` segue o mesmo padrão de nome da antiga `campanhas_meta` (ex.: `cs.grad.whats.0826.terapia-ocupacional`). O `bwipo_contacts`/`bwipo_deals.origem` não serve: `origem` está vazia e o `source` "Facebook" só existe nos importados do Kommo em 22/09.
- **Coleta (`services/bwipo_campanhas.py`):**
  - **Tabela:** `bwipo_contact_ads`, uma linha por contato. É criada em `db._ensure_bwipo_sync_tables`, junto com o índice `idx_bwipo_deals_created`.
  - **Job:** `bwipo_contact_ads` roda a cada 5 min (primeira vez 60 s após o boot), com até 240 s e 800 contatos por rodada. `BWIPO_CONTACT_ADS_JOB=0` desliga.
  - **Fila:** contatos de negócio do Pipeline Principal criado desde 01/10, do mais novo para o mais antigo. O dia de hoje enche primeiro e o backfill vem atrás (~0,65 s por contato, então ~46 mil contatos levam umas 11 h).
  - **Recoleta:** erro (até 5 tentativas, 30 min de intervalo), anúncio ainda sem `adResolveStatus=ok` (depois de 1 h) e negócio novo criado depois da coleta. A origem vale por contato, então é o último toque.
  - **Token:** só o comercial (`BWIPO_COMERCIAL_API_TOKEN`). Em 401/403 o lote para.
- **Classificação (`origem()`):**
  - **Meta:** `adCtwaClid` ou `adSourceType=ad`, utm_source facebook/instagram/fb/ig/meta*, ou só `fbclid`.
  - **Google:** `gclid` ou utm_source com "google". Fica fora desta parte; o Google continua no `services/google_ads_insights.py`.
  - **Outra:** qualquer outra UTM. Não entra em lugar nenhum.
  - **Sem Campanha:** nenhum sinal.
  - **Pendente:** contato ainda não coletado. Não conta.
- **Leitura:**
  - **Lead:** um contato com negócio no Principal criado no período (dia em BRT), contado **por safra**: aberto/ganho/perdido pelo `status` do negócio mais recente do período (WON/LOST), como no Google.
  - **Matrículas Meta:** RGMs da `bwipo_painel_base` com `data_matricula` no período, cujo negócio é de contato Meta.
  - **Cache:** 3 min por período. O mês inteiro leva cerca de 2 s.
- **Corte:** `/api/meta/campaigns` e `/api/sem-campanha/leads` juntam as duas fontes. Até 30/09 continuam no n8n/Supabase; de 01/10 em diante vêm do Bwipo (`periodo_legado` / `periodo_bwipo`). Se uma das partes falha, a outra continua. A resposta traz `pendentes`, e a tela mostra "coletando origem de N contatos".
- **Front:** o pie "Total" agora soma aberto + ganho + perdido também para Meta e Sem Campanha (safra).
- **Não muda:** o Google (Supabase + `google_ads`), as permissões das rotas e o `/api/meta/webhook`.
- **Testes:** `tests/test_bwipo_campanhas.py` (classificação e corte).

### 2026-11-26 — Permissão no servidor nas rotas que faltavam (mapa central em `permissoes.py`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** fechar o que a Fase 1 deixou para depois: as rotas sem checagem (dashboard, engagement, inadimplência, crm, logs etc.). A lista foi aprovada pelo Raphael em 26/11, antes de aplicar.
- **Levantamento (26/11):**
  - **Total:** das 411 rotas, 180 não checavam nada. Não entram nessa conta as que já usam `exige_pagina`, checagem de papel ou um auxiliar com 403 (`_gate`, `_allowed`), nem as 38 de preço protegidas no `app.py`.
  - **Origem das páginas:** cada rota recebeu as telas que a chamam (grep em `static/js` e `templates`), para ninguém perder o que já usa pela tela.
- **Como aplica:**
  - **Mapa central:** `permissoes.PAGINAS_POR_ENDPOINT` (endpoint → páginas) é aplicado por `aplicar_mapa(app)` no `app.py`, depois de registrar todas as rotas. Cada view é embrulhada com `exige_pagina` e devolve 403 em JSON sem a página. Admin e `user_id` 0 passam.
  - **Por que um mapa e não decorators:** decorators espalhados mexeriam em 26 arquivos.
  - **Endpoint inexistente:** só avisa no log.
- **O que ficou:**
  - **170 endpoints com página.** Exemplos:
    - `/api/dashboard*` e `/api/lista-alunos/*` → `dashboard` (+ `rematricula`);
    - `/api/engagement/*`, `/api/comm/*` e `/api/snapshots/*` → `intelligence`;
    - `/api/inadimplencia/*` → `inadimplencia`;
    - `/api/sync*` → `sync`;
    - `/api/comercial-rgm/*` → `comercial_rgm`, `bwipo_comercial` (`data` e `metas` também `minha_performance`);
    - `/api/bwipo/painel*` → `bwipo_comercial`;
    - `/api/logs*` → `logs`, `config`.
    - Lista completa no próprio mapa.
  - **14 só admin:** diagnóstico e manutenção que nenhuma tela chama (`*/connection-test`, `_diag/kommo*`, `debug/address`, `comercial-rgm/diagnostics|cache/clear|populate-from-matriculados`, `kommo/reconcile-aceites`, `kommo/sync-responsible-history`, `kommo/merge/session-status`, `comm/dispatch`, `comm/callback`, `meta/webhook`). Os três últimos já exigiam login, então nenhum sistema externo os alcançava.
  - **Livres de propósito (`ROTAS_SEM_PAGINA`):** `/health`, `/login`, `/logout`, `/` (o menu já filtra), `/api/track-page-view` e `/api/avisos*` (popup para todos). O webhook do Motor continua com o HMAC.
  - **Do próprio usuário (`ROTAS_DO_PROPRIO_USUARIO`):** `minhas-matriculas` (POST) e `solicitacoes_ti/meus`. A rota já filtra pelo `user_id` da sessão; exigir página tiraria o acesso do dono.
- **Regra para rota nova:** toda rota `/api/` precisa de `@exige_pagina(...)` na própria rota ou de uma linha no mapa. O `tests/test_permissao_rotas.py` lê o código e falha se aparecer rota sem nenhum dos dois, e fora das listas de livres e do próprio usuário.
- **`dashboard` segue a regra do menu, não só a tabela (`permissoes._dashboard_pelo_menu`):**
  - **O que o banco mostrou (consulta só de leitura, 26/11, autorizada pelo Raphael):** só 7 de 40 usuários não admin têm `dashboard` em `user_permissions`. Um deles é a supervisora comercial id 53, que não tem.
  - **Por que a tabela não basta:** o menu (`app.inject_nav_perms` / `nav_can`) mostra a home para quem não é comercial nem suporte comercial, sem consultar a tabela. Exigir só a tabela tiraria os dados da home de 33 pessoas.
  - **Regra no servidor:** rota com `dashboard` passa para quem tem a página **ou** para quem não é `Comercial` nem suporte comercial (categoria ou login), como no menu.
  - **Alcance:** a exceção vale só para `dashboard`. Supervisor sem `inadimplencia` continua barrado na inadimplência.
- **Não muda:** as rotas que já checavam, as de preço, `exige_pagina`/`tem_alguma_pagina`, o login e o `require_auth`.
- **Testes:** `tests/test_permissao_rotas.py` (624, inclusive a regra do menu para `dashboard`). Para cada endpoint do mapa: 403 sem a página, 200 com qualquer uma das páginas, admin passa. Também conferem que o endpoint existe no código, a amostra aprovada e a guarda de rota nova.

### 2026-11-26 — Senha: troca pelo próprio usuário e troca obrigatória (`troca_senha.py`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** os importados do Kommo/DataCrazy entram com a mesma senha padrão e não conseguiam trocá-la sozinhos.
- **Tela `/trocar-senha` (GET/POST, `routes/auth.trocar_senha`):**
  - **Campos:** senha atual + nova senha duas vezes.
  - **Regras (`troca_senha.validar_nova_senha`):** 8 a 128 caracteres; a confirmação precisa conferir; a nova precisa ser diferente da atual.
  - **Gravação:** scrypt (`gerar_hash`) e flag desligada, na mesma `UPDATE`. A senha atual também confere com o hash SHA-256 antigo.
  - **Acesso:** qualquer usuário logado, pelo link "Trocar minha senha" no card Sessão do Meu Perfil. O login de emergência (`user_id` 0) não tem senha no banco e recebe uma mensagem em vez da troca.
  - **Visual:** a tela estende `login.html`, que ganhou os blocos `titulo` e `conteudo`. O HTML do login não mudou.
- **Flag `app_users.precisa_trocar_senha` (BOOLEAN NOT NULL DEFAULT FALSE):**
  - **Criação:** feita no boot por `troca_senha.garantir_coluna` (`ALTER … ADD COLUMN IF NOT EXISTS`). Se falhar, só avisa no log.
  - **Login:** copia a flag para a sessão. Sem a coluna, lê FALSE e o login segue.
  - **Bloqueio (`troca_senha.bloqueio`, chamado no `require_auth`):** com a flag ligada, página redireciona para `/trocar-senha` e `/api/*` recebe 403 `{"redirect": "/trocar-senha"}` (redirect quebraria os `fetch`). Ficam livres `/trocar-senha`, `/logout`, `/login`, `/health` e `/static/`. `DEV_SKIP_AUTH` não é afetado.
- **Decisão do Raphael (26/11): nada liga a flag automaticamente.**
  - **Fora deste PR:** a importação, o login com a senha padrão e a senha definida pelo admin na tela de Usuários.
  - **Para obrigar alguém a trocar:** `UPDATE app_users SET precisa_trocar_senha = TRUE WHERE id = <id>;`, que vale no próximo login da pessoa. Hashes scrypt têm sal, então achar quem ainda usa a senha padrão pede conferência em Python, um a um, e não um `UPDATE` em SQL puro.
- **"Entrar como" (decisão do Raphael, 26/11):**
  - **Por quê:** o admin precisa continuar com acesso às contas dos consultores, mesmo depois que eles troquem a senha. O painel só tem dados da empresa. **Não guardar nem pedir a senha de ninguém para isso.**
  - **Entrar:** `POST /api/users/<id>/entrar-como`, só com `role = admin`, pelo botão "Entrar como" (ícone `login`) na tela de Usuários. Troca o usuário, o login e o papel da sessão, e o painel passa a mostrar o menu e as permissões da pessoa. O admin fica guardado em `session["admin_original"]` (só o primeiro, se encadear) e a flag de troca é desligada nessa sessão.
  - **Voltar:** `POST /voltar-admin`, pela faixa amarela no topo (`partials/_entrar_como.html`, incluída no `index.html`). A rota fica livre mesmo com a flag ligada. `/logout` encerra tudo.
  - **Registro:** cada entrada e cada volta vão para o log do app (`ENTRAR_COMO: admin X (id) entrou como Y (id)` / `voltou`). Não há tabela de auditoria.
  - **Não mexe:** senha, banco e permissões gravadas.
- **Não muda:** a senha padrão das importações, a edição e a redefinição de senha pelo admin, o login de emergência, `_db_auth` e as permissões.
- **Testes:** `tests/test_troca_senha.py` (34) e `tests/test_entrar_como.py` (17), com banco simulado.

### 2026-11-26 — Modernização, Fase 2: testes no CI antes do build
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** o pytest roda antes do build da imagem. Se algum teste falhar, não há build nem deploy. Os testes também rodam em PR para a `master`.
- **Workflow (`.github/workflows/deploy-easypanel.yml`):**
  - **Novo job `testes`:** Python 3.14, a mesma versão do `Dockerfile`. Instala `requirements.txt` + `pytest` e roda `python -m pytest tests -q -rs`. Usa só `contents: read`, nenhum segredo e nenhum Postgres.
  - **`build-and-deploy`:** tem `needs: testes` e `if: github.event_name != 'pull_request'`. Em PR só os testes rodam: sem imagem, sem webhook.
  - **Fila do deploy (`concurrency: deploy-easypanel`):** passou do workflow para o job de deploy. No nível do workflow, um PR entraria na mesma fila e poderia substituir um deploy pendente.
- **Testes de banco no CI:** `test_motor_pg.py` e `test_siaa_avisos.py` (23 casos) continuam pulando sem `MOTOR_TEST_DSN` / `SIAA_AVISOS_TEST_DSN`. **Não colocar DSN nem segredo no job de testes.** Os harness JS (`tests/js`) rodam com o Node que o `ubuntu-latest` já traz; sem Node, eles pulam.
- **Teste que dependia da ordem:** `tests/test_caminhos_siaa.py` trocava `psycopg2.connect` e `openpyxl.load_workbook` no import, inclusive quando os pacotes reais estavam instalados. Com isso, `test_precos_siaa_site_ia.py` falhava na suíte inteira (2 falhas) e passava sozinho. Agora o stub só entra quando o pacote não está instalado.
- **Não muda:** os passos de build, push no GHCR, visibilidade do pacote, pull anônimo e o webhook do EasyPanel. Nenhum código do painel mudou.
- **Testes:** `tests/test_ci_workflow.py` lê o YAML e confere os gatilhos, o `needs`, a ausência de segredo no job de testes e a fila só no deploy.
- **Proteção da branch (fora do repo):** para o PR não poder ser mergeado com teste vermelho, marcar **"Testes (pytest)"** como check obrigatório da `master` em Settings → Branches no GitHub. Isso depende de alguém com admin no repo.

### 2026-11-26 — Inscrição - Automática: todas as telas e conferência SIAA em segundo plano
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** a página tem de ter todas as telas da interface do serviço, inclusive as que tinham ficado de fora; e a conferência no SIAA passou a responder 202 na hora (mudança no serviço).
- **Abas novas** (porte de `painel.js`, `crm.js`, `afiliados.js`, `manutencao.js` da V2), na ordem: Visão geral, Fila, Nova inscrição, **Painel de inscrições**, Cadastros, **Integração CRM**, **Afiliados**, Inscrições × SIAA, Agente, **Manutenção**, Configuração (admin), Eventos, Simulação.
  - **Painel de inscrições:** `GET painel?de&ate&busca&erros=1` + card de Afiliados (`GET afiliados?de&ate`); KPIs, barras de erros/forma/departamento, últimos registros, botão "Só erros". Atualiza a cada ~15 s.
  - **Integração CRM** (entrada antiga Kommo/n8n): `GET crm`; `POST crm/token` (admin; o token aparece uma vez, 5 min na tela, com confirmação ao trocar); `PUT crm/modo` (admin; "real" pede confirmação e só se o serviço permitir). O endpoint mostrado é o do serviço (`/api/crm/inscricoes`), não o do painel: o proxy não repassa essa rota.
  - **Afiliados:** `GET afiliados`; `PUT afiliados` (admin; ligar a partir de 0% pede confirmação) e `PUT afiliados/chave` (admin). Operar vê o percentual sem poder mudar.
  - **Manutenção:** Problemas (`GET manutencao`), diagnóstico por IA (`POST manutencao/diagnosticos` — operar — e acompanhamento por `GET manutencao/diagnosticos/:id` a cada 2 s com a aba visível), ações propostas com "Aplicar" (`POST manutencao/aplicar`, admin, confirmação; ação em modo real avisa de duplicidade) e chave da IA (`PUT manutencao/ia`, admin, mascarada). A reexecução aplicada é acompanhada na aba Nova inscrição. Lista de diagnósticos recentes com "Ver".
- **Conferência no SIAA:** `POST orq/conferencia/siaa` (admin) responde 202 e roda no serviço; a aba acompanha `GET orq/conferencia/siaa` (operar) a cada ciclo de 5 s enquanto `emAndamento`, mostra "Conferindo…" e depois o resultado da última conferência, e recarrega o relatório quando termina. Saiu o tratamento de 504 "continua no serviço"; o timeout dessa rota voltou ao padrão (20 s). Serviço sem a rota GET (404): a tela não mostra o bloco.
- **Proxy:** `ROTAS` ganha `GET orq/conferencia/siaa`; as rotas das abas novas já estavam na lista. Teste novo confere que toda rota chamada pela página bate com alguma regra.
- **Não muda:** regras de papel, cabeçalhos, demais abas.
- **Testes:** `tests/test_inscricao_auto.py` (59).

### 2026-11-26 — Inscrição - Automática: página no painel com proxy autenticado
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** levar para o painel as telas da inscrição automática (interface V2 do repo `inscricao-completa`, que rodava só no Mac, sem login), numa página "Inscrição - Automática" com todas as abas, duas permissões (operar e administrar) e o serviço na rede interna do EasyPanel.
- **Permissões:** `inscricao_automatica` (operar) e `inscricao_automatica_admin` (administrar). Admin do painel e o login de emergência administram. Quem tem só a sub de admin vê o menu: `_nav_load_user_data` promove sub → master, como no `disparador_whatsapp_*`. Registradas em `helpers.ALL_PAGES`, `utils.js` (`PAGES`, `PAGE_TITLES`, `navigate()` → `loadInscricaoAutomatica()` / `inscricaoAutoSair()`), sidebar (Comercial, ao lado de "Inscrições", flag `_show_comercial`), `index.html` e `config.js` (`PAGE_LABELS`, `PAGE_GROUPS_CONFIG`, as duas caixas no grupo Comercial).
- **Proxy (`routes/inscricao_auto.py`):** `/api/inscricao-auto/<rota>` → `{INSCRICAO_API_URL}/api/<rota>` (padrão `http://banco-inscricao:8788`), com query e corpo repassados.
  - **Cabeçalhos:** `X-Painel-Token` (`INSCRICAO_API_TOKEN`, nunca no navegador), `X-Usuario` (`display_name_from_login`, só Latin-1), `X-Inscricao-Papel` (`admin`|`operar`) e sempre `Content-Type: application/json` nos não-GET (o serviço devolve 415 sem ele; corpo vazio vira `{}`).
  - **Lista branca `ROTAS` (método + regex + papel):** rota fora da lista = 404, mesmo para admin (ex.: `crm/inscricoes`, que tem Bearer próprio, e o `POST orq/inscricao` síncrono). Operar: as leituras, `POST inscricoes` em validar/ensaio, `cupom/validar`, `polos/testar`, `manutencao/diagnosticos`, `orq/fila/J…/(reprocessar|cancelar)`. Admin: o resto (mesma lista do `acesso-painel.js` do serviço) e `POST inscricoes` com `"modo":"real"` (conferido no corpo).
  - **Respostas:** status do serviço repassado; CSV com `Content-Disposition` repassado; 401 do serviço vira 502 (token do painel errado — repassar 401 mandaria o usuário para o `/login`). Rede: Timeout → 504, ConnectionError → 503, outro → 502.
  - **Timeouts curtos (1 worker / 4 threads):** 20 s (`INSCRICAO_API_TIMEOUT_S`); 60 s em `orq/agente/verificar` e `atendente/testar`. (A conferência no SIAA: ver a entrada seguinte, acima.)
- **Página (`_inscricao_automatica.html` + `inscricao_automatica.js`):** porte de `automacao.js`, `app.js` e `polos.js` da V2, com os mesmos textos, confirmações e avisos (revisão só com `?force=true` e aviso de duplicidade; inscrição real com confirmação). Abas `.ds-segment` como `switchConfigTab`: Visão geral (modo, disjuntor, KPIs, motores, últimas), Fila (filtros, % de andamento, "Ensaio OK (sem pedido)", reprocessar/cancelar, devolver ao crm1, devolvidos ao crm1), Nova inscrição (validar/ensaio; real só para admin e só se o serviço liberar), Cadastros (campanha/cupom, atendente, polos, chave Google), Inscrições × SIAA (sessão por cURL, KPIs, filtros, CSV, conferir), Agente, Configuração (só admin; diff por seção + histórico; campo `linkAluno` aparece quando o serviço o devolver), Eventos e Simulação (só com `orq/health.simulacao`). Papel vem de `data-papel` no template; o que é de admin fica escondido para operar e o servidor recusa de qualquer forma. Atualiza a cada 5 s só com a página aberta e a aba do navegador visível; acompanhamento da inscrição avulsa a cada 2,5 s.
- **Diferenças do original:** sem o campo "Operando como" (o nome vem do login); a página diz quando um motor da configuração não foi carregado no serviço (`ORQ_ENGINES`); "Execuções recentes" ganha "Ver" para reabrir o acompanhamento.
- **Não muda:** as páginas "Inscrições" (`inscricao`, logs Supabase) e "Leads em Inscrição Automática". Anotado e não mexido: `/api/inscricao*` dessas páginas continua sem checagem de permissão no servidor.
- **Variáveis novas (`.env.example`):** `INSCRICAO_API_URL`, `INSCRICAO_API_TOKEN`, `INSCRICAO_API_TIMEOUT_S`.
- **Testes:** `tests/test_inscricao_auto.py` (50): login/permissão, papel por permissão, rotas de admin recusadas para operar, inscrição real só admin, rota fora da lista, JSON forçado, CSV, 401→502, erros de rede, timeouts e registro da página nos lugares do painel.

### 2026-11-25 — Modernização, Fase 1: SECRET_KEY, cookies, hash de senha e permissão no servidor
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** fechar as três brechas do diagnóstico antes de separar front e back.
- **SECRET_KEY (`seguranca.chave_da_sessao`):** o padrão `"dcz-sync-default-key-change-me"` estava público no GitHub e permitia forjar um cookie de admin. Agora, sem a variável (ou com esse valor), o painel gera uma chave aleatória por processo e avisa no log.
  - **Não derruba o boot de propósito:** ninguém confirmou se a variável existe no EasyPanel.
  - **Custo sem a variável:** todos logam de novo a cada reinício.
  - **⚠️ Antes de subir mais de 1 worker do Gunicorn, definir `SECRET_KEY` no EasyPanel.** Sem ela, cada worker teria uma chave diferente e as sessões cairiam a cada request.
- **Cookies (`configurar_cookies`):** HttpOnly e SameSite=Lax. Secure vem de `SESSION_COOKIE_SECURE`: `1` no Dockerfile, desligado no localhost em http.
- **Senhas:**
  - **Hash novo:** `helpers._hash_pw` e `routes/auth._hash_pw` gravam scrypt do Werkzeug, com sal (~0,5 s por hash).
  - **Migração no login:** o `_db_auth` ainda aceita o SHA-256 antigo (64 hex) e o troca pelo novo no primeiro login certo de cada usuário. Ninguém precisa trocar a senha.
  - **Importação Kommo/DataCrazy:** calcula o hash da senha padrão uma vez só, fora do laço.
- **Permissão no servidor (`permissoes.exige_pagina`):**
  - **Regra:** a mesma do `bwipo_precos._allowed`. Admin e `user_id` 0 passam; os demais precisam de **qualquer uma** das páginas na `user_permissions`; sem isso, 403 em JSON.
  - **Origem das páginas:** cada rota recebe as páginas cujas telas a chamam (mapeado por grep no front em 25/11).

  | Rota (só escrita) | Páginas |
  |---|---|
  | 38 rotas `/api/` do `atualizar_preco_app` (publicam preço em sites, IA e CRM) | `atualizar_preco`, `precos_bwipo` |
  | `/api/schedules` POST/DELETE/toggle | `schedule`, `config` |
  | `/api/turmas*` e `/api/ciclos*` POST/PUT/DELETE/seed | `config` |
  | `/api/distribuicao` POST | `distribuicao` |
  | `/api/macro-email/consultores` POST | `macro_email` |
  | `/api/upload`, `/api/upload-batch`, `/api/upload-folder` | `update` |
  | `/api/debug` | só admin |

- **Não muda:** as leituras (GET) dessas telas, as rotas que já tinham checagem própria (usuários, Bwipo preços, chamados, repasse etc.), o login de emergência e o `DEV_SKIP_AUTH`.
- **Fica para a próxima etapa:**
  - as demais rotas sem checagem (`dashboard`, `engagement`, `inadimplencia`, `crm`, `logs`...), começando pelas de escrita;
  - a **senha padrão fixa** dos usuários importados do Kommo/DataCrazy, que está no código e sem tela de troca de senha pelo próprio usuário.
- **Testes:** `tests/test_seguranca_fase1.py` (53), inclusive cookie forjado com a chave antiga sendo recusado e o mapa rota → páginas. Localhost sem `SECRET_KEY`: sobe, avisa no log e o admin continua acessando.

### 2026-11-25 — Modernização, Fase 0: compressão e cache dos estáticos (`entrega_web.py`)
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** primeira fase da modernização do painel. Medição em produção (admin, 25/11): página utilizável em 52 a 70 s. O Python montava o HTML em 0,05 a 0,8 s; o tempo ia na entrega. Os 1,8 MB de HTML e os 55 JS saíam sem compressão, pelo Gunicorn, a 60 a 100 KB/s. Os estáticos prendiam as 4 threads, e o `/api/me` (1 KB) chegou a esperar 22 s na fila.
- **Agora:**
  - **Compressão:** o Flask-Compress comprime HTML, JSON, CSS e JS dinâmicos (br nível 4, senão gzip 6; mínimo de 500 bytes). Custa ~25 ms de CPU por abertura do index.
  - **Estáticos:** `/static` passa a ser servido pelo WhiteNoise, antes do Flask. As versões `.gz`/`.br` são geradas no build (`python -m whitenoise.compress static` no Dockerfile). Sem o `.gz`/`.br`, sai o arquivo cru, como antes.
  - **Versão dos estáticos:** `?v=` passa a ser o hash do conteúdo de `static/` (`versao_estaticos`). Saiu o `{% set _v = range(100000) | random %}` do `index.html`, que sobrescrevia a versão de `app.py` desde 03/2026 e obrigava a baixar todos os JS a cada abertura.
  - **Cache:** pedido em `/static` com `?v=` igual à versão do processo (ou `?v=<versão>-<sufixo manual>`, como `-hide-crgm`) recebe `public, max-age=31536000, immutable`. Qualquer outro pedido (sem `?v=` ou com versão de outro deploy) recebe `no-cache` + ETag (304), o mesmo de antes. Isso evita que, num deploy com o container velho e o novo no ar, o conteúdo velho fique preso no cache sob a versão nova.
  - **Desenvolvimento:** `STATIC_AUTOREFRESH` vale `1` por padrão (local relê os arquivos e nunca usa cache longo). O Dockerfile define `STATIC_AUTOREFRESH=0`.
- **Regra para quem mexer no front:** todo `<script>`/`<link>` de `static/js` no `index.html` precisa de `?v={{_v}}` (há teste que confere). Arquivo sem `?v=` continua funcionando, só sem cache longo.
- **Ganho esperado (medido nos arquivos, não em produção):** JS de 1.672 KB para ~350 KB (br) na primeira visita e zero nas seguintes; HTML de ~1.860 KB para ~260 KB. Medir de novo em produção após o deploy.
- **Teste em localhost (25/11):** `app.py` real com banco simulado (só para a página abrir), Waitress com 4 threads e cada resposta limitada a 100 KB/s, como a saída medida em produção. Perfil admin.

  | | antes (`master`) | depois |
  |---|---:|---:|
  | 1ª visita: página utilizável | 23,0 s | 3,9 s |
  | Visita seguinte: página utilizável | 22,5 s | 3,7 s |
  | HTML baixado | 1.815 KB | 254 KB |
  | JS baixados | 1.630 KB, sempre | 352 KB na 1ª visita, 0 KB depois (55/55 do cache) |

  Minha Performance, Dashboard Comercial Bwipo, Preços, Chamados e Config abriram sem erro de JS.
- **Não muda:** rotas, templates (fora a linha do `_v`), regras de negócio, autenticação (`/static` já era liberado sem sessão em `require_auth`), CSVs e downloads (`send_file`), que ficam sem compressão.
- **Testes:** `tests/test_entrega_web.py`.

### 2026-11-19 — Renovação seletiva: ajustes finais (proposta reduzida)
- **Modelo usado:** Claude Opus 5.5.
- Módulo solicitado e não tentado aparece como **"Não executado"** com o motivo (`motivo_situacao`: "a execução parou no módulo anterior" / "parou antes da coleta"), nunca como falha de coleta; o bloqueio conservador mostra "não executado (a execução parou antes)".
- O selo mostra o resultado (Sucesso completo/Parcial/Falha) e acrescenta o estado só quando ele informa algo a mais (`incerta`, `interrompida`).
- A API não reabre módulo resolvido no mesmo episódio; "Renovar agora" atinge os não resolvidos. Módulo resolvido que expira de novo enquanto o outro está bloqueado só volta pela semeadura manual ("Atualizar sessão SIAA") ou quando o episódio fechar.

### 2026-11-19 — Conexão SIAA: renovação seletiva por módulo no painel
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** mostrar e controlar a renovação seletiva por módulo (contrato da session-api, seção "Renovação seletiva por módulo (2026-11-19)" de `CONTRATO-RENOVACAO-SIAA.md` no `eduit-insights`).
- **Lista branca (`routes/conexao_siaa.py`):** passam, todos opcionais, `alvos` (`renovar_agora`/`liberar`/`automatica`), `episodio.modulos_estado` (por módulo: `tentativas_automaticas`, `bloqueado`, `bloqueio_motivo`, `resolvido`, `resolvido_em`, `liberado_em`), `modulos_solicitados`, `resultado` e `modulos[m].situacao` nas execuções (status, histórico e diagnóstico) e `modulos_liberados` no liberar. Rótulos só `[a-z0-9_]`, listas só `academico`/`financeiro` (sem repetição), ISO só `…Z`. Lista ausente continua `null` (não vira `[]`), para a tela distinguir backend antigo. `_mods` passou a descartar repetidos.
- **Tela (`static/js/conexao_siaa.js`):** "Módulos solicitados" e coluna "Situação" por módulo (Renovado, Falhou, Incerto, Pendente, Não solicitado). Módulo fora de `modulos_solicitados` aparece como "Não solicitado", nunca como falha; sem o campo = os dois. Resultado: "Sucesso completo", "Parcial" (com "Renovado: X · não renovado: Y") ou "Falha"; sem `resultado` mostra o estado como antes. O histórico troca a coluna Estado por Resultado (mesma regra). Episódio com `modulos_estado` ganha tabela por módulo (bloqueado + motivo, resolvido, tentativas, liberado em). "Renovar agora" e "Liberar nova tentativa" dizem, no card e no `confirm`, quais módulos atingem (`alvos`); sem `alvos`, texto atual + "os dois módulos". `alvos.liberar` vazio esconde o Liberar. Sem `situacao` (backend antigo), a situação só é derivada como Renovado (publicação ok + sessão validada) ou Pendente (execução ativa); o resto fica "—", sem inventar falha.
- **Não muda:** corpos dos POSTs (`{pedido_id, confirmacao}`, `{episodio_id, versao}`), polling só lê, inscrições fora da renovação. Sem deploy.
- **Testes:** `tests/test_conexao_siaa_renovacao_seletiva.py`, `tests/js/conexao_siaa_renovacao_seletiva_harness.js` (rodado por `tests/test_conexao_siaa_renovacao_js.py`).

### 2026-11-18 — Consulta de inscrições (sessão manual por cURL)
- **Modelo usado:** Claude Opus 5.5 (coordenação), testes por agentes Sonnet.
- **Pedido:** terceiro módulo de sessão SIAA, `inscricoes`, atualizado só manualmente e mantido pelo ping JSF do keepalive (backend no `eduit-insights`, `docs/CONTRATO-CONEXAO-SIAA.md`, seção do módulo manual).
- **Atualizar sessão SIAA:** card "Consulta de inscrições" (grid `auto-fit`, responsivo) com cURL, Testar e Salvar. `/siaa-sessao/testar|salvar` repassam `{"curl": texto}` a `/session/inscricoes[/test]`; quem valida destino e extrai o cookie é a session-api (o painel nunca interpreta nem registra o cURL; log só módulo/len/status). Resposta passa por lista branca (`_resposta_curl`): sem cookie, ViewState ou hash. Campo limpo após salvar com sucesso. Acadêmico/financeiro inalterados.
- **Conexão SIAA:** card Inscrições (estados `nao_configurada` e `atualizacao_manual`, com avisos próprios), filtro do histórico e exportação incluem o módulo (`_MODULOS_STATUS`). A renovação automática continua só com acadêmico/financeiro (`_MODULOS`, `CS_MODULOS_RENOVACAO`).
- **Não muda:** gatilho, fluxo e critério de sucesso da renovação; rotas existentes. Sem deploy.

### 2026-11-18 — Preços dos cursos: Pós-Graduação no CRM e nos sites Wix
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** subir a planilha de Pós (export com PRODUTO, CURSOS, CERTIFICADORA, CANAL, DURAÇÃO "6 meses", PARCELAS "12 parcelas", PREÇO SIAA, DESCONTO 0,35, VALOR COM DESCONTO, SKU_CRM, STATUS_CRM; uma linha por curso × duração × canal) no CRM e nos sites. eduit.com.br e IA ficam para depois (o eduit.com.br tem quase só ofertas de "12 Meses", a planilha traz 6 e 9).
- **CRM (`routes/bwipo_precos.py`):** `parse_pos_workbook` agrupa por SKU_CRM (comparado por `_sku_key`: sem acento, minúsculo, sem espaço); STATUS_CRM "Inativo" e linha sem SKU ficam de fora (voltam em `inativos`/`sem_sku`). `_pos_index` indexa por SKU os produtos cujo `courseConfig.level` não é GRADUATION. **Pós nunca cria produto**: sem SKU no CRM vira `sem_produto`. `_apply_prices_pos` faz o PUT como na graduação, mas o `course` copia de volta tudo do `courseConfig` que não é preço (menos id/datas), e a cota escolhida grava `months` e `installments` da planilha. Rotas: `POST /api/bwipo-precos/pos/preview` (devolve `amostra` com nível, campos e cotas de um produto de Pós do CRM, para conferência) e `POST /api/bwipo-precos/pos/aplicar` com `canais` (e `so_sku` opcional: botão "Testar em 1 produto", para conferir no Bwipo antes de subir todos — o formato do produto de Pós no CRM não foi visto no desenvolvimento, sem token local). Sem histórico (as tabelas de snapshot são da graduação). **Aplicar roda em segundo plano** (corrigido no mesmo dia): a primeira subida dos 572 caiu com 500 após ~23 s — o cliente do Bwipo só converte erro HTTP/URLError em `BwipoComercialError`, então timeout de leitura/conexão derrubada escapava e matava o request. Agora `POST .../pos/aplicar` devolve `job_id` e um thread (`_job_pos`, um por vez, em memória — 1 worker) faz os PUTs; cada produto tenta até 3 vezes em erro passageiro (timeout, conexão, 429/5xx; 4xx não repete) e qualquer outra falha vira linha de erro sem parar o job. A tela acompanha por `GET .../pos/aplicar/<job_id>?desde=N`. Reiniciar o servidor no meio perde o acompanhamento; rodar de novo é seguro (o PUT é idempotente).
- **Sites Wix:** a tela nova chama as rotas de Pós que já existiam (`/api/atualizar_wix{,2,3}_pos_canais`, casamento por nome + meses em `atualizar_preco_app.py`) com `mapping` = cada campo marcado → o canal escolhido (ou `__SIAA__`). Campos vêm de `/api/listar_campos_wix*_pos`; `mix` vem desmarcado.
- **Não muda:** graduação inteira (CRM, sites, IA, histórico, duplicatas); rotas antigas de Pós.
- **Testes:** `tests/test_bwipo_precos_pos.py`.

### 2026-11-18 — Site eduit.com.br: a duração da planilha também é gravada
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** "Segurança No Trabalho" tem DURAÇÃO 6 na planilha, mas o site seguia mostrando "4 semestres". O "Atualizar este site" do eduit.com.br só gravava o preço.
- **Agora:** `_grad_duracoes_chave` (`routes/atualizar_preco_app.py`) lê a coluna DURAÇÃO (semestres) com o mesmo filtro de canal e a mesma chave do índice de preço dos sites (nome + submodalidade + grau, "Cst Em " no Tecnólogo). `services/site_eduit_precos.py`: `indice_duracao` formata "N semestres"; `planejar` marca troca quando a duração da oferta **ou** do curso difere; `aplicar` grava `curso_ofertas.duracao` e `cursos.duracao` na mesma transação do preço. O log mostra "duração X → Y".
- **Ambíguo:** chave com mais de uma duração na planilha (hoje "Ciências Econômicas 2.0" EAD e "Engenharia Ambiental 2.0" Semipresencial) não muda e volta em `duracao_ambigua`.
- **Simulação 18/11 (`preço corigido.xlsx` × banco do site, só leitura):** mudam 5 durações: Segurança No Trabalho 4→6, Produção Cultural 4→6, Ciência Política EAD 8→6, Física e Matemática Semipresencial 8→6.
- **Não muda:** sites Wix (seguem só preço), IA, CRM, Pós.
- **Testes:** `tests/test_site_eduit_precos.py` (`DuracaoTest`).

### 2026-11-18 — Preços dos cursos: opção "Preço SIAA (sem desconto)" no site e na IA
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** poder subir o preço base da planilha (coluna PREÇO SIAA), sem desconto e sem cota, para os sites e para o agente de IA.
- **Agora (`static/js/precos_bwipo.js`):** os seletores "Canal do site" e "Canal da IA" ganham a opção "Preço SIAA (sem desconto)", valor `__SIAA__`. O backend já tratava esse canal (`SIAA_SENTINEL` em `routes/atualizar_preco_app.py`): `_build_canal_xlsx` (sites Wix 1/2/3 e eduit.com.br), `_build_grad_index_canal` (IA, `documents_precos`) e `atualizar_salesbot_canal` (`cursos_salesbot`) leem todas as linhas e usam o PREÇO SIAA no lugar do VALOR COM DESCONTO. A confirmação e o log dizem "o PREÇO SIAA, sem desconto e sem cota". O padrão continua sendo o Mar Aberto quando a planilha tem.
- **Arredondamento:** igual ao dos canais, inteiro (`_fmt_valor_int` na IA, `int(round())` nos sites). Ex.: 410,50 vira 410.
- **Não muda:** Aplicar no CRM (cotas), histórico, rotas do backend.
- **Testes:** `tests/test_precos_siaa_site_ia.py` (pulado fora do Python 3.14, porque o módulo carrega um `.pyc` 3.14).

### 2026-11-18 — Preços dos cursos: "Cst Em X" e "X (4.0)" não criam produto no CRM; limpeza das duplicatas
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** a subida de 17/11 20:51 (`data (2).xlsx`, snapshot 4) trazia 81 nomes "Cst Em …" e 47 "… (4.0)/(4.0I)/(4.0I Mais)". Como o casamento era por nome exato + grau, o painel criou produtos novos (ex.: "Cst Em Análise E Desenvolvimento De Sistemas" ao lado de "Análise E Desenvolvimento De Sistemas"), e a subida das 21:04 atualizou as cópias, não os originais. O certo é existir só o curso de nome limpo.
- **Agora (`routes/bwipo_precos.py`):** `nome_base()` tira o prefixo "Cst Em " e o sufixo "(4.0…)"/"(Ead)" e junta espaços duplos; `eh_variante()` diz se o nome tinha isso. `parse_workbook` junta as linhas pelo nome base: se a planilha traz o nome limpo, ele manda; senão fica a variante sem sufixo; as demais voltam em `ignorados` (log da tela). O curso passa a se chamar pelo nome base. `_grad_index` só indexa produto de nome limpo (espaços duplos juntados): uma variante do CRM nunca é alvo da subida, mesmo sem o original (senão "X (4.0)" Semipresencial levaria o preço do "X" Digital). Variante da planilha sem produto no CRM é **pulada** (`_acao_item` → "pular"), nunca criada; aparece como "Pular" na pré-visualização e "pulado" no resumo.
- **Limpeza:** cartão "Duplicatas no CRM" na tela. `GET /api/bwipo-precos/duplicatas` só lista. Em "apagar" entra a variante de graduação que tem o original no mesmo grau **e** SKU no formato de `_sku()` (nome sem espaços + EAD/HIBRIDO), a marca dos produtos criados pela subida. À parte: "outros" (tem original, SKU de outro formato, pode ser legítimo) e "sem_base" (sem original); nenhum dos dois é apagado. `POST /api/bwipo-precos/duplicatas/apagar` com `{ids}` relê o CRM e apaga (`DELETE /api/products/{id}`) só os ids que ainda estão em "apagar"; o original nunca entra. Cada apagado vai para o log do servidor com o usuário. Os DELETEs rodam dentro do request (~1–2 min para ~128), como já faz Aplicar no CRM; é ação rara e manual.
- **Histórico:** o snapshot grava o curso pelo nome base; linhas antigas "Cst Em X" do histórico ficam como estão.
- **Não muda:** sites Wix (lá o alias "Cst Em" é o formato certo), eduit.com.br, IA, cotas livres, histórico. Pós não é tocada (só `level = GRADUATION`).
- **Testes:** `tests/test_bwipo_precos_duplicatas.py`.

### 2026-11-17 — Preços dos cursos: cotas livres na subida para o CRM
- **Modelo usado:** Claude Fable 5.1.
- **Pedido:** Balcão Estratégico e Mar Aberto subiam sempre, como chips fixos; as outras cotas entravam pelo +. A planilha nova não tem o APROVADOS e a equipe quer escolher livremente quais cotas sobem, uma ou várias.
- **Agora:** nenhuma cota é obrigatória. Ao carregar a planilha, Estratégico e Mar Aberto vêm marcados **se existirem**, mas são chips removíveis como qualquer outro; o + oferece todos os canais da planilha, global ou por curso. Sem nenhuma cota marcada, Aplicar no CRM para com "Escolha ao menos uma cota". A confirmação e o log listam as cotas que sobem.
- **Backend (`routes/bwipo_precos.py`):** `parse_workbook` não descarta mais curso sem Estratégico — a linha-base é o Estratégico quando existe, senão a primeira cota (o PREÇO SIAA é igual em todos os canais do curso). `aplicar` deixa de injetar `BASE_CANAL`/`APROVADOS_CANAL` e de exigir Estratégico na planilha. `_apply_prices` sobe só as cotas escolhidas (`_rows_para_subir`); o canal principal do produto (`course.channel`/`discountPercent`) é o Estratégico se escolhido, senão a primeira cota escolhida. `pricingOptions` de canais não escolhidos continuam no produto, só com o preço base atualizado (como antes).
- **Não muda:** site e IA (seletores próprios); histórico (`/api/bwipo-precos/painel` segue com Estratégico como canal padrão de consulta).
- **Testes:** `tests/test_bwipo_precos_cotas.py`.

### 2026-11-17 — Preços dos cursos: o site www.eduit.com.br entra no "Atualizar este site"
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** o `www.eduit.com.br` (React + Express, não é Wix) também vende os cursos e ficava de fora da atualização de preço.
- **Agora:** o seletor de sites tem **eduit.com.br**. A rota `POST /api/atualizar_site_eduit_canal` monta o mesmo índice de preço dos sites Wix (`_build_canal_xlsx` + `_wix_grad_price_index`, canal escolhido em `#bp-canal-site`) e chama `services/site_eduit_precos.py`. O serviço grava no Postgres `site_anhanguera`, só na graduação (`cursos.tipo = 'graduacao'`). Casa pela `curso_ofertas.chave`, que tem o formato da `chave1` do Eduit New, com o alias `Cst Em`, e muda `curso_ofertas.valor` e `cursos.preco` juntos (o site lê `cursos.preco`), numa transação só. Conexão: `SITE_EDUIT_DB_HOST`, `SITE_EDUIT_DB_PORT`, `SITE_EDUIT_DB_NAME`, `SITE_EDUIT_DB_USER`, `SITE_EDUIT_DB_PASS`. São variáveis separadas, sem URL, porque a senha tem caracteres que quebram uma URL. Sem o host, o botão responde "Site eduit.com.br não configurado".
- **Pendente:** 8 licenciaturas estão com `curso_ofertas.chave` vazia (física, geografia, filosofia, ciências biológicas, matemática, química, história, letras português e inglês) e aparecem no log como "Sem chave no site" até alguém preencher a chave.
- **Não muda:** as rotas e coleções dos três sites Wix, a IA, Aplicar no CRM, o histórico e a Pós do site (350 ofertas, 70 cursos com mais de uma duração).

### 2026-11-17 — Preços dos cursos: o canal que sobe para os sites também é escolhido na tela
- **Modelo usado:** Claude Fable 5.1.
- **Pedido:** mesmo caso da entrada abaixo (IA), agora nos sites: o botão **Atualizar este site** parava com "A planilha não tem o canal Mar Aberto". A planilha de Graduação de 17/11 traz 9 canais (Balcão Estratégico, Balcão|Comercial 10/15/20%, Captadora, Clube de Benefícios, Educação Corporativa, Esquenta Black, Segunda Grad e Transferência) e nenhum APROVADOS; o site vai usar outro canal.
- **Agora:** ao lado do seletor de site há `#bp-canal-site`, preenchido com os canais da planilha, igual ao `#bp-canal-ia`. Vem marcado o APROVADOS quando existe; sem ele fica vazio e o botão pede a escolha. O `VALOR COM DESCONTO` do canal escolhido vai para o site pelas rotas que já recebiam o canal no form (`/api/atualizar_wix_canal`, `wix2`, `wix3`). A confirmação e o log citam o canal.
- **Não muda:** backend; as cotas fixas no CRM (Balcão Estratégico e Mar Aberto). O canal do site e o da IA são escolhidos separadamente.

### 2026-11-17 — Preços dos cursos: o canal que sobe para a IA é escolhido na tela
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** o canal APROVADOS (Mar Aberto) saiu da planilha de Graduação, e o botão **Atualizar a IA** parava com "A planilha não tem o canal Mar Aberto".
- **Agora:** ao lado do botão há um seletor (`#bp-canal-ia`) com os canais da planilha carregada. Vem marcado o APROVADOS quando existe; se não existir, fica vazio e o botão pede a escolha. O `VALOR COM DESCONTO` do canal escolhido vai para `documents_precos` e `cursos_salesbot` (`/api/atualizar_grad_canal` e `/api/atualizar_salesbot_canal`, que já recebiam o canal no form).
- **Não muda:** os sites continuam usando só o APROVADOS. As cotas fixas no CRM (Balcão Estratégico e Mar Aberto) também não mudam.

### 2026-11-17 — Motor de Ativações: instalar as tabelas pela página (revisa "o SQL só se aplica por script")
- **Modelo usado:** Claude Fable 5.2.
- **Decisão do Marcelo (17/11):** em vez de alguém rodar `scripts/apply_motor_sql.py` no servidor, a página ganha o botão "Instalar o motor" (`POST /api/motor/instalar`).
- **Trilhos:** só `role = admin`; roda o mesmo `sql/motor_ativacoes.sql` (só cria o que falta) pela conexão do próprio painel; registra no diário quem instalou. **Continua não rodando no boot.** Instalar não liga o motor, não inicia worker e não fala com o CRM.
- O script `scripts/apply_motor_sql.py` continua valendo para quem preferir o servidor.
- **PR 131 mergeado em 17/11** (autorizado pelo Marcelo): em produção o motor está desligado e em simulação até existirem as tabelas, o serviço do worker e o token.

### 2026-11-17 — Motor de Ativações: resultado do ciclo (motor × controle), comportamento e previsão, textos dos avisos
- **Modelo usado:** Claude Fable 5.2.
- **Objetivo do Marcelo (17/11):** motor organizado, com avaliação dos KPIs, integrado ao CRM, que meça melhor os resultados, tire o processo da mão da operação, ative mais cedo dosando a fila, melhore os textos e entenda o comportamento para prever.
- **Resultado do ciclo (`motor/resultados.py`, `GET /api/motor/resultados`):** comparação por **intenção de tratar**. De um lado todos os alunos fora do controle (recebendo ou não mensagem), do outro os 5% sorteados. Como o sorteio é por aluno, a diferença é o efeito do motor. **Não comparar "quem recebeu" com "quem não recebeu"**: quem recebe é escolhido pelo estado, e a conta sairia viciada.
  - Medidas: pagou em dia (baixa até o prazo efetivo do dia 10 + folga), pagou até o dia 25, diferença em pontos percentuais com intervalo de 95%, alunos a mais em dia, por estado do aluno ao entrar no ciclo.
  - "Conclusivo" só com o intervalo inteiro acima (ou abaixo) de zero **e** controle com 100 alunos ou mais. Com 5% de controle em ~15 mil alunos, efeito menor que uns 3,5 pontos não dá para afirmar num ciclo só.
  - Por mensagem e variante: entrega, leitura, resposta, pedidos para parar ou reclamações por 1.000, e pagou em dia de quem recebeu (descritivo, serve para comparar variantes, não para medir efeito).
  - Avisos na tela: ciclo ainda aberto, controle pequeno, controle contaminado (aluno do controle que recebeu mensagem), ciclo sem envio real.
- **Comportamento e previsão (`motor/previsao.py`):** dos ciclos fechados do semestre, de cada estado quantos pagaram em dia, depois ou não pagaram no ciclo seguinte; aplica essas taxas aos alunos de hoje (previsão com margem) e compara o que já foi pago com a curva dos ciclos fechados (ritmo). Estado com menos de 30 casos usa a taxa geral; sem ciclo fechado não há previsão. **É leitura: não muda decisão do motor.**
- **Textos (`motor/textos.py`):** 9 propostas (6 mensagens, variantes A/B onde faz sentido) e `avaliar`, que aponta variável faltando, texto começando ou terminando em variável, tamanho, CAIXA ALTA, pontuação repetida, palavra de ameaça fora da mensagem firme, botão longo e botão que o motor não lê sem IA. **Nenhuma proposta está cadastrada na Meta nem no CRM** (`aprovado=False`); o envio real segue usando `MOTOR_TEMPLATE_<MENSAGEM>`.
- **Limite conhecido:** uma campanha por dia = um template, então A e B da mesma mensagem não saem no mesmo dia. Comparar variantes pede alternar por dia ou por ciclo (decisão em aberto).
- **Ambiente local:** `python scripts/motor_local.py demonstrar` põe 6.000 alunos de mentira (`demo-...`) no banco local para ver os blocos; `zerar` apaga.
- **Histórico (`motor/historico.py`, `scripts/motor_importar_historico.py`):** CSV `contact_id,ciclo,baixa_em` (baixa vazia = fechou sem pagar), exportado do histórico do campo "Dia 10" do CRM. Sem `--aplicar` o script só confere e mostra o resumo; com `--aplicar` grava em `motor_aluno_ciclo` com `ON CONFLICT DO NOTHING` (nunca sobrescreve o que o motor registrou). Só entram ciclos anteriores ao corrente; ciclo importado não tem grupo de controle. **O CSV não entra no repositório e a aplicação em produção depende de autorização.** Sem importar, a previsão só aparece depois do primeiro ciclo completo.
- **Base da trava de fila (`MOTOR_FILA_BASE`), decisão do Marcelo em 17/11: `dia`.** A subida da fila é medida desde antes do 1º lote do dia, o dia inteiro, sem expirar (regra da simulação de referência): a fila não pode subir um degrau por lote. `lote` (subida desde o último lote, valendo `MOTOR_FILA_BASE_VALIDADE_MIN`) continua disponível por configuração. O intervalo entre lotes segue em `MOTOR_VIDA_MIN_LOTE_MIN`.

### 2026-11-13 — Motor de Ativações: ambiente local de teste e reprise de rodada real
- **Modelo usado:** Claude Fable 5.2.
- **Pedido do Marcelo (13/11):** "o motor pode ser publicado local para testes" e refazer rodadas reais "simulando exatamente o que aconteceu, mas com disparo mais cedo, dosando por fila", sem nenhuma mensagem real.
- **Ambiente local (`scripts/motor_local.py`, tudo em `.motor-local/`, fora do git):**
  - `python scripts/motor_local.py preparar` — uma vez: cria o ambiente Python (`requirements.txt`), um Postgres próprio em `127.0.0.1:55440` (banco `dcz_sync_local`) e as tabelas `motor_`.
  - **Subir:** configuração `motor-local` do `.claude/launch.json` (ou `python scripts/motor_local.py`). Sobe o Postgres local, o worker em processo próprio (`.motor-local/worker.log`) e o painel em `http://127.0.0.1:5057/`, já logado (`DEV_SKIP_AUTH`), página Acadêmico → Motor de Ativações.
  - **Parar:** parar a configuração (ou Ctrl+C). Se sobrar processo: `python scripts/motor_local.py parar`.
  - **Zerar o banco local:** `python scripts/motor_local.py zerar`.
  - **Travas do script:** o banco é sempre o local (as variáveis de banco do script valem sobre o ambiente e sobre qualquer `.env`); `MOTOR_ENABLED=1` e `MOTOR_DRY_RUN=1` fixos, então a escrita no CRM fica travada; os agendamentos do painel não rodam; só escuta em 127.0.0.1.
  - **Leitura do CRM ao vivo (opcional, só GET):** `EDUIT_CRM_TOKEN` em `.motor-local/env` (modelo: `.env.motor-local.example`). Sem token o worker fica sem fila e sem carga, e o ambiente serve para a reprise.
  - **Limite:** local não recebe o webhook de respostas do CRM (precisaria de endereço público). Resposta de aluno ao vivo não aparece; a página avisa.
- **Reprise (`motor/reprise.py`, `routes/motor_reprise.py`, bloco "Reprise de uma rodada real" na página):** lê `rodada_AAAA-MM-DD.json` da pasta `MOTOR_REPRISE_DIR` (local: `.motor-local/rodadas`). Sem a pasta, o bloco não aparece, então **em produção nada muda**. Não chama o CRM, não lê nem grava no banco, não envia.
  - **Arquivo de rodada:** sem identificador de contato, sem texto e sem nome de atendente. `destinatarios` na ordem real ({`envio_s`, `humano_s`...}), `fila_humana_de_fundo_por_minuto` (chegadas de fundo, 1.440 posições), `atendentes_ativos_por_meia_hora` (48). **Arquivo de rodada não entra no repositório.**
  - **Conta:** relógio simulado minuto a minuto; fila = fundo + alunos da rodada que chegam a atendente conforme o motor envia; equipe real do dia atendendo em ordem de chegada, 7,8 primeiros atendimentos por atendente por hora (calibrado em 10/11). Quem decide é `governador.decidir`, com os limites da tela.
  - **Parâmetros da tela:** início (9h às 10h; nunca depois das 10h), lotes (1º, 2º, demais) ou tamanho pela folga (`governador.tamanho_do_lote`), trava de segurar e de pausar, base da trava, intervalo entre lotes, parcela que a IA segura a mais (0 a 40%), atendimentos por hora, teto do dia.
  - **Padrão da tela = simulação de referência de 13/11:** lotes 300/600/1.200, 15 min entre lotes, +30/+60 sobre o nível de antes do 1º lote do dia, sem teto do dia. Conferido: 10/11 às 10h dá 14 lotes, último envio 18h04, pico 118 (referência: 14, 18h03, 117); 26/10 dá 7 lotes, 11h58, pico 23 (referência: 7, 11h58, 24). Diferença de 1 por arredondamento de minuto.
- **Gráfico da reprise (17/11):** dois painéis com o mesmo eixo de horas, envios acumulados em cima e fila embaixo, motor e dia real (nunca dois eixos no mesmo painel). Toda rodada já começa passando o dia como filme (pedido do Marcelo: "antes era animado, eu via a evolução"): o gráfico é revelado das 8h às 22h em 15 s a 2 min, com relógio, envios, fila e estado do motor (enviando, segurando, pausado). "Mostrar tudo" pula para o fim e "Assistir de novo" repete.
- **Divergência a decidir (a página avisa):** o padrão da reprise não é igual ao código do motor em três pontos. No código, a trava mede a subida desde o **último lote** (valendo 2 h), o intervalo é de **30 min** e o teto é de **4.000 por dia**. Com as regras do código (base "último lote", 30 min, tamanho pela folga), 10/11 dá 14 lotes, último envio 18h10 e pico 109; 26/10 dá 8 lotes, 13h32 e pico 22. Qual das duas vale no piloto é decisão do Marcelo.

### 2026-11-13 — Motor de Ativações: prazo efetivo (dia 10 ou 25 em fim de semana ou feriado vale no primeiro dia útil seguinte)
- **Modelo usado:** Claude Fable 5.2.
- **Decisão do Marcelo (13/11):** "use 26/10 como referência, foi o primeiro dia útil, isso pode acontecer tanto com dia 10, assim como dia 25. O pagto pode ser feito no prox dia util".
- **Uma função só:** `motor.calendario.prazo_efetivo(ciclo, dia, feriados)`. Ninguém mais no motor usa o dia 10 ou o dia 25 "cru". Feriados = nacionais (fixos, Carnaval, Sexta-feira Santa, Corpus Christi) mais os de `MOTOR_FERIADOS`.
- **Onde entra:**
  - corte de "pagou em dia" e corte do dia 25: prazo efetivo + `folga_baixa_du` dias úteis (`estados.corte_dia10`, `estados.corte_dia25`);
  - lembretes: os 3 dias úteis **antes do prazo efetivo** (`agenda.datas_de_lembrete`);
  - recuperação: dias úteis **depois do prazo efetivo do dia 25**, até o dia 30 (`agenda.datas_de_recuperacao`). No dia do prazo ainda é dia de pagar: o ciclo só conta como "sem pagar" a partir do dia seguinte (`estados.historico_com_ciclo_vencido`);
  - texto do aviso: a variável `{{3}}` do template leva o prazo efetivo (`envio.componentes_do_template`). Em outubro/2026 o lembrete do dia 25 diria "26/10".
- **Casos cobertos por teste (`PrazoEfetivoTest`):** 25/10/2026 (domingo) → 26/10; 10/10/2026 (sábado, com feriado em 12/10) → 13/10; 25/12/2026 (Natal) → 28/12; 10/01/2027 (domingo) → 11/01.
- **Efeito no calendário:** outubro/2026 teria lembretes do dia 10 em 7, 8 e 9/10 e do dia 25 em 21, 22 e 23/10, com recuperação de 27 a 30/10. Dezembro/2026: lembretes do dia 25 em 22, 23 e 24/12, recuperação em 29 e 30/12, corte do dia 25 em 31/12. **Novembro/2026 não muda** (10 e 25 são dias úteis): lembretes do dia 25 em 19, 23 e 24/11.
- **Ponto de atenção:** 24/12 e 31/12 contam como dia útil. Se a equipe não atende nesses dias, incluir as datas em `MOTOR_FERIADOS` (o motor então recua os lembretes para 21, 22 e 23/12). Feriado municipal ou bancário fora da lista nacional também só entra por `MOTOR_FERIADOS`.

### 2026-11-13 — Motor de Ativações: envio real vira UMA campanha por dia sobre a etiqueta do dia (substitui o "lote com etiqueta por aluno")
- **Modelo usado:** Claude Fable 5.2.
- **Decisão do Marcelo (13/11, "vamos vamos"):** o motor não etiqueta mais aluno por aluno a cada lote. Esta entrada **substitui o item 7 e os "Trilhos do envio real" da entrada abaixo**, e tira do envio real a ordem por risco e o loteador por lista de contatos (a ordem de envio é a do CRM). Em simulação nada disso muda: o motor continua montando lotes por aluno para mostrar o que consideraria elegível.
- **Público:** a etiqueta do dia, aplicada pela importação diária da equipe. O motor não cria nem aplica essa etiqueta. O nome (ou o id) é informado **na aprovação do plano**: no plano real a página pede a etiqueta e a rota recusa a aprovação sem ela (`motor_planos.etiqueta_dia`).
- **Uma campanha por dia de envio** (`motor/envio.py`): `POST /api/integrations/campaigns` com `filters.tagIds: [etiqueta do dia]`, `filters.excludeTagIds: [...]`, `sendRate: 1` e o primeiro nome por `templateComponents`; confere o rascunho; lança; **dosa com `pause` e `resume`**. Um "lote" passou a ser uma **janela**: retoma, deixa andar N segundos (cerca de N mensagens) e pausa. N vem da mesma conta da folga da equipe, com a rampa de 300, 600 e até 1.200. A janela seguinte só abre quando o governador libera (30 min desde a anterior e fila menos de 30 acima do nível da janela anterior). Cada janela é uma linha de `motor_lotes`; o estado da campanha fica em `motor_planos.campanha` (JSON).
- **Exclusões por `excludeTagIds`:** "Sem lembretes de mensalidade", "Sem mensagens automáticas" (as que existirem no CRM) e a etiqueta do grupo de controle do mês. O filtro depende do `caiovpinheiro/backend_crm1#278`, ainda aberto. **Se a API responder `AUDIENCE_FILTERS_INVALID`, o motor não lança**: a campanha do dia vai para `erro` com o motivo no diário, e não há tentativa sem a exclusão.
- **O motor só etiqueta o grupo de controle:** 5% sorteados, uma vez por mês, com a etiqueta `motor-controle-AAAA-MM` (de 300 a 460 chamadas, dentro do orçamento de 60 por minuto, em vários ticks). Começa depois da aprovação do plano e a campanha só é lançada com o controle inteiro etiquetado (`motor_aluno_ciclo.controle_etiquetado`). **Atenção:** etiquetar dispara o gatilho `tag_added` do CRM em cada um desses contatos.
- **Conferência do público antes de lançar (`conferir_publico`):** pela base de negócios, o motor conta quem está na etiqueta do dia, quem sai pelas etiquetas de exclusão ou pelo controle já etiquetado, e quem receberia sem dever (fora da graduação, fora de Em Curso, sem mensalidade em aberto, negócio em dois níveis, sem telefone, controle sem etiqueta, quem pediu para parar pelo motor e não tem a etiqueta de exclusão). **Se os indevidos passarem de 2% de quem receberia (`MOTOR_PUBLICO_DIFERENCA_MAX`, teto de 10% em código), ou se a etiqueta do dia não tiver ninguém na base, o lançamento é recusado**, com a contagem por motivo no diário. Uma carga nova refaz a conferência. Com isso o filtro de graduação e as regras por aluno deixam de ser aplicados no envio: valem como conferência, não como seleção.
- **Trilho depois de lançar:** o público é fixado no lançamento. Se a campanha tiver mais destinatários do que a conferência previu (folga de 2% ou 10 pessoas), é cancelada e vai para `erro`.
- **Fim do dia:** no fim do expediente, na parada geral, na trava de experiência no nível de encerrar ou ao bater o teto do dia, a campanha é **cancelada** (`cancel`). Os pendentes não recebem, e o motor registra quantos ficaram (`campanha.nao_enviados`). **A campanha nunca atravessa o dia**: quem pagou depois do lançamento ainda receberia ao retomar. Se o worker ficou fora do ar, a primeira coisa que ele faz no dia seguinte é cancelar a campanha antiga.
- **A pausa pega em cerca de 5 s:** cada janela fecha 5 s antes do tamanho pedido (`pausa_atraso_s`). Como o worker anda de 30 em 30 segundos, uma janela pode passar até uns 30 do tamanho pedido; a medição diz quanto saiu de verdade.
- **Livro-razão por aluno:** como o motor não escolhe mais aluno por aluno, `motor_mensagens` em envio real é preenchido **depois**, pela leitura dos destinatários da campanha (quando ela termina e de novo 24 h depois do lançamento; uma página por tick). Pendente de campanha cancelada entra como `nao_enviado` (o CRM marca como FAILED sem hora de envio e sem código de erro) e não conta no limite do aluno nem no teto do dia. Erro 131026 bloqueia o número. Consequência: **o limite de 2 mensagens por aluno por ciclo e a regra de uma por dia não são mais garantidos pelo motor no envio real**; dependem de a etiqueta do dia vir certa.
- **Respostas em dia de campanha:** como o livro-razão por aluno só enche depois, o receptor grava a resposta de quem está na base com a mensalidade em aberto enquanto existir campanha do motor no dia.
- **Plano fecha depois da carga do dia, não por horário:** a carga do campo `dia` é importada à mão entre 10h e 14h (16h51 em 10/11). O motor relê a base a cada 30 min durante o expediente e só monta o plano quando vê pelo menos 50 mudanças no Dia 10 no dia (`MOTOR_CARGA_DIA_MIN_MUDANCAS`; 0 não espera). 8h30 continua sendo só o horário mais cedo. Antes de lançar, a base também é relida a cada 30 min.
- **Uma campanha = um template:** em dia com mais de uma mensagem no calendário (dias do vencimento do dia 10: aviso do calouro, lembrete e reconhecimento) o envio real manda só a de mais elegíveis. Para o piloto de 24/11 isso não pesa (é só o lembrete do dia 25); precisa de decisão antes de dezembro.
- **Quem pediu para parar pelo motor:** o bloqueio fica no banco do motor, não no CRM. Enquanto ninguém aplicar "Sem lembretes de mensalidade" nesses contatos, eles aparecem na conferência como indevidos. São poucos, mas contam no limite de 2%.
- **Tabelas:** `motor_alunos.etiquetas`, `motor_aluno_ciclo.controle_etiquetado`, `motor_cargas.mudancas`, `motor_planos.etiqueta_dia` e `motor_planos.campanha`; saíram de `motor_lotes` as colunas do desenho antigo (etiqueta do lote, etiquetados, leituras).
- **Testado só com CRM de mentira** que envia 1 mensagem por segundo do relógio do teste: `tests/test_motor_envio.py` (uma campanha no dia, só o controle etiquetado, exclusões no filtro, janelas e intervalo de 30 min, pausa pela fila, cancelamento no fim do expediente com os não enviados, campanha de dia anterior, teto do dia, API sem `excludeTagIds`, conferência recusando e reabrindo, público maior que o conferido, 403, passo sem resposta) e a espera pela carga do dia. 177 testes sem rede e sem banco.

### 2026-11-13 — Motor de Ativações: o que falta para o piloto "19/11 observa, 24/11 envia" (rotas de integração, respostas, Jev, trava de experiência, aprovação e envio real travado)
- **Modelo usado:** Claude Fable 5.2.
- **Pedido (Marcelo, 13/11, pela consolidação):** construir o que falta para o piloto do financeiro. Em 19/11 o motor só simula ao lado da fila; em 24/11 saem lotes reais pequenos. Sem merge, sem envio real nesta entrega, nada no código do CRM. Esta entrada **substitui** o que as duas entradas de 12/11 dizem sobre: limite de 180 req/min, fila em `/api/painel/service`, tetos fixos de fila (60/100) e maior espera nas regras, janela 9h–14h, tamanho do lote `÷ 0,04`, dias 8/9 e 22/24, "o envio real não existe" e "`ClassificadorJev` sem chamada".
- **Decisões do Marcelo (13/11):** piloto só com graduação; controle de 5% sorteado a cada mês (como já estava); Jev liberado para classificar respostas mascaradas; agente da Anthropic só em dezembro (piloto "fórmula"); o plano do dia é montado pelo motor e aprovado por ele.
- **1. Cliente das rotas de integração (`motor/crm.py`):** fila por `GET /api/integrations/service-queue` (a hora da leitura é o `asOf` da resposta; o cliente guarda a resposta por 15 s, igual ao cache do CRM). Campanha por `/api/integrations/campaigns` (criar, ler, `launch`, `pause`, `resume`, `cancel`, `stats`, `recipients`). **Orçamento de 60 chamadas por minuto, com teto de 60 em código** (`MOTOR_CRM_RPM` só aperta), em janela deslizante, com 10 reservadas para a prioridade alta: fila, resultado e comandos de campanha passam na frente de etiquetagem, destinatários e carga. No 429 espera o `Retry-After`; se repetir, dobra a cada tentativa. A resposta de destinatários traz nome e telefone: `limpar_destinatario` descarta e fica só com contato, status, código de erro e horários. O tick do worker passou de 90 s para **30 s** (fila lida a cada 30 s = 2 chamadas por minuto).
- **Conferência ao vivo das rotas de integração (13/11, só GET, feita pela sessão de consolidação) e o que mudou por causa dela:**
  - **A fila do CRM não é a fila viva.** `awaitingReply` veio em 90 num dia comum, com 57 conversas paradas havia mais de 1 hora e 8 com agente de IA; `longestWait` veio em 38 horas úteis com `overSla` ligado. Por isso **a maior espera saiu das regras** (continua na tabela da página) e **o teto fixo de fila (60/100) deu lugar à subida desde o lote**: o motor guarda o `awaitingReply` de quando o lote é lançado, **segura o próximo quando a fila sobe 30 acima desse nível e pausa quando sobe 60** (`MOTOR_FILA_SOBE_SEGURA`, `MOTOR_FILA_SOBE_PAUSA`; tetos de 30 e 60 em código, o ambiente só aperta). Sem lote anterior, o número absoluto não segura. O mínimo de atendentes online continua (veio 5 de 23).
  - **Premissa minha:** o nível do lote vale por 2 horas (`MOTOR_FILA_BASE_VALIDADE_MIN`; 87% das respostas de um lote chegam nesse prazo). Depois disso a subida já não é daquele lote, e o próximo mede a partir do nível do momento. Sem isso, uma subida natural da fila à tarde seguraria o motor até o fim do dia. Risco conhecido da regra: como cada lote mede a partir do nível do anterior, a fila pode subir em degraus de até 29; a rever com a leitura de 19/11.
  - **`stats` e `recipients` medem coisas diferentes.** Na campanha de 10/11 o `stats` dizia 0 pendentes e 100% de entrega, com 183 destinatários `PENDING` numa campanha concluída. **O lote é medido pelos destinatários por status** (`recipients?status=X&perPage=1`, lendo o campo `total`: 4 chamadas por minuto por campanha ativa, em prioridade alta); do `stats` ficam só o status da campanha, o total e os respondidos. **`PENDING` em campanha concluída é "não enviado"**: a mensagem fica com status `nao_enviado`, não conta no limite de 2 por ciclo do aluno nem no teto do dia, e o lote registra quantos ficaram sem envio.
  - **O cliente se dosa sozinho.** As respostas normais não trazem `X-RateLimit-*` (só o 429 traz); nada no cliente depende desse cabeçalho. O limite do CRM é de 240 por minuto por token (lido no código); o motor segue em 60. Nenhum teste provoca o 429 de propósito: os testes de 429 usam transporte de mentira.
- **Decisão do Marcelo (13/11) sobre essas três mudanças:** "seguir as recomendações, desde que o motor não seja enviesado pelo produto". A trava de fila relativa e a medição por destinatários ficam no motor; nenhuma rota do CRM muda.
- **Token de papel mínimo e 403:** o token do motor será de um usuário com o mínimo (ver relatórios; ver, criar, enviar e cancelar campanha; ver negócios; ver e editar contatos). Quando o CRM responde 403, o cliente guarda a permissão que faltou (`required`) e **não chama a mesma rota de novo até o worker reiniciar** (`CrmMotor.negadas`). O diário recebe uma entrada com a permissão. No envio real o lote vai para `erro` (se a campanha já estava enviando, o motor tenta pausá-la antes). **Lote em `erro` segura o dia:** nenhum lote real novo sai até alguém conferir. Para voltar: corrigir o papel do usuário do token e reimplantar o worker.
- **2. Receptor de respostas:** `POST /webhooks/motor/respostas` no painel (rota em `routes/motor_ativacoes.py`), para o webhook de integração `message_received` do CRM. **Sem sessão:** `routes/auth.py` libera exatamente esse caminho com POST, e a rota exige a assinatura `X-Eduit-Signature: sha256=<HMAC-SHA256 do corpo>` com `MOTOR_WEBHOOK_SECRET` (sem segredo responde 503; assinatura errada, 401). Confere a organização se `MOTOR_CRM_ORG_ID` estiver definido. A rota não chama serviço externo: botão é classificado na hora; texto livre é mascarado e fica em `motor_respostas.texto_pendente` até o worker classificar (no tick seguinte) e apagar. Só grava resposta de quem recebeu **envio real do motor nos últimos 7 dias**; com `MOTOR_RESPOSTAS_OBSERVAR=1` (dia 19/11) grava também de quem está na base com a mensalidade em aberto. No máximo 3 por aluno por dia; `waMessageId` evita duplicar. A resposta fica ligada ao último envio do contato e ao lote.
- **Limite conhecido do webhook:** o CRM não emite `message_received` quando o robô ou uma automação assume a mensagem, e não reenvia se a entrega falhar. A contagem do motor é **menor** que o total de respostas. O `repliedAt` da leitura de destinatários (2 h e 24 h) cobre o "respondeu ou não"; o webhook cobre o conteúdo.
- **Guardar o texto mascarado por 30 dias (D6, pendente):** atrás de `MOTOR_GUARDAR_TEXTO=1`, **desligado**. Ligado, o texto vai para `texto_mascarado` com `expira_em` e é apagado pelo worker no prazo (teto de 30 dias em código).
- **3. Jev de verdade (`motor/classificador.py`):** `POST https://api.typesafe.ai/v1/systemone`, `model` de `MOTOR_JEV_MODELO` (padrão `jev-latest`), `state` = texto mascarado, uma pergunta `tema` do tipo `choice` com 11 rótulos; lê `answers.tema.choice` e `.confidence`. Chave em `TYPESAFE_API_KEY`, só no serviço do worker. Rótulos novos: `pediu_parar`, `reclamou`, `numero_errado` (e `desconto_valor_prazo`, `outro_assunto`, `sem_conteudo`). Confiança abaixo de 0,7 vai para revisão. Sem chave, ou em qualquer falha (rede, 4xx/5xx, formato), valem as regras por palavra-chave, que por terem confiança 0,6 também vão para revisão. O texto nunca vai para log nem para mensagem de erro. **Formato lido no script do teste de 12/11, não na documentação da TypeSafe; nenhuma chamada ao vivo foi feita nesta entrega.**
- **Mascaramento:** e-mail, toda sequência de 5 ou mais dígitos e nome que vem depois de "meu nome é", "sou o/a", "aqui é", "me chamo". **Limite:** nome solto no meio da frase, sem essas marcas, não é reconhecido (o evento do CRM não traz o nome do contato para comparar).
- **Efeito da resposta no aluno:** "já paguei" tira dos lembretes por 5 dias úteis; `pediu_parar` e o botão "Não quero lembretes" bloqueiam todas as réguas (`motor_alunos.bloqueio_motor`, que a carga não apaga); `numero_errado` bloqueia; `reclamou` suspende a régua até o próximo ciclo. **`pediu_parar`, `reclamou` e `numero_errado` valem mesmo com confiança baixa** (na dúvida, não insistir); os demais só com confiança de 0,7 ou mais.
- **4. Variante e nível:** `motor_lotes.variante` e `motor_mensagens.variante`, padrão `A`. **Só entra quem tem o campo `nivel` igual a "graduacao"** depois de tirar espaços, acento e maiúsculas (`MOTOR_NIVEL_ALVO`; vazio aceita todos). O campo `nivel` passou a ser obrigatório na carga. Contato com negócio em mais de um nível fica de fora. Etiquetas do contato que sempre excluem: **"Sem lembretes de mensalidade"** e **"Sem mensagens automáticas"** (mais as de `MOTOR_TAGS_BLOQUEIO`).
- **5. Trava de experiência (trilho novo, tetos em código):** alunos distintos que pediram para parar ou reclamaram no dia, por 1.000 mensagens entregues (enquanto a entrega não é conhecida, por 1.000 enviadas). **8 por 1.000 com pelo menos 5 casos: segura o próximo lote. 15 por 1.000: encerra o dia** (pausa a campanha em andamento, adia os lotes pendentes, plano vira `encerrado`). O ambiente só aperta esses números. Aparece na página por lote e no dia. Segurou, não volta sozinha no mesmo dia: a taxa só cai com entregas novas, e não há entregas novas.
- **6. Aprovação do plano:** botão "Aprovar plano" na página (`POST /api/motor/plano/aprovar`), grava quem e quando. **Em modo real nenhum lote sai sem o plano aprovado** (`trilhos.validar_lote`, recusa `plano_nao_aprovado`). O plano real e o simulado são linhas separadas; a página mostra o real quando existe.
- **7. Envio real (`motor/envio.py`), travado [SUBSTITUÍDO no mesmo dia pela entrada acima: uma campanha por dia sobre a etiqueta do dia]:** só com as quatro chaves juntas: `MOTOR_ENABLED=1`, `MOTOR_DRY_RUN=0`, motor ligado na página e plano aprovado. Etapas do lote: `etiquetando` (cria a etiqueta `motor-AAAAMMDD-lN-mensagem` e etiqueta os contatos a cerca de 50 por minuto, em vários ticks; um lote de 1.200 leva uns 24 minutos) → campanha TEMPLATE em rascunho com `filters.tagIds`, `sendRate` 1 e `templateComponents` com `{{contact.name | first}}`, mês e prazo → **confere o rascunho** (status `DRAFT` e filtro igual à etiqueta do lote; se não bater, cancela) → `launch` → `enviando` (resultado a cada 1 min, medido pelos destinatários por status) → `concluido`. Pausa na emergência (fila 60 acima do nível do lançamento, falha acima de 6%, trava de experiência, fim do expediente, motor desligado) e retoma quando o governador libera; **parada geral cancela**. Destinatários lidos 2 h e 24 h depois do lançamento, uma página por tick; erro 131026 bloqueia o número. Um lote real por vez; os 30 minutos de vida contam do lançamento.
- **Trilhos do envio real:** campanha com mais destinatários que o lote é cancelada (etiqueta casando onde não devia); lote pausado ou em etiquetagem de dia anterior é cancelado, não retomado (a lista é refeita a cada dia); cada passo que escreve é registrado antes em `motor_acoes`. **Passo que ficou "iniciado" sem resposta não é repetido às cegas:** a etiqueta é reencontrada pelo nome, o lançamento é conferido pelo status da campanha, e a criação da campanha (que não tem como conferir) põe o lote em `erro` à espera de uma pessoa.
- **Canal e template por ambiente:** `MOTOR_CANAL_ID` e `MOTOR_TEMPLATE_<MENSAGEM>` (piloto: `MOTOR_TEMPLATE_LEMBRETE_DIA25=motor_lembrete_dia25_a`). Sem os dois, o lote real é recusado. Variáveis do corpo, nesta ordem: primeiro nome, mês por extenso, prazo (`25/11`).
- **Testado só com CRM de mentira:** nenhum POST ao vivo. `tests/test_motor_envio.py` cobre as quatro chaves, a ordem das chamadas, o público igual ao lote, o orçamento de 60 por minuto, pausa, retomada, cancelamento e os passos incertos.
- **Regras que vieram do simulador (`crm1/docs/simulador-motor-financeiro.html`):**
  - **Tamanho do lote pela folga da equipe** (mesma conta de `tamanhoLote`): folga por hora = atendentes online × 6 − 1,1 × 28 conversas que já chegam sem campanha; conversas = folga × 30 min + metade do espaço que a fila ainda pode subir até o limite de segurar; lote = conversas ÷ 0,025 (91 respostas por 1.000 envios × 27% indo para atendente), de 50 em 50, entre o mínimo de 200 e o teto. Substitui a conta `÷ 0,04`.
  - **Rampa do dia do piloto:** primeiro lote até 300, segundo até 600, depois até 1.200 (`MOTOR_RAMPA`); **teto do dia 4.000** (era 7.000).
  - **Último lote 1 hora antes do fim do expediente** (`MOTOR_CORTE_MIN`, padrão 60). Decisão do Marcelo (13/11): "atendimento vai até as 22h". **Expediente padrão das 9h às 22h (`MOTOR_JANELA_FIM_H`, teto de 22h em código); o último lote sai até as 21h, e nenhum lote novo sai depois das 21h, seja qual for a configuração.** Às 22h o que ainda estiver enviando é pausado.
  - **Público dividido nos 3 dias úteis antes do vencimento** (`MOTOR_DIAS_ANTES_VENCIMENTO`): no primeiro dia um terço, no segundo dois terços (com o que sobrou), no último todos; quem cai em cada dia é sorteio estável por aluno. Novembro: lembrete do dia 25 em **19, 23 e 24/11**; do dia 10 em 5, 6 e 9/11. Substitui "dias 8 e 9" e "dias 22 e 24".
- **Premissa minha (o simulador não define): prioridade por risco na ordem dos lotes.** Risco pelo estado: Primeiro atraso > Não reage > Tardio > Sem histórico > Em dia > Silenciado; calouro pesa mais dentro do mesmo estado. **Dentro do mesmo risco a ordem é sorteada**, então quem fica de fora pelo teto do dia é amostra ao acaso do seu grupo. No piloto ninguém tem histórico importado, logo todos ficam em "Sem histórico" e a ordem é sorteio puro, como a página do piloto descreve; só o calouro sai antes. Se a decisão for outra, muda `motor/seletor.py::risco`.
- **Banco:** `sql/motor_ativacoes.sql` ganhou a tabela `motor_respostas` e colunas novas em `motor_alunos`, `motor_aluno_ciclo`, `motor_planos`, `motor_lotes` e `motor_mensagens`. Como nada foi aplicado em produção, as colunas entraram direto nos `CREATE TABLE`; **quem aplicou a versão de 12/11 em algum banco de teste precisa recriar as tabelas `motor_`.**
- **Variáveis novas do worker:** `TYPESAFE_API_KEY`, `MOTOR_CANAL_ID`, `MOTOR_TEMPLATE_LEMBRETE_DIA25`. **Do painel:** `MOTOR_WEBHOOK_SECRET`, `MOTOR_CRM_ORG_ID`, `MOTOR_RESPOSTAS_OBSERVAR` (só no dia 19). Lista completa no `.env.example`.
- **Cadastro do webhook (não feito):** `POST /api/integration-webhooks` no CRM com `{"url": "https://<painel>/webhooks/motor/respostas", "events": ["message_received"], "name": "Motor de Ativações"}`; o `secret` da resposta vai para `MOTOR_WEBHOOK_SECRET` do painel. É escrita no CRM: fica para o Marcelo ou para quem tiver o token, depois do merge.
- **Testes:** 177 sem rede e sem banco (`python -m unittest discover -s tests -p "test_motor_*.py"`) e 8 de integração contra um Postgres de teste (`MOTOR_TEST_DSN`).
- **Não muda:** o controle de 5% por mês, os estados do aluno, a parada geral, a trava de escrita em simulação, nenhuma outra tela do painel, o `requirements.txt`. Nada entrou no código do CRM.
- **Não coube ou depende de decisão:** conferência ao vivo das rotas de integração e do Jev (falta token e chave); remoção das etiquetas de lote depois do envio (ficam no CRM; seria mais uma chamada por contato); nome do aluno para mascarar texto livre; tela de configuração, variantes disputando e sugestão de texto (dezembro); histórico de setembro a novembro; D6 (guardar texto).

### 2026-11-12 — Motor de Ativações: versão 1 em simulação (pacote `motor/`, worker separado, página no painel)
- **Modelo usado:** Claude Fable 5.2.
- **Pedido:** primeira versão do motor que comanda os avisos de mensalidade da Cruzeiro EaD de fora do CRM, em lotes dosados pela fila de atendimento. Especificação: `crm1/docs/motor-ativacoes-plano.html` (v7). Nesta versão nada é enviado.
- **Onde fica:** pacote `motor/` (Python puro, sem lib nova), tabelas `motor_` no `dcz_sync` (`sql/motor_ativacoes.sql`), página `motor_ativacoes` (Acadêmico → Motor de Ativações; `routes/motor_ativacoes.py`). O laço roda em processo próprio, `python -m motor.worker`, nunca dentro do gunicorn.
- **Entrada segura:** `MOTOR_ENABLED=0` e `MOTOR_DRY_RUN=1` por padrão. Para simular são precisas duas chaves: `MOTOR_ENABLED=1` no serviço do worker **e** "Ligar" na página. **O envio real não existe nesta versão:** com `MOTOR_DRY_RUN=0` o worker avisa no diário e segue em simulação, e o cliente do CRM é criado com a escrita travada (`CrmMotor(permitir_escrita=False)`: POST/PUT/DELETE levantam `EscritaBloqueada` antes de sair para a rede). O envio real depende da F0 (entrada abaixo).
- **Trilhos (código, `motor/trilhos.py` + `motor/config.py`; o piloto não altera):** janela 9h–14h em dia útil (horário de Brasília; feriados nacionais + `MOTOR_FERIADOS`), teto diário 7.000, lote de até 1.200, no máximo 2 mensagens por aluno por ciclo, nunca duas no mesmo dia para o mesmo aluno (também `UNIQUE (contact_id, dia, simulado)` em `motor_mensagens`), uma mensagem por frente por ciclo, mensagem firme uma vez por ciclo, quem declarou pagamento sai por 5 dias úteis, exclusões (situação ≠ "Em Curso", sem telefone, erro 131026, recusou mensagens, grupo de controle), parada geral. As variáveis `MOTOR_*` só apertam: os tetos absolutos estão em `motor/config.py` (lote 1.500, dia 12.000, janela 9–14, 2 mensagens, 240 req/min).
- **Grupo de controle:** `sha256(sal|ciclo|contact_id)` em 10.000 faixas, 5% (`motor/sorteio.py`). Determinístico, refeito a cada ciclo, gravado em `motor_aluno_ciclo.controle` na abertura do ciclo; depois de gravado vale o que está gravado (trocar `MOTOR_CONTROLE_SAL` não muda ciclo aberto).
- **Aluno = contato do CRM** (`contact_id`), porque a mensagem vai para o telefone do contato. Contato com mais de um negócio vira um aluno só (vale o negócio Em Curso e em aberto). O motor não guarda nome, telefone, CPF, RGM nem e-mail.
- **Estados (`motor/estados.py`):** Não reage (2 ciclos seguidos sem pagar ou 2 lembretes sem reação) > Primeiro atraso (último ciclo sem pagar) > Silenciado (pediu, ou 3 ciclos em dia) > Em dia (2 ciclos em dia) > Tardio (último ciclo com baixa depois do corte) > Sem histórico. Atraso vence pedido de silêncio. O histórico recomeça a cada semestre (jan–jun, jul–dez). "Em dia" = baixa até o dia 10 **+ 3 dias úteis** (`MOTOR_FOLGA_BAIXA_DU`), porque a baixa chega D+1 a D+3. Depois do dia 25, ciclo corrente com Dia 10 e Financeira em Sim já conta como ciclo sem pagar (é o que põe o aluno em Primeiro atraso na janela de 26 a 30).
- **Calendário (`motor/agenda.py`):** dias 8 e 9 (aviso do calouro na 1ª mensalidade, lembrete do dia 10 para Tardio e Sem histórico, reconhecimento para Em dia), dias 22 e 24 (lembrete do dia 25 para quem segue em aberto, inclusive Silenciado), dias 26 a 30 (recuperação para Primeiro atraso, firme para Não reage). Dia que cai em fim de semana ou feriado é antecipado para o dia útil anterior. **20/11/2026 é feriado nacional:** o lembrete nominal do dia 22 cai em 19/11, e o piloto "20 e 24/11" do plano vira 19 e 24/11.
- **Governador (`motor/governador.py`):** PAUSAR (fora da janela, fila > 100, maior espera > 60 min, falha > 6% com 50+ enviados, campanha em andamento sem leitura da fila), SEGURAR (fila ≥ 60, menos de 5 atendentes online, lote anterior com menos de 30 min, sem leitura da fila nos últimos 5 min), senão LIBERAR. Tamanho do lote = (atendentes online × 8 − conversas aguardando) ÷ 0,04, limitado ao máximo. **Os limites 60/100/5/60 são provisórios**, à espera da decisão do usuário.
- **Piloto:** interface `motor/piloto.py`. `PilotoFormula` funciona sem chave (solta o próximo lote quando o governador libera). O piloto pode segurar quando o governador libera; não pode liberar quando o governador segura. `PilotoAgente` (Claude Sonnet pela API da Anthropic) é só o ponto de extensão, com a lista fechada `FERRAMENTAS_DO_AGENTE`; **nenhuma chamada à API da Anthropic foi escrita** (falta a chave; ao escrever, carregar a skill `claude-api`). Sem `anthropic` no `requirements.txt` por enquanto.
- **Classificador:** `motor/classificador.py`. Botão é tratado em código. `ClassificadorRegras` (palavra-chave) funciona sem chave. `ClassificadorJev` é ponto de extensão, sem chamada. Todo texto passa por `anonimizar` (e-mail, qualquer sequência de 5+ dígitos, nomes informados) antes de ir a qualquer classificador; confiança < 0,7 vai para revisão.
- **Carga (`motor/carga.py`):** lê a base inteira por `GET /api/deals` (páginas de 1.000) e confere o valor de cada campo aqui, pelo id. Não usa o filtro por campo do CRM porque ele falha aberto. Baixa só quando o campo vem explicitamente "Não", nunca por ausência. Carga com erro não altera nada. O ciclo do mês só abre quando há pelo menos `MOTOR_CARGA_MIN_ABERTOS` (8.000) alunos com Dia 10 = Sim; antes disso os "Sim" ainda são do mês anterior. Nomes dos campos por ambiente: `MOTOR_CAMPO_DIA10` (`dia`), `MOTOR_CAMPO_FINANCEIRA` (`situacaofinanceira`), `MOTOR_CAMPO_SITUACAO` (`situacao`), `MOTOR_CAMPO_CALOURO` (`calouro`), `MOTOR_CAMPO_NIVEL` (`nivel`); **conferir os nomes reais** com `scripts/motor_f0_check.py`.
- **Cliente do CRM (`motor/crm.py`):** usa `services/eduit_crm.request_full` (novo: devolve status e headers). Host `MOTOR_CRM_BASE_URL` (padrão `https://integrations.bwipo.com`, o único que aceita Bearer). 180 req/min em janela deslizante (o CRM corta em 240 e o balde da organização é dividido com n8n e Disparador), repete em 429 respeitando `Retry-After`. `criar_campanha` só aceita público `tagIds` não vazio (filtro vazio ou desconhecido faria o CRM enviar para todos).
- **Um único motor ativo:** `pg_try_advisory_lock(2026111201)` numa conexão dedicada. Segundo worker fica em espera e assume se o primeiro cair.
- **Tabelas:** o SQL **não roda no boot** do painel nem do worker. Aplicar com `python scripts/apply_motor_sql.py` (pede confirmação; só cria `motor_*`). Sem as tabelas a página mostra "não instalado" e o worker espera. **Em produção, aplicar só com autorização do usuário.**
- **Segundo serviço no EasyPanel (ainda não criado):** serviço novo do tipo App, mesma imagem do painel (`ghcr.io/mfpi1209/dcz-crm-sync:latest`), comando `python -m motor.worker`, sem porta e sem domínio, 1 réplica. Variáveis: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME` (as do painel), `EDUIT_CRM_TOKEN`, `MOTOR_ENABLED=0`, `MOTOR_DRY_RUN=1`. Opcionais: `MOTOR_PIPELINE_ID`, `MOTOR_TAGS_BLOQUEIO`, `MOTOR_FERIADOS`, limites `MOTOR_*` (lista no `.env.example`). O webhook do GitHub Actions só reimplanta o painel: o worker é reimplantado à mão ("Implantar"), fora da janela de envio, e é isso que impede um push de parar o laço. Ordem: aplicar o SQL → criar o serviço → conferir "Worker: Ativo" na página → `MOTOR_ENABLED=1` → "Ligar".
- **Ensaio sem a fila real:** `MOTOR_SIM_FILA="aguardando,atendentes"` (ex.: `45,10`) dá uma fila hipotética **só em simulação** e só quando a leitura real falha; cada leitura fica marcada como ensaio.
- **Permissão:** `motor_ativacoes` entrou em `ALL_PAGES` e no grupo Acadêmico da Config. Admin vê sempre; os demais precisam da permissão marcada.
- **Testes:** `python -m unittest discover -s tests -p "test_motor_*.py"` (116 testes, sem rede e sem banco). `tests/test_motor_pg.py` roda contra um Postgres de teste quando `MOTOR_TEST_DSN` aponta para um banco terminado em `_teste`.
- **Não muda:** nenhuma outra tela, o agendador do painel, o `requirements.txt`, a página "Ativações Acadêmicas" (proxy do n8n).
- **Pendente:** envio real (depende da F0), histórico de setembro a novembro (o motor aprende de novembro em diante; para começar com estados prontos é preciso importar a classe de cada ciclo a partir do `deal_events`), bloqueio por erro 131026 (precisa ler os destinatários das campanhas), leitura de respostas (webhook `message_received`), piloto agente, Jev.

### 2026-11-12 — Motor de Ativações — F0: o que a API pública do CRM acadêmico permite (lido no código, `backend_crm1` `origin/main` 5d1352b)
- **Modelo usado:** Claude Fable 5.2.
- **Como foi feito:** leitura do código do backend do CRM. **Nenhuma chamada ao vivo:** o checkout local do painel não tem `.env`, então não havia `EDUIT_CRM_TOKEN`. `python scripts/motor_f0_check.py` (só GET, não imprime token nem dado de aluno) faz a conferência ao vivo quando houver token.
- **Achado principal: com Bearer `eduit_…` não dá para ler a fila pelo painel nem pausar ou acompanhar campanha; criar e lançar só pelo construtor de campanhas.** `GET /api/painel/service`, todas as rotas `/api/campaigns/*` (inclusive pause, resume, cancel, stats e recipients) e `/api/meta/whatsapp/message-templates` usam `withOrgContext` (só cookie de sessão) e respondem 401 a um token válido. O plano supunha o contrário. Com Bearer funcionam: `/api/deals`, `/api/contacts`, `/api/contacts/{id}/tags`, `/api/tags` (GET/POST), `/api/custom-fields`, `/api/conversations` (com `counts=1` devolve contadores por aba: possível aproximação da fila, a validar), `/api/whatsapp-template-configs/approved`, `/api/integration-webhooks` e o construtor de campanhas.
- **Construtor de campanhas (aceita Bearer):** `GET/POST/PATCH /api/campaign-builder/drafts`, `POST …/drafts/{id}/preview` e `POST …/drafts/{id}/launch` usam `authenticateApiRequest`. Todas exigem o recurso `campaign_builder_v2` ligado para a organização (padrão desligado; desligado responde 403 `FEATURE_DISABLED`). `GET /api/campaign-builder/drafts` sem `id` responde 400 `DRAFT_ID_REQUIRED` quando o recurso está ligado, e é assim que `scripts/motor_f0_check.py` confere sem escrever nada. O rascunho (`src/features/campaign-builder/schema.ts`) aceita `name`, `type`, `channelId`, `filters` (`tagIds`, etapa, funil…), `templateName`, `templateLanguage`, `sendRate`, `scheduledAt`. Limites desse caminho: **não tem `templateComponents`** (sem nome por destinatário; o template não pode ter variável), descarta chave de filtro desconhecida (o filtro vira `{}` = todos os contatos com telefone), o lançamento não checa `campaign:send` (só dono do rascunho ou papel ADMIN/MANAGER) e não há pausar, cancelar, stats nem recipients. `motor/crm.py` ainda aponta `criar_campanha` e `lancar_campanha` para `/api/campaigns` (só sessão); a troca fica para quando o caminho for decidido.
- **Token e limite:** Bearer só em `https://integrations.bwipo.com`. O token não tem escopo: age como o usuário que o criou e herda o papel dele. 240 req/min por token e 240 por organização (janela deslizante de 60 s, balde da organização dividido entre todos os tokens). 429 traz `Retry-After` e corpo `{"error":"rate_limit","retryAfterSec":N}`.
- **(a) Fila:** com sessão, `?section=agora` devolve `agora.data = {asOf, awaitingReply, inService, longestWait{ms, overSla, slaMinutes}, agents{online,total}}`. `longestWait.ms` é em horário comercial por padrão. Não há contagem de "SLA estourado" (só o booleano da conversa mais antiga) nem filtro por departamento. `motor.crm.interpretar_fila` já lê esse formato.
- **(b) Negócios:** `GET /api/deals` aceita Bearer. Filtro por campo: `filters={"dealCustomFields":[{"name":"dia","operator":"eq","value":"Sim"}]}`; **falha aberto** (nome de campo inexistente ou JSON inválido devolve tudo). `updatedSince` **não enxerga** mudança de campo personalizado feita por `PUT /deals/{id}/custom-fields`. Paginação por offset, `perPage` até 1.000, sem desempate. `customFields` vem por id. Base de ~20 mil = 20 requisições; ~33 mil = 34.
- **(c) Resultado da campanha (só sessão):** `/stats` traz `totalRecipients, sentCount, deliveredCount, failedCount, readCount, repliedCount, pendingCount` e `failureReasons[{reason,count,code}]`. `/recipients` traz `status`, `errorMessage`, `sentAt/deliveredAt/readAt/repliedAt` por destinatário; o código da Meta vem dentro do texto (`… (code 131026)`) e há filtro `errorCode=131026`.
- **(d) Nome por destinatário:** sim. `templateComponents` aceita `{{contact.name | first}}`, resolvido por destinatário no envio. Campo com acento, espaço ou hífen no nome não casa e vai literal.
- **(e) Permissões:** `campaign:view`, `campaign:create`, `campaign:edit`, `campaign:send`, `campaign:cancel`. Não existe rota para um Bearer consultar as próprias permissões; o 403 traz `required`. Como as rotas `/api/campaigns/*` são só de sessão, não dá para testar com o token hoje; o construtor não checa essas permissões.
- **(f) Etiquetar:** `POST /api/contacts/{id}/tags` aceita Bearer, **uma etiqueta por chamada**, sem rota em lote. Lote de 1.200 = pelo menos 5 min a 240/min (6 min 40 s a 180/min), disputando o balde da organização. **Cada etiqueta nova dispara o gatilho `tag_added`** (automações e webhooks). A importação CSV com etiqueta não dispara, mas é só de sessão e por padrão sobrescreve nome, e-mail e telefone do contato.
- **Riscos que viraram trava no motor:** campanha com `filters` vazio ou com chave desconhecida **envia para todos os contatos com telefone**; `tagIds` casa também etiqueta em negócio do contato; pausar não é imediato (cerca de 5 s, perto de 100 mensagens a 20/s) e pausar durante a montagem do público trunca a campanha; `POST /api/campaign-builder/drafts/{id}/launch` aceita Bearer e lança sem checar `campaign:send` se a flag `campaign_builder_v2` estiver ligada (nesta versão o motor não chama essa rota).
- **Restrição do usuário (12/11): nada específico deste caso de uso entra no código do CRM.** Qualquer mudança lá tem de ser recurso genérico do produto, decidida por quem mantém o CRM. Nenhuma mudança no CRM foi feita nem está em andamento. Ficam dois caminhos, e a escolha é do usuário:
  - **(A) Sem tocar no código do CRM:** público por etiqueta de lote (`POST /api/contacts/{id}/tags`: pelo menos 5 min por 1.200 e dispara `tag_added`), criar e lançar pelo construtor (depende de `campaign_builder_v2` ligado na organização, que é configuração, não código), fila aproximada por `GET /api/conversations?counts=1`, respostas pelo webhook `message_received`. O que esse caminho não tem: pausar campanha em andamento (a pausa de emergência vira "não soltar o próximo lote", e a exposição máxima passa a ser um lote inteiro, o que pede lotes menores e `sendRate` baixo), falha por destinatário e erro 131026, taxa de falha do lote, nome do aluno no template.
  - **(B) Recursos genéricos na API do CRM:** token de API aceito, com a mesma checagem de permissão da sessão, nas rotas que hoje são só de sessão (fila do painel; pausar, retomar, cancelar, stats e recipients de campanha) e público de campanha por lista de contatos. Servem a qualquer integração, não só ao motor. Fora desta entrega.
- **Decisão do usuário (12/11, depois da F0):** o motor usa os recursos de API que já existem; o que faltar vira **rota nova na API pública do CRM**, sem afetar o frontend dos usuários. Na prática: arquivos de rota novos, que o front não chama, reaproveitando os serviços e as checagens de permissão que já existem; as rotas que o front usa hoje (`/api/campaigns/*`, `/api/painel/service`) não mudam. O que falta e vira rota nova: fila de atendimento agora, pausar / retomar / cancelar campanha, resultado da campanha (stats e destinatários com erro) e criação de campanha com `templateComponents`. Público por lista de contatos é o único item que encosta em código compartilhado (resolução do público no dispatch), de forma aditiva; até lá o público do lote é por etiqueta. Esse trabalho é um PR próprio no `backend_crm1`, fora deste PR.
- **Não feito na F0:** criação da campanha de teste em rascunho (pelo construtor seria possível se o recurso estiver ligado, mas é escrita: não foi feita nem foi pedida a confirmação) e a medição ao vivo de etiquetar 1.200 contatos (escreveria em contatos de alunos; o tempo acima vem do limite de requisições).
### 2026-11-16 — Intervalo aleatório e exportação na Conexão SIAA
- **Modelo usado:** Claude Sonnet 5.5.
- **Pedido:** configurar o intervalo aleatório do keepalive e exportar diagnóstico na página Conexão SIAA (contrato `CONTRATO-INTERVALO-DIAGNOSTICO.md` do eduit-insights; o backend é feito à parte).
- **Decisão:** o editor da agenda ganha "Intervalo" Fixo (como antes) ou Aleatório (mín./máx. em minutos, 1–30, mín ≤ máx). Os campos vêm com 5 e 8 só como proposta; o modo exibido é sempre o salvo e o polling nunca o troca. O PUT envia `intervalo_modo`/`intervalo_min_min`/`intervalo_max_min` (sem `intervalo_modo` o corpo é o antigo; no fixo o par só segue se válido, senão null), validado no painel (`_validar_agenda`) e no cliente; início/fim null com agenda desativada e o 409 seguem iguais. Cards mostram modo, configuração, intervalo sorteado e próxima execução agendada, com aviso de que a aleatoriedade só distribui as verificações no tempo (não evita detecção nem aumenta a duração da sessão). Lista branca ampliada (`worker`, `agenda`).
- **Exportação:** `GET /api/conexao-siaa/diagnostico?periodo=6h|24h|3d|7d` (admin; padrão 24h) calcula `desde/ate` em UTC, repassa a `/conexao/diagnostico` (timeout de leitura 30 s) e aplica lista branca estrita da estrutura do contrato: rótulos só `[a-z0-9_]`, datas ISO, texto livre só em `motivo_estado` (descarta o que parece token ou chave=valor), listas limitadas a 5000/500. Devolve download com `Content-Disposition: attachment` e `Cache-Control: no-store`; erros viram mensagem curta sem detalhe. O botão baixa via fetch (GET, sem escrita).
- **Testes:** `python -m unittest tests.test_conexao_siaa tests.test_conexao_siaa_renovacao tests.test_conexao_siaa_renovacao_js tests.test_conexao_siaa_intervalo` (inclui `tests/js/conexao_siaa_intervalo_harness.js`, Node, sem rede). Sem deploy; depende do backend com os campos novos e `/conexao/diagnostico`.

### 2026-11-16 — Renovação automática na página Conexão SIAA
- **Modelo usado:** Claude Sonnet 5.5.
- **Pedido:** controlar e acompanhar a renovação automática das sessões (contrato `CONTRATO-RENOVACAO-SIAA.md` do eduit-insights) na mesma página, só admin.
- **Decisão:** rotas de repasse no mesmo blueprint (`/api/conexao-siaa/renovacao/status|execucoes|config|suspensao|solicitar|liberar`), token só no servidor, lista branca estrita (campos de lista fixa só passam se forem rótulos `[a-z0-9_]`; qualquer outro valor vira null). Erros traduzidos sem detalhe; 409 repassa `versao`/`execucao_ativa`/`pausa_manual`. As rotas do executor (`/renovacao/executor/*`) não existem no painel. Card "Renovação automática" + histórico de execuções paginado, com o mesmo polling do histórico de eventos (recarrega a 1ª página em silêncio, preserva "Carregar mais").
- **Segurança de UX:** o polling (~30 s) só faz GET. "Renovar agora" exige confirmação (credenciais enviadas ao SIAA, sessões substituídas), gera um `pedido_id` por confirmação no cliente, fica desabilitado enquanto envia e enquanto houver execução ativa; após falha ambígua (timeout/502/504) reaproveita o mesmo `pedido_id` por até 2 min (a API é idempotente). "Liberar nova tentativa" só com episódio bloqueado. "Publicação confirmada" e "Sessão validada" são colunas distintas.
- **Suposição:** `liberar` usa `config.versao` como `versao` (o contrato não diz de qual objeto vem).
- **Testes:** `python -m unittest tests.test_conexao_siaa_renovacao tests.test_conexao_siaa_renovacao_js` (o segundo roda `tests/js/conexao_siaa_renovacao_harness.js` com Node, sem rede). Sem deploy; depende da session-api com as rotas `/renovacao/*`.

### 2026-11-16 — Conexão SIAA na pasta TI
- **Modelo usado:** Claude Sonnet 5.5.
- **Pedido:** mostrar no painel o estado real da sessão do SIAA (acadêmico e financeiro), editar a agenda do keepalive e acionar a pausa manual, sem abrir o EasyPanel.
- **Decisão:** página admin-only `conexao_siaa` (grupo TI, ícone `sync_alt`) e blueprint `routes/conexao_siaa.py`, que repassa à session-api (`/conexao/status|historico|agenda|pausa-manual`, contrato em `eduit-insights/docs/CONTRATO-CONEXAO-SIAA.md`) com `SIAA_SESSION_TOKEN` só no servidor. Resposta passa por lista branca de campos; erro da session-api vira mensagem curta, sem URL, token ou corpo cru; 409 de versão é repassado. Registrada em `ALL_PAGES`, `_NAV_ADMIN_ONLY`, `PAGES`/`PAGE_TITLES` e no grupo TI da Config.
- **Regra de exibição:** o estado vem só do campo `estado`; "serviço no ar" ou "cookie armazenado" nunca viram sessão válida. A página lê só o que o keepalive registrou (atualiza a cada ~30 s) e lista as chamadas que não obedecem à agenda (`fora_da_agenda`).
- **Não muda:** nenhuma rota existente; agenda nasce desativada, sem horários padrão. Teste: `python -m unittest tests.test_conexao_siaa`.

### 2026-11-11 — Matérias dos Alunos travado: pausa da session-api por cookie morto, mensagem agora diz o motivo
- **Modelo usado:** Claude Fable 5.1.
- **Sintoma:** Analisar funcionava, Iniciar processamento devolvia 503 "Consulta ao vivo adiada. Exibindo captura de data indisponivel." Cookie do Acadêmico carregado, nenhum job travado.
- **Causa:** `/availability` da session-api respondia `estado=pausa, motivo=http_403`: o Acadêmico tinha morrido (SIAA 302 → `access_denied.jsf`) e o keep-alive do `eduit-insights` tratava isso como pausa controlada global, não como sessão expirada. `mensagem_preservada` não mapeava esse motivo e caía no texto genérico. A correção de raiz está no `eduit-insights` (AGENTS de lá, 2026-11-11): 302+access_denied vira `expirada` por módulo, `/test` roda durante pausa por 403, e salvar cookie novo sonda na hora.
- **Aqui:** `siaa/availability.py` ganha mensagem para `pausa` + `http_403`/`inconclusivo` ("o SIAA recusou a sonda do cookie, atualize em Sessao SIAA"), `expirada` mostra o módulo, e o fallback inclui `estado/motivo`. `/api/materias-alunos/diag` passa a devolver o `/availability` cru para não precisar abrir log do EasyPanel.
- **Operação:** quando aparecer pausa por 403 ou expirada, colar cURL novo em Sessao SIAA e Salvar; com a session-api atualizada o botão Iniciar volta a funcionar na sequência. Com a session-api antiga, Salvar grava mas a pausa só sai na próxima sonda do backoff (até 45 min).

### 2026-11-10 — Lentidão: base do painel grava só o que mudou e o RGM do Kommo vem de uma cópia indexada
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** todo filtro, atualização e carregamento do painel demorava muito.
- **Causa 1:** o job `bwipo_painel_base` (10 min) apagava a tabela inteira, regravava tudo e chamava `clear_painel_response_cache()`, que zera o cache do Dashboard Bwipo e o da Minha Performance (20–45 s por recorte). A cada 10 min, quem abria qualquer recorte pagava a conta inteira, mesmo sem nada ter mudado.
- **Causa 2:** `vw_leads_rgm` abre o `custom_fields_json` de todos os leads do Kommo em cada consulta e só depois filtra pelo RGM. Nenhum índice ajuda. Ela roda na montagem da base, no dono da venda, no Repasse, na Minha Performance, no Supervisor e no Comercial antigo.
- **Agora (base):** `_rebuild_painel_base_locked` compara com a tabela e grava só as linhas novas ou alteradas (`ON CONFLICT DO UPDATE`) e apaga só as que saíram. O job chama com `invalidar="suave"`: sem mudança, os caches ficam como estão; com mudança, `_expirar_caches_painel()` vence as respostas sem apagar, e a próxima leitura devolve a anterior na hora e refaz em segundo plano. Upload, tombamento, depara e sync de agentes continuam "forte" (limpam os caches sempre). Uma montagem "forte" pedida enquanto outra roda fica pendente e roda em seguida; antes ela se perdia.
- **Agora (RGM do Kommo):** `services/leads_rgm.py` cria no `kommo_sync` a materialized view `mv_leads_rgm (lead_id, rgm)` com índice único em `lead_id` e índice em `rgm`. As 15 consultas passam o SQL por `leads_rgm.sql(...)`, que troca `vw_leads_rgm` por `mv_leads_rgm` quando a cópia está pronta; antes disso, voltam para a view.
- **Kommo desativado (mesmo dia, pedido do usuário):** o espelho `kommo_sync` não muda mais. Por padrão (`LEADS_RGM_REFRESH_MIN=0`) a cópia é criada uma vez e reaproveitada no boot, sem refresh periódico. Um job confere a cada 10 min se ela ainda existe e recria se sumir. Um sync manual do Kommo ainda refaz a cópia no fim. Com `LEADS_RGM_REFRESH_MIN > 0` volta o refresh periódico (`REFRESH ... CONCURRENTLY`, sem bloquear leitura), e a cópia vence após `LEADS_RGM_MAX_AGE_MIN` (180) sem refresh.
- **Jobs da API do Kommo desligados (mesmo dia):** `aceite_reconcile` (a cada 10 min) e `responsible_history_daily` (04:30) só falhariam com o Kommo desativado. Agora só são registrados com `KOMMO_API_JOBS=1`. Os botões manuais (sync do Kommo, mini-sync, Leads Parados, importar usuários) ficaram como estavam.
- **O Kommo continua sendo lido:** dono da venda (regra 3, responsável no Kommo, para quem não está tombado), Repasse e conversão até 30/09, e o NOVO do Bwipo herdando o responsável. Tudo isso lê o espelho `kommo_sync`, não a API. **Não apagar o banco `kommo_sync`.**
- **Não muda:** as regras de crédito, o dono da venda, o TTL de 5 min dos caches e a view `vw_leads_rgm` (continua existindo e é a fonte da cópia). Para mudar a view, siga o passo a passo no topo de `sql/vw_leads_rgm.sql` (DROP e CREATE da cópia na mesma transação).
- **A conferir no EasyPanel:** o Upload Comercial grava no Kommo, a menos que `UPLOAD_COMERCIAL_CRM=bwipo` esteja definido (`match_merge_lib.use_bwipo_crm`). Sem isso, com o Kommo desativado, ele deve falhar.
- **Ainda pendente (não feito aqui):** a montagem lê todos os uploads de matriculados desde 2018 (`all_snapshots=True`), sem retenção; `get_conn()` abre uma conexão nova a cada chamada (sem pool); os jobs rodam no mesmo processo das 4 threads do gunicorn.

### 2026-11-09 — Conversão do ranking Bwipo usa os leads do Kommo até 30/09/2026
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** filtrando o ciclo 2026/1, a coluna Conv. do ranking dava 0%. O denominador eram só os leads do Pipeline Principal do Bwipo, que começa em 15/09/2026.
- **Agora:** `_bwipo_leads_criados` soma os leads do Kommo (pipeline **Funil de vendas**) para os dias **até 30/09/2026** e os do Bwipo **de 01/10/2026 em diante**. É o mesmo corte do Repasse. Entre 15 e 30/09 os dois CRMs rodaram juntos, e o Bwipo tem 49.734 negócios importados em 22/09. Por isso esse trecho fica só no Kommo.
- **Consultor do lead do Kommo:** o primeiro dono que **não** é administrador do Kommo, segundo a `lead_responsible_history`. O lead entra sem dono (0) ou num administrador (Admin Sistema, Isabela, TI, Ana Paula), que distribuem. A Isabela repassou 98% dos leads que recebeu da fila, em mediana 15 min depois. Sem troca registrada, vale o responsável atual. Sem consultor nenhum, vai para o Admin Sistema. Consultor excluído é reatribuído (`_consultor_reassign_map`).
- **Tabela pronta:** `kommo_leads_dia (dia, kommo_user_id, n)` no dcz_sync, montada por `services/kommo_leads_dia.py`, 2 min após o boot e todo dia às 04:00 (cerca de 11 s). O request só soma linhas. Sem a tabela, a parte do Kommo fica zerada, como antes.
- **Trade-off conhecido:** o numerador continua com a regra do dono da venda (responsável **atual** no Kommo). Para os consultores ativos, a época e o atual dão quase o mesmo número. Quem teve leads redistribuídos (Bruno, Andreina, Sabrina no 2026/1) fica perto de 0%. O Admin Sistema fica alto (31,9%), porque recebe as vendas sem lead. A alternativa (responsável atual nos dois lados) foi descartada: dava Diogo 87%, Jessyca 33% e Admin com 40 mil leads.
- **Não muda:** o numerador, Aberto e Perdidos (carteira atual do Bwipo) e a conversão de períodos a partir de 01/10/2026.

### 2026-11-09 — Dashboard Bwipo: Ciclo e Turma voltam a preencher De/Até
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** escolher "1º Semestre 2026" no Ciclo não mudava as datas do filtro.
- **Causa:** `bwipo_comercial.js` lia `window._crgmCiclosData` e `window._crgmTurmasData`. No `comercial_rgm.js` elas são `let` de topo, que ficam no escopo global entre scripts mas não em `window`. A busca dava sempre vazio desde a criação da tela (`7137385`).
- **Agora:** `_bwcCiclos()` e `_bwcTurmas()` leem pelo nome. O ciclo preenche De/Até com as datas do cadastro (`ciclos_comercial`). A turma preenche De/Até e o nível.
- **Não muda:** ciclo e turma **não** vão como parâmetro para `/api/bwipo/painel` (nunca foram). O nome do cadastro (`26.1`, `Fevereiro`) não bate com `bwipo_painel_base.ciclo` (`2026/1`, do SIAA) nem com `turma` (lá é o nome do curso). A conta continua por data de matrícula.
- **Atenção:** o cadastro `26.1` vai de 01/01 a 30/06/2026, mas a 1ª matrícula do ciclo SIAA 2026/1 é de 18/11/2025 e as turmas do ciclo começam em 18/11/2025. Existem também `2026/2` (14/05–15/12) e `26.2` (13/05–16/08) duplicados. Ajustar as datas é decisão do usuário.
- **Ajuste feito (mesmo dia, pedido do usuário):** `ciclos_comercial` id 3 (`26.1`) passou a começar em **18/11/2025** (era 01/01/2026). O fim continua em 30/06/2026.
- **2026/2 duplicado (mesmo dia, pedido do usuário):** apagado o `26.2` (id 6, 13/05–16/08/2026, sem turma). Fica o `2026/2` (id 5, 14/05–15/12/2026, com as 10 turmas), agora com descrição "2º Semestre 2026", ano 2026 e semestre 2. Ao ser criado em 06/10, o `26.2` gravou `26.2` como ciclo vigente da Graduação em `ciclo_atual_comercial`. Esse campo é comparado com o ciclo do SIAA (`2026/2`) no recorte padrão e no prefixo dominante do `comercial_rgm.py`, e preenche o ciclo do formulário de matrícula. Voltou para `2026/2`.
- **Regra para ciclo novo:** quem marca "ativo" no "+ Novo ciclo" grava o **nome** do ciclo em `ciclo_atual_comercial`. Então o nome do ciclo vigente tem que estar no formato do SIAA (`2026/1`, `2027/1`), não `26.1`.

### 2026-10-30 — SIAA 403 "Sessão morta" era o WAF barrando User-Agent antigo
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** cURL recém-capturado do Acadêmico dava "Sessão morta (HTTP 403)" no Testar, e Matérias dos Alunos acusava sessão expirada.
- **Causa:** o mesmo cookie, deste PC, responde 200 com User-Agent Chrome/Edge 150 ou 156 e **403** com Chrome/Edge 147. A página de 403 é o "Opss, página não encontrada!" do WAF, com código de suporte. Cookie inválido com UA novo dá 302 para `access_denied.jsf?codigoErro=5`, não 403. Então o 403 é bloqueio de navegador antigo, não sessão morta.
- **Agora:** `siaa_http_client.py` e `materias_alunos_client.py` mandam UA e `sec-ch-ua` do Edge 156, iguais ao cURL do usuário.
- **Servidor de sessão (mesmo dia):** o `SIAA_SESSION_URL` (porta 5056) é o `session-api` do repo `eduit-ead/eduit-insights` (serviço `eduit_insights` no EasyPanel, deploy manual pelo Implantar). Lá o `src/siaa_http_client.py` também passou para Edge 156 (commit `ff3d62d`), e o `/session/<module>/test` voltou a responder `alive:true`. Por algumas horas o `POST /siaa-sessao/testar` testou direto no SIAA; voltou a chamar o `/test` da porta 5056, como antes.
- Se o WAF subir o corte de novo, trocar a versão aqui (`siaa_http_client.py`, `materias_alunos_client.py`) e no `eduit-insights`.

### 2026-10-26 — Sync Bwipo não remonta o depara dos leads com o Kommo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** a incremental parava em "Montando depara Kommo → Bwipo" (cerca de 51 mil casamentos). O Kommo não é mais o CRM do dia. Tirar esse passo da sync do botão, para ela ficar mais rápida.
- **Agora:** `_run_sync` não chama `_rebuild_depara`. Contatos, negócios, etapas, responsável e origem continuam vindo da API do Bwipo. O mapa dos consultores (`_rebuild_user_depara`) continua, porque meta e crédito usam o `kommo_user_id`.
- **Não muda:** a tabela `bwipo_kommo_depara` e a busca manual na tela de Sync. Elas ficam com o último resultado. Dono da venda e origem do negócio não liam essa tabela.

### 2026-10-19 — Matrícula no Bwipo que nunca entrou no SIAA conta como evasão
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** RGM 50574191 (Luciana, card #87480, matrícula 13/10) foi matriculada no polo errado ou se transferiu no mesmo dia. Nunca apareceu no relatório, então não entrava nem como venda nem como evasão.
- **Agora:** Ganho no Bwipo com RGM e data no recorte, e esse RGM nunca apareceu em nenhum relatório de matriculados, entra na evasão como TRANSFERIDO, no consultor da venda. Não soma matrícula. No período 01–18/10 são 6 RGMs; a Luciana (50574191 e o outro card 50574663) fica na evasão da Sabrina, que continua com 72 vendas.

### 2026-10-19 — Atualizar lead do Dashboard Bwipo grava no Bwipo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** o painel "Atualizar lead" aberto no Dashboard Comercial Bwipo ainda buscava o lead no Kommo.
- **Agora:** o botão abre o painel do Bwipo. Número do card ou RGM de 8 dígitos chama `POST /api/bwipo/painel/mini-sync`, que lê o negócio na API e grava no espelho. Vários cards com o mesmo RGM pedem a escolha. Ganho com responsável que tem depara continua fixando a venda. ID de lead Kommo nesse botão não sincroniza mais.

### 2026-10-13 — Preços dos cursos no Bwipo: snapshot + PUT do produto
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** a planilha de update (CURSO, GRAU, CANAL, preço base, desconto, valor final) atualiza o produto no CRM comercial e guarda histórico para o painel comparar (mais barato, mais caro, menor preço em 6 meses).
- **Chave:** curso + grau, depois de eliminar a repetição por polo. Graduação EAD deste arquivo = 143 produtos. Digital → `EAD`, Semipresencial → `HYBRID`.
- **Cotas fixas:** `BALCÃO ESTRATÉGICO` gravada como **Balcão Estratégico**, e `APROVADOS` gravada como **Mar Aberto** (casa com a cota que já existe com esse nome). As duas sobem sempre. Outra bolsa entra pelo `+` de todos os cursos ou pelo `+` de um curso só. Canais de fora mantêm o desconto e recebem o preço base novo. Preço do produto = PREÇO SIAA. Desconto = percentual do 1º semestre.
- **Não mexe:** grade curricular, imagem, descrição, vínculo Meta. Produto que não existe é criado sem imagem e sem grade.
- **Dois uploads (14/10):** a página tem lugar para Graduação e Pós. O que funciona (CRM, sites, histórico, cotas) é só Graduação. A Pós aceita o arquivo na tela e não envia: a leitura espera uma amostra do arquivo de preço.
- **API:** `GET/PUT/POST https://integrations.bwipo.com/api/products` com `BWIPO_COMERCIAL_API_TOKEN`. Página `precos_bwipo`.
- **Sites (14/10, seletor em 15/10, Mar Aberto em 15/10):** um seletor escolhe Site Cruzeiro, Eduit-ead ou Eduit New, e **Atualizar este site** manda o `VALOR COM DESCONTO` do canal Aprovados (Mar Aberto) só para o escolhido. Troca o seletor e atualiza o próximo. O log lista curso a curso o CRM e o site que rodou. Não grava snapshot junto com o site.
- **IA no Postgres (14/10, botão em 15/10, salesbot e Mar Aberto em 15/10):** `documents_precos` e `documents_corporativa` também existem no `dcz_sync` (`id`, `content`, `metadata`, `updated_at`). A primeira leitura copia o catálogo do Supabase. **Atualizar a IA** na página de preços manda o `VALOR COM DESCONTO` do canal Aprovados (Mar Aberto) da planilha de Graduação: casa o curso que já existe e troca o preço no Postgres e no Supabase. O log lista cada troca. O fluxo n8n do agente (`robocsv`) lê `cursos_salesbot` (`curso_sinonimo`, `Curso`, `Preço eduit 1` e as colunas de tipo/grau/semestre). Essa tabela foi copiada para o `dcz_sync` (225 linhas, chave `curso_sinonimo`) e o mesmo botão atualiza `Preço eduit 1` nos dois bancos. A busca por nome (`cursos_salesbot_nome`, embedding 1536) continua no Supabase: este Postgres não tem a extensão `vector`. Pós da IA espera a amostra do arquivo.

### 2026-10-08 — Subir Blog: editor cola limpo, H1/H2, desfazer, fonte, rascunho e capa opcional
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** o time de conteúdo não conseguia tirar a formatação do Ctrl+V (link visual que não abre), não tinha H1/H2, a barra sumia no scroll, post novo vinha com linhas vazias, publicar exigia capa, Ctrl+Z não valia nos tópicos, faltavam fonte, rascunho, Ctrl+K e o atalho no tooltip.
- **Colar:** o editor descarta estilo de Word/Docs. Mantém negrito, itálico, sublinhado, H1–H3, listas, citação e link com `http`, `https` ou `mailto`. Link sem endereço, ou redirect do Google, deixa de parecer link. Tamanho de fonte feito na própria barra (`data-sb-size`) sobrevive ao colar.
- **Barra:** gruda no topo enquanto o campo de conteúdo está na tela. Tooltips trazem o atalho (Ctrl+B, Ctrl+K, Ctrl+Alt+1, Ctrl+Alt+2, Ctrl+Shift+8, Ctrl+Shift+7, Ctrl+[, Ctrl+], Ctrl+Z, Ctrl+Y).
- **Desfazer:** pilha própria, porque tópico e citação mexem no HTML na mão e o Ctrl+Z do navegador não via essa edição. Vale o botão e o Ctrl+Z / Ctrl+Y dentro do texto, inclusive no tópico.
- **Linhas vazias:** sequência de parágrafos vazios vira uma quebra, na hora de colar, ao abrir o post e ao salvar (também no servidor).
- **Capa:** publicar e agendar aceitam post sem imagem (`""`). Se a URL vier preenchida, a validação https continua. A capa entra depois, editando o post.
- **Rascunho:** fica no `localStorage` deste navegador (autosave e o botão Salvar rascunho). Não é tabela nova. Publicar ou agendar apaga o autosave e o rascunho que estava aberto.
- **Não muda:** permissão `subir_blog`; admin e `mikami@eduit.com.br` entram sem o checkbox.

### 2026-10-06 — Repasse: Kommo até 30/09, owner_id do Bwipo de 01/10 em diante
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** venda anterior a 01/10/2026 continua no responsável do Kommo. Daqui em diante o valor do repasse vai para o `owner_id` do negócio no CRM novo. Sem cruzar os dois CRMs na mesma venda.
- **Corte:** `data_matricula` na `bwipo_painel_base`. Antes de 01/10/2026, ou RGM que não está nessa tabela, o dono é o responsável do lead no Kommo (id mais alto, como já era). De 01/10/2026 em diante o dono é o `owner_id` do negócio no Pipeline Principal (`bwipo_deals`). Sem `owner_id`, a venda nova não cai no Kommo.
- **Card:** venda antiga agrupa pelo `kommo_user_id`. Venda nova agrupa pelo `owner_id`, com o nome do dono no Bwipo. O depara só serve para o consultor logado enxergar o card novo que é dele.
- **Planilha e CSV do admin:** só `comercial_recebimentos` (as 22.921). `comercial_pagamentos` é a cópia de 20/03 já contida nessa base; juntar as duas gerava as 37 mil.
- **Não muda:** taxa (padrão 20%), card do consultor ainda lê `comercial_pagamentos` + `comercial_recebimentos`, filtro de mensalidade, ciclo e turma.

### 2026-10-06 — Migração comercial Kommo → Bwipo: estado vigente
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** fechar o que já passou para o Bwipo e o que continua no Kommo, para a próxima sessão não refazer a troca.
- **Acessos (só o nome da variável; valor fica no `.env` e no EasyPanel):** painel `https://banco-dcz-crm-sync.6tqx2r.easypanel.host/` (deploy pelo push na `master`). Banco `dcz_sync` = `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASS`. Espelho Kommo = banco `kommo_sync`, mesmo host, `KOMMO_PG_*` (se vazio, usa `DB_*`). API Kommo `https://admamoeduitcombr.kommo.com` = `KOMMO_TOKEN`. CRM comercial Bwipo: site `https://comercialcruzeiro.bwipo.com`, API `https://integrations.bwipo.com`, token `BWIPO_COMERCIAL_API_TOKEN` (org Comercial Cruzeiro). CRM acadêmico (`cruzeiro-ead.bwipo.com` / `crm.eduit.com.br`, `EDUIT_CRM_TOKEN`) não entra neste funil. Matriculados do Disparador = banco `disparos` (`DISPAROS_DB_NAME`). Disparador WhatsApp = `WHATSAPP_TOOL_BASE_URL` e `WHATSAPP_TOOL_API_KEY`. `GET /api/users` do Bwipo responde 401; o dono vem do card.
- **Já no Bwipo** (detalhe nas entradas de 18/09 a 05/10): menu só o Dashboard Comercial Bwipo; venda = matrícula SIAA (sumido vira transferido); dono = manual > Bwipo se tombado e a matrícula é da data dele em diante > Kommo > Bwipo > Admin Sistema (`8261837`). Tombado: só o Hugo, desde 29/09. Diego sem id no Bwipo fica sem responsável. Leitura em `bwipo_painel_base`, resposta 5 min. Conversão = matrícula do período / leads criados no Pipeline Principal no período. Cards de 6 meses e 1 ano = a mesma janela, deslocada. **No topo da página ficou só o YTD.** Funil da home e aceite da Minha Performance = Pipeline Principal. Matrículas da Minha Performance = as do painel. Nome do aluno = Nome Completo; ciclo = ciclo vigente. Conflito = 2+ negócios Bwipo no Pipeline Principal, mesmo RGM, data dentro do filtro (Kommo fora). **Lista do consultor no formato antigo, com busca de RGM. Solicitações de ajuste continuam no canto, com o número de pendentes.** Lead novo herda o responsável do lead dessa pessoa no Kommo; sem esse lead, Admin Sistema.
- **Não mudou:** Sync do Kommo (histórico e espelho). Repasse. Regra de quem leva a venda, meta, funil de etapas e ticket. CRM acadêmico, Disparador e Match & Merge ficaram fora desta troca. A página e as APIs do painel antigo continuam no código.

### 2026-10-05 — Subir Blog entra na lista de permissões da Config
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** em Config, a tela `#subir_blog` não aparecia nas permissões, então não dava para liberar a visualização de um usuário cadastrado.
- **Lista:** checkbox **Subir Blog** no grupo Ferramentas (novo usuário e editar usuário). A permissão `subir_blog` já existia em `ALL_PAGES`; o modal escondia o slug.
- **Acesso:** admin e o login extra (`mikami@eduit.com.br`) continuam entrando sem o checkbox. Os demais entram quando a permissão está marcada — menu, hash e a API do blog.
- **Não muda:** presets de categoria; o restante das telas.

### 2026-10-05 — Vendas em conflito: só o Bwipo, 2 negócios com o mesmo RGM
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** o card cruzava consultor em Ganho entre Kommo e Bwipo e ficava em 0. O Kommo não entra mais.
- **Regra:** conflito = 2 ou mais negócios do Pipeline Principal no Bwipo com o mesmo RGM e data de matrícula dentro do De/Até. Negócio de outra data não entra na lista. Fixar a venda continua na mesma decisão manual.
- **Não muda:** Consultar RGM ainda mostra Kommo e Bwipo. Duplicatas segue a mesma conta de negócios repetidos.

### 2026-10-05 — Dashboard Comercial Bwipo: tabela pronta + cache de 5 min
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** a abertura do painel fazia a varredura de todos os relatórios SIAA na hora (~18s) mais o dono (~4s). Fazer o cache da resposta e uma tabela pronta.
- **Tabela** `bwipo_painel_base`: uma linha por RGM, com situação (sumido vira TRANSFERIDO), data, polo, nível, ciclo, turma e dono da regra `_atribuir_rgms`. A página filtra essa tabela. O prefixo dominante do ciclo fica em `bwipo_painel_meta`, gravado na mesma montagem. A conta continua em todos os snapshots, não no arquivo mais recente.
- **Quando remonta:** upload de matriculados, tombamento, depara de consultor, sync de agentes, e um job a cada 10 min. Fixar ou desfazer uma venda atualiza só aquele RGM. Meta e "contar venda" só limpam o cache, porque entram na leitura.
- **Cache:** a resposta de `GET /api/bwipo/painel` vale 5 min. Pedido repetido devolve a anterior e refaz em segundo plano. Minha Performance lê a mesma tabela.
- **Não muda:** a regra do dono, a meta, o funil e o ticket. A primeira montagem depois do deploy ainda leva o tempo da varredura; o job dispara 30s após o boot.
- **Conversão do ranking:** matrículas do período / leads do Pipeline Principal criados no mesmo período (dia em BRT). A conta matrícula / (matrícula + evasão) deixava quase todo mundo em 100%.
- **Cards vs 6 meses e vs 1 ano:** o mesmo intervalo de datas, deslocado. O número é a matrícula bruta daquele passado; o percentual compara com o período da tela. Servem de norte da meta. A tabela do painel guarda o histórico dos relatórios (não só o ciclo atual). Período que não está em nenhum relatório cai em `comercial_rgm`. Não são a carteira aberta/perdida do funil.

### 2026-10-05 — Menu: só o Dashboard Comercial Bwipo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** ocultar o Dashboard Comercial antigo e deixar só o Bwipo.
- **Menu:** o link `comercial_rgm` saiu da sidebar. Quem abre `#comercial_rgm` cai em `bwipo_comercial`.
- **Não muda:** a página e as APIs do painel antigo continuam no código (Minha Performance e o Bwipo ainda leem essas funções). A permissão `comercial_rgm` continua existindo.

### 2026-10-05 — Formulário de matrícula: Nome Completo e ciclo do painel
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** o nome do aluno no sync Bwipo tem que vir do campo Nome Completo, e o ciclo preenche sozinho com o ciclo vigente do painel.
- **Nome:** `nome_completo` do painel do negócio (`dealPanelFields`). Título `Lead +telefone` não entra no campo.
- **Ciclo:** `ciclo_atual_comercial` do nível (Graduação / Pós-Graduação). Hoje os dois estão em `2026/2`. O consultor ainda pode editar antes de salvar.
- **Não muda:** o crédito da venda; o ciclo do aluno no SIAA.

### 2026-10-03 — Aceite preso sai no Processar do Novo CRM
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** Aceite só fica com quem fechou hoje. Matrícula de outro dia não pode continuar parada; cancelado/trancado/transferido vai para Perdido.
- **Causa:** o Novo CRM só gera MATRICULADO com `data_matricula >= data_corte`. Quem ficou em Aceite no dia 30/09 não entrou no Processar de 02/10 (corte 01/10).
- **Decisão:** depois do D-1, um passe só nos negócios que **ainda estão em Aceite**. Janela de 14 dias, mesmos polos e tipos do D-1. Matrícula ativa anterior a hoje → MATRICULADO (Ganho). Só Cancelado/Trancado/Transferido, sem matrícula ativa → MOVER_PERDIDO. Quem o SIAA marca como matrícula de hoje, ou quem não casa, permanece em Aceite. A janela de 7 dias do Kommo não volta.
- **Não muda:** Kommo; D-1 do card Matriculados; órfão 60d desligado no Novo CRM.

### 2026-10-02 — Perdido não come matrícula ativa no mesmo CPF
- **Modelo usado:** Cursor Grok 4.7.
- **Caso:** Manoela dos Santos Silva (`48505313852`) matriculou em 01/10 (RGM `50506846`) e o Aceite foi para MOVER_PERDIDO. O CPF tinha outra linha Cancelado (Psicologia Hospitalar, 31/08).
- **Causa:** `cpfs_cancelado_transferido` marcava o CPF inteiro se qualquer linha de `mm_cruzado` fosse Cancelado/Transferido. A matrícula ativa de hoje não impedia o Perdido.
- **Decisão:** o CPF só entra nessa lista se **não** houver nenhuma linha `Matriculado` em `mm_matriculados`. Quem tem matrícula ativa segue para MATRICULADO (negócio existe) ou NOVO em Ganho (não existe). Se o D-1 gerar MATRICULADO no mesmo lead, o MOVER_PERDIDO daquele lead sai.
- **Não muda:** Perdido de duplicata quando já existe Ganho com o mesmo RGM; quem está só Cancelado/Transferido, sem matrícula ativa, continua indo para Perdido.

### 2026-10-02 — Minha Performance: meta diária e aceites no Bwipo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** ranking, calendário, sequência e meta diária passam a olhar o Aceite no Bwipo, da própria pessoa.
- **Regra:** `_bwipo_aceites` lê `bwipo_deals` na etapa Aceite do Pipeline Principal. O dono entra pelo depara `bwipo_kommo_user_depara`. A fila é o estoque atual. O dia do calendário e da meta é a data de atualização do negócio (BRT). Matrícula do período continua a do painel Bwipo; a meta do dia soma matrícula + esses aceites.
- **Não muda:** faixas de PIX (se o dia não tem faixa, a tela segue "sem meta"); Repasse.
- **Crédito guardado no processo (02/10):** `_bwipo_periodo_credito` leva 24–43s (SIAA oficial + `_atribuir_rgms`). Antes, cada abertura da tela refazia isso em vários pedidos e as 4 threads do gunicorn travavam. Agora: 1 conta por período por vez, resultado vale 5 min; vencido, devolve o anterior e refaz em background. Job `mp_bwipo_periodo_warm` (4 min) refaz o recorte sem data e os períodos abertos nos últimos 30 min.

### 2026-10-02 — Funil da home lê o Bwipo ao vivo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** o Funil de Vendas do Dashboard deixa o token do Kommo e mostra o estoque do CRM comercial Bwipo.
- **Leitura:** `dashboard_funnel_live` conta o Pipeline Principal com `GET /api/deals?stageId=`. A fila segue o Kanban: Sem Resposta, Em Atendimento, Aguardando Resposta, Aguardando Inscrição, Inscrição, Processo Seletivo, Em Processo, Aprovado/Reprovado, Boleto Enviado, Pagamento Confirmado, Aceite. Ganho e Perdido ficam fora do total de ativos. Sem Resposta não é a coluna Perdidos.
- **Novos hoje:** negócios criados no dia, filtrando `createdAt` nas páginas de `updatedSince` (a API ignora `createdSince`). **Ganho ontem:** `status=WON` com `closedAt` no dia. **Leads ontem:** espelho `bwipo_deals`, porque a listagem não filtra data de criação e o dia anterior passa de 10 mil atualizações.
- **Cache:** `_FUNNEL_API_VERSION` 6→8. Se a API falhar, o funil cai no espelho Bwipo. O job de aquecimento não pega mais o lock do Kommo.
- **Pedido HTTP não varre o SIAA.** Ganho de ontem zerado no espelho fica 0. A home não chama `/api/comercial-rgm/data/kpis` para completar esse card: essa varredura segurava as 4 threads do gunicorn (timeout 600s) e o login parava de responder.
- **Não muda:** o arquivo de matriculados no rodapé do Dashboard; Sync Kommo; aceite da Minha Performance.

### 2026-10-02 — Minha Performance conta as mesmas vendas do Dashboard Comercial Bwipo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** a lista e a contagem da Minha Performance passam a ser as vendas do painel Bwipo, como antes eram as do painel Kommo.
- **Crédito:** `_bwipo_periodo_credito` em `routes/minha_performance.py` usa o SIAA oficial (`_crgm_periodo_data_oficial`, sumido vira TRANSFERIDO) e o dono de `_atribuir_rgms` (manual > Bwipo se o consultor está tombado e a matrícula é da data dele em diante > Kommo > Bwipo > Admin Sistema). Entra na meta quem está em curso e passa no padrão de RGM, igual ao ranking do painel.
- **Medido 01/10:** 55 em curso no SIAA; 4 com a Gabriela no Bwipo e 0 no Kommo. A regra do painel já entrega essas 4 para ela.
- **Não muda:** fila de aceite da Minha Performance continua a etapa Aceite do Kommo; Repasse não entrou nesta troca. Se a leitura do painel falhar, a lista cai no responsável do Kommo.

### 2026-10-01 — Novo CRM: NOVO herda o responsável do lead no Kommo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** lead criado no Bwipo pelo Upload Comercial não pode cair todo em Admin Sistema. O responsável tem que ser o mesmo do lead dessa pessoa no Kommo (ex.: Sabrina).
- **Decisão:** no `NOVO` de `executar_acoes_bwipo`, o `ownerId` vem do `responsible_user_id` do funil Kommo (CPF, pipeline 5481944, lead ativo antes de 142/143). O nome casa com o usuário do Bwipo lido nos negócios recentes (`GET /api/users` continua 401). Sem lead no Kommo ou sem nome único no Bwipo, permanece Admin Sistema. Kommo em si não muda (NOVO lá segue `KOMMO_NOVO_RESPONSIBLE_UID`).
- **Log:** a execução no Novo CRM grava na tela só a linha final (OK/SKIP/ERRO), como o Kommo. A linha "…" de início saiu.
- **Não muda:** Origem=SIAA só no NOVO; etapa do NOVO (PS / Aprovado / Ganho).

### 2026-09-30 — Novo CRM: MATRICULADO D-1 sem janela de 7 dias
- **Modelo usado:** Cursor Grok 4.6.
- **Pedido:** Processar no Novo CRM (Bwipo) com as mesmas regras do Kommo, inclusive D-1, mas **só daqui pra frente** — não listar 300+ Matriculados (backlog de Aceite dos últimos 7 dias).
- **Causa:** o bloco MATRICULADO em `gerar_acoes` varria `data_matricula >= data_corte - 7 days`. No Kommo isso não infla o card (a maioria já está em 142). No Bwipo os mesmos RGMs ainda estão em Aceite → o Processar vira catch-up da semana.
- **Decisão:** `use_bwipo_crm()` usa `data_matricula >= data_corte` (D-1; segunda = D-2 sábado). Kommo **não muda** (continua 7 dias). Órfão 60d e Opção A já estavam desligados no Novo CRM.
- **Não muda:** tipo/polo do D-1; DATA_CORTE_NOVO_OVERRIDE só no Kommo; Executar/replay.

### 2026-09-29 — Fila de chamados: fases do Kanban
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** renomear as fases do Kanban e incluir Novo Ticket, A Fazer e Pausado.
- **Fases:** Novo Ticket → A Fazer → Em Produção → Pausado → Concluído. Abertura grava **Novo Ticket**. **Abertos** = as quatro que não são Concluído.
- **Migração no boot:** `Pendente` → `Novo Ticket`, `Em andamento` → `Em Produção`. O histórico em `ti_chamado_evento` fica com o nome da época.
- **Não muda:** arraste continua no mesmo PATCH de status; Lista, filtros e Meus chamados usam os mesmos nomes.

### 2026-09-25 — Ranking do Dashboard Comercial conta só EM CURSO
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** o ranking (MATR. PER.) não pode incluir evasão. Só quem está em curso no último relatório.
- **O que saiu:** o bloco de recuperação em `_build_agent_ranking_completa_vw` (`routes/comercial_rgm.py`) que recolocava RGM cancelado no arquivo atual quando ele tinha estado EM CURSO em algum upload desde o dia 01 do mês. No recorte 14–30/09 isso somava 24 matrículas (Rahi 125→123, Tamires 106→104, Gabriela 105→102, e 1–3 nos demais). Esses 24 estão CANCELADO no último SIAA e seguem na evasão.
- **Cache:** `_CRGM_DATA_CACHE_VER` 7→8.
- **Não muda:** card de evasão; Minha Performance e Repasse; painel Bwipo (já contava só em curso).

### 2026-09-18 — Sync comercial Bwipo (org Comercial Cruzeiro) + depara Kommo
- **Modelo usado:** Cursor Grok 4.6.
- **Pedido:** sair do Kommo para o Bwipo; começar pelo Sync (espelho para o Dashboard Comercial / Minha Performance) pegando os mesmos campos de hoje (ID, nome, RGM, data de matrícula, etc.) e um depara Kommo→Bwipo para histórico. Leads antigos ainda não foram migrados — o CRM comercial está sendo montado.
- **Conta certa:** org **Comercial Cruzeiro** em `https://comercialcruzeiro.bwipo.com`, API `https://integrations.bwipo.com`. Token `BWIPO_COMERCIAL_API_TOKEN`. **Não** usar o token/host do CRM acadêmico (`cruzeiro-ead.bwipo.com` / Disparador / `EDUIT_CRM_*`).
- **Pipeline Principal** (espelho do funil Kommo): Lead de Entrada → … → Aceite → Ganho (`isWon`) / Perdido (`isLost`). Pipeline **Ativações** não entra no crédito comercial.
- **Espelho no `dcz_sync`:** `bwipo_pipelines`, `bwipo_stages`, `bwipo_users`, `bwipo_contacts`, `bwipo_deals`, `bwipo_sync_metadata`, `bwipo_kommo_depara`, view `vw_bwipo_deals_rgm`. Kommo (`kommo_sync` + tela Sync Comercial) **fica ligado**.
- **Campos achatados** a partir de `dealPanelFields` (slugs da org: `rgm`, `cpf`, `data_de_matricula`, `curso_de_inscricao`, `polo`, `situacao`, `e_mail`…). Sync grava o que tiver valor. Lead de teste Rapha (#4997) confirmou o painel; valores custom ainda vinham vazios — owner/etapa já vêm no deal.
- **Depara leads:** RGM > CPF > telefone > e-mail contra `kommo_sync`; manual não é sobrescrito. **Depara consultor:** `bwipo_kommo_user_depara` por e-mail (Claudia `cmu3129do0005uap8fm9vpjoj` ↔ Kommo `8240438` via `claudia.beatriz@cruzeiroead.com.br`).
- **Permissão:** página `bwipo_sync`; boot copia grant de quem já tem `kommo_sync`.
- **Sync:** Incremental e Full **só no botão**. Incremental usa `updatedSince` na API de deals (folga 5 min, teto `BWIPO_DELTA_LOOKBACK_DAYS=7`); contatos do delta vêm do embed do deal (`/api/contacts?updatedSince` não filtra). Full continua paginando tudo e tombstonando o que sumiu. Sem cron de sync. **Responsável dos leads novos** (23/09): job `bwipo_owner_recente` a cada 5 min pega negócios do Pipeline Principal criados na última 1h30 ainda sem `ownerId` e grava o id Bwipo do mesmo consultor do contato no Kommo (`services/bwipo_owner.py`, mapa dos 16 usuários). Não cria o lead — a entrada continua na integração de fora. Consultor sem id na tabela (ex.: Diego) fica sem responsável.
- **Não muda nesta leva (confirmado 2026-09-21, de novo 2026-09-22):** crédito (Dashboard Comercial / Minha Performance) **fica no Kommo** até a migração da semana de 28/09. Não ligar o ranking no Ganho do Bwipo antes disso. O desenho da leitura, fechado em 23/09, está na entrada abaixo. Match/Merge outra pessoa, não mexer. Dist. Comercial só depois do sync/espelho/depara estarem redondos. Commit desta leva é **aditivo** (página + tabelas `bwipo_*`); Sync Comercial Kommo, ranking e Match **não são trocados**.

### 2026-09-23 — Painel: histórico no banco, migrados com depara, atualização pelo Bwipo
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** subiu ~60 mil leads do funil comercial para o Bwipo. Não vamos espelhar a base inteira do Kommo (~500 mil). O histórico que o painel já tem fica no banco (`kommo_sync`). Quem migrou entra no depara. O painel segue com esse histórico e passa a atualizar pelo Bwipo.
- **Leitura:** histórico = banco local. Lead migrado = linha em `bwipo_kommo_depara` (RGM > CPF > telefone > e-mail; manual não é sobrescrito). Atualização de etapa, responsável e ganho desses casados vem do Bwipo. Quem não migrou permanece só no histórico, sem negócio novo no Bwipo.
- **Estado medido em 23/09:** depara **1.690**; espelho `bwipo_deals` **7.161**. Os ~60 mil importados ainda não estão no espelho, então o depara deles só fecha depois de um sync do Bwipo para o banco. Não é outro import para o CRM.
- **Não fazer:** subir Backup, Licenciado ou o restante dos perdidos antigos para o Pipeline Principal.

### 2026-09-24 — Página Dashboard Comercial Bwipo (painel de vendas, só observação)
- **Modelo usado:** Cursor Grok 4.7.
- **Pedido:** página para observar o CRM, no visual do Dashboard Comercial do Kommo. Sem funil/sync/depara na tela — o que aparece é venda por pessoa.
- **Página** `bwipo_comercial`, grupo Comercial, para quem já tem Dashboard Comercial ou Sync Bwipo. **Não** alimenta ranking, Minha Performance nem Repasse. O crédito continua no Kommo até a semana de 28/09.
- **Número do hero = o mesmo recorte do Dashboard Comercial** (`_crgm_periodo_data_oficial` + fora do padrão de prefixo). 14–30/09: **847** matrículas, **791** em curso, **49** evasão, **7** fora do padrão. Não é a contagem de cards em Ganho no Bwipo (essa dava 536 e deixava gente de fora).
- **Consultor:** dono do negócio no espelho Bwipo, casado pelo RGM. Sem dono no card, ou RGM sem negócio no Bwipo, a venda conta em **Admin Sistema** — a mesma regra do painel quando não há responsável. Enquanto o espelho estiver sem os PUTs de responsável, o ranking inteiro cai em Admin Sistema; o sync incremental separa quem já tem dono.
- **Filtro De/Até** é a data de matrícula do relatório SIAA (o mesmo De/Até do Comercial), não o `created_at` do import.
- **Funções de leitura alinhadas ao Comercial (25/09):** ranking com meta da campanha (depara do consultor), clique no agente lista as matrículas, clique no dia filtra o ranking, evasão e fora do padrão abrem a lista, consultar RGM, ticket = média do valor do negócio × 30%. Em aberto e perdidos são a carteira atual do Pipeline Principal, não o recorte da data. Interação de WhatsApp, horas e inscritos do Match continuam só no painel Kommo — o Bwipo não tem essa fonte. Crédito (Minha Performance, Repasse, conflito, sync Kommo) não muda.

### 2026-09-25 — Dashboard Comercial Bwipo vira o novo Dashboard Comercial; herda o histórico do Kommo por consultor
- **Modelo usado:** Claude Opus 5.5.
- **Pedido:** a página `bwipo_comercial` substitui o Dashboard Comercial do Kommo, que será excluído. O histórico que não foi migrado continua valendo e continua com o consultor (vendas do Hugo no Kommo seguem do Hugo). Todas as funções do painel antigo (mini-sync, contar venda, conflito etc.) precisam existir no novo.
- **Identidade do consultor = `kommo_user_id`** (já usado em login, metas, campanha, PIX, conflito). Bwipo → Kommo por `bwipo_kommo_user_depara`. Dono do Bwipo sem depara aparece pelo nome do Bwipo.
- **Tombamento é por consultor, não por data.** Tabela `bwipo_consultor_tombamento (kommo_user_id, desde)`. Primeiro só o Hugo (`12158628`) em 29/09, como teste; os demais entram se der certo. **Não usar 28/09 como data de corte.**
- **Dono da venda (`_atribuir_rgms` em `routes/bwipo_sync.py`):** manual (`comercial_rgm_conflito_resolucao`: conflito, ajuste aprovado, mini-sync) > dono no Bwipo se o consultor está tombado e a matrícula é ≥ data dele > responsável no Kommo (mesma regra do painel antigo, 142 primeiro) > dono no Bwipo > Admin Sistema. Consultor excluído é reatribuído e oculto sai do ranking (`comercial_consultor_ajuste`). Substitui a regra de 24/09 "sem dono no Bwipo → Admin Sistema".
- **Medido 14–30/09:** Admin Sistema 286 → 48; 892 vendas pelo Kommo, 29 manuais, 1 pelo Bwipo, 12 sem dono. Por consultor bate com o ranking antigo com 1–3 a menos (o antigo ainda recupera RGM que sumiu do último relatório).
- **Depara de leads:** `_rebuild_depara` não segura mais a conexão do `dcz_sync` em transação durante as consultas ao Kommo (era o que derrubava a conexão); grava tudo em lote no final. Rodado em 25/09: 49.342 leads Kommo ↔ 44.448 negócios Bwipo.
- **Etapa 2 (feita 25/09), mesmas tabelas do painel antigo — decisão numa tela vale na outra:**
  - **Vendas em Conflito** (`POST /api/bwipo/painel/conflitos`, body com os RGMs do recorte): conflito = mais de um consultor **em Ganho** entre leads Kommo e negócios Bwipo; Admin Sistema não disputa. 14–30/09: 25 conflitos, 9 sem decisão. Fixar/desfazer em `/api/bwipo/painel/conflitos/resolver` grava em `comercial_rgm_conflito_resolucao` (`resolved_by='conflito_bwipo'`).
  - **Mini-sync** (`POST /api/bwipo/painel/mini-sync`): `{lead_id}` delega ao mini-sync Kommo existente; `{deal_id|number}` atualiza o negócio no espelho e, se estiver em Ganho com RGM e dono com depara, fixa a venda (`mini_sync_bwipo`). Responsável sem depara não fixa.
  - **Contar venda** (admin, `/api/bwipo/painel/contar-venda`) → `comercial_rgm_outlier_contagem`.
  - **Consultar RGM** (`GET /api/bwipo/painel/rgm`) mostra dono, regra usada, decisão manual e todos os leads/negócios com o RGM, com botões de fixar e mini-sync.
  - Cuidado: o card de teste Rapha #4997 está em Ganho com RGM 47277581; mini-sync nele fixa venda (fixação de teste já removida).
- **Etapa 3 (feita 25/09):** barra no topo da página.
  - **Reaproveitado** (só depende do `kommo_user_id`): renomear/ocultar/excluir (`/api/comercial-rgm/consultores`), metas avulsas (`/api/comercial-rgm/metas`), sync de usuários Kommo.
  - **Editar consultores** (`GET /api/bwipo/painel/consultores`, admin): cada consultor Kommo com o usuário Bwipo ligado, data "No Bwipo desde" (`POST /api/bwipo/painel/tombamento`) e ligação manual Bwipo→Kommo (`POST /api/bwipo/painel/user-depara`, `match_type='manual'`, não é sobrescrita pelo sync). Em 25/09 só Caio Vinicius (1 negócio) sem depara.
  - **Sync agentes** (`POST /api/bwipo/painel/sync-agentes`, admin): usuários Kommo + depara de consultores.
  - **Duplicatas** (`POST /api/bwipo/painel/duplicatas`): RGM do recorte com mais de um negócio ativo no Bwipo. 14–30/09: 307 — o import trouxe a mesma pessoa em vários cards, muitos em Ganho.
  - **Sem data** (`POST /api/bwipo/painel/sem-data`): negócio sem data de matrícula (0 em 25/09) e matrícula SIAA sem negócio no Bwipo (209).
  - **Detalhe do agente:** CSV das matrículas + carteira no Bwipo por etapa (`GET /api/bwipo/painel/agente-carteira`).
  - **Metas:** mostra campanha ativa, meta aplicada no ranking e metas avulsas (editar/excluir/adicionar). Campanha continua sendo editada em Premiação.
  - **Diagnóstico** (`GET /api/bwipo/painel/diagnostico`): espelho, depara por tipo, donos sem depara, decisões manuais, últimos syncs e de onde veio o dono das vendas do recorte.
- **Ordem combinada:** 1) dono da venda e ranking (feito); 2) conflito, mini-sync e contar venda (feito); 3) o resto (feito) (duplicatas, sem data, editar consultores, metas, sync agentes, diagnóstico). Ciclos/turmas/upload/congelar continuam onde estão (são do SIAA).
- **Até o tombamento de cada um:** Minha Performance e Repasse seguem no Kommo.

### 2026-09-15 — Kanban da fila: visual da referência (colunas pastel + card claro)
- **Modelo usado:** Cursor Grok 4.6.
- **Pedido:** "deixe a fila do Kanban mais parecida tbm" (screenshot da fila escura vs quadro do app React do briefing).
- **Só visual do modo Kanban.** Lista, KPIs, filtros, drag e PATCH de status não mudam. Status continuam Pendente / Em andamento / Concluído (não viram Novo Ticket / A Fazer / Em Produção).
- **Colunas** no tema do painel (não pastel claro da referência — destoa do navy): poço âmbar / azul / verde semitransparente, as mesmas cores dos KPIs. Cards `var(--bg-card)` com chip de tipo, título, nota (recorte de `status_nota` ou `descricao` — campo `nota` na listagem, sem mandar o briefing), linha "Prazo de entrega" + badge e rodapé com iniciais do responsável (dado real, não avatar inventado).
- **Fora de propósito:** barra de progresso, checklist e subtarefas — não existem em `ti_chamado`.

### 2026-09-15 — Briefing de Marketing alinhado à referência do ZIP
- **Modelo usado:** Cursor Grok 4.6.
- **Pedido:** o formulário de Marketing tinha campos a mais que o ZIP (`formulário-de-briefing-de-design.zip`), dificultando o uso. Alinhar **exatamente** à tela que o app React renderiza (`SectionGeneral` + `SectionSpecs`).
- **O que o ZIP mostra de verdade (App.tsx):** nome, bandeira, público-alvo, formato da peça, título/assunto, informações obrigatórias, telefone na peça, e as specs do formato. `SectionContent` (assunto/tema, objetivo) **não entra no App** — ficou de fora.
- **Saiu do formulário de Marketing:** prazo, objetivo da peça, assunto/tema extra, observações/patrimônio, opção "Outro", dimensão específica (imagem), duração (vídeo), referências UX, quantidade de páginas (e-book) e fornecedor do brinde. **Setor e prioridade ficam** — pedido explícito depois do alinhamento.
- **Não muda:** formulário de TI; chamados antigos continuam legíveis (chaves extras no JSONB só deixam de ser gravadas em envios novos).

### 2026-09-14 — Fila de chamados: modo Kanban (Lista ⇄ Kanban) com drag & drop de status
- **Modelo usado:** Opus 5.
- **Pedido:** "na fila de chamados vc adicione o modo de Kanban, para que eu possa alternar do modo em Lista e kanban". Referências enviadas: um segmented control (Kanban/Flow/Lista, ativo em azul sólido) e um quadro Kanban do app React do briefing.
- **Só front-end — backend intocado.** A listagem (`GET /api/solicitacoes_ti/chamados`) já devolve `is_admin`, `user_id` e `departamentos_exclusivos`, e cada item traz `departamento` + `responsavel_user_id`; com isso o JS reproduz `_pode_alterar_status` sem endpoint novo. O drag grava no **mesmo PATCH** `…/chamados/<id>/status` da lista.
- **Seletor:** `ds-segment ds-segment--compact` + `ds-segment__btn--active` (componente que já existe no `_head.html`, usado em Avisos/Config) — nada de componente novo. Fica na mesma linha das abas de departamento (`justify-between`), à direita; com uma fila só as abas ficam escondidas e o seletor continua no canto. **"Flow" da referência não foi implementado** (não foi pedido e não existe conceito equivalente no módulo). Escolha persistida em `localStorage` `cti_view_v1`.
- **Chip de status manda nas colunas (decisão):** `Abertos` → Pendente + Em andamento; `Todos` → as 3; chip de status único (inclusive clique nos KPIs, que chamam `ctiSetStatus`) → **1 coluna só**, com uma linha de aviso "escolha Abertos ou Todos para arrastar entre colunas". Alternativa descartada: sempre renderizar as 3 colunas ignorando o chip — o filtro deixaria de valer no Kanban e a coluna mostraria dado que o usuário filtrou fora. Prioridade, Setor, Tipo/formato, busca e abas de departamento valem igual nos dois modos (o Kanban lê o mesmo `_items` da mesma query).
- **Card sem campo inventado:** protocolo, urgência, título, pill de departamento (`sti-dep-*`, mesma cor da lista), categoria/formato, solicitante · setor, responsável (ou pill "Livre") e prazo. `prazo_desejado` (só Marketing) vira badge no estilo da referência: **vermelho** "Atrasado há N dias", **amarelo** "Vence hoje"/"Falta N dias" (≤ 2 dias), neutro "Prazo dd/mm"; em chamado `Concluído` o badge nunca fica vermelho. Sem prazo, o rodapé mostra a data de abertura. **Barra de progresso, checklist, subtarefas e avatar da referência ficaram fora** — não existem no modelo `ti_chamado`.
- **Drag & drop nativo (HTML5), zero dependência.** `draggable` só quando o usuário pode alterar o status; sem direito o card não arrasta e o `title` explica ("Só o responsável altera o status deste chamado"). O `_dragId`/`_dragFrom` ficam em módulo porque `dataTransfer` não é legível no `dragover`. Em vez de UI otimista: o card vai para a coluna alvo em estado `is-saving` e, **se o backend recusar (403/409/rede), volta para a posição exata de origem** (guarda `nextElementSibling`) com o `toast` da mensagem do backend; no sucesso recarrega a fila (fonte de verdade). Clique/Enter no card abre a **mesma modal** `#cti-modal` via `ctiOpen` — nenhuma modal duplicada.
- **Onde o "não arrastável" aparece (honestidade sobre o escopo):** com as regras atuais, todo chamado que o usuário vê na fila ele pode mexer (em Marketing a fila já é "livres + meus"; em TI todos operam tudo). O guard é o espelho do backend e só se manifesta com a **tela desatualizada** — outra pessoa puxou o chamado enquanto a aba estava aberta. Esse é exatamente o caso testado.
- **Layout:** `#cti-kanban` é flex com `overflow-x: auto` e coluna `min-width: 270px` (em tela estreita rola na horizontal, não estoura); corpo da coluna com scroll próprio (`max-height: calc(100dvh - 26rem)`). Cores de status/prioridade e o `.sti-badge` da lista foram reaproveitados — nenhuma cor da lista foi alterada.
- **Não muda:** os 3 KPIs do topo, a tabela (modo Lista), fluxo Pendente → Em andamento → Concluído, escopo por departamento, puxar/devolver, allowlist, modal no portal `dcz-modal-portal`, `.sti-ticket-card` com scroll, filtros `.cti-ctrl` 40px e a coluna "Abrir" `nowrap`.
- **Validado no navegador (app local + PG de produção, sessão admin e sessão `vitoria.cristina` / Marketing viewer):** seletor alterna e o modo sobrevive ao F5; `Abertos` = 2 colunas, `Todos` = 3, `Pendentes` = 1 + aviso; busca "Banner" deixa só o card do chamado; badge vermelho "Atrasado há 3 dias" e amarelo "Falta 1 dia" renderizam; arrastar Pendente → Em andamento gravou no banco (confirmado por SELECT) e persistiu no reload; clique no card abre a modal; com o chamado puxado por outro usuário por trás da tela, o PATCH devolve **403** ("Chamado é de Diego Vicente — só o responsável altera o status.") e o card **volta para a coluna Pendente**. 3 chamados de teste inseridos por SQL (sem disparar aviso) e removidos ao final — a base voltou a 5 chamados (2 pendentes, 1 em andamento, 2 concluídos) e 11 eventos.

### 2026-09-11 — Chamados: categoria Marketing, abertura restrita a supervisores+ e responsável ("puxar")
- **Modelo usado:** Opus 5. Continuação da entrada abaixo (mesmo dia).
- **Pedido:** "temos que criar a categoria marketing. quem pode abrir chamados são da categoria dos supervisores para cima. e o Marketing vai ter acesso a aba dos meus chamados e fila de chamados, desde que o chamado esteja com o consultor responsável." Confirmado por ele: abrem chamado **Supervisor Comercial, Supervisor Acadêmico, Marketing e admins**; o responsável é criado no chamado e **o próprio time de Marketing se atribui** ao pegar.
- **Quem abre chamado — fonte única:** `CHAMADOS_ABRIR_CATEGORIAS` em `helpers.py` (+ `pode_abrir_chamado(role, categoria)`, comparação por `fold_name`, tolerante a acento/caixa — "Acadêmico" tem acento). A mesma constante é usada no gate do POST (`routes/solicitacoes_ti.py::_pode_abrir`, 403), no menu (`nav_can` em `app.py`) e na reconciliação de boot (`CHAMADOS_ABRIR_CATEGORIAS_SQL`, variantes com e sem acento para o `LOWER(TRIM(categoria)) = ANY(...)`). **Categoria é a autoridade** — não existe grant avulso por checkbox que sobreviva ao boot; para liberar alguém de fora, muda-se a categoria dele.
- **`solicitacoes_ti` e `meus_chamados_ti` saem do `_NAV_ALWAYS`.** `_ensure_chamados_ti_page()` passou a reconciliar as 4 permissões: concede abrir a admin+categorias e **revoga dos outros**; `meus_chamados_ti` vai para essa turma **mais quem já tem chamado** (`EXISTS` em `ti_chamado` por `solicitante_user_id` ou username) — ninguém fica com histórico órfão ao perder o direito de abrir novos.
- **Efeito no banco de produção (medido):** `solicitacoes_ti` **55 → 13** (8 admins + 3 Supervisor Comercial + 2 Supervisor Acadêmico); `meus_chamados_ti` **55 → 14** (os 13 + Mikami, que tem o CH-00002). Perdem a abertura **42 pessoas**: 16 Comercial, 12 Acadêmico, 4 Suporte Comercial, 10 sem categoria. **Mikami e Eduardo Tang (allowlist da fila de TI, sem categoria) operam a fila mas não abrem chamado** — se o time quiser, é dar categoria a eles ou incluir a categoria na constante.
- **Fila de Marketing agora vem da categoria:** o boot concede `chamados_marketing` a `categoria = 'Marketing'` **e** à `CHAMADOS_MARKETING_ALLOWLIST` do `.env`. Só revoga quando existe fonte de verdade (alguém na categoria ou allowlist preenchida) — com as duas vazias não mexe em nada, preservando grant manual, como antes.
- **Responsável ("puxar"):** `ti_chamado` ganhou `responsavel_user_id` (FK `ON DELETE SET NULL`), `responsavel_nome` (snapshot) e `responsavel_desde`, + `idx_ti_chamado_resp`, via `ADD COLUMN IF NOT EXISTS`. `POST …/chamados/<id>/assumir` faz `UPDATE … WHERE responsavel_user_id IS NULL` (claim atômico, mesma filosofia do `academico_atendimento_claim`): o segundo clique simultâneo recebe **409** com o nome de quem levou, nunca sobrescreve. `POST …/liberar` devolve à fila (responsável ou admin). Cada atribuição/liberação entra em `ti_chamado_evento` (status igual nos dois campos, a nota conta o que houve).
- **Visibilidade da fila (resolução da ambiguidade do pedido):** em Marketing, quem não é admin vê os chamados **sem responsável** (disponíveis para puxar) **mais os seus**; o que outra pessoa puxou fica oculto. Admin vê tudo, com a coluna Responsável — senão ninguém supervisiona a fila. Vale para listagem, KPIs, detalhe e PATCH. O **solicitante** continua vendo o próprio chamado (e o responsável) em Meus chamados, mesmo puxado por outro.
- **Status exige ser o responsável — só em Marketing** (`DEPTOS_RESPONSAVEL_EXCLUSIVO`). Chamado livre: o próprio PATCH **assume** o chamado antes de gravar (mexer é puxar), senão haveria beco sem saída. Admin altera sem assumir.
- **TI segue como estava:** ganhou o botão de puxar/devolver e a coluna Responsável (informativo, útil pra saber quem está cuidando), mas **sem restrição** — todos os operadores continuam vendo a fila inteira e alterando status de qualquer chamado. A exclusividade é só de Marketing.
- **Config:** `Marketing` já existia nos dropdowns/preset (entrada abaixo); ganhou pill própria (`tag-cat-marketing`). `solicitacoes_ti`/`meus_chamados_ti` entraram nos presets de Supervisor Comercial e Supervisor Acadêmico; o preset de **TI** deixou de marcar `solicitacoes_ti` (TI não abre chamado) e passou a marcar `chamados_ti`.
- **Validado (migration 2×, test client + PG real, 24 casos):** 2ª execução da migration não altera nada; Comercial toma 403 no POST e não vê o menu; Supervisor Comercial abre; categoria Marketing recebe as 3 permissões e vê `fila_departamentos=['Marketing']` (403 em `?departamento=TI`); **duas threads disputando o mesmo chamado → um 200 e um 409**, responsável = o vencedor; o perdedor perde o chamado da fila, toma 403 no detalhe e no PATCH; PATCH em chamado livre atribui; liberar devolve; admin vê o puxado e quem puxou; em TI o colega ainda vê e altera chamado alheio; Comercial com chamado antigo mantém "Meus chamados" e perde "Abrir chamado". Chamados/eventos/avisos/usuários de teste removidos ao final (voltou a 2 chamados, 4 eventos, 55 usuários).
- **Decisões tomadas sem confirmação:** (a) fila = livres + meus (conciliação das respostas 2 e 3 do usuário); (b) admin vê tudo; (c) PATCH em chamado livre auto-assume; (d) puxar **não** gera aviso para o solicitante (evita ruído — ele vê o responsável no modal); (e) TI mantém fila aberta; (f) `meus_chamados_ti` preservado por histórico; (g) categoria manda mais que checkbox no `solicitacoes_ti`.
- **Alternativas descartadas:** gate por permissão em vez de categoria (grant manual valeria só até o próximo boot — divergência silenciosa entre menu e API); esconder só o link do menu (o POST continuaria aberto); tabela de claim separada como em `academico_atendimento_claim` (a coluna na própria linha basta, e o `UPDATE` condicional já é atômico); aplicar a exclusividade também em TI (mudaria a operação de 5 pessoas que hoje trabalham na fila inteira, sem pedido); revogar `meus_chamados_ti` de todos os 42 (deixaria histórico inacessível).

### 2026-09-11 — Chamados: módulo passa a atender TI **e** Marketing (briefing de design)
- **Modelo usado:** Opus 5.
- **Pedido:** o módulo de chamados só tinha gente de TI; o time quer abrir também chamados de **Marketing**, com o formulário de **briefing de design** (ZIP `formulário-de-briefing-de-design.zip` entregue pelo usuário), separando os departamentos para acompanhar melhor.
- **O ZIP não era documento:** era um app **React 18 + Vite + TS** com o formulário implementado. Nada do código entrou no repo — extraí só o **modelo de campos** (`src/types.ts`, `initialState.ts`, `SectionGeneral.tsx`, `utils/formatBriefing.ts` e os 6 `specs/*.tsx`). Obrigatórios (via `calculateCompletion`): título/assunto, formato da peça, público-alvo, bandeira, informações obrigatórias + 1 condicional por formato. Bandeiras: `Cruzeiro do Sul | DNA WORK | Sumaré`.
- **Decisão — 1 tabela, 1 coluna de departamento:** `ti_chamado` ganha `departamento TEXT NOT NULL DEFAULT 'TI'` (+ `briefing JSONB`, `prazo_desejado DATE`, índice `idx_ti_chamado_depto`), tudo via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` no `_ensure_ti_chamado_tables()`. Histórico não quebra: os chamados existentes viram `TI` pelo default.
- **Briefing vai em JSONB, não em ~25 colunas.** Reaproveita o que já existe: `categoria` = **formato da peça** (Imagem/Vídeo/UX-UI/E-book/Brinde/Outro), `urgencia` = prioridade, `descricao` = informações obrigatórias, `observacoes` = outras informações, `setor` = setor do solicitante (lista inalterada). `prazo_desejado` virou **coluna real** (fila de design precisa de prazo operacional, não dá pra deixar escondido no JSON). O JSONB **nunca é gravado cru**: `_sanitize_briefing` whitelista as chaves por categoria (`BRIEF_COMUNS` + `BRIEF_SPECS`) e limita tipo/tamanho.
- **Fila: 1 página, escopo por permissão.** Em vez de duplicar página/partial/JS (~350 linhas), a página continua sendo `chamados_ti` e `chamados_marketing` é uma **permissão de escopo**. `_fila_departamentos()` (memoizado em `flask.g`) resolve quais filas o usuário vê; a query base filtra `COALESCE(departamento,'TI') = ANY(%s)` e os KPIs também (antes o KPI varria a tabela inteira). Quem só tem Marketing abre a mesma página via a promoção de sub-permissão já existente em `_nav_load_user_data` (`chamados_marketing` → `chamados_ti`), igual ao `disparador_whatsapp_*`. **Slugs preservados** — nenhum grant em `user_permissions` quebra.
- **Allowlist de Marketing fica vazia de propósito:** `CHAMADOS_MARKETING_ALLOWLIST` (env, mesmo padrão do TI, via `_parse_allowlist_env`). Enquanto estiver vazia o boot **não revoga nada** — grant manual na tela de Config continua valendo. `CHAMADOS_TI_ALLOWLIST` (5 pessoas, hardcoded) não mudou. **Pendente: os nomes/e-mails do time de Marketing.**
- **Aviso por departamento:** `criar_aviso_por_permissao(DEPTO_PAGE[departamento], ...)` — chamado de Marketing avisa só quem tem a fila de Marketing.
- **Menu:** novo grupo **Chamados** (`support_agent`) com Abrir chamado / Meus chamados / Fila de chamados. O grupo **TI** fica só com as ferramentas admin (IA Comercial, Uso do Dashboard, Match Inadimplentes, Matérias dos Alunos) — renomear o grupo antigo misturaria coisas que não são chamado. Labels perderam o "TI"; na Config a fila aparece como `Fila de chamados · TI` e `· Marketing`. Novo preset de categoria **Marketing**.
- **Não muda:** fluxo Pendente → Em andamento → Concluído; campos e validações do chamado de TI (briefing fica `NULL`); regra de quem vê a fila (admin + allowlist); modais no portal `dcz-modal-portal`, `.sti-ticket-card` com scroll, `.cti-ctrl` 40px e coluna Abrir `nowrap`.
- **Validado (test client + PG real):** migration idempotente (2 execuções, 2 chamados antigos viraram `TI`); POST Marketing grava o briefing saneado e descarta chave fora da whitelist; 400 em categoria de TI usada no Marketing, em bandeira/público-alvo faltando e em prazo fora do ISO; TI sem briefing segue igual; usuário só-Marketing vê `fila_departamentos=['Marketing']` e toma **403** no `?departamento=TI`, no detalhe e no PATCH de chamado de TI, mas conclui o de Marketing. Chamados de teste removidos ao final.
- **Alternativas descartadas:** tabela separada `marketing_chamado` (duplicaria fila/eventos/avisos e a tela "Meus chamados" do solicitante); colunas dedicadas para cada campo do briefing (~25 colunas quase sempre NULL, e o briefing muda por formato); segunda página de fila só para Marketing (duplicação de UI sem ganho, já que o escopo é backend); renomear o grupo "TI" do menu (ele guarda ferramentas admin que não são chamado); recusar `departamento` desconhecido no filtro da fila (o arquivo já ignora valor inválido nos outros filtros e o fallback nunca amplia além das filas permitidas).
- **Do briefing que não cabe no modelo atual (reabrir se pedirem):** quadro Kanban próprio e status de board do app original (aqui o ciclo é o de 3 status do módulo); rascunhos/templates de briefing salvos; subtarefas, tags e barra de progresso; **upload de anexos/referências** (hoje só link em texto — é a lacuna mais sentida); os tipos `impresso` e `audio` existem no `types.ts` mas nem o próprio app os oferecia no dropdown, então ficaram fora.

### 2026-09-10 — Acadêmico: Transferido/Rematrículas inflados (cache no upload, não D-1)
- **Modelo usado:** Cursor Grok 4.5.
- **Sintoma:** ontem Transferido ~**500** (ok); hoje Total **45.794**, Rematrículas **26.668**, Transferido **18.565** com ciclo `2026/2 (27.229)`. Em Curso/Cancelado/Trancado batiam com o snapshot.
- **Causa:** no upload do Disparador o sumidos rodou com **latest incompleto** (mesmo `snapshot_id`, poucas linhas) → milhares de RGMs “sumidos” fantasmas; o cache em memória **ficava preso** nesse `snapshot_id` o dia inteiro. Esses fantasmas ainda entravam nos cards de **tipo** (Remat/Novos).
- **O que NÃO fazer:** restringir sumidos a D-1 (snapshot anterior só) — o card Transferido é o **acumulado** de quem saiu e não voltou (~centenas), não o delta do dia (~unidades).
- **Fix:** sumidos voltam ao critério **histórico vs latest** (last-seen); linhas `TRANSFERIDO` (sumidos) contam **só** no card de situação, não no Total/Novos/Rematrículas.
- **4 guardas no cache (`_compute_sumidos`):** (1) invalida por `snapshot_id` **e** `COUNT(*)` do latest; (2) latest < 80% do snapshot anterior → mantém cache antigo e não grava; (3) recontagem depois da varredura — se o snapshot cresceu no meio, descarta o resultado; (4) sanidade `len(sumidos) > 20%` do latest → descarta (transferência é centena, não milhar). Sem resultado confiável devolve o **último cache bom** (nunca fantasma).
- **Single-flight:** `_SUMIDOS_COMPUTE_LOCK` — a varredura leva ~15–30s; sem lock, N requests após o upload disparavam N varreduras no `disparos`.
- **Exceção do filtro:** ao clicar no card **Transferido**, o `continue` não se aplica (`f_sit_norm == 'transferido'`) — senão a tela filtrada zerava tipo/polo/nível.
- **Números esperados (ciclo 2026/2):** Total tipo ≈ **27.229**; Rematrículas ≈ **17.8k**; Transferido ≈ **~400–500** (acumulado real), não 18k. Sem filtro de ciclo: Total = **55.164** (= linhas do snapshot) e Transferido **520**.
- **Não muda:** inbound TRANSFERIDO do arquivo → Em Curso.

### 2026-09-08 — Rematrícula zerada: Data Matrícula do Disparador é serial Excel
- **Modelo usado:** Cursor Grok 4.5.
- **Sintoma:** aba Rematrícula com dropdown `2026/2 (N rematr.)` mas cards/gráficos em **0**.
- **Causa:** após a migração para Bases Matriculados do Disparador, `Data Matrícula` no JSONB vem como **serial Excel** (ex.: `46218` = 15/07/2026), não `dd/mm/aaaa`. O `_MAT_COLS` do Dashboard só parseava `DD/MM/YYYY` e `YYYY-MM-DD` → `data_matricula` NULL. O dropdown conta estoque **sem data**; a timeline exige data no intervalo (clamp liberação `2026/2` ≥ 14/06/2026) → série vazia.
- **Fix:** em `routes/dashboard.py`, `_DATA_MAT_SQL` aceita também serial numérico `30000–60000` via `DATE '1899-12-30' + FLOOR(serial)`. Texto da página Rematrícula atualizado (fonte = Disparador, não Upload Acadêmico).
- **Não muda:** regra de liberação por ciclo; empresa 12 no escopo remat; Comercial/`xl_rows`.

### 2026-09-04 — Acadêmico: observação de transferências inbound no card Em Curso
- **Modelo usado:** Cursor Grok 4.5.
- **Pedido:** no card **EM CURSO** (Graduação e Pós), avisar quantos alunos vieram de outro polo e permitir ver quem são.
- **Backend:** `inbound_transfers` em `/api/dashboard/students` (flag `inbound_transfer` no `_MAT_COLS` = `situacao ~* '^transfer'` já remapeada para Em Curso). Lista em `GET /api/dashboard/inbound-transfers` (RGM, nome, polo, tipo, ciclo; respeita ciclo/nível/período/polo/tipo).
- **UI:** no card Em Curso, pill roxa “N transferências de outro polo · ver quem são”; abre painel `#stu-inbound-panel` com a tabela. Fecha ao trocar filtros.
- **Contagem:** `inbound_transfers` = **RGMs distintos** (não `SUM(total)`). No XLSX o mesmo aluno pode ter 2 linhas (TRANSFERIDO + EM CURSO); o GROUP BY juntava e inflava o pill (ex. 77) vs a lista (42).
- **Não muda:** contagem de Em Curso (inbound já entra nela); card Transferido continua = só sumidos (saíram).

### 2026-09-03 — Dashboard Acadêmico: fonte = Bases Matriculados do Disparador
- **Modelo usado:** Cursor Grok 4.5.
- **Decisão do usuário:** o Dashboard Acadêmico (Graduação **e** Pós) deixa de ler `xl_snapshots`/`xl_rows` (Upload Acadêmico do dcz) e passa a ler o último upload de **Matriculados** do Disparador (`tool_whatsapp_alunos`).
- **Fonte:** DB `disparos` (mesmo host do `dcz_sync`), tabelas `matriculados_snapshots` + `matriculados_rows`. Conn nova `get_disparos_conn()` em `db.py` (`DISPAROS_DB_NAME`, default `disparos`).
- **Por quê:** o upload do Disparador é o operacional diário (ex.: 03/09 56.054 linhas) e já traz as ~1.400 rematrículas de Pós em 2026/2; o snapshot do Upload Acadêmico (#248) só tinha 3.
- **Mapeamento JSONB:** headers SIAA em Title Case (`Tipo Matrícula`, `Situação Matrícula`, `Ciclo`, `Empresa`, `Negócio`, `Polo`, `RGM`, `Data Matrícula`) via COALESCE com aliases ASCII/lowercase.
- **Regras preservadas:** empresas `^(12|7) -`; TRANSFERIDO no arquivo → EM CURSO; sumidos entre snapshots do Disparador → TRANSFERIDO; Pós veteranos via `tipo_matricula`.
- **Escopo:** só Dashboard Acadêmico (`routes/dashboard.py`). Comercial / Match & Merge / Upload Acadêmico **não** mudam.
- **Números (snapshot Disparador 56.054, ciclo 2026/2):** Pós Veteranos **1.462** (antes 3); Pós Novos ~733; total do ciclo ~27.235.
- **Alternativas descartadas:** usar `mm_matriculados` do Match & Merge (TRUNCATE por nível apaga Grad ou Pós); copiar Disparador→`xl_snapshots` a cada upload.

### 2026-09-03 — Dashboard Acadêmico: Transferido tem dois sentidos (inbound ≠ sumiu)
- **Modelo usado:** Cursor Grok 4.5.
- **Problema:** no relatório SIAA, `situacao=TRANSFERIDO` significa aluno que **veio de outro polo para a gente** (está conosco). Já quem **sumiu do relatório atual** é o oposto — foi transferido **para outro polo**. O card "Transferido" misturava os dois e os 42 inbound sumiam da leitura de Em Curso.
- **Decisão (Acadêmico only):**
  1. No `_MAT_COLS` (leitura do snapshot), `situacao ~* '^transfer'` é remapeada para **`EM CURSO`** — inbound conta como aluno nosso ativo.
  2. Quem existia em snapshot anterior e **não está no mais recente** entra via `_SUMIDOS_QUERY` / `_academic_sumidos_rows` como **`TRANSFERIDO`** (last-seen: tipo/ciclo/polo da última aparição), mesclado em `api_dashboard_students`.
- **Performance:** a varredura dos sumidos cobre todos os snapshots (~20–30s). Cache memoizado por `snapshot_id` + warm assíncrono no boot (`warm_academic_sumidos_cache` em `app.py`); se o snapshot mudou, devolve cache velho e recalcula em background.
- **Números (ciclo 2026/2):** raw TRANSFERIDO no arquivo = **42** → passam a Em Curso; sumidos = **393** (313 nova + 68 retorno + 12 recompra) → card Transferido. Novos sem filtro sobe ~393 (passa a incluir os que saíram, na situação Transferido — simétrico a já incluir Cancelado/Trancado).
- **Comercial não muda:** lá inbound TRANSFERIDO do SIAA é raro/outro significado operacional; sumidos já viram TRANSFERIDO na evasão via `mark_missing_as_transferido`.
- **Alternativa descartada:** manter TRANSFERIDO do relatório como está e só adicionar sumidos — o card continuaria mostrando os 42 "que são nossos" misturados com quem foi embora.

### 2026-09-03 — Dashboard Acadêmico: alinhamento ao Comercial revertido por completo (Novos volta ao snapshot)
- **Modelo usado:** Opus 5.
- **Decisão do usuário:** desfazer o alinhamento do card **Novos** do Dashboard Acadêmico ao "em curso" do Comercial. O Acadêmico volta a contar **calouros + regresso + recompra do snapshot mais recente**, filtrado por ciclo/nível/polo/período, incluindo todas as situações do recorte. **As duas telas voltam a ter recortes próprios e números diferentes — isso é esperado.**
- **O que saiu de `routes/dashboard.py`:** o bloco em `api_dashboard_students` que trocava as linhas de novos por `_academic_novos_from_comercial`, os helpers `_academic_novos_from_comercial` / `_comercial_period_for_siaa_ciclo`, o cache `_NOVOS_COM_*` e as chaves `novos_periodo` da resposta. `novos_fonte` fica fixo em `"snapshot"` e `novos_fora_padrao` em `0` (mantidos só para não quebrar o front antigo).
- **Card laranja "Alunos / RGM fora do padrão" removido do Acadêmico:** ele só fazia sentido no modo comercial, onde os fora do padrão eram **excluídos** do número principal. No snapshot eles já estão dentro dos Novos — manter o card duplicaria a contagem na leitura. No Dashboard Comercial o card continua.
- **Números após a reversão (ciclo 2026/2, snapshot de 03/09):** Novos **9.083** (7.103 calouros + 1.757 regresso + 223 recompra), rematrículas 16.245, total 25.328. Bate com o comportamento pré-alinhamento (era 8.989 no snapshot de 02/09; a diferença é o arquivo novo).
- **Preservado:** todo o lado Comercial (TRANSFERIDO na evasão, `require_latest_presence` no ranking, cards com botão `!`) e o fix de veteranos de Pós-Graduação em `_is_rematricula_empresa`.
- **Alternativa descartada:** manter o alinhamento atrás de um toggle na UI — mais superfície para manter e o usuário quer explicitamente o número antigo como padrão único do Acadêmico.

### 2026-09-03 — Sumidos do CSV atual viram TRANSFERIDO na evasão do Comercial; Acadêmico volta ao critério anterior
- **Modelo usado:** Opus 5.
- **Problema:** a entrada anterior fazia os "sumidos do CSV atual" simplesmente **desaparecerem** do Comercial (eram descartados). O usuário quer que eles apareçam — como **evasão/transferência** — em vez de sumirem da conta. Além disso, o alinhamento tinha vazado para o Acadêmico, que deveria manter o critério antigo.
- **Bug de performance corrigido junto:** `require_latest_presence` estava implementado como `EXISTS` correlacionado (`regexp_replace` sobre JSONB) varrendo **todos** os snapshots. `/api/dashboard/students` levava **~5 min** e a tela ficava no skeleton. A checagem passou a ser feita em Python via `_crgm_latest_snapshot_rgms()` (um `SELECT DISTINCT` no snapshot mais recente).
- **Decisão:** novo parâmetro `mark_missing_as_transferido` em `_crgm_periodo_data` / `_crgm_periodo_data_oficial`. Em vez de descartar quem não está no CSV mais recente, marca `situacao = 'TRANSFERIDO'` (+ flag `sumiu_do_csv`). Efeito: sai do EM CURSO (não conta venda) e **entra na evasão**.
- **Onde aplica cada modo:**
  - `_crgm_compute_kpis` e `_crgm_compute_grids` → `mark_missing_as_transferido=True` (hero bruto + painel de evasão).
  - `_build_agent_ranking_completa_vw` e `_crgm_compute_agentes` → seguem com `require_latest_presence=True` (drop). Transferência não credita/descredita consultor no ranking — regra de pagamento inalterada.
  - `routes/dashboard.py` `_academic_novos_from_comercial` → `require_latest_presence=False`. O **Acadêmico volta ao critério anterior** (last-seen, sem remover transferidos).
- **Números validados (período 14/05–15/12):** bruto **9.562** = em curso contando **7.868** + fora do padrão **192** + evasão **1.502**. Evasão passa de 1.109 → 1.502; `TRANSFERIDO` vai de 1 → **394** (393 sumidos + 1 já existente). `EM CURSO` cai de 8.453 para 8.060 — exatamente os 393 que viraram transferência.
- **Alternativas descartadas:** manter o descarte silencioso (o aluno some da conta sem aparecer em lugar nenhum); marcar como `CANCELADO` (mistura transferência com evasão real e polui o motivo); aplicar a marcação também no ranking por consultor (mudaria crédito/pagamento, fora do pedido).
- **Follow-up — Matrículas Oficiais do consultor:** `_fetch_agent_matriculas` (`routes/minha_performance.py`) usa last-seen em todos os snapshots e continuava mostrando o transferido como `EM CURSO`, **contando a venda**. Corrigido: aplica a mesma marcação (`TRANSFERIDO` + `sumiu_do_csv`, `conta_para_meta=False`) e o filtro `only_em_curso` passou de SQL para Python, **depois** da marcação — assim a linha some das contagens mas continua visível na lista do consultor. Caso validado: RGM `50166077` (Tamires) aparece como TRANSFERIDO e a contagem oficial fica **27** (antes 28).
- **Cache:** `_crgm_latest_snapshot_rgms()` é memoizado por `snapshot_id` (invalida sozinho no próximo upload). Sem isso, cada agente do ranking repetiria o scan do snapshot inteiro.
- **Follow-up 2 — pílula na Minha Performance:** `_mpClassifySituacao` (`static/js/minha_performance.js`) só mandava para `evadido` situações com `CANCEL/EVAD/DESIST/TRANC`, então `TRANSFERIDO` caía em `OUTROS` — divergindo do card Evasão do Comercial. Decisão do usuário: **`TRANSFERIDO` passa a contar em `EVADIDOS`**. O badge da linha ganhou cor própria (roxo, como no Comercial) para separar transferência de cancelamento sem criar uma 4ª pílula. Nada muda em contagem de venda/meta — isso já estava resolvido.

### 2026-09-03 — Comercial/Acadêmico: sumidos do CSV atual deixam de contar; cards exibem regra; Pós mostra veteranos
- **Modelo usado:** Auto.
- **Problema:** o alinhamento anterior ainda carregava RGMs vistos só em snapshots antigos (`all_snapshots`), mas o usuário confirmou que os "sumidos do CSV atual" são, na prática, **transferências para outro polo** e não devem continuar como alunos da carteira. Além disso, no filtro `Pós-Graduação` o card de rematrícula aparecia como "Veteranos" no front, mas o backend zerava quase tudo por aceitar rematrícula só na empresa `12 -`.
- **Decisão:** o universo "oficial" usado nos cards/KPIs passa a exigir **presença no CSV mais recente** (`require_latest_presence=True`) mesmo quando usa last-seen para dedupe. Isso vale para `_crgm_compute_kpis`, `_crgm_compute_agentes`, `_crgm_compute_grids`, `_build_agent_ranking_completa_vw` e para o card Novos do Dashboard Acadêmico alinhado ao Comercial.
- **Ranking/supp:** a regra suplementar do comercial deixa de recuperar o caso "sumiu do CSV mais recente"; mantém apenas casos ainda presentes no arquivo atual. Motivo: sumiço agora é interpretado como transferência para outro polo.
- **UI explicativa:** cards principais ganham botão `!` com texto do recorte aplicado, para deixar explícito o filtro/regra e evitar reabrir a dúvida meses depois.
- **Pós/Veteranos:** `rematricula` deixa de depender só de `empresa=12` quando `nivel='Pós-Graduação'`; no dashboard acadêmico, o backend passa a mostrar veteranos de pós usando o próprio `tipo_matricula` do snapshot.

### 2026-09-03 — Dashboard Acadêmico: card Novos alinha ao "em curso" do Comercial
- **Modelo usado:** Cursor Grok 4.6 (principal).
- **Problema:** mesmo recorte teórico (ciclo 2026/2 / 2º semestre 2026) dava Novos **8.989** no Acadêmico vs **8.185 em curso** no Comercial. O usuário pediu um número só — não dá para ter 2 resultados.
- **Causa (medida no banco, snapshot 246):** o Acadêmico lia só o último XLSX + coluna `ciclo=2026/2` + todas as situações (1.095 cancelado/trancado dentro dos 8.989). O Comercial corta por `data_matricula` 14/05–15/12, last-seen em todos os snapshots, só `NOVA MATRICULA/RECOMPRA/RETORNO`, só EM CURSO que conta pra meta (tira 202 fora do padrão). Interseção: os 7.894 EM CURSO do Acadêmico cabiam no Comercial; sobravam 493 só no Comercial (380 sumiram do arquivo de 02/09, 108 com ciclo SIAA ≠ 2026/2, 5 mudaram de ciclo/tipo).
- **Decisão:** o card **Novos** (calouros + regresso + recompra) do Dashboard Acadêmico passa a usar `_crgm_periodo_data_oficial` + regra `vendas_liquidas` do Comercial quando há ciclo SIAA mapeável em `ciclos_comercial` (ex.: `2026/2` → 14/05–15/12) ou De/Até explícito. Fonte oficial = **8.185**.
- **Rematrículas:** continuam no snapshot mais recente + coluna `ciclo` (o Comercial não conta rematrícula).
- **UI:** subtítulo do card Novos indica "Mesmo critério do Comercial (em curso, padrão RGM)".
- **Supervisor Acadêmico:** consome a mesma `/api/dashboard/students` — o KPI "Em curso" sobe um pouco (entra last-seen + sai cancelado dos novos). Aceito.
- **Alternativas descartadas:** mudar o Comercial para o último arquivo (quebra ranking/meta); só filtrar situação no Acadêmico (ainda ficava 7.894 ≠ 8.185).
- **Trade-off:** com ciclo selecionado, Novos deixa de ser "o que está no XLSX de hoje" e vira a conta oficial de venda. Sem ciclo e sem datas, o card volta ao snapshot (comportamento antigo).

### 2026-07-22 — DataCrazy CRM acadêmico: UI/sync/write desativados; Upload + Kommo preservados
- **Modelo usado:** Composer/Auto + Executor.
- **Decisão:** Descontinuar operações de escrita/sync no CRM acadêmico (DataCrazy): aba Pipeline vira placeholder ("Atualização CRM Acadêmico"); Sync Acadêmico some do menu; bloco Update CRM some do Upload; agendamentos `sync_delta`/`sync_full` desativados. Jobs APScheduler (`register_delta_interval`, `_run_scheduled_sync`) viraram no-op. Rotas de write removidas/410. Scripts Python de write deletados (`update_crm.py`, `sanitize_crm.py`, etc.); `sync.py` mantido para cleanup futuro. Botão "Atualizar Inadimplência" pós-upload em Distribuição removido (empurrava pro DC).
- **Preservado:** Upload Acadêmico (snapshots/dashboards), Sync/Delta Kommo (comercial), Match/Merge, busca no espelho (`/api/search`) e search-xl. Espelho local `leads` read-only.
- **Deixado pra depois:** apagar `sync.py`/tabelas espelho; página Buscar DC; `datacrazy_user_id`; Disparador WhatsApp (outro app).
- **Alternativas descartadas:** deletar espelho agora (quebra busca histórica); migrar já pro CRM novo (ainda indefinido).

### 2026-09-02 — Fila TI: modal cortada + acesso só à allowlist
- **Modelo usado:** Grok 4.6.
- **Sintoma:** modal de Meus chamados cortava o andamento; na fila, Prioridade/Setor/busca e o "Abrir" da tabela saíam cortados. Fila visível para todos.
- **Causa UI:** overlay dentro de `main overflow-auto` + `.glass-card { will-change: transform }` (containing block) + card `overflow: hidden` sem max-height; `.input-glass` padding 12px + `h-9` (36px) clipava o texto; `overflow-x-auto` no mesmo nó do card arredondado clipava a última coluna.
- **Decisão UI:** `#mct-modal` / `#cti-modal` entram no portal `dcz-modal-portal` (body). Card `.sti-ticket-card` com `max-height: 100dvh - 2rem` e corpo com scroll. Filtros `.cti-ctrl` (40px, padding 8/12). Tabela com wrapper interno de overflow e coluna Abrir `nowrap`.
- **Permissão:** fila `chamados_ti` sai de `_NAV_ALWAYS`. Só **admin** + allowlist (`CHAMADOS_TI_ALLOWLIST`): Emanuel Filipe (`emanuel.felipe@cruzeiroead.com.br`), Camila Ferreira (`camila@cruzeiroead.com.br`), Vanessa Mikami (`mikami@eduit.com.br`), Katia Cabeço (`katia.policeno@cruzeiroead.com.br` / `katia@cruzeiroead.com.br`), Eduardo Tang (`eduardo.tang@eduit.com.br`). Boot reconcilia: concede à lista e **revoga** o resto (não-admin). Formulário e Meus chamados continuam para todo autenticado.
- **Não muda:** fluxo Pendente → Em andamento → Concluído; aviso de abertura ainda vai para quem tem `chamados_ti`.
- **Alternativas descartadas:** deixar a fila no checkbox Config sem allowlist (voltaria a vazar no próximo grant); preset categoria TI incluindo a fila (os nomes não são categoria TI).

### 2026-09-02 — Solicitações TI: chamados no Postgres (fila + meus chamados)
- **Modelo usado:** Grok 4.6.
- **Pedido:** o formulário `solicitacoes_ti` gravava no Google Sheets via Apps Script; o time precisa persistir no banco, uma fila para o TI alterar status (em andamento / concluído) e uma página para o solicitante acompanhar o próprio chamado.
- **Decisão:** tabela `ti_chamado` (+ `ti_chamado_evento` de auditoria) no PG `dcz_sync`. Submit deixa de chamar Apps Script. Status: `Pendente` (abertura) → `Em andamento` → `Concluído`. Três páginas:
  - `solicitacoes_ti` — formulário (`_NAV_ALWAYS`).
  - `meus_chamados_ti` — só os tickets do usuário logado, read-only (`_NAV_ALWAYS`).
  - `chamados_ti` — fila operacional (admin + allowlist; ver entrada do mesmo dia).
- **Não muda:** campos do formulário (setor/categoria/urgência); responsável no Kommo; planilha antiga (fica órfã, sem sync retroativo).
- **Aviso:** abertura notifica quem tem `chamados_ti`; troca de status notifica o solicitante.

### 2026-09-02 — Match/Merge D-1: CPF `00000000009` deixava matriculados presos no Aceite
- **Modelo usado:** Grok 4.6. Subido na `master`.
- **Sintoma:** 1º Processar gerou 66 MATRICULADO; o 2º gerou 2; ~40 leads de 01/09 continuaram em Aceite (RGM preenchido, SIAA Matriculado).
- **Causa:** o relatório SIAA veio com CPF inválido `00000000009` nessas matrículas. O D-1 usava esse CPF como chave e agrupava todos os leads Kommo com o mesmo lixo — incluindo 142 de outras pessoas. `ganho_perm` atualizava esse 142 alheio e dava `continue`, sem mover o Aceite real. O bloco órfão já ignorava CPF inválido (`_cpf_valido`), então essas pessoas não ganhavam NOVO tampouco.
- **Decisão:** D-1 (e o índice CPF do órfão) só casa CPF com `_cpf_valido`. 142 com o mesmo RGM mas CPF inválido não bloqueia o Aceite — fecha esse 142 (`dup_rgm_cpf_invalido`) e gera MATRICULADO no Aceite. 2º curso: se já existe Aceite/ativo, move ele (`d1_segundo_curso`) em vez de criar NOVO. Ao atualizar um 142 legítimo, fecha os irmãos ativos (`dup_update_ganho`).
- **Não muda:** match por RGM/e-mail/telefone; fechar Aceite quando o 142 da mesma pessoa (CPF válido) já tem o RGM; Daniele/Byanca sem linha em `mm_matriculados` continuam de fora.
- **Efeito no lote de 01/09:** ~37 Aceite passam a MATRICULADO; duplicatas reais (Manoela, Silvana) continuam MOVER_PERDIDO; Waldirene/Rosisella (2º curso) movem o Aceite.

### 2026-08-31 — Deploy EasyPanel: não abortar no GHCR público (sem botão de rebuild)
- **Modelo usado:** Grok 4.6. Subido na `master`.
- **Sintoma:** todos os "Deploy EasyPanel" falhavam em ~1 min; o painel/banco no EasyPanel não tem Rebuild. Código na `master` não ia para produção.
- **Causa:** o job builda e publica a imagem, depois `Tornar pacote GHCR público` chama `PUT /user/packages/.../visibility` com `GITHUB_TOKEN` → 404. Esse `exit 1` pulava o webhook `PAINEL_EDUIT_TOKEN`, que é o único gatilho de deploy.
- **Decisão:** os dois passos de visibilidade/pull anônimo passam a `continue-on-error`. Build+push continua obrigatório; o webhook sempre dispara em seguida.
- **Não muda:** o pacote pode continuar privado; se o EasyPanel puxar GHCR sem credencial, o pull lá pode falhar — aí o caminho é credencial no EasyPanel ou `GHCR_PAT` / pacote Public.

### 2026-08-31 — Mini-sync manual: verde ≠ crédito; fixar atribuição no lead sincronizado
- **Modelo usado:** Grok 4.6. Subido na `master`.
- **Sintoma:** "Atualizar lead na base Kommo" fica verde (Joseane `#21870683` RGM `50124269`; Gilson `#21841599` RGM `50121821`) mas a venda não passa para o responsável daquele lead nem sai de quem estava indevido.
- **Causa:** o botão só faz GET+upsert daquele ID (SQLite + `kommo_sync`). O ranking credita por RGM via `DISTINCT ON (rgm) … status 142, id DESC`. Outro lead com o mesmo RGM (ex.: Autolead `21888919` / Hugo na distribuição) continua elegível; o verde não pinava `comercial_rgm_conflito_resolucao` nem atualizava os irmãos.
- **Decisão:** após mini-sync de um **Ganho (142)** com RGM 8 dígitos, upsert em `comercial_rgm_conflito_resolucao` (`resolved_by='mini_sync'`) para o `responsible_user_id` daquele lead — mesmo mecanismo de Vendas em Conflito, sem PATCH no Kommo. Em seguida ressincroniza até 8 leads irmãos com o mesmo RGM. `_crgm_kommo_lookup_rgms`, ranking, detalhe do agente e Dist. Consultor (fechadas) passam a aplicar o override. Os dois RGMs acima foram pinados agora (Claudia / Gabriela).
- **Não muda:** responsável no Kommo; leads que não estão em 142 não roubam crédito.

### 2026-08-31 — Transferência/Regresso: lead existe com RGM antigo (142) ou só no Backup; atualizar ganho / criar NOVO
- **Modelo usado:** Grok 4.6. Diagnóstico nos 12 da aba + ajuste de regra em `match_merge_lib.py`. Subido na `master`.
- **Sintoma:** aba Transferência/Regresso lista RGMs "sem lead em vw_leads_rgm". Na prática quase todos TÊM lead: ou 142 no Funil/Licenciado com **RGM da matrícula anterior**, ou 143 em **Backup - Antigos**. A aba só casa o RGM **atual** do snapshot comercial.
- **Causa raiz (duas):** (1) `executar_acoes` MATRICULADO dava `skip` se `status_id==142`, então nova matrícula/retorno nunca gravava RGM/data novos — Laura (49612301→49996126) é o caso-tipo; (2) bloco órfão só criava NOVO quando não havia match por CPF/email/tel/RGM, então quem tinha lead antigo (mesmo perdido no Backup) nunca ganhava 142 comercial. `search_lead_by_cpf` ainda bloqueava o NOVO.
- **Regra nova:** MATRICULADO em lead já 142 **atualiza campos** (RGM, Matrícula, curso, polo, situação) e **não mexe no status**. D-1 / órfão: se o RGM novo não está no Kommo, atualiza o 142 do funil comercial, **exceto** quando o RGM antigo ainda está Matriculado (2º curso) — aí cria NOVO. Lead só em Backup/143 → NOVO `novo_matriculado` (Admin Sistema), e o guard de CPF libera nesse caso.
- **Não muda:** atribuição do 142 existente (consultor permanece); anti-dup do mesmo RGM em 142 (fecha duplicata ativa).

### 2026-08-24 — Interações Acadêmicas: tag de quem puxou o atendimento (lock anti-conflito)
- **Pedido:** se duas pessoas abrem a lista ao mesmo tempo, o 2º clique não pode roubar o lead; a linha precisa mostrar quem puxou.
- **Decisão:** tabela `academico_atendimento_claim` (PG local, PK `telefone_key` = dígitos). `POST /atender` faz INSERT … ON CONFLICT DO NOTHING **antes** de atribuir no CRM. Quem já puxou reabre; outro operador recebe 409. A lista traz `puxado_por` e faz poll de 8s em `/claims`.
- **Alternativas descartadas:** (a) só o owner do CRM — lead já atribuído fora da fila acadêmica bloquearia indevidamente; (b) coluna no Supabase — a lista acadêmica é outro projeto, lock do painel fica no PG que o app controla.

### 2026-08-24 — Deep-link do atender abre o front EasyPanel, não crm.eduit.com.br
- **Sintoma:** após atribuir, o browser ia para `https://crm.eduit.com.br/pipeline?deal=…` (SaaS/marketing). O time usa `https://frontend-front.v74knz.easypanel.host/`.
- **Decisão:** `EDUIT_CRM_WEB_URL` default = EasyPanel. API continua em `EDUIT_CRM_BASE_URL` (`crm.eduit.com.br`). Se o env ainda apontar para `crm.eduit.com.br`, o código ignora e usa o EasyPanel.

### 2026-08-24 — Config: excluir usuário falhava com HTML ("Unexpected token <")
- **Causa:** `DELETE FROM app_users` estoura FK em `agent_matriculas` (e outras) sem `ON DELETE`. O Flask devolve página HTML de erro; o front faz `res.json()` e mostra `Unexpected token '<'`.
- **Decisão:** FKs históricas passam a `ON DELETE SET NULL` (matrículas/avisos/ajustes/premiação interna permanecem). Endpoint de delete devolve JSON em caso de erro. Front não assume JSON.

### 2026-08-24 — Interações Acadêmicas: clique no telefone atribui o lead no CRM EduIT ao usuário logado
- **Modelo usado:** Grok 4.6 (Cloud Agent).
- **Pedido:** na página Interações Acadêmicas, clicar no telefone abre um popup de confirmação; ao confirmar, o lead no CRM EduIT (org Cruzeiro EaD) é atribuído ao operador logado no painel (ex.: Wesley Guerreiro / wesley.guerreiro@cruzeiroead.com.br) e o browser abre o lead.
- **Decisão:**
  - Token de org `EDUIT_CRM_TOKEN` (prefixo `eduit_`) + `EDUIT_CRM_BASE_URL` (API, default `https://crm.eduit.com.br`) + `EDUIT_CRM_WEB_URL` (front, default `https://frontend-front.v74knz.easypanel.host`). Deep-link NÃO usa crm.eduit.com.br.
  - Match painel→CRM: e-mail (`app_users.email_cruzeiro` ou username se for e-mail) e nome derivado (`wesley.guerreiro` → `Wesley Guerreiro`). O `GET /api/users` do CRM só aceita sessão NextAuth; com Bearer a busca é `GET /api/conversations?search=` (indexa `assignedTo.name` e `assignedTo.email`).
  - Atribuição: `PUT /api/deals/:id { ownerId }` — o backend do CRM propaga o responsável para o contato e as conversas (`propagateOwnerToContactAndChat`). `POST /api/conversations/:id/actions` (assign) **não** aceita Bearer.
  - Deep-link: `/pipeline?deal={number}` (lead). Fallback `/inbox?c={number}` se não houver negócio.
  - UI: telefone vira botão; modal de confirmação mostra nome do painel + nome/e-mail casado no CRM + lead; só então atribui e abre nova aba.
  - Permissão: `_ensure_academico_interacoes_page()` concede `academico_interacoes` a usuários cuja `categoria` contém "Acadêmico" (inclui Supervisor). Sem isso a página só existia para admin.
  - Config → Editar usuário: checkbox no grupo Acadêmico + presets Acadêmico/Supervisor Acadêmico; páginas de `ALL_PAGES` fora de grupo caem em "Outras" para não sumirem do modal.
- **Endpoints:** `GET /api/academico-interacoes/atender-preview`, `POST /api/academico-interacoes/atender`. `/api/me` passa a devolver `email_cruzeiro` e `display_name`.
- **Alternativas descartadas:** (a) round-robin da distribuição do CRM — o pedido é self-assign do logado; (b) abrir só o Kommo — a fila acadêmica vive no CRM EduIT; (c) exigir `GET /api/users` com Bearer — a rota não aceita token de API.

### 2026-08-21 — Dashboard Comercial: virada da série de RGM (49→50) derrubou vendas; régua de outlier passa a considerar o ciclo inteiro
- **Modelo usado:** Opus 5 (principal). Diagnóstico + fix, com escolha do usuário entre as alternativas ("dominante por ciclo").
- **Sintoma:** após o upload do relatório de matriculados, o KPI caiu (563 bruto / **331** contando), "Fora do padrão RGM" saltou para **230** (prefixo 49 = 225, mais 37/41/44/46/48 com 1 cada) e as vendas dos consultores no ranking diminuíram. Card mostrando "Padrão do período: prefixo **50**".
- **Causa raiz:** a instituição virou a série de RGM de **49 para 50** em **18/08** (confirmado parseando o XLSX: prefixo 50 aparece pela 1ª vez em 18/08 com 72 registros; em agosto inteiro 49=1.634 vs 50=72). A régua `_is_rgm_prefix_outlier` (outlier = prefixo **menor** que o dominante) é correta, mas o **dominante era recalculado sobre as linhas já recortadas pelo filtro de data da tela**. Numa janela curta (últimos dias) os 50 (331) passaram os 49 (225) → dominante virou 50 → os 225 RGMs 49, que são matrículas legítimas do ciclo corrente, viraram outlier e saíram da contagem. Os três sintomas têm a mesma origem porque a régua é aplicada em pontos independentes: KPI (`_crgm_compute_kpis`), ranking (`_build_agent_ranking_completa_vw`), grids/Fora do Padrão (`_crgm_compute_grids`), detalhe do agente (`crgm_agente_detalhe`), `crgm_outlier_context` e `comercial_periodo_vendas_resumo` (painel Suporte).
- **Decisão:** novo `_crgm_ciclo_dominant_prefix()` apura o prefixo dominante por SQL agregado sobre **o(s) ciclo(s) de `ciclo_atual_comercial` inteiro(s)** (mesmos filtros de tipo_matricula/empresa/EM CURSO, todos os snapshots `matriculados`), sem recorte de polo/nível/turma/data — a série de RGM é sequencial na instituição, recortá-la só adiciona instabilidade. Novo `_crgm_effective_dominant_prefix(periodo_rgms)` retorna o **MÍNIMO** entre o dominante do período e o do ciclo, e substitui `_compute_dominant_rgm_prefix` nos 6 pontos acima.
- **Por que o mínimo (e não só o do ciclo):** o mínimo só **afrouxa** a régua em relação ao comportamento anterior — nada que contava antes deixa de contar. Com o dominante do ciclo puro, um período histórico (ex.: 2025, prefixos 44) passaria a ser inteiramente descartado, porque a régua do ciclo atual (49) é mais alta. Casos: hoje (período=50, ciclo=49 → 49, conta 49 e 50 ✓); agosto cheio (49,49 → 49 ✓); período de 2025 (44,49 → 44, preserva o legado ✓); próximo ciclo (ciclo=50 → 49 antigo corretamente descartado ✓).
- **Cache:** resultado memoizado 300s em `_CRGM_CICLO_PFX_CACHE`, limpo junto com `clear_crgm_data_cache()` (upload/sync/CRUD de metas). `_CRGM_DATA_CACHE_VER` 6→7 para invalidar payloads antigos.
- **Conexão própria de propósito:** a consulta abre e fecha `_pg()` em vez de reaproveitar a `conn` do chamador — se falhasse numa conexão emprestada, a transação abortada derrubaria as queries seguintes do KPI/grids.
- **Fallback:** se a consulta do ciclo falhar ou não retornar nada, `_crgm_effective_dominant_prefix` cai no dominante do período (comportamento anterior, sem regressão).
- **Workaround imediato (sem código):** ampliar o filtro de data para o mês/ciclo faz o dominante do período voltar a 49. Resolve na hora, mas volta a quebrar quando os 50 ultrapassarem os 49 na janela larga.
- **Alternativas descartadas:** (a) marcar os 225 como "contar venda" em `comercial_rgm_outlier_contagem` — não escala, cresce todo dia; (b) outlier só se `prefixo < dominante - 1` — resolve a virada com 1 linha, mas enfraquece o filtro de forma permanente (com dominante 49, o 48 — ciclo antigo de verdade — passaria a contar); (c) fixar "aceita 49 e 50" no código — vira manutenção manual a cada virada de série.

### 2026-07-24 — Match/Merge: leads matriculados presos em Aceite (duplicata com gêmeo já em Venda Ganha); fechar a duplicata em vez de só pular
- **Modelo usado:** Opus 4.8 (principal). Diagnóstico + fix de regra, com aval do usuário ("apenas arruma a regra pra nunca mais deixar essas pessoas presas na atualização, os de hoje eu arrumo manualmente").
- **Sintoma:** todo dia sobram pessoas paradas em **Aceite** (ou outra fase ativa) que já matricularam e deveriam estar em Venda Ganha. Testado com a imagem do usuário: `17835279` (Juliana/Juh), `21648173` (Evellyn), `21652797` (Victor) — cada um com **RGM preenchido** e matrícula recente na base SIAA. (Outros da imagem — `21645053`/`21645871`, Aprovado sem matrícula — estão **corretos** em Aceite.)
- **Causa raiz:** cada preso tem um **segundo lead, em Venda Ganha (142), com o MESMO RGM** — o "gêmeo" foi criado pelo próprio pipeline (novo_matriculado/órfão) em vez de mover o lead existente. Isso aconteceu porque o match rodou contra um `kommo_sync` **defasado/zerado** durante o bloqueio 403 de 22–23/07 (tudo parecia "novo" → criou duplicata), somado a dado ruim na base (ex.: Evellyn matriculada com CPF inválido `00000000009`, nome "KAREN ROSA"). Com o gêmeo em 142, a regra anti-duplicidade do bloco **MATRICULADO D-1** (`if ganho_lids & rgm_lids: continue`) passava a **pular para sempre** o lead ativo — que ficava encalhado e reaparecia diariamente.
- **Modelo de atribuição da venda (confirmado no código):** a venda é contada **por RGM**; `_crgm_kommo_lookup_rgms` (`routes/comercial_rgm.py`) escolhe o responsável via `ORDER BY v.rgm, CASE WHEN status_id=142 THEN 0 ELSE 1 END, l.id DESC` — ou seja, **o lead em Venda Ganha vence** (mais recente se houver vários); leads em Aceite/outras fases **não** influenciam. Override seguro por RGM: tabela `comercial_rgm_conflito_resolucao (rgm→user_id)`, que sobrescreve a atribuição **sem tocar no Kommo** (sobrevive ao sync). Consequência-chave: **fechar a duplicata ativa NÃO muda quem leva a venda** (o 142 mantém); logo é seguro.
- **Decisão (fix de regra):** no bloco MATRICULADO D-1 de `gerar_acoes` (`match_merge_lib.py`), quando já existe lead 142 com o mesmo RGM entre os candidatos, em vez de só `continue`, o pipeline agora **gera `MOVER_PERDIDO`** para cada lead ativo do mesmo RGM (via `idx_rgm`) que esteja em pipeline permitido e **não** em 142/143 (e não deletado — passei a carregar `is_deleted` no `lead_pipe`). Fecham como loss reason **"Duplicado - Já Matriculado"** (mecanismo já existente). Não move nenhum lead ativo para 142 (evitaria 2 ganhos/atribuição dupla). Validado por simulação contra dados reais: Juliana→fecha `17835279` mantendo 142 `21653295`; Victor→fecha `21652797` (lixo do Admin Sistema) mantendo 142 `21640507` (Hugo).
- **Escopo/limites:** o gatilho é o bloco D-1, que varre matriculados dos **últimos 7 dias** — cobre a recorrência diária (toda matrícula nova é reconciliada dentro da janela). Encalhados com matrícula >7d o usuário limpa manualmente. A **atribuição** (o 142 é do consultor certo?) continua sendo decisão à parte: pipeline-junk (Origem=SIAA / Admin Sistema) fica com o 142; consultor-vs-consultor real → resolução manual via `comercial_rgm_conflito_resolucao`. Fechamento é reversível (lead vai pra Venda Perdida com motivo claro; override por RGM continua possível).
- **Prevenção de raiz (operacional):** rodar o match/merge só com `kommo_sync` **fresco** — isso evita a criação do gêmeo em 142 (com sync fresco o próprio D-1 **move** o lead do consultor para 142, creditando-o, em vez de criar um 142 novo do Admin Sistema).
- **Alternativas descartadas:** (a) mover a duplicata ativa para 142 — geraria 2 ganhos/duplo-crédito e poderia flipar a atribuição pro lead recém-movido; (b) só limpar os dados de hoje sem tocar na regra — repetiria amanhã (usuário pediu explicitamente a regra); (c) decidir automaticamente atribuição consultor-vs-consultor — arriscado, deixado para resolução manual.

### 2026-07-23 — Match/Merge NOVO: leads criados iam todos para "TI" após regeneração do bearer; fixar `responsible_user_id` = Admin Sistema
- **Modelo usado:** Opus 4.8 (principal). Diagnóstico + correção de dados no Kommo + fix de raiz, com aval do usuário ("arrume e me gere o local atualizado").
- **Sintoma:** ao executar os NOVO do Match/Merge (rodando localmente), os **38 leads criados hoje** (25 Indefinido→Processo Seletivo, 13 Aprovado→Aprovado/Reprovado) caíram **todos no usuário "TI" (id 13304804)**, em vez do padrão histórico. Etapas estavam corretas (por design: Indefinido→PS, Aprovado→AR, Matriculado→Venda Ganha); o erro era **só o responsável**.
- **Causa raiz:** o `create_lead` no bloco NOVO de `executar_acoes` (`match_merge_lib.py`) montava o payload **sem `responsible_user_id`**. O Kommo atribui lead criado por API ao **dono do token**. Ao gerar um **novo bearer** (pra contornar o bloqueio de IP 403 — ver entrada 22/07), o token passou a pertencer ao **TI**; antes era do **Admin Sistema (8261837)**. Comprovação histórica: leads `Origem=SIAA` (marca única dos NOVO) nos 45 dias anteriores eram ~100% **Admin Sistema** (ex.: 21/07=14, 15/07=11, 10/07=11, 06/07=21); **22/07 foi o único dia com TI=38**.
- **Correção de dados (Kommo, aplicada):** reatribuídos os **38 leads** de TI → **Admin Sistema (8261837)** via bulk `PATCH /api/v4/leads` (filtro: criados hoje + `responsible_user_id=13304804` + custom field `Origem=SIAA`, pra não pegar lead alheio). 0 erros; verificado ao vivo.
- **Fix de raiz (código):** nova constante `NOVO_RESPONSIBLE_USER_ID = int(os.getenv("KOMMO_NOVO_RESPONSIBLE_UID", "8261837"))` e o payload do NOVO passa a incluir `"responsible_user_id": NOVO_RESPONSIBLE_USER_ID`. Assim o responsável independe do dono do token (imune a nova regeneração de bearer). MATRICULADO/ATUALIZAR/RESTAURAR não mudam (agem em lead existente via PATCH, preservam o responsável atual). Override por env var se algum dia o padrão mudar.
- **Alternativas descartadas:** (a) distribuir os NOVO em round-robin entre consultores ativos — não é "como sempre foi" (o histórico mostra Admin Sistema, não distribuição); a distribuição real é externa (webhook n8n `edicao_distrib`); (b) regenerar o token pelo Admin Sistema — resolve sem código mas depende do IP do Admin passar e volta a quebrar se o token for trocado de novo; (c) só corrigir os dados sem o fix de código — repetiria no próximo run enquanto o token for do TI.

### 2026-07-22 — Sync Kommo travado em `failed`/`error`: bloqueio transitório (403 WAF) sem retry envenenava o run inteiro
- **Modelo usado:** Opus 4.8 (principal). Diagnóstico + fix imediato + hardening, com aval do usuário ("veja porque a sync está assim e ajustar... já possui acesso ao rep e ao banco").
- **Sintoma:** página "Status das Entidades" do Sync Kommo mostrando `users=error`, `pipelines=failed`, `custom_fields_leads=failed`, `leads=failed`, `contacts=failed`, só `custom_fields_contacts=completed`, com `Sync falhou (código 1)` e "Total de Leads no banco: 0" no log.
- **Diagnóstico:** o `sync_metadata` (PG `kommo_sync`, host `31.97.91.47`) mostrava o último avanço de cursor em **21/07 15:27** (todos `completed`), mas os status atuais estavam `failed`/`error` **sem** timestamp novo — ou seja, um run **posterior** falhou em `set_sync_status(...,'failed')` (que não mexe em `last_sync_at`) sem nunca chegar em `update_sync_metadata` (sucesso). A assinatura "todas as entidades caem de uma vez" + `custom_fields_contacts` ficar `completed` (porque `custom_fields_leads` falha **primeiro** e dá `raise`, abortando `sync_custom_fields` antes do bloco de contatos) prova que **todas as chamadas à API do Kommo falharam simultaneamente** naquele run. O `kommo_sync.log` local terminava em 13/07 → o run que falhou rodou **no servidor de produção** (mesmo PG remoto), não nesta máquina.
- **Causa raiz (código):** em `kommo_lib/api_client.py._request`, qualquer 4xx que não fosse 429/401 caía direto em `response.raise_for_status()` **sem retry**. O Kommo (WAF/nginx/limite de IP — mesmo cenário do bloqueio de 27/06 documentado abaixo) devolve **403** por alguns minutos e depois libera; sem retry, o bloqueio transitório derrubava TODA chamada de uma vez e deixava todas as entidades `failed`/`error` até alguém re-rodar manualmente (ficou ~20h travado; só `lead_responsible_history`, que é job in-process separado, rodou às 07:30 de 22/07).
- **Fix imediato:** rodei `python main.py` (delta) contra o PG remoto → 24 users, 5 pipelines, 122+5 custom fields, 3.527 leads, 999 contacts; todas as entidades voltaram a `completed`. Token/API validados (HTTP 200 ao vivo).
- **Hardening (durável):** `api_client._request` passa a tratar **403 como transitório** — backoff exponencial (`min(2**attempt,30)`) + retry, igual ao 5xx. Se persistir por todas as `max_retries` tentativas, ainda cai no `raise ConnectionError` final (não mascara bloqueio permanente). Não commitado (aguardando o usuário); precisa deploy no servidor de produção pra valer no sync agendado.
- **Observação de arquitetura:** o job agendado `_run_scheduled_sync` (a cada 5 min) roda `sync.py` (**DataCrazy** → DB `dcz_sync`), **não** o `kommo_lib/main.py`. O sync completo de entidades Kommo (users/pipelines/leads/contacts/custom_fields) é disparado pelo **botão manual** (`routes/kommo_sync.py::api_kommo_sync`). Por isso um run manual que falha fica preso até o próximo run manual — o retry de 403 reduz a chance de o run manual falhar por bloqueio transitório.
- **Alternativas descartadas:** (a) só re-rodar e não mexer no código — resolve agora mas repete no próximo bloqueio; (b) retry em 401 — inútil (token não muda no mesmo run); (c) agendar o `main.py` Kommo no scheduler — mudança estrutural maior, fora do escopo do pedido (deixado como sugestão).
- **Follow-up (mesmo dia) — o 403 é penalidade de IP, não taxa; incremental passa a ESPERAR a janela:** após deploy, o log de produção mostrou o 403 estalando **já na 1ª requisição** (`users`, tentativa 5/5) e o `pipelines` seguinte também 403. Como o 403 vem na primeira chamada (antes de qualquer rajada) e o IP residencial (meu run) passa 100%, a causa é **penalidade temporária de IP do Kommo/Cloudflare sobre o IP do servidor (datacenter)**, não excesso de requisições da própria incremental. O retry curto (5 tentativas ~31s) desistia antes da janela passar. **Ajuste (a pedido do usuário: "só esse ajuste, sem mexer no processo, e o erro não aparecer mais"):** `api_client._request` reescrito de `for range(max_retries)` para `while True` com **dois orçamentos separados** — `attempt` (429/5xx/conexão/timeout, limitado por `max_retries`) e **espera por TEMPO só para 403** (`_BLOCK_403_MAX_WAIT`, default 300s, env `KOMMO_403_MAX_WAIT_S`), respeitando `Retry-After`, backoff até 60s; 403 **não** consome o orçamento normal. Assim a incremental **aguarda a penalidade (minutos) passar e conclui**, em vez de falhar. Se persistir além do teto, ainda cai em `raise_for_status()` (não mascara bloqueio longo). Removido o dump verboso de diagnóstico do 403 (headers/corpo) — usuário não quis investigação, quis o fix. Validado: happy-path (`--only users`) e delta completo passam; nada mais mudou (mesmos endpoints/ordem/dados). Precisa **rebuild** no servidor. Se ainda assim persistir por >5min recorrente, aí é bloqueio sustentado de IP → whitelist do IP no Kommo ou egress por IP não-datacenter (fora do escopo de código).

### 2026-07-20 — Conversão rematrícula = 0: `master_key` NULL + categoria errada + captura parcial; correção + backfill DataCrazy
- **Modelo usado:** Opus 4.8 (principal). Correção de dados + backfill executados com aval do usuário ("arrumar urgentemente... atualize o que foi respondido hoje e dos outros dias").
- **Sintoma:** aba Conversão mostrava **Rematrícula 3.143 enviados / 0 respondidos / 0%**, enquanto Processos CAA mostrava retorno. Sem isso o time não tem controle da taxa de resposta nem de quais templates convertem.
- **Varredura (banco `disparos`):** em 10 dias houve só **183 disparos** `processos-caa` (106 únicos) mas **286 respostas** gravadas como `processos-caa` — impossível, logo as respostas de rematrícula (3.179 disparos em 20/07; 3.395 em 15/07) estavam caindo em CAA. Três defeitos somados no webhook n8n:
  1. **`master_key` vem NULL** na maioria das respostas (grava só `external_id`/`telefone`/`rgm`). O painel correlaciona resposta↔disparo por `master_key`+`category` — sem `master_key` a resposta **nunca conta**, mesmo com categoria certa. Esse é o motivo do 0 (a decisão 2026-07-15 só tratou categoria, não o `master_key`).
  2. **Categoria fixa `processos-caa`** (mesma raiz da entrada 2026-07-15).
  3. **Captura parcial (~5%)**: o webhook registra uma fração dos inbounds. Confirmado via DataCrazy (`GET /api/v1/conversations`, sort `-lastReceivedMessageDate`, `contact.externalId`=`datacrazy_lead_id`): disparo de 20/07 teve **177 respostas reais** (~5,6%), o webhook capturou só **8**.
- **Chave da reconstrução:** o disparo grava `master_key='RGM:'||rgm` e `datacrazy_lead_id`; a resposta tem `rgm` preenchido (234/270) e `datacrazy_lead_id` (244/270). Logo dá pra reconstruir `master_key='RGM:'||rgm` e derivar a categoria pelo disparo mais recente em 72h (match por `master_key` efetiva OU `datacrazy_lead_id`). **Telefone não casa** (resposta `5511…` 13 díg. vs disparo `11…` sem DDI).
- **Fase 1 — correção do que foi capturado (`activation_responses`, `received_at >= 2026-07-10`):** 50 linhas atualizadas (46 `processos-caa`→`rematricula`, 5→`financeiro`, 32 `master_key` preenchidos) + 1 duplicata removida (conflito com o unique `(external_id, category, dia)` — categoria-destino já existia no mesmo dia). Backup em `_backup_convfix_20260720_120412.csv`. Painel passou de 0 → 49 respostas rematrícula correlacionáveis (por dia do disparo: 20/07=8, 15/07=28, 08/07=3, 07/07=12).
- **Fase 2 — backfill das respostas REAIS via DataCrazy (só o que o webhook perdeu):** para os destinatários do disparo de 20/07, cruzou `datacrazy_lead_id`↔`contact.externalId`; responder = `lastReceivedMessageDate` entre o disparo e +72h. Inseriu **169 respostas** novas (dedup por `master_key`+`rematricula`+dia; as 8 já capturadas ficam), marcadas `origem_ativacao='datacrazy_backfill'`, `external_id='dcbf_'||convId`, `response_kind='message'`. Painel de 20/07 foi de 8 → **177** (~5,6%). Reversível: `DELETE FROM activation_responses WHERE origem_ativacao='datacrazy_backfill'`.
- **Limitação retroativa (dias antigos):** o DataCrazy `/conversations` só expõe o **último** inbound (`lastReceivedMessageDate`); dias após o disparo ele já foi sobrescrito por mensagens novas. Por isso 15/07 só pôde ficar com os 28 capturados pelo webhook (medir retroativo daria 10, subestimado). **Regra operacional:** rodar o backfill **no mesmo dia do disparo**.
- **Ferramenta durável:** `scripts/backfill_conversao_datacrazy.py` (dry-run por padrão; `--apply`, `--category=`, `--since=`, `--window=`). Rodar logo após cada disparo mantém o painel fiel. Reversível pela tag.
- **Correção de raiz (FORA deste repo, n8n):** o webhook precisa (a) **gravar o `master_key`** (`'RGM:'||rgm` ou resolver por `datacrazy_lead_id`), (b) **derivar a categoria** do disparo mais recente em 72h (parar de fixar `processos-caa` — ver spec 2026-07-15), e (c) **capturar todas as mensagens recebidas** (hoje perde ~95%). Enquanto não corrigido, a fonte de verdade é o DataCrazy `/conversations` via o script acima.
- **Alternativas descartadas:** (a) só re-etiquetar categoria (2026-07-15) — não resolve, o `master_key` NULL mantém o 0; (b) backfill retroativo de dias antigos via `/conversations` — impreciso (último inbound sobrescrito); (c) varrer `/messages` por conversa p/ recuperar dias antigos — 3k+ chamadas, custo alto, não feito.

### 2026-07-15 — Conversão (disparador): respostas de rematrícula/financeiro sendo gravadas como `processos-caa` (misatribuição no webhook n8n)
- **Modelo usado:** Opus 4.8 (principal). Correção de dados executada com aval do usuário ("realiza ambas").
- **Sintoma reportado:** disparo de rematrícula (3.395 envios em 15/07) aparecia com **0 respondidos / 0%** na aba Conversão, enquanto CAA mostrava retorno normal. O usuário insistiu que "só rematrícula não conta" — estava certo.
- **Causa raiz (banco `disparos`, mesmo host, DB `disparos`):** o webhook do n8n que grava `activation_responses` estava atribuindo a **categoria errada**. Respostas de quem recebeu **só** o disparo de rematrícula (ex.: `RGM:41145658`, `RGM:45086290` — sem nenhum disparo CAA) foram gravadas como `category='processos-caa'`. A aba Conversão correlaciona resposta↔disparo por `master_key`+`category` (decisão 2026-06-10); com a categoria errada, a resposta não casa com o disparo de rematrícula → rematrícula zera e **CAA infla** (44 respostas cruas em 6h vs 25 disparos). Quebra começou **~13/07**, quando a campanha de CAA entrou no ar (o fluxo de resposta do CAA no n8n virou "pega-tudo"). Antes disso, rematrícula era gravada correta (2.241 respostas históricas conferidas; ondas de 386 em 07/07, 845 em 23/06).
- **Diagnóstico quantitativo (respostas de hoje, regra "categoria = disparo mais recente antes da resposta, dentro de 72h"):** 22 respostas → 7 CAA corretas, **6 que eram rematrícula**, 1 financeiro, 9 sem disparo casável em 72h.
- **Correção de dados aplicada (parte 1):** re-etiquetadas **7 linhas** (`received_at >= 2026-07-13`) em `activation_responses` para a categoria do disparo mais recente antes da resposta (72h): 6→`rematricula`, 1→`financeiro`. Backup em `_backup_catfix_20260715_143713.csv` (id, external_id, master_key, cat_antiga, cat_nova, received_at). **0 conflitos** com o unique `(external_id, category, dia)`. Só toca linhas onde a categoria correta é determinável e diverge; as 9 "sem disparo em 72h" ficaram intactas. Escopo `>= 13/07` porque antes disso não havia misatribuição. Pós-fix: painel mostra rematrícula=6 respondidos, CAA=5 (correto).
- **Correção da raiz (parte 2 — FORA deste repo, no n8n):** o workflow de resposta do n8n deve **derivar a categoria dinamicamente** em vez de assumir CAA. Regra correta (mesma do painel):
  ```sql
  -- categoria = a do disparo mais recente da pessoa ANTES da resposta, dentro de 72h
  COALESCE(
    (SELECT d.category FROM activation_dispatch_events d
     WHERE d.master_key = :resp_master_key           -- 'RGM:<num>' do respondente
       AND d.created_at <= :resp_received_at
       AND d.created_at >= :resp_received_at - interval '72 hours'
     ORDER BY d.created_at DESC LIMIT 1),
    :fallback_atual)                                  -- só se não houver disparo casável
  ```
  Se o webhook não tiver o `master_key`, casar por `datacrazy_lead_id`/`telefone` contra `activation_dispatch_events`. O ponto central: **parar de gravar `processos-caa` fixo** — a categoria da resposta tem que vir do disparo que a originou. Caveat multi-campanha: pessoa com disparos de 2 categorias → vence o **mais recente antes da resposta** (idêntico à correlação do painel; ver 2026-06-08 sobre UNIQUE por `(external_id, category, dia)` permitir contar em N categorias quando há disparos pendentes distintos).
- **Blindagem opcional (parte 3 — repo externo `tool_whatsapp_alunos`, não feita):** o painel poderia ignorar `response.category` e derivar a categoria pelo disparo (via `master_key`) na hora de contar, ficando imune a erro do webhook. Não aplicada (repo externo, fora desta máquina). Reabrir se o n8n não for corrigido.
- **Alternativas descartadas:** (a) tratar como "timing/normal" — refutado pelos dados (respostas chegando e caindo em CAA); (b) corrigir só o painel sem tocar dados — não desfaz a inflação já gravada no CAA; (c) re-etiquetar todo o histórico — desnecessário (pré-13/07 estava correto) e arriscado para relatórios passados.
- **Follow-up (descoberta maior — o webhook captura só uma fração das respostas):** ao usuário reportar que "a fila cresceu bastante" e 6 estava baixo demais, cruzei os destinatários do disparo com a **API do DataCrazy** (`GET https://api.g1.datacrazy.io/api/v1/conversations`, sort `-lastReceivedMessageDate`, campos `contact.contactId`=telefone, `contact.externalId`=datacrazy_lead_id, `lastReceivedMessageDate`, `isPending`, `finished`). Resultado do disparo de 15/07 (~3,5h após): **~371–374 pessoas responderam (≈11% de 3.357)**, corroborado por 2 chaves independentes (datacrazy_id=371, telefone-8=335). Destes, 56 ainda `isPending` (na fila) e 251 `finished`. **Mas o `activation_responses` só tinha ~19 dessas 374** (≈5%). Conclusão: o problema principal **não** é só a categoria errada — o webhook do n8n **não registra a maioria das respostas** (grava só uma fração, e ainda por cima na categoria errada). Portanto **o painel de Conversão é não-confiável para volume de rematrícula**; a fonte de verdade é o DataCrazy (`/conversations`).
- **Implicação para o fix de raiz:** além de corrigir a categoria (parte 2 acima), o webhook precisa **capturar todas as mensagens recebidas** (hoje perde ~95%). Alternativa robusta de leitura: o painel/tool consultar o DataCrazy `/conversations` (cruzando `externalId`/telefone com os dispatches) em vez de depender só do `activation_responses`. Método de medição validado: contar conversas com `lastReceivedMessageDate >= início_do_disparo` cujo `contact.externalId ∈ datacrazy_lead_id dos dispatches` (ou telefone dos últimos 8 dígitos como corroboração).

### 2026-07-14 — Dashboard Comercial: ranking/KPIs alinham a Matrículas Oficiais (bypass ciclo_atual com datas + all_snapshots)
- **Modelo usado:** Composer/Auto + Strategist (diagnóstico); Executor (Sonnet 4.6) implementou.
- **Problema:** Rahi tinha 8 matrículas oficiais vs 6 no Dashboard Comercial (RGMs 49497464, 49504291 ausentes); "Consultar RGM" retornava "não encontrado na base" para alunos de ciclo ≠ ciclo_atual.
- **Causa raiz:**
  1. `_crgm_compute_kpis/_agentes/_grids` chamavam `_crgm_periodo_data` SEM `dt_ini/dt_fim` → forçava `ciclo IN ciclo_atual_comercial`; matrículas do dia com ciclo diferente somiam.
  2. `_build_agent_ranking_completa_vw` lia `comercial_rgm_atual` (view Postgres = EM CURSO + ciclo_atual) — mesmo gap.
  3. Snapshot: `_crgm_periodo_data` usava só o snapshot mais recente; Matrículas Oficiais (`_fetch_agent_matriculas` em `minha_performance.py`) usa TODOS snapshots `tipo='matriculados'` com `DISTINCT ON … ORDER BY s.id DESC`.
- **Decisão:**
  - Novo parâmetro `all_snapshots=False` em `_crgm_periodo_data`: quando `True`, usa `s.tipo = 'matriculados'` (todos os snapshots) em vez de só o mais recente, e adiciona `s.id DESC` no ORDER BY do DISTINCT ON.
  - Novo parâmetro `situacao_filter=None` em `_crgm_periodo_data`: quando fornecido, filtra situação na camada deduplicada.
  - Novo helper `_crgm_periodo_data_oficial(...)`: chama `_crgm_periodo_data(..., all_snapshots=True)`. Usado em todas as funções que devem bater com Matrículas Oficiais.
  - `_crgm_compute_kpis`, `_crgm_compute_agentes`, `_crgm_compute_grids`: substituem `_crgm_periodo_data(...)` por `_crgm_periodo_data_oficial(dt_ini=_pd_dt_ini, dt_fim=_pd_dt_fim, ...)` — bypass de ciclo_atual já existia quando datas são passadas.
  - `_build_agent_ranking_completa_vw`: substitui SELECT FROM `comercial_rgm_atual` por `_crgm_periodo_data_oficial(situacao_filter='EM CURSO')`; fix no bloco `supp_cw` para também remover `ciclo_atual_comercial` quando `dt_ini or dt_fim` (antes só removia quando `ciclo` manual).
  - `_crgm_dashboard_rgm_list`: substitui SELECT FROM `comercial_rgm_atual` por `_crgm_periodo_data_oficial(situacao_filter='EM CURSO')`.
  - `crgm_rgm_atribuicao`: adiciona fallback quando não encontrado na view — query DISTINCT ON em todos os snapshots matriculados para o RGM específico.
- **Escopo desta entrega:** só o painel Dashboard Comercial (`routes/comercial_rgm.py`). `minha_performance` (Matrículas Oficiais / ranking interno) e `supervisor_dashboard` **não** foram alterados.
- **View `comercial_rgm_atual` preservada:** intacta no banco — congelamento de ciclo depende dela. Apenas a camada Python do painel comercial parou de usá-la para períodos com datas.
- **Alternativas descartadas:** mudar a view; só avançar `ciclo_atual_comercial` como "fix"; forçar Matrículas Oficiais a usar a view; ampliar de cara para supervisor/MP.
- **Trade-off conhecido:** KPIs/ranking do painel comercial com filtro de data podem subir (convergência com Matrículas Oficiais — não é regressão, é correção). Sem datas (estado atual) segue com ciclo_atual via SQL.

### 2026-07-14 — Ranking Comercial: recuperar matrícula ativa que some do último relatório SIAA (janela ancorada no dia 01 da meta)
- **Modelo usado:** Opus 4.8 (principal). Decisão aprovada pelo usuário (opção 3 — janela de tolerância).
- **Problema:** consultor Rahi aparecia com **6** matrículas no Ranking do Dashboard Comercial (08/07) mas **8** na Minha Performance. Investigação: as duas telas usam fontes diferentes — o **Ranking** (`_build_agent_ranking_completa_vw` → view `comercial_rgm_atual`) lê **só o último snapshot SIAA**; a **Minha Performance** (`_fetch_agent_matriculas`) usa o **último status visto** de cada RGM em **qualquer** snapshot. Quando uma matrícula EM CURSO some do relatório SIAA mais recente sem virar cancelada (volatilidade do relatório — os 2 RGMs do Rahi, 49497464 e 49504291, estavam em relatórios de 10–13/07 e sumiram do de 14/07, mesmo com o relatório total *crescendo*), ela cai do ranking mas fica na Performance. Impacto geral no período: **~17 matrículas** de 8 consultores (Rahi, Gabriel Messias, Hugo, Beatriz, Juliana, Paloma, Kamilly, Claudia).
- **Decisão:** no bloco "supp" de `_build_agent_ranking_completa_vw`, **ancorar a janela de recuperação no início da meta (dia 01 do mês)** em vez de limitar a uploads feitos `<= dt_fim`. Ou seja: recupera RGMs que estiveram **EM CURSO em QUALQUER relatório enviado a partir do dia 01** (`s.uploaded_at::date >= meta_start`, `meta_start = (dt_ini or dt_fim)[:7]+'-01'`), com `data_matricula` dentro do período filtrado (`>= dt_ini AND <= dt_fim`), escopo/ciclo atuais, e que **não estejam no último relatório**. Cobre tanto **cancelamento-pós-meta** (regra que já existia por design: "EM CURSO durante a meta, cancelado depois = conta") quanto **sumiço por volatilidade do SIAA**.
- **Bug lateral corrigido (crítico):** o `_DM_EXPR` do bloco supp usava `E'^\\d{2}/\\d{2}/\\d{4}$'`. Em **E-string do Postgres, `\d` colapsa para `d`** (verificado: `'08/07/2026' ~ E'^\d{2}...'` → **FALSE**), então o filtro de data do supp **nunca casava datas dd/mm/yyyy** — a recuperação de cancelados-pós-meta estava **silenciosamente inoperante** em qualquer view com filtro de data. Trocado para classe `[0-9]` em string normal (padrão que `_crgm_periodo_data` já usava). *(Bug análogo latente na regex de polo do supp `E'^\\d+\\s*[-–]\\s*'` — só afeta quando há filtro por polo; NÃO corrigido nesta entrega, anotado para depois.)*
- **Escopo:** aplicado **apenas ao Ranking por consultor** (view que define crédito/pagamento). O **KPI-topo "vendas"** e o heatmap de KPIs (`_crgm_compute_kpis` / `_crgm_periodo_data`) continuam lendo só o último snapshot — podem ficar alguns poucos abaixo da soma do ranking. Alinhar o KPI exigiria mexer em `_crgm_periodo_data` (usado também em comparativos 6m/1a e no contexto de outlier) e no tratamento sem-data; deixado como decisão separada a pedir aval.
- **Guarda:** o bloco só roda quando `dt_fim` está setado (view com período/filtro de data). A view padrão do dashboard manda o período da meta, então cobre o caso; view "sem data nenhuma" não recupera (comportamento pré-existente).
- **Fix follow-up (grid / cross-filter por dia):** a 1ª entrega só somava o RGM recuperado em `rgm_nome` (→ `matriculas_periodo`), mas **não** em `rgm_date_map`. O `matriculas_grid` (RGM→data, `if not dt_str: continue`) descartava os recuperados, e o **cross-filter por dia do front** (`_crgmCrossFilter.date`, filtragem client-side sobre `matriculas_grid`) **não os contava** — por isso o ranking do período mostrava 8, mas ao filtrar o dia 08/07 voltava a 6. Correção: a query supp passou a trazer também `{_DM_EXPR} AS dm` e o loop popula `rgm_date_map[n]` com a data de matrícula do recuperado. Validado: grid do Rahi em 08/07 = **8** (antes 6). O comentário antigo em `matriculas_grid` ("suplementares não têm data rastreada… diferença marginal") ficou desatualizado e foi corrigido.
- **Fix follow-up 2 (conflito com o dedup de PÓS multi-ciclo):** após consertar o regex, a recuperação passou a trazer **79 RGMs** no período 01–14/07 — **65 concentrados no dia 06/07**, sendo **64 PÓS**. Investigação: esses 64 estão **presentes e EM CURSO no último relatório (snapshot 188), mas com ciclo `2026/1`** (o **dedup de PÓS multi-ciclo** de 07/07 os rebaixou para o ciclo antigo). Como a supp varre uploads desde o dia 01, ela pegava a **linha 2026/2 antiga** desses alunos e os **puxava de volta para 2026/2**, desfazendo o dedup (as duas features brigavam). Decisão do usuário: **respeitar o dedup**. Correção: a supp passou a excluir qualquer RGM que esteja **EM CURSO no último relatório em QUALQUER ciclo** (subquery `NOT IN` sobre o último snapshot). Assim recupera só **sumiço real** (ausente do último relatório, ex. Rahi) e **cancelado-pós-meta** (presente mas não-EM CURSO). Resultado: recuperados **79 → 15**; Rahi 08/07 mantém **8**; os 64 pós ficam em 2026/1 (como o dedup definiu).
- **Alternativas descartadas:** (1) *não mexer* — some do ranking, prejudica pagamento do consultor mesmo com matrícula ativa; (2) *alinhar o ranking à lógica last-seen da Performance* — passaria a contar matrícula que saiu do SIAA de verdade sem confirmar cancelamento; (3-escolhida) *janela de tolerância ancorada no dia 01* — recupera sumiços do ciclo/meta corrente sem puxar histórico antigo (o filtro de `data_matricula` no período + ciclo atual delimita).

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

### 2026-07-07 — Dist. Comercial: botão TURNO DIA/NOITE respeita snapshot anterior (comportamento esperto)
- **Modelo usado:** Opus 4.7 (principal). Implementação direta após aprovação da opção "esperto".
- **Contexto:** O botão `TURNO NOITE/DIA` na UI forçava **TODOS** os consultores do turno alvo pra `ATIVO`, ignorando quem estava `INATIVO` por folga/férias. O gestor precisava lembrar de ajustar manualmente antes de SALVAR — se esquecesse, ativava alguém de férias.
- **Decisão:** `dcAplicarTurno(turnoAlvo)` no `static/js/dist_comercial.js` passa a buscar o snapshot do turno alvo (`GET /api/dist-comercial/snapshot`) **antes** de calcular o preview. Comportamento:
  - **Se snapshot existe:** turno alvo recebe status do snapshot (ATIVO/INATIVO). Turno oposto → INATIVO. Consultores no turno alvo mas fora do snapshot (novos) → ATIVO por default.
  - **Se snapshot não existe (primeira aplicação):** comportamento bruto anterior — todos do turno alvo ficam ATIVO.
- **UX:** modal de confirmação mostra explicitamente qual dos dois caminhos vai executar ("respeitando snapshot anterior" vs "primeira aplicação").
- **Backend inalterado:** endpoint `/apply/<turno>` do job automático continua com a lógica original (`_compute_target_statuses`) que já respeitava o snapshot desde o início. Essa mudança alinha a UI manual à mesma semântica que a regra automática já usava.
- **Trade-off:** um round-trip HTTP a cada clique de TURNO DIA/NOITE (~200ms). Aceitável — é gesto manual, não critical path.
- **Alternativas descartadas:**
  - **Botão só inativa o turno oposto, não toca no alvo:** UX confusa ("por que clicar em TURNO DIA se ele não ativa ninguém?").
  - **Manter bruto e treinar gestor:** o requisito explícito foi "não ativar todos" — vale mais gastar o round-trip que apostar em disciplina operacional.
  - **Cachear snapshot no `dcState` e evitar o fetch:** cache fica stale (outro gestor pode ter clicado limpar snapshot em outra aba). Fetch on-demand é mais seguro.

### 2026-07-07 — Dist. Comercial: regras de turno vira modelo de JANELA (revisa entrada abaixo)
- **Modelo usado:** Opus 4.7 (principal). Implementação direta após aprovação (usuário escolheu "modelo de janela").
- **Contexto:** A entrada abaixo (mesmo dia, mais cedo) implementou regras como `{hora, turno_alvo}` — cada regra dispara UMA vez ao chegar naquele horário. Pra ter "noite ativa das 17:00 às 22:00 + dia o resto", o gestor precisava criar DUAS regras separadas (17:00→NOITE e 22:00→DIA) e mentalmente entender que uma "cancela" a outra. UX confusa e pouco explícita sobre o efeito operacional.
- **Decisão:** Trocar o modelo pra **janela horária**. Cada regra descreve uma janela inteira `{hora_inicio, hora_fim, turno_alvo}`. Semântica: `NOITE das 17:00 às 22:00` significa "aplica Modo NOITE às 17:00; aplica Modo DIA (turno oposto) às 22:00". UMA regra = DOIS gatilhos por dia.
- **Schema (migration idempotente em `_ensure_dist_comercial_schedule_tables`):**
  - `dist_comercial_schedule.hora` → **removido**; substituído por `hora_inicio TIME NOT NULL` + `hora_fim TIME NOT NULL`.
  - `dist_comercial_schedule.last_run_date` → **removido**; substituído por `last_run_inicio_date DATE` + `last_run_fim_date DATE` (dedup independente por gatilho).
  - Bloco `DO $$ ... $$` no CREATE detecta versão anterior (col `hora` existe) e migra: renomeia `hora`→`hora_inicio`, deriva `hora_fim = hora + 5h` como valor padrão de migração, dropa colunas antigas. Idempotente.
  - Tabela tinha 0 regras no momento da migração — nenhuma perda de dados. Se em produção houver regras existentes, o fallback `+5h` é seguro (o gestor pode ajustar depois).
- **Job APScheduler `_run_scheduled_apply` refatorado:**
  - A cada minuto varre regras `enabled = TRUE`.
  - Pra cada regra monta lista de gatilhos pendentes:
    - Se `now >= hora_inicio` AND `last_run_inicio_date != today` → `(rule_id, 'inicio', turno_alvo)`.
    - Se `now >= hora_fim` AND `last_run_fim_date != today` → `(rule_id, 'fim', oposto)` onde `oposto = 'noite' se turno_alvo=='dia' else 'dia'`.
  - Executa cada gatilho pelo `_apply_turno` normal e marca a coluna correspondente. Concorrência protegida pelo mesmo `_apply_lock` (sem mudança).
  - Grace: se o servidor estava parado no minuto exato, o gatilho ainda dispara no próximo tick (dedup só impede repetir no mesmo dia).
- **UI redesenhada (`dist_comercial.js` + CSS em `_dist_comercial.html`):**
  - Regra vira **card** com badge do turno (NOITE=azul #4c6ef5, DIA=laranja #f59e0b), inputs `hora_inicio`/`hora_fim`, select de turno, toggle enabled, botão remover.
  - **Timeline visual 24h** por regra: barra colorida mostrando o segmento da janela alvo vs oposto; marcadores 00h/06h/12h/18h/24h; indicador vermelho na hora atual. Cobre cross-midnight (ex: 22:00 às 06:00 pinta 2 segmentos).
  - Texto explicativo abaixo da timeline: "Das X às Y: aplica Modo NOITE (DIA inativo, NOITE conforme snapshot). Às Y: volta pro Modo DIA (NOITE inativo, DIA conforme snapshot)".
  - `dcAdicionarRegra` pede turno → hora_inicio → hora_fim (com defaults inteligentes: NOITE = 17:00-22:00, DIA = 07:00-17:00).
- **Endpoints:** `POST /rules` e `PATCH /rules/<id>` aceitam `hora_inicio` + `hora_fim` (era `hora`). Validação: `hora_inicio != hora_fim`, ambas HH:MM válidas 00:00-23:59. `GET /rules` retorna o novo formato.
- **Compatibilidade:** front antigo (usando `hora` no body) quebraria — não é problema porque só o próprio `dist_comercial.js` chamava e foi atualizado junto. Backend não aceita mais o campo `hora`.
- **Alternativas descartadas:**
  - **Manter modelo antigo (2 regras) + melhorar preview visual:** simples de fazer mas continua com a fricção do gestor precisar criar 2 regras e pensar no "complemento". Descartado porque o usuário disse explicitamente "precisa estar claro visualmente".
  - **UMA regra com N janelas (array de intervalos):** flexível pra futuros casos como "manhã + noite, tarde livre", mas overkill agora — se surgir, criar múltiplas regras cobre o mesmo caso.
  - **Suportar `hora` legado no backend + auto-derivar `hora_fim`:** compat mais robusta, mas o front foi migrado junto e não há callers externos. Custo sem valor.
- **Trade-off conhecido:** múltiplas regras podem definir janelas conflitantes (ex: NOITE 17-22 + DIA 15-20 se sobrepõem). O último gatilho a disparar ganha (comportamento consistente: aplica o Modo X, que sobrescreve o anterior). Aceitável — é responsabilidade do gestor não configurar janelas conflitantes; a timeline visual ajuda a evitar isso.

### 2026-07-07 — Dist. Comercial: regras automáticas de troca de turno (Dia/Noite) com snapshot
- **Modelo usado:** Opus 4.7 (principal). Implementação direta após aprovação das decisões críticas (execução no backend, escopo global, snapshot no clique manual, múltiplas regras).
- **Problema:** Operação alterna entre turnos (dia/noite) todo dia. Ativar/inativar consultores manualmente 2x ao dia é operacional e sujeito a esquecimento. Regra automática precisa **respeitar quem estava ativo da última vez** — não pode ativar todos do noturno, porque nem todos trabalham todo dia (folga, férias, etc.).
- **Decisão:** Módulo aditivo com 4 tabelas novas + blueprint Flask + job APScheduler:
  - **`dist_comercial_schedule(id, hora, turno_alvo, enabled, last_run_date, last_run_at, last_run_result)`** — regras `{HH:MM, dia|noite}`. Múltiplas permitidas (ex: 07:00→DIA e 20:00→NOITE). `last_run_date` faz dedupe (não dispara 2x no mesmo dia).
  - **`dist_comercial_turno_map(id_lead PK, turno)`** — mapa global promovido do localStorage anterior. Todos os gestores compartilham a mesma divisão. Frontend faz PUT do mapa completo a cada toggle.
  - **`dist_comercial_snapshot(turno PK, payload JSONB, taken_at, taken_by)`** — payload `{id_lead: 'ATIVO'|'INATIVO'}` dos consultores do turno alvo, capturado no clique manual dos botões TURNO DIA/NOITE + SALVAR. É o "quem estava ativo da última vez".
  - **`dist_comercial_apply_log`** — histórico imutável de disparos (manual/auto) pra auditoria.
- **Algoritmo `_apply_turno(turno_alvo)`:**
  1. Se `snapshot[turno_alvo]` não existe → **aborta** com erro claro. Isso protege contra ativar/inativar tudo por acidente na primeira execução.
  2. Consultores com `turno_map == outro_turno` → força **INATIVO** (independente do status atual).
  3. Consultores com `turno_map == turno_alvo` → aplica **status do snapshot** (preserva ATIVO/INATIVO da última aplicação).
  4. Consultores fora do `turno_map` → **não toca** (segurança: quem não foi categorizado não é modificado).
  5. Chama `POST /webhook/edicao_distrib` (n8n) — mesma rota que o botão SALVAR usa. Único ponto de contato com o CRM.
- **Execução:** APScheduler roda `_run_scheduled_apply` a cada 1 min, busca regras `enabled AND hora <= now.time AND (last_run_date IS NULL OR last_run_date < today)`. Concorrência protegida por `_apply_lock` (threading.Lock) — impossível 2 disparos paralelos.
- **UI:** botão novo **⚙ REGRAS** no header abre modal com:
  - Cards de snapshot (dia/noite): mostra timestamp, contagem ATIVO/INATIVO, quem tirou, botão limpar. **Estado vazio destacado em vermelho** — deixa óbvio que a regra não vai rodar sem snapshot.
  - Lista de regras editável inline (horário + turno + enable/disable + delete).
  - Histórico das últimas 20 execuções em `<details>` colapsável.
- **Endpoints (todos com gate `dist_comercial` server-side, admin passa direto):**
  - `GET/POST/PATCH/DELETE /api/dist-comercial/rules[/<id>]`
  - `GET/PUT /api/dist-comercial/turno-map`
  - `GET/POST/DELETE /api/dist-comercial/snapshot[/<turno>]`
  - `POST /api/dist-comercial/apply/<turno>` (dispara manualmente; útil pra debug)
  - `GET /api/dist-comercial/apply-log?limit=N`
- **Alternativas descartadas:**
  - **Frontend com setInterval + localStorage:** só roda com aba aberta. Basta o gestor fechar o navegador antes do horário pra operação inteira ir pro turno errado a noite toda. Descartado pelo requisito "cautela e atenção nos detalhes".
  - **Snapshot no primeiro disparo automático (auto-capture):** menos controle explícito, mais mágico. O usuário escolheu "no clique manual" — o gesto de aplicar TURNO NOITE + SALVAR já é a intenção clara de "este é o padrão do turno".
  - **Snapshot = estado atual do CRM lido no momento do disparo:** eliminaria a necessidade da tabela snapshot, mas perderia a semântica "última aplicação manual" (o estado atual pode ter sido tocado durante o dia por outros motivos).
  - **Uma regra global por turno (Dia/Noite) em vez de N regras:** limitaria futuros casos como "domingo aplica turno diferente". Múltiplas regras cobre com o mesmo custo.
  - **Escopo por usuário:** fragmentaria a operação. Global é a fonte única.
- **Compatibilidade:** aditivo puro. Se o módulo for removido, basta desregistrar o blueprint + o job + `DROP TABLE dist_comercial_*`. Painel principal continua funcionando 100% (webhooks n8n de load/save/create não mudaram).
- **Trade-off conhecido:** o `turno_map` global significa que se dois gestores discordarem sobre quem é dia/noite, o último a mover ganha. Aceitável — é uma escala operacional, deveria ter consenso.
- **Migração transparente do localStorage:** ao abrir a página pela primeira vez após deploy, o `loadDistComercial` chama `GET /turno-map`, que vem vazio → todos aparecem no Dia. Se o gestor tinha divisão salva no localStorage antigo (`dist_comercial_turno_noite_v1`), essa chave fica órfã (não é lida). Se ele quiser recuperar, precisa remover manualmente da UI (mover pra noite novamente). Custo aceitável — divisão nova é global e a antiga era per-browser.

### 2026-07-07 — Rate limit Kommo: throttle + mutex + escalonamento (root cause do bloqueio 27/06 03:00)
- **Modelo usado:** Opus 4.7 (principal). Implementação direta após aprovação.
- **Problema:** Kommo bloqueou a conta em 27/06/2026 03:00 AM por passar de 7 req/s. Auditoria revelou 4 fontes de request paralelas disparando simultaneamente no minuto :00 de cada hora — no 03:00 é pior porque `responsible_history_daily` (03:00) coincide com `sync_delta_interval` (:00,:05,...), `funnel_cache_warm` (:00,:05,...) e `aceite_reconcile` (:00,:10,...). Sem mutex compartilhado nem throttle uniforme, o pico agregado chegava a 15–30 rps.
- **Hotspots identificados (rps sem freio antes da correção):**
  - `_count_leads_in_stage` (funnel warm): `sleep(0.08)` = ~12 rps máx.
  - `_count_new_leads_between` (novos/dia): `sleep(0.05)` = ~20 rps máx.
  - `reconcile_aceite_leads` paginação: `sleep(0.05)` = ~20 rps máx.
  - `reconcile_aceite_leads` GET individual stale: `sleep(0.05)` = ~20 rps máx.
  - `_sync_responsible_history` paginação `/events`: **zero sleep** — 5–10 rps sustentado.
  - `_fetch_leads_em_atendimento` (leads_parados): `sleep(0.05)`.
  - `_fetch_funnel_live` (fallback): `sleep(0.05)`.
  - `kommo_lib/api_client.py` (subprocess sync): **já tinha** `RateLimiter(120, 60)` = ~1,8 rps. Único que estava OK, isolado.
- **Decisão em 3 camadas (aditivas, sem mudança de schema):**
  1. **Camada 1 — throttle uniforme:** todos os loops que usam `_kommo_get` (routes/kommo_sync.py) passam a esperar `_KOMMO_BG_PAGE_SLEEP = 0.20s` entre páginas (env var `KOMMO_BG_PAGE_SLEEP` sobrescreve). Cap teórico por loop: ~5 rps. `_sync_responsible_history` ganha o sleep que **não tinha antes**.
  2. **Camada 2 — mutex global background:** novo `_kommo_api_bg_lock = threading.Lock()` em `routes/kommo_sync.py`. Adquirido em `_warm_funnel_cache_sync` (timeout 300s), `reconcile_aceite_leads` (timeout 300s), `_sync_responsible_history` (timeout 600s). Se timeout, pula silenciosamente. **Sync via `kommo_lib/main.py` fica FORA do lock** — tem seu próprio rate limiter (1,8 rps) e é o único caminho oficial de sync massivo; misturar no lock atrasaria demais o delta a cada 5min. Endpoints admin (`/api/kommo/reconcile-aceites`, `/api/kommo/sync-responsible-history`) também passam pelo lock (bloqueiam o worker HTTP até a vez, aceitável em admin trigger).
  3. **Camada 3 — escalonar cron:**
     - `sync_delta_interval`: mantido em `:00, :05, :10, ...` (5 min).
     - `funnel_cache_warm`: mudou de `IntervalTrigger(minutes=5)` (que pegava :00, :05, :10) para `CronTrigger(minute="2,7,12,17,22,27,32,37,42,47,52,57")` — offset +2 min.
     - `aceite_reconcile`: quando `ACEITE_RECONCILE_INTERVAL=10` (default), passa a usar `CronTrigger(minute="3,13,23,33,43,53")` — offset +3 min. Env var custom volta pro IntervalTrigger antigo (compat).
     - `responsible_history_daily`: movido de **03:00 → 04:30 BRT** (fora do horário nobre de sync/warm/reconcile).
- **Cap teórico pós-fix:**
  - 1 job background × 5 rps (throttle 0,2s) + sync kommo_lib 1,8 rps + endpoints ad-hoc = **~7 rps de teto real, exatamente no limite** do que Kommo tolera. Bursts virtualmente impossíveis (mutex garante 1 background por vez; escalonamento espalha os minutos).
- **Compatibilidade:**
  - **Latência aceitável:** aceite reconcile ~30s → pode ir a ~2 min no pior caso (500 stale × 0,2s + paginação). Roda a cada 10 min, sem impacto operacional. Funnel warm em background continua não afetando UX (endpoint sempre responde do cache 5 min TTL).
  - **Reversível:** git revert. Nenhum schema mudou, nenhum endpoint quebrou. Env var `KOMMO_BG_PAGE_SLEEP=0.05` volta o comportamento antigo se algum dia precisar.
  - **Sync manual do painel** (`/api/kommo/sync`) e **match/merge pipeline** (`match_merge_lib.KommoAPI(rate_per_sec=5)`) já tinham throttle próprio, não foram alterados.
- **Alternativas descartadas:**
  - **Rate limiter global em `_kommo_get`** (token bucket compartilhado): arquiteturalmente mais limpo, mas exige refatorar 6+ chamadores diferentes. Trade-off ruim vs sleep uniforme, que resolve o problema imediato com risco mínimo. Reabrir se voltar a saturar.
  - **Cancelar `responsible_history_daily`:** perde histórico usado em auditoria/dashboard. Não vale.
  - **Só serializar via `max_instances=1`:** já estava setado em cada job, mas não evita colisão entre jobs *diferentes*. Precisava do mutex compartilhado.
  - **`kommo_lib` sync entrar no mutex:** atrasaria o delta a cada 5min, e ele já tem rate limiter próprio (1,8 rps). Não vale.
- **Env vars novas:** `KOMMO_BG_PAGE_SLEEP` (default 0.20, segundos entre páginas em background). `ACEITE_RECONCILE_INTERVAL` continua respeitado; `10` ativa o cron escalonado, qualquer outro valor volta ao IntervalTrigger.
- **Arquivos alterados:**
  - `routes/kommo_sync.py` — throttle uniforme + `_kommo_api_bg_lock` + wrap `_sync_responsible_history` → `_sync_responsible_history_impl` + cron escalonado no `register_funnel_cache_job`.
  - `routes/config.py` — `register_aceite_reconcile` com CronTrigger escalonado quando intervalo=10 + `register_responsible_history_job` movido 03:00 → 04:30.
  - `routes/leads_parados.py` — sleep entre páginas 0,05 → 0,20.

### 2026-07-02 — Premiações Internas: colaboradores vinculados a `app_users` (Opção A — `categoria` como cargo sugerido)
- **Modelo usado:** Opus 4.7 (principal). Implementação direta após aprovação da Opção A pelo usuário.
- **Problema:** o campo "Nome" do colaborador era texto livre — o gestor precisava digitar manualmente, sem garantir que fosse um usuário real do sistema, e sem forma de distinguir homônimos.
- **Decisão:** trocar por combobox HTML nativo (`<input list="...">` + `<datalist>`) ligado a `app_users`. Ao selecionar um usuário:
  - Nome ← `username`.
  - Email ← `email_cruzeiro` (exibido abaixo do nome no card; serve pra distinguir homônimos).
  - Cargo ← pré-preenche com `categoria` do user; **campo continua editável** para o gestor refinar (ex: "Comercial" → "Consultor Sênior").
  - Setor ← herda do lote (padrão já existente).
- **Schema (migration idempotente em `_ensure_premiacao_interna_tables`):**
  - `premiacao_interna_colaborador.app_user_id INTEGER REFERENCES app_users(id) ON DELETE SET NULL` — FK nullable pra rastreabilidade em relatórios/folha, `SET NULL` preserva o lote quando o user é deletado.
  - `premiacao_interna_colaborador.email TEXT` — snapshot do email no momento da premiação (idem: sobrevive à exclusão do user).
  - `CREATE INDEX idx_pic_user` para lookups reversos "premiações de fulano".
- **Novo endpoint:** `GET /api/premiacoes-internas/usuarios-disponiveis` — retorna `[{id, username, email, categoria}]` ordenado alfabeticamente. Gate `_has_any_permission([PAGE_GESTOR, PAGE_APROVADOR])` (aprovador também precisa ler pra futuros filtros por consultor).
- **Validação server-side:** payload aceita `app_user_id` opcional; se vier, valida FK no `app_users` (retorna 400 se não existir) e usa dados do banco como fonte canônica pra nome/email (evita divergência do que a UI mandou vs realidade do banco).
- **Escolha da Opção A vs B/C:**
  - **A (usar `categoria` como cargo, editável):** ✅ escolhida — zero migração de dados; `categoria` já está preenchida em 90%+ dos users; cargo permanece editável então quando "Comercial" é vago demais, o gestor refina.
  - **B (nova coluna `cargo` em `app_users` + UI na Config):** descartada — exige preencher retroativamente todos os users e adicionar UI de edição na tela Config; overhead grande pra valor marginal (o cargo do colaborador na premiação vale como snapshot pontual, não como registro canônico da vida do funcionário).
  - **C (cargo 100% manual):** descartada — o pedido explícito foi "puxar o cargo automático quando possível".
- **Escolha combobox `<datalist>` vs autocomplete lib (Select2/Choices.js):** `<datalist>` nativo é suficiente pra até ~500 usuários e é zero-dependency; se o número explodir e a UX ficar ruim, migrar pra lib depois.
- **Trade-off conhecido:** Firefox não renderiza o atributo `label` do `<option>` no dropdown (só Chrome/Edge). Compensado exibindo o email logo abaixo do input assim que o gestor seleciona um usuário — visível em todos os browsers.
- **Compatibilidade:** aditivo — colunas novas são nullable, lotes antigos continuam válidos com `app_user_id=NULL`. Não requer backfill.

### 2026-07-02 — Módulo Premiações Internas: workflow de aprovação via helper de aviso por permissão
- **Modelo usado:** Opus 4.7 (principal). Implementação direta.
- **Escopo:** módulo novo com **duas páginas independentes**:
  - `premiacoes_internas` — Gestor cria/edita/envia lotes de premiação com N colaboradores (nome, cargo, setor, valor, justificativa).
  - `aprovacao_premiacoes` — Aprovador (CEO/diretor) analisa e decide (aprovar, reprovar, solicitar ajuste) com justificativa obrigatória em reprovar/ajustar.
- **Naming:** `premiacao_interna_*` (tabelas) + slugs `premiacoes_internas` / `aprovacao_premiacoes`. Coexiste sem colidir com `premiacao_*` (campanhas comerciais já existentes: `premiacao_campanha`, `premiacao_grupo`, `premiacao_tier_bonus`, etc.).
- **Persistência:** 3 tabelas novas em `db.py` via `_ensure_premiacao_interna_tables()` (idempotente, roda no boot):
  - `premiacao_interna_lote` — cabeçalho (mes_referencia, setor, gestor_user_id, status, valor_total cache, aprovador_*).
  - `premiacao_interna_colaborador` — N linhas por lote (nome/cargo/setor/valor/justificativa/observacoes/is_auto_premiacao/ordem).
  - `premiacao_interna_evento` — histórico imutável (tipo, status_anterior→status_novo, autor, justificativa, payload_diff JSONB).
- **FKs relaxadas:** `gestor_user_id` e `autor_user_id` viram `NULL` permitidos — pra aceitar admin logado pelo fallback `APP_USER/APP_PASS` (uid=0 não existe em `app_users`). Snapshot em `*_nome` preserva a auditoria mesmo sem FK.
- **Estados:** `rascunho` → `aguardando_aprovacao` → (`aprovado` | `reprovado` | `ajuste_solicitado`); `ajuste_solicitado` → `aguardando_aprovacao` (loop até decisão terminal). Editável só em `rascunho` e `ajuste_solicitado`. Delete só em `rascunho`.
- **Notificação — nova abordagem sem alterar schema de `avisos`:**
  - Dois helpers programáticos em `helpers.py`:
    - `criar_aviso_para_usuarios(user_ids, titulo, corpo, ...)` — insere direto em `avisos` com `target_user_ids=[...]`.
    - `criar_aviso_por_permissao(page, ...)` — resolve `user_ids` via `SELECT DISTINCT u.id FROM app_users u LEFT JOIN user_permissions p ON p.user_id=u.id AND p.page=%s WHERE u.role='admin' OR p.user_id IS NOT NULL` e delega ao anterior. Suporta `extra_user_ids` e `excluir_user_ids`.
  - Envio para aprovação: `criar_aviso_por_permissao("aprovacao_premiacoes", ..., excluir_user_ids=[gestor_id])`.
  - Decisão do aprovador: `criar_aviso_para_usuarios([gestor_id], ...)`.
  - **Zero mudança em `avisos`/`aviso_lido`.** Reaproveita 100% o sininho de notificações existente.
- **Regra "primeira ação decide"** (aprovação simples, sem consenso multi-aprovador). Se surgir demanda de multi-aprovador ou consenso, reabrir.
- **Backend:** `routes/premiacoes_internas.py` com blueprint `premiacoes_internas_bp` + 9 rotas:
  - 6 do gestor: `GET /api/premiacoes-internas/lotes` (lista com filtros mês/setor/status/busca — filtra por `gestor_user_id == session.user_id` **exceto para admins**), `GET .../<id>` (detalhe), `POST .../` (criar), `PUT .../<id>` (editar), `DELETE .../<id>` (deletar rascunho), `POST .../<id>/enviar`.
  - 3 do aprovador: `GET .../aprovacao/pendentes` (lista + KPIs), `GET .../aprovacao/lotes/<id>` (detalhe read-only), `POST .../aprovacao/lotes/<id>/decidir` (aprovar/reprovar/ajustar + justificativa condicional).
  - Gate server-side inline `_has_permission(page)`; restrições: `gestor_user_id == session.user_id` bloqueia auto-decisão (403); estado inválido → 409; justificativa faltando em reprovar/ajustar → 400.
- **Frontend:** 2 partials Jinja + 2 JS vanilla; wire em `templates/index.html` (2 includes + 2 scripts), `static/js/utils.js` (2 slugs em `PAGES` + `PAGE_TITLES` + hooks em `navigate()`), `static/js/config.js` (2 labels + grupo "Premiações Internas" em `PAGE_GROUPS_CONFIG`), `templates/partials/_sidebar.html` (grupo colapsável novo com 2 links protegidos por `nav_can()`).
- **Setor fixo:** dropdown com 4 opções (`Acadêmico | Comercial | TI | Marketing`) validado server-side.
- **Autofill "Auto-premiação":** quando o gestor marca o checkbox, JS busca `GET /api/me` e pré-preenche nome/email/cargo do próprio gestor.
- **Fix UI:** modais reparent'am pra `document.body` ao abrir + `align-items:flex-start` + `max-height:calc(100dvh - 4rem)` — evita corte de topo em containers com stacking context.
- **Permissões:** `ALL_PAGES` ganhou os 2 slugs — checkboxes de permissão aparecem automaticamente na tela Config. Admin ganha acesso automático.
- **Compatibilidade:** módulo é **aditivo** — nenhum código existente é tocado, nenhuma tabela é migrada. Se o módulo for removido, basta desregistrar blueprint + remover 2 partials + `DROP TABLE premiacao_interna_*`.
- **Alternativas descartadas:**
  - **Reaproveitar `matricula_ajustes`** — workflow parecido mas modelo é ajuste unitário, não lote com N colaboradores.
  - **Alterar schema de `avisos` para suportar target por permissão** — mais limpo semanticamente, mas exige refactor no back/front do sininho. Helper agregando `user_ids` no app-layer entrega mesmo resultado sem alterar tabelas.
  - **Vincular colaboradores a tabela de funcionários** — nenhuma tabela canônica existe hoje; texto livre + FK opcional a `app_users` é mais flexível.
- **Fora de escopo (reabrir se pedir):** upload de anexos; exportação CSV/PDF; notificação por e-mail/whatsapp; aprovação multi-nível; integração com folha de pagamento.

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
- **Git (Raphael):** desenvolvimento e commits do Raphael Castro vão **sempre na branch `raphael`**, nunca direto na `master`. Fluxo: `git checkout raphael` → trabalhar/commitar → `git push origin raphael` → merge para `master` só quando o usuário pedir (ou via PR). A `master` recebe integração; não é branch de trabalho diária.
