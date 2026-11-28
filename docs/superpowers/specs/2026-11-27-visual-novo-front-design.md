# Visual novo do front novo (`/app/`) — desenho

Data: 27/11/2026. Fase 4 da modernização (separar front e back). Aprovado na conversa com o usuário; este documento registra o que foi decidido.

## Intenção

A casca do front novo (Etapa 2) ficou crua: menu lateral em lista corrida de 66 itens, sem ícones nem grupos recolhíveis, sem barra de cima, ações jogadas no fim do menu. O usuário achou feia e pediu **um visual novo, mais moderno, mantendo as cores do eduit e as funcionalidades atuais**. Entre três propostas, escolheu a **C (menu lateral agrupado, com fixadas)**, somando duas peças da **A**: a **busca de telas com Ctrl K** e as **barras de proporção nas tabelas**.

Quem usa: consultores comerciais (home = Minha Performance), supervisores, equipe acadêmica e admins, todo dia, em telas de dados (rankings, tabelas, indicadores). O visual novo vale para todas as telas que forem migradas daqui em diante; a primeira é Uso do Dashboard.

## Objetivos

1. **Menu lateral novo** (desktop, largura fixa ~248px):
   - marca "eduit." no topo;
   - botão "Buscar tela" (abre a busca, mostra o atalho Ctrl K);
   - seção **Fixadas** (ver abaixo);
   - seção **Geral** com as telas fora de grupo (Dashboard, Buscar, Avisos, Meu Perfil…), tela inicial primeiro — regra atual de `montarMenu`;
   - **grupos com ícone que abrem e fecham** (Comercial, Acadêmico, Premiações, Ferramentas, Vocacional, Campanhas Meta, Chamados, TI, Sistema); o grupo da tela aberta começa aberto; o estado aberto/fechado de cada grupo fica no navegador (`localStorage`, chave `eduit-menu-grupos`);
   - item ativo com barra lateral azul;
   - rodapé com o usuário (iniciais + nome) e botão sair.
2. **Barra de cima** em cada tela: título da tela (e o grupo, em texto pequeno), sino de avisos com a contagem de não lidos, botão de tema claro/escuro e espaço para ações da tela (ex.: Atualizar).
3. **Busca de telas (Ctrl K / Cmd K)**: caixa sobreposta com as telas que o usuário pode ver (mesma lista do menu), filtro por nome sem diferenciar acento e maiúscula, setas para navegar, Enter abre, Esc fecha. Tela migrada abre em `/app/<tela>`; as demais em `/#<tela>` do painel atual.
4. **Fixadas por usuário, guardadas no banco** (opção 2 aprovada, com o detalhe da 3):
   - cada pessoa fixa/desafixa telas por uma estrela no item do menu (aparece ao passar o mouse; sempre visível nas fixadas) e na barra de cima da tela migrada;
   - enquanto a pessoa não fixou nada, a seção mostra **"Mais usadas"**: as 3 telas que ela mais abriu nos últimos 30 dias (`page_views`), só entre as que ela pode ver;
   - máximo de 8 fixadas.
5. **Celular**: o menu vira gaveta aberta pelo botão do topo; fecha ao escolher uma tela.
6. **Uso do Dashboard no visual novo**: filtros de período como pílulas, cartões de resumo com contexto (`58 de 78 telas`, `20 telas sem uso`), tabela de páginas acessadas com **barra de proporção** dos acessos (maior = 100%), tabela de páginas sem acesso igual à atual, com os selos.
7. Tema claro e escuro, com os tokens de cor atuais (`frontend/src/estilo.css`).

## Fora do escopo

- Painel atual em `/` (templates, `static/`) — não muda.
- Mini gráfico de tendência no cartão "Acessos" — a API atual não tem o total por dia; fica para quando houver motivo.
- Migrar outras telas, mudar login/troca de senha.
- Abrir os avisos dentro do front novo: o sino mostra a contagem e leva aos Avisos do painel atual (`/#avisos`).

## O que seria um fracasso mesmo funcionando

- Alguém ver no menu (ou na busca) tela que hoje não vê, ou deixar de ver alguma — a visibilidade continua vindo só do `/api/me` (`navegacao.py`).
- Fixar uma tela que a pessoa não pode ver, ou uma fixada continuar aparecendo depois que a permissão foi tirada.
- Menu ilegível ou cortado no celular; busca que não funciona só com teclado.
- Telas mais lentas para abrir por causa das fixadas (não pode bloquear o menu).

## Restrições

- Mesmo endereço, login e permissões (Flask serve `/app/`); nada de CORS.
- Banco: só criação (`CREATE TABLE IF NOT EXISTS`), nada apagado ou alterado.
- Rota nova de API tem de entrar no mapa de permissões ou nas listas de livres (`tests/test_permissao_rotas.py`).
- Versões exatas no `package.json`; sem biblioteca nova sem motivo. Ícones: Material Symbols, que o front já carrega.

## Abordagens consideradas

**Visual** (mostradas ao usuário em desenho):
- A — trilho de ícones com gaveta por grupo: mais espaço útil, um clique a mais para ver as telas.
- B — menu no topo com menus suspensos: bom para telas largas, mas 9 grupos não cabem e parte cai em "Mais".
- **C — menu lateral agrupado com fixadas (escolhida)**, com a busca Ctrl K e as barras da A: muda a cara sem obrigar a equipe a reaprender onde ficam as coisas.

**Onde guardar as fixadas:**
- 1 — no navegador: rápido, mas some ao trocar de computador (o próprio usuário alterna PCs).
- **2 — no banco, por usuário (escolhida)**, com o "Mais usadas" da opção 3 enquanto não houver fixadas.
- 3 — só automático pelas mais usadas: zero esforço, mas a pessoa não controla.

## Partes já decididas do como

**Banco** (`db.py`, mesmo padrão de `_ensure_page_views_table`, chamado no boot do `app.py`):

```sql
CREATE TABLE IF NOT EXISTS nav_fixadas (
    user_id   INTEGER NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    page      TEXT    NOT NULL,
    ordem     SMALLINT NOT NULL DEFAULT 0,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (user_id, page)
);
```

**Regra "visível no menu"** (decisão de quem escreveu, após a revisão): `pode(p)` sozinho deixa passar telas que o menu esconde (ex.: tela do grupo TI para quem não é admin). Fixadas e Mais usadas seguem a mesma regra do menu: novo `navegacao.Menu.visivel(p)` = `pode(p)` e, se `p` pertence a algum grupo de `GRUPOS`, a seção desse grupo está visível em `secoes()`. No front, os itens ainda são cruzados com os do `montarMenu` (que também tira telas sem nome).

**API** (novo blueprint, rotas do próprio usuário → `permissoes.ROTAS_DO_PROPRIO_USUARIO`):
- `GET /api/me/fixadas` → `{"fixadas": [...], "mais_usadas": [...]}`. `fixadas` em ordem, já filtradas por `Menu.visivel` (permissão tirada some sozinha). `mais_usadas`: top 3 de `page_views` do usuário nos últimos 30 dias (fuso America/Sao_Paulo), filtradas por `visivel` e por `navegacao.CONHECIDAS`, desempate pelo nome.
- `PUT /api/me/fixadas` com `{"paginas": [...]}` → substitui a lista (ordem = posição). Valida: lista de textos, sem repetidos, no máximo 8, cada uma em `CONHECIDAS` e `visivel`; senão 400 com mensagem. Transação: apaga as do usuário e insere as novas.
- Decisões menores (de quem escreveu): tela recém-fixada entra no fim da lista (sem tela de reordenar nesta rodada); desafixar tudo faz a seção voltar a "Mais usadas"; o "de 78" do cartão é o total do próprio relatório (telas com acesso + telas sem acesso), o universo que a tela mostra; no celular, a barra de cima ganha o botão que abre a gaveta do menu.
- Login de emergência (`user_id` 0, sem linha em `app_users`): GET devolve listas vazias; PUT responde 400 explicando que esse login não guarda fixadas.

**Front:**
- `useFixadas()` com TanStack Query; fixar/desafixar faz PUT com atualização otimista e volta atrás se der erro (com aviso discreto).
- Fixadas e Mais usadas aparecem só se a tela tiver nome em `titulos.ts` (subpermissão não vira item).
- Se `GET /api/me/fixadas` falhar, a seção simplesmente não aparece — o resto do menu segue normal.
- Ícones por grupo (Material Symbols): comercial `trending_up`, acadêmico `school`, premiações `emoji_events`, ferramentas `construction`, vocacional `explore`, meta `campaign`, chamados `support_agent`, TI `memory`, sistema `settings`.
- Busca: componente próprio (sem biblioteca), `role="dialog"`, foco preso enquanto aberta, volta o foco ao fechar.
- Contagem de avisos: `GET /api/avisos/nao-lidos` (`count`), atualizada a cada 2 min; zero = sem bolinha.

## Deixado para quem constrói

Espaçamentos, tamanhos de fonte, tons exatos de hover e borda dentro dos tokens existentes; animação de abrir/fechar grupo (curta, respeitando `prefers-reduced-motion`); textos de vazio e erro, no tom do painel (frase curta, diz o que fazer).

## Como conferir

- vitest: montagem do menu com Fixadas/Mais usadas, grupo da tela atual aberto, estado salvo, busca (filtro sem acento, teclado, Enter leva ao destino certo), estrela fixa/desafixa com volta atrás em erro, falha do GET não quebra o menu, Uso do Dashboard com barras proporcionais.
- pytest: tabela criada, GET com fixadas e com fallback das mais usadas, filtro por permissão, PUT com validações (desconhecida, sem permissão, repetida, mais de 8), usuário 0, rota no mapa de permissões.
- No navegador (local e, depois do deploy, produção como admin): desktop e celular, claro e escuro, Ctrl K, fixar e desafixar, recarregar e ver as fixadas mantidas.
- O PR #159 (link do menu antigo para `/app/page_views`) só é publicado depois deste visual estar no ar.
