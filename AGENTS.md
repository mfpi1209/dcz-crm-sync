# AGENTS.md — dcz-crm-sync

Este arquivo registra decisões técnicas tomadas em conjunto com agentes Opus, para que execuções futuras (qualquer modelo) sigam o que já foi acordado sem refazer trade-offs.

## Decisões técnicas

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
