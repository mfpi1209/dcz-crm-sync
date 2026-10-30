# CLAUDE.md — Painel DCZ / Comercial Cruzeiro do Sul EAD

Contexto para qualquer sessão do Claude que pegar este trabalho. Compilado em 30/10/2026 a partir do `RESUMO_CHATS.md` (95 chats do Cursor, 17/03 a 28/10/2026), do `AGENTS.md` do painel e da sessão de configuração no Claude.ai.

**Regra de ouro:** o `AGENTS.md` do `dcz-crm-sync` é a fonte das decisões já fechadas. Ler antes de mexer e não refazer trade-offs que estão lá. Toda decisão nova entra no topo do `AGENTS.md`, no mesmo formato (data, pedido, regra, o que não muda).

---

## 1. Objetivo

Painel interno da operação comercial e acadêmica (polos Cruzeiro do Sul EAD / EduIT). Ele junta:

- **Crédito de venda dos consultores**: ranking, metas, Minha Performance, PIX diário, Repasse. A venda é a matrícula do SIAA, não o card em Ganho no CRM.
- **Migração do CRM comercial do Kommo para o Bwipo** (desde 22/09/2026), mantendo o histórico do Kommo.
- **Acadêmico**: matriculados, rematrícula, evasão, interações.
- **Ferramentas internas**: chamados TI/Marketing com Kanban, Subir Blog, preços dos cursos (CRM, sites e IA), Match & Merge, Distribuição Comercial.

## 2. Repositórios

| Repo | GitHub | Branch de trabalho | Deploy |
| --- | --- | --- | --- |
| Painel `dcz-crm-sync` | `github.com/mfpi1209/dcz-crm-sync` | `raphael` | push na `master` → EasyPanel `https://banco-dcz-crm-sync.6tqx2r.easypanel.host/` |
| Disparador / Sync Novo CRM `tool_whatsapp_alunos` | `github.com/Mikyxx1234/tool_whatsapp_alunos` | `main` | push → EasyPanel (rebuild via Dockerfile) |
| Landing de influenciadores `lp-influenciadores` | `github.com/Rapha-Tomita/lp-influenciadores` | `raphael` | — |

- **Nunca commitar direto na `master` do painel.** Trabalhar na `raphael` e fazer merge quando aprovado.
- O repo da landing foi achado testando os autores de commit do painel. **Confirmar com o usuário** se é o mesmo que está publicado.
- O resumo não trazia a URL do Disparador. Ela veio do `AGENTS.md` do painel (é o repo conectado ao EasyPanel).

## 3. Stack

**Painel (`dcz-crm-sync`)**
- Python 3.14, Flask 3 (blueprints em `routes/`), psycopg2, APScheduler (jobs em processo), openpyxl, requests, python-dotenv.
- Produção: `gunicorn --workers 1 --threads 4 --timeout 600 app:app` na porta 5001. Com só 4 threads, **qualquer varredura pesada dentro de um request trava o login**. Por isso existem os caches e as tabelas prontas.
- Local: `python app.py` → `http://127.0.0.1:5001` (`use_reloader=False`). A máquina do usuário roda Windows/PowerShell, então usar `;` no lugar de `&&`.
- Front: templates Jinja + JS estático em `static/`, navegação por hash (`#bwipo_comercial`, `#subir_blog`…).

**Disparador (`tool_whatsapp_alunos`)**: Node/Express (`server/`) + React/Vite/Tailwind (`src/`), pg, Supabase JS, xlsx/papaparse. Scripts: `npm run dev`, `npm run migrate`, `npm run build`.

**Landing (`lp-influenciadores`)**: React/Vite/Tailwind + servidor Express em TS (`tsx server/index.ts`), pg, `@google/genai`. Cria o lead no Kommo com campos UTM.

**Bancos (mesmo host Postgres)**
- `dcz_sync`: banco do painel (`DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASS`).
- `kommo_sync`: espelho do Kommo (`KOMMO_PG_*`; se estiver vazio, usa `DB_*`).
- `disparos`: matriculados do Disparador (`DISPAROS_DB_NAME`).
- Supabase: catálogo da IA (`documents_precos`, `cursos_salesbot`, busca vetorial `cursos_salesbot_nome`). O Postgres próprio não tem `vector`.

**CRMs e APIs externas**
- Kommo: `https://admamoeduitcombr.kommo.com`, `KOMMO_TOKEN`.
- **Bwipo comercial** (org Comercial Cruzeiro): site `https://comercialcruzeiro.bwipo.com`, API `https://integrations.bwipo.com`, `BWIPO_COMERCIAL_API_TOKEN`. Só o **Pipeline Principal** entra no crédito; o pipeline Ativações fica de fora. `GET /api/users` responde 401, então o dono vem do card.
- **Bwipo acadêmico** (`cruzeiro-ead.bwipo.com` / `crm.eduit.com.br`, `EDUIT_CRM_TOKEN` / `NOVO_CRM_API_TOKEN`): **é outro sistema. Nunca usar esse token no funil comercial.**
- DataCrazy: a aba de escrita no painel foi desligada em 22/07. Upload e Kommo foram preservados.

## 4. Estrutura do painel

```
app.py                 # cria o Flask, registra blueprints, agenda jobs (453 linhas)
db.py / helpers.py     # conexão Postgres, utilitários (normalize_polo_display etc.)
routes/                # um blueprint por tela/API
  bwipo_sync.py        # sync/espelho Bwipo, Dashboard Comercial Bwipo, dono da venda, mini-sync, conflitos
  comercial_rgm.py     # Dashboard Comercial antigo (Kommo); funções ainda usadas pelo Bwipo
  minha_performance.py # meta diária, aceites, crédito por período
  repasse.py           # repasse com corte em 01/10/2026
  bwipo_precos.py      # planilha de preços → produtos Bwipo, sites, IA
  solicitacoes_ti.py   # chamados TI/Marketing (Kanban)
  ...                  # dashboard, siaa, upload, match_merge, kommo_sync, blog_posts, etc.
services/              # clientes: bwipo_comercial, bwipo_crm, bwipo_owner, bwipo_match, eduit_crm, atividade_kommo
kommo_lib/             # sync do Kommo para o kommo_sync (leads, contatos, campos, delta)
siaa/                  # parser e cliente HTTP do relatório SIAA, matérias dos alunos
sql/ schema.sql        # DDL e índices
templates/ static/     # front
AGENTS.md              # decisões técnicas (LER ANTES)
```

Tabelas-chave do Bwipo no `dcz_sync`: `bwipo_pipelines`, `bwipo_stages`, `bwipo_users`, `bwipo_contacts`, `bwipo_deals`, `bwipo_kommo_depara` (leads), `bwipo_kommo_user_depara` (consultores), `bwipo_consultor_tombamento`, `bwipo_painel_base`, `bwipo_painel_meta`, view `vw_bwipo_deals_rgm`. Decisões manuais: `comercial_rgm_conflito_resolucao`. Relatórios SIAA enviados: `xl_rows`.

## 5. Regras de negócio vigentes (não rediscutir)

**Venda e ranking**
- Venda = matrícula no relatório SIAA. RGM que sumiu do relatório vira **TRANSFERIDO** (evasão).
- O ranking conta só **EM CURSO** no último relatório (desde 25/09). A antiga recuperação ancorada no dia 01 foi removida.
- Ganho no Bwipo com RGM e data no recorte, cujo RGM **nunca** apareceu em nenhum relatório, entra na evasão como TRANSFERIDO e **não soma venda** (19/10). Caso de referência: Luciana, RGMs 50574191 e 50574663, na evasão da Sabrina, que continua com 72 vendas.
- Conversão = matrículas do período / leads criados no Pipeline Principal no período (dia em BRT).
- Conflito = 2 ou mais negócios Bwipo no Pipeline Principal com o mesmo RGM e data dentro do filtro. O Kommo fica fora.

**Dono da venda** (`_atribuir_rgms`), nesta ordem:
1. decisão manual (conflito, ajuste aprovado, mini-sync);
2. dono no Bwipo, **se o consultor está tombado** e a matrícula é da data dele em diante;
3. responsável no Kommo;
4. dono no Bwipo;
5. Admin Sistema (`8261837`).

Tombado hoje: **só o Hugo (`12158628`), desde 29/09/2026**. Tombamento é por consultor; **não existe corte geral em 28/09**. Consultor excluído é reatribuído; consultor oculto sai do ranking. Diego não tem id no Bwipo e fica sem responsável.

**Repasse**: matrícula até 30/09/2026 fica no responsável do Kommo. De 01/10/2026 em diante, vai para o `owner_id` do negócio no Bwipo. Sem `owner_id`, a venda nova não cai no Kommo. A base do admin é só `comercial_recebimentos`.

**Outras regras**
- O botão "Atualizar lead" do Dashboard Bwipo chama `POST /api/bwipo/painel/mini-sync` (número do card ou RGM de 8 dígitos) e grava no espelho. ID de lead Kommo ali não sincroniza mais.
- Lead NOVO no Bwipo herda o responsável do lead da mesma pessoa no Kommo; sem esse lead, vai para Admin Sistema.
- Preço de graduação: a planilha atualiza o produto no Bwipo, guarda histórico e grava as cotas fixas **Balcão Estratégico** e **Mar Aberto** (= canal APROVADOS). O botão "Atualizar a IA" manda o valor com desconto do canal Aprovados para o Postgres e o Supabase.
- Dashboard Acadêmico lê o último upload de Matriculados do Disparador. TRANSFERIDO **no arquivo** é aluno que veio de outro polo (conta em Em Curso). Quem **sumiu** do arquivo é quem saiu.
- Chamados só podem ser abertos por supervisor ou acima. Fases: Novo Ticket, A Fazer, Em Produção, Pausado, Concluído.

## 6. Trechos de código importantes

**Dono da venda**, `routes/bwipo_sync.py`:

```python
def _atribuir_rgms(rgm_datas: dict[str, Any]) -> tuple[dict, dict, dict]:
    """Dono de cada venda: manual > Bwipo (consultor tombado, a partir da data dele) > Kommo > Bwipo > Admin."""
    ...
    for rgm in rgms:
        nk = _normalize_rgm(rgm)
        b_uid, b_nome = bwipo.get(rgm, (None, None))
        k_uid = kommo.get(nk)
        dm = _as_date(rgm_datas.get(rgm))
        if nk in manual:
            key, src = manual[nk], "manual"
        elif b_uid and b_uid in tomb and dm and dm >= tomb[b_uid]:
            key, src = b_uid, "bwipo"
        elif k_uid:
            key, src = int(k_uid), "kommo"
        elif b_uid:
            key, src = b_uid, "bwipo"
        elif b_nome:
            key, src = f"bw:{b_nome}", "bwipo"
        else:
            key, src = admin, "sem_dono"
        if isinstance(key, int) and key in reassign:
            key = reassign[key]          # consultor excluído → reatribuído
        dono[rgm] = key
        fonte[rgm] = src
```

A chave do agente é o `kommo_user_id`. Um dono do Bwipo sem depara vira `"bw:<nome>"`.

**Evasão de quem nunca entrou no SIAA**, `routes/bwipo_sync.py`:

```python
def _bwipo_evasao_fora_relatorio(dt_ini, dt_fim, ja: set) -> list[dict]:
    conds = [
        "NOT d.is_deleted",
        _PRINCIPAL_SQL,                       # só Pipeline Principal
        "COALESCE(s.is_won, FALSE)",          # em Ganho
        "d.rgm_norm ~ '^[0-9]{8}$'",
        f"{_DM_SQL} IS NOT NULL",
    ]
    ...
    # descarta quem já apareceu em QUALQUER relatório enviado
    cur.execute("SELECT DISTINCT data->>'rgm' FROM xl_rows WHERE data->>'rgm' = ANY(%s)", (rgms,))
    ja_relatorio = {r[0] for r in cur.fetchall()}
    return [row for row in rows if row["rgm"] not in ja_relatorio]   # situacao = "TRANSFERIDO"
```

**Corte do repasse**, `routes/repasse.py`:

```python
# Venda com matrícula até 30/09/2026 fica no responsável do Kommo.
# De 01/10/2026 em diante o valor vai para o owner_id do negócio no Bwipo.
_CORTE_REPASSE = date(2026, 10, 1)

def _rgms_venda_nova(cur):
    cur.execute("""
        SELECT rgm FROM bwipo_painel_base
        WHERE data_matricula >= %s AND rgm IS NOT NULL AND rgm <> ''
    """, (_CORTE_REPASSE,))
    return {r[0] for r in cur.fetchall() if r[0]}
```

**Tabela pronta do painel** (uma linha por RGM). A página filtra esta tabela em vez de varrer o SIAA a cada request:

```sql
CREATE TABLE IF NOT EXISTS bwipo_painel_base (
    rgm TEXT PRIMARY KEY,
    nome TEXT NOT NULL DEFAULT '',
    situacao TEXT NOT NULL DEFAULT '',      -- sumido → TRANSFERIDO
    data_matricula DATE,
    polo TEXT, nivel TEXT, ciclo TEXT, tipo_matricula TEXT, turma TEXT,
    sumiu BOOLEAN NOT NULL DEFAULT FALSE,
    dono_key TEXT, dono_nome TEXT NOT NULL DEFAULT '',
    fonte TEXT NOT NULL DEFAULT '',          -- manual | bwipo | kommo | sem_dono
    built_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bwipo_painel_base_data ON bwipo_painel_base (data_matricula);
```

A tabela é remontada no upload de matriculados, no tombamento, no depara de consultor, no sync de agentes e por um job a cada 10 min (o primeiro roda 30 s após o boot). `GET /api/bwipo/painel` usa cache de 5 min e refaz em segundo plano.

## 7. O que já foi feito

**Nos chats do Cursor (março a outubro):**
- **Março a junho:** sync de campos do Kommo, Meus Atendimentos, PIX diário por faixas e equipe, Suporte Comercial, correções de FK e de RGM fora do padrão, filtros clicáveis.
- **Junho:** no Disparador, progresso real, resposta única por categoria e dia, Meu Painel, iframe, dark mode, congelar ciclo.
- **Julho:** crédito de venda (ranking × Minha Performance), consultor excluído vai para Admin Sistema, dedup de pós, turnos da Distribuição Comercial, Match/Merge.
- **Julho e agosto:** CRM acadêmico novo, no repo do Disparador. Sync da madrugada, órfãos, dedupe, multi-curso sem sobrescrever RGM.
- **Setembro:** Dashboard Acadêmico sobre os Matriculados do Disparador, correção do serial Excel na rematrícula, chamados TI/Marketing com Kanban.
- **22/09 a 28/10:** migração Kommo → Bwipo. Espelho, depara, Dashboard Comercial Bwipo (que virou o único no menu), dono da venda, cache, funil da home ao vivo, Minha Performance e Repasse no Bwipo, preços/sites/IA, evasão de quem nunca entrou no SIAA. Último commit na `raphael`: `de6b2e2`.
- **Avulsos:** landing de influenciadores (25/08) e espelho do repo na conta pessoal do GitHub (31/07).

**Nesta sessão (Claude.ai, 30/10/2026):**
- Os três repos foram clonados no container: painel na `raphael`, Disparador na `main` (`cac1985`), landing na `raphael` (`06174d7`).
- Os três `.env` foram extraídos do zip `env_compilado_1.zip`, com CRLF convertido para LF e `chmod 600`. Confirmado que o `.gitignore` de cada repo cobre o `.env` (`git status` limpo).
- Teste de rede: o Postgres (5432) não conecta e `integrations.bwipo.com` não resolve. **O `.env` está correto; o bloqueio é da rede do container.**

## 8. O que falta / pendências

**Ambiente**
- [ ] Variáveis que estão sem valor nos `.env` locais e provavelmente existem só no EasyPanel:
  - painel: `SUPABASE_*` (inclui `_ACADEMICO_` e `_INSCRICOES_`), `EDUIT_CRM_*`, `EDUIT_PG_DB`, `KOMMO_BASE_URL`, `WHATSAPP_TOOL_API_KEY`;
  - Disparador: `WHATSAPP_*`, SMTP/e-mail, `APP_API_KEY`, `WEBHOOK_VERIFY_TOKEN`, ajustes de scheduler e de cache do DataCrazy;
  - landing: `GEMINI_API_KEY`, `APP_URL`, `KOMMO_UTM_*_FIELD_ID` (sem eles o lead chega sem a origem da campanha).
- [ ] Acesso aos bancos e às APIs. O container do Claude.ai só alcança GitHub/PyPI/npm. Para rodar contra dados reais, usar o **Claude Code na máquina do usuário**. Liberar domínios na organização resolveria só as APIs HTTP; o Postgres provavelmente continua bloqueado.
- [ ] Confirmar que `Rapha-Tomita/lp-influenciadores` é o repo da landing publicada.

**Produto**
- [ ] Tombar os demais consultores no Bwipo (hoje só o Hugo), um por vez, via "Editar consultores" → "No Bwipo desde".
- [ ] Preços de **Pós**: a tela aceita o arquivo, mas a leitura espera uma amostra do arquivo de preço (vale para CRM, sites e IA).
- [ ] Grupos de duplicata com o mesmo RGM e pessoas diferentes no Bwipo. Não se apagam sozinhos e pedem análise manual. As duplicatas com o mesmo nome (377) já foram apagadas.
- [ ] Matérias dos Alunos: o usuário perguntou sobre o último sync em 20/10; verificar se está rodando.
- [ ] As páginas e APIs do Dashboard Comercial antigo (Kommo) continuam no código, porque o Bwipo ainda usa funções de `comercial_rgm.py`. Remover só depois de desacoplar.

## 9. Como trabalhar

- Ler o `AGENTS.md` do repo antes de qualquer mudança. Registrar a decisão nova no topo dele.
- Branch `raphael` → merge na `master` só com o ok do usuário (o push na `master` publica em produção).
- Não colocar `.env`, zip ou token no Git, nem em commit, issue ou PR. Só o **nome** da variável vai para documentação.
- Não usar o token do CRM acadêmico no comercial, nem o inverso.
- Nada pesado dentro de request HTTP no painel (são 4 threads do gunicorn). Use cache, tabela pronta ou job em background.
- Ao mexer em regra de crédito, conferir contra um caso conhecido (ex.: recorte 01–18/10, Sabrina com 72 vendas e 6 RGMs na evasão de "nunca entrou no SIAA").
- Os transcripts dos 95 chats estão na máquina do usuário, pelos ids listados no `RESUMO_CHATS.md`.
