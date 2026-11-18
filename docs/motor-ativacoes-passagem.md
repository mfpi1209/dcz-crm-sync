# Motor de Ativações — passagem para o Raphael (18/11/2026)

Documento para retomar o projeto no seu Claude. Leia inteiro uma vez; depois, entregue-o ao Claude junto com o
`AGENTS.md` do repositório (as entradas "Motor de Ativações" de 12/11 a 17/11 têm o detalhe de cada decisão).
Dono do produto: Marcelo Pinheiro (EduIT). Comunicação e textos de tela em português.

---

## 1. O que é

O **Motor de Ativações** comanda os avisos de mensalidade da Cruzeiro EaD **de fora do CRM**: escolhe quem recebe
qual aviso em cada dia, dispara em lotes dosados pela fila de atendimento humano, para quando a fila sobe, mede o
resultado contra um grupo de controle e prevê o pagamento do ciclo.

Objetivos do Marcelo (17/11): organizar a régua de lembretes financeiros, avaliar KPIs, integrar ao CRM, medir
resultado de verdade, **tirar o processo da mão da operação** (hoje o disparo é manual: em 17/11 a campanha
`Inadim_1711` mandou 4.944 mensagens de uma vez entre 12h15 e 12h35), **ativar mais cedo dosando pela fila**,
melhorar os textos (templates) e entender o comportamento dos alunos para ter previsibilidade.

## 2. Onde está o código

| O quê | Onde |
|---|---|
| Repositório do painel (Python 3.14, Flask, gunicorn, Postgres `dcz_sync`) | `https://github.com/mfpi1209/dcz-crm-sync` — deploy automático a cada push na `master` (GitHub Actions → GHCR → EasyPanel) |
| Painel em produção | `https://banco-dcz-crm-sync.6tqx2r.easypanel.host/` → Acadêmico → Motor de Ativações |
| Branch de trabalho | `feat/motor-ativacoes` |
| Backend do CRM (só leitura para nós; ver regra 5.1) | `https://github.com/caiovpinheiro/backend_crm1` (`main` = produção, `DEV_BRANCH` = DEV) |
| API pública do CRM (única que aceita token Bearer) | `https://integrations.bwipo.com` |

### Estado do git em 18/11
- **Na `master` (produção):** PR #131 (o motor inteiro) e PR #138 (botão "Instalar o motor"), mergeados em 17/11, deploy concluído.
- **Só no branch `feat/motor-ativacoes` (3 commits, sem merge):** roteiro das chamadas ao CRM na reprise (`motor/reprise_rotas.py`), aviso de "rodada de hoje ainda aberta" e o ajuste do ambiente local para `MOTOR_CRM_BASE_URL`, mais este documento. A `master` andou depois (preços): faça merge da `master` no branch antes de abrir o PR.

### Mapa dos arquivos
```
motor/                      pacote do motor (Python puro, sem lib nova)
  config.py                 MotorConfig + carregar(): todas as variáveis MOTOR_*, com tetos absolutos em código
  calendario.py             BRT, dias úteis, feriados nacionais, prazo_efetivo (dia 10/25 em fim de semana ou feriado → próximo dia útil)
  estados.py                classe do ciclo (em dia, tardio, não pagou) e estado do aluno (6 estados)
  agenda.py                 que mensagem sai em cada dia (3 dias úteis antes do prazo; recuperação até o dia 30)
  seletor.py, loteador.py   quem está no alvo e a ordem dos lotes (simulação)
  trilhos.py                exclusões e travas que nada passa por cima
  governador.py             liberar / segurar / pausar pela fila; tamanho do lote pela folga da equipe
  crm.py                    cliente da API pública do CRM (orçamento de 60 chamadas/min, trava de escrita, 403 sem insistir)
  carga.py                  lê os negócios do CRM e consolida por aluno
  razao.py, razao_pg*.py    livro-razão (memória para testes; Postgres em produção)
  envio.py                  envio real: UMA campanha por dia sobre a etiqueta do dia, dosada por pausar/retomar
  laco.py, worker.py        o laço (tick de 30 s) e o processo do worker (advisory lock: um só motor ativo)
  classificador.py          classifica respostas (botão, Jev da TypeSafe AI, regras por palavra-chave)
  respostas.py              webhook message_received do CRM (assinatura HMAC)
  resultados.py             motor × grupo de controle (intenção de tratar), por estado e por mensagem/variante
  previsao.py               comportamento por estado, previsão do ciclo e ritmo de pagamento
  textos.py                 9 propostas de texto + verificação de qualidade (nada cadastrado na Meta)
  historico.py              leitura do CSV de histórico de pagamento
  reprise.py, reprise_rotas.py  refaz um dia real com o governador; roteiro das chamadas ao CRM
routes/motor_ativacoes.py   página, estado, diário, controle, aprovar plano, instalar, webhook
routes/motor_resultados.py, routes/motor_reprise.py
sql/motor_ativacoes.sql     12 tabelas motor_ (idempotente; NÃO roda no boot)
scripts/motor_local.py      ambiente local (preparar | subir | parar | zerar | demonstrar)
scripts/motor_f0_check.py   conferência ao vivo da API do CRM, só GET
scripts/motor_importar_historico.py, scripts/apply_motor_sql.py
templates/partials/_motor_*.html, static/js/motor_*.js
tests/test_motor_*.py       211 testes sem banco + 10 com Postgres (test_motor_pg.py)
```

## 3. Como funciona (resumo técnico)

1. **Carga (7h e a cada 30 min no expediente):** `GET /api/deals` pagina os negócios; campos "Dia 10", "Financeira",
   "Situação", "Calouro", "Nível". Baixa = "Dia 10" virou "Não". Grava em `motor_alunos` e `motor_aluno_ciclo`.
2. **Estado do aluno**, pelo histórico do semestre: sem histórico, em dia, silenciado, tardio, primeiro atraso, não reage.
3. **Agenda:** lembrete do dia 10 e do dia 25 nos 3 dias úteis antes do **prazo efetivo** (público dividido 1/3, 2/3,
   tudo); recuperação nos dias úteis depois do prazo do dia 25 até o dia 30. Mensagens: `lembrete_dia10`,
   `aviso_calouro`, `reconhecimento`, `lembrete_dia25`, `recuperacao`, `firme`.
4. **Grupo de controle:** 5% por sorteio determinístico (`sha256(sal|ciclo|contact_id)`), por mês, nunca recebe.
   Etiquetado no CRM como `motor-controle-AAAA-MM` e excluído da campanha.
5. **Plano do dia:** fecha depois de detectada a carga do dia (≥ 50 mudanças no Dia 10). No envio real, uma pessoa
   **aprova o plano e informa a etiqueta do dia** (aplicada pela importação diária da equipe). É a única ação manual.
6. **Envio real:** cria UMA campanha no CRM sobre a etiqueta do dia, com `excludeTagIds` (bloqueios + controle),
   confere o público antes de lançar (recusa se > 2% não deveria receber), lança e dosa por **pausar/retomar**.
   Cancela no fim do expediente, na parada geral, na trava de experiência ou no teto do dia. Nunca atravessa o dia.
7. **Governador (a cada tick):**
   - PAUSAR: fora do expediente; fila **60 acima do nível de antes do 1º lote do dia**; falha de envio > 6%;
     trava de experiência ≥ 15 pedidos para parar/reclamações por 1.000; campanha andando sem leitura da fila.
   - SEGURAR: depois do corte (último lote às 21h); fila +30; menos de 5 atendentes; trava ≥ 8‰; lote anterior com
     menos de 30 min; sem leitura recente da fila.
   - Tamanho do lote: rampa 300 → 600 → até 1.200, pela folga da equipe. Teto de 4.000 por dia.
8. **Respostas:** webhook `message_received` → `POST /webhooks/motor/respostas` (HMAC `X-Eduit-Signature`). Texto
   mascarado (sem nome, telefone, CPF, RGM, e-mail) vai ao Jev; confiança < 0,7 vai para revisão.
9. **Medição:** resultado do ciclo por intenção de tratar (todos fora do controle × controle), em pontos percentuais
   com intervalo de 95%; "conclusivo" só com controle ≥ 100 alunos. Com 5% de ~15 mil alunos, efeito menor que
   ~3,5 pontos não se afirma num ciclo só.

### Modelo de segurança (não afrouxar sem o Marcelo)
- Padrão: `MOTOR_ENABLED=0`, `MOTOR_DRY_RUN=1`. **Envio real exige quatro chaves juntas:** `MOTOR_ENABLED=1`,
  `MOTOR_DRY_RUN=0`, motor ligado na página e plano do dia aprovado.
- `CrmMotor(permitir_escrita=False)` levanta `EscritaBloqueada` em qualquer método que não seja GET.
- Tetos absolutos em `motor/config.py` (lote 1.500, 12.000/dia, janela 9h–22h, último lote 21h, 60 chamadas/min,
  trava de fila ≤ +30/+60, trava de experiência ≤ 8‰/15‰). Variável de ambiente só aperta, nunca afrouxa.
- Campanha com filtro vazio ou desconhecido no CRM **envia para todos os contatos**: `validar_filtros_de_campanha`
  só aceita `tagIds` e `excludeTagIds`.

## 4. Fatos da API do CRM que mandam no desenho

- Token Bearer só em `https://integrations.bwipo.com`; age como o usuário que o criou (use um usuário de papel mínimo).
  Limite de 240/min por token e por organização; o motor usa no máximo 60/min e nunca provoca 429 de propósito.
  403 traz `required` (permissão que faltou): o motor registra no diário e não insiste.
- Rotas de integração criadas para o motor (backend_crm1 #273 na DEV, **#274 na `main` em 13/11**):
  `GET /api/integrations/service-queue?clock=business` (fila: `awaitingReply`, atendentes online, `asOf`) e
  `/api/integrations/campaigns` (+ `/{id}`, `/launch`, `/pause`, `/resume`, `/cancel`, `/stats`, `/recipients`).
- `excludeTagIds` na campanha veio no **backend_crm1 #278, que está só na `DEV_BRANCH`**. Sem ele em produção,
  a criação da campanha volta `AUDIENCE_FILTERS_INVALID` e o motor marca a campanha do dia como `erro` sem lançar.
- `GET /api/deals`: filtro por campo **falha aberto** (campo inexistente devolve tudo); `perPage` ≤ 1.000.
- Etiquetar é uma chamada por contato e **dispara o gatilho `tag_added`** de automações.
- Pausar leva ~5 s (umas 5 mensagens a mais por janela). O público da campanha é fixado no lançamento.
- `awaitingReply` não é "fila viva" (veio 90 num dia comum): por isso a trava mede **subida**, não nível absoluto.
- `stats` e `recipients` divergem; o motor mede o lote por `recipients` por status; `PENDING` em campanha
  concluída = não enviado.

## 5. Regras de trabalho combinadas com o Marcelo

1. **Nada enviesado no código do CRM.** Mudança lá só como recurso genérico da API pública, que não afete o front
   dos usuários, decidida por quem mantém o CRM. O motor usa a API; não mexe no banco de produção do CRM.
2. Não disparar, não lançar campanha, não alterar automações, templates, campanhas existentes nem dados de alunos
   sem ordem explícita do Marcelo.
3. No banco do painel, só tabelas `motor_`, e em produção só com autorização.
4. Nunca imprimir, registrar ou commitar token, senha ou `.env`. Se algum `.env*` (exceto `.env.example`) apontar
   para `ondigitalocean.com`, pare e avise mostrando só arquivo e nome da variável.
5. Jev (TypeSafe AI, `POST https://api.typesafe.ai/v1/systemone`, modelo `jev-latest`) só recebe texto mascarado.
6. Branch próprio + PR; merge na `master` só com o Marcelo pedindo (o merge vai direto para produção).
7. Commits sem `Co-Authored-By`. Arquivos com menos de 500 linhas. Testes em `tests/` com `unittest`.
8. Pergunta que só o Marcelo resolve: curta, fechada, com a opção recomendada.

## 6. O que está pronto

- Motor completo em simulação e o envio real escrito e testado contra um CRM de mentira (não usado ao vivo).
- Página no painel com: estado do motor e do worker, plano do dia, lotes, fila e decisões do governador, diário,
  liga/desliga/parada geral, aprovação do plano, **"Instalar o motor"** (só admin), resultado do ciclo,
  comportamento e previsão, textos propostos e a **reprise**.
- **Reprise de rodada real:** refaz um dia que aconteceu (mesmos alunos, mesma ordem, mesma equipe) começando cedo e
  dosando pela fila, com animação (envios acumulados e fila, motor × dia real), parâmetros editáveis e o roteiro de
  chamadas ao CRM com horário. Referência conferida: 10/11 às 10h → 14 lotes, último envio 18h04, pico 118
  (dia real 148). 17/11 (parcial até 12h45) → 6 lotes, 10h–11h41, pico 15 contra 28, espera mediana < 1 min contra 10 min.
- Importador de histórico (`scripts/motor_importar_historico.py`, CSV `contact_id,ciclo,baixa_em`).
- Ambiente local completo (seção 8).

## 7. O que falta para o motor operar — na ordem

| # | Passo | Quem | Observação |
|---|---|---|---|
| 1 | Merge do que está só no branch | Raphael, com OK do Marcelo | merge da `master` no branch, PR, testes |
| 2 | Clicar em **"Instalar o motor"** no painel de produção | admin (Marcelo) | cria as 12 tabelas `motor_`; motor segue desligado |
| 3 | Token do CRM de papel mínimo | Marcelo | permissões de campanha (view, create, edit, send, cancel), leitura de negócios e etiquetas |
| 4 | Conferência ao vivo, só GET: `python scripts/motor_f0_check.py` | Raphael | confirma se `/api/integrations/*` responde em produção e os nomes dos campos |
| 5 | Levar o backend_crm1 #278 (`excludeTagIds`) para a `main` do CRM | quem mantém o CRM | sem ele a campanha real não sai (ver seção 4) |
| 6 | Criar o serviço do worker no EasyPanel | quem tem o EasyPanel | mesma imagem `ghcr.io/mfpi1209/dcz-crm-sync:latest`, comando `python -m motor.worker`, sem porta, 1 réplica; variáveis `DB_*` do painel, `EDUIT_CRM_TOKEN`, `MOTOR_ENABLED=0`, `MOTOR_DRY_RUN=1`. Reimplantar à mão, fora da janela de envio |
| 7 | Conferir "Worker: Ativo" e rodar dias em **simulação** em produção | Raphael | `MOTOR_ENABLED=1` com `MOTOR_DRY_RUN=1`, ligar na página |
| 8 | Webhook de respostas | Marcelo | `POST /api/integration-webhooks` no CRM com `{"url": "https://<painel>/webhooks/motor/respostas", "events": ["message_received"], "name": "Motor de Ativações"}`; o `secret` vai para `MOTOR_WEBHOOK_SECRET` |
| 9 | Templates | Marcelo aprova; cadastro na Meta | propostas em `motor/textos.py` (aba "Textos dos avisos"); depois `MOTOR_CANAL_ID` e `MOTOR_TEMPLATE_<MENSAGEM>` |
| 10 | Histórico set–out/2026 | quem lê o CRM gera o CSV | sem ele todos começam "Sem histórico" e a previsão só aparece após o 1º ciclo completo |
| 11 | Chave do Jev (`TYPESAFE_API_KEY`) | Marcelo | sem ela vale a classificação por regras |
| 12 | Envio real | **só com o Marcelo** | `MOTOR_DRY_RUN=0`, plano aprovado com a etiqueta do dia |

O piloto combinado era "19/11 observa, 24/11 envia" (`crm1/docs/motor-piloto-2411.html`); as datas dependem dos passos acima.

## 8. Rodar localmente

Pré-requisitos: Python 3.12+ e PostgreSQL instalado (Windows: `C:\Program Files\PostgreSQL\<versão>\bin`, ou
`MOTOR_LOCAL_PG_BIN`).

```bash
git clone https://github.com/mfpi1209/dcz-crm-sync && cd dcz-crm-sync && git checkout feat/motor-ativacoes
python scripts/motor_local.py preparar     # venv, Postgres próprio em 127.0.0.1:55440, tabelas motor_
python scripts/motor_local.py              # sobe Postgres, worker e painel em http://127.0.0.1:5057/ (já logado)
python scripts/motor_local.py demonstrar   # 6.000 alunos de mentira para ver resultado e previsão
python scripts/motor_local.py zerar        # apaga o banco local
python scripts/motor_local.py parar
```
- Tudo fica em `.motor-local/` (fora do git). O script fixa banco local, simulação e agendamentos do painel desligados.
- **Reprise:** ponha os arquivos `rodada_AAAA-MM-DD.json` (vão no pacote de anexos) em `.motor-local/rodadas/`.
  Formato: `destinatarios` na ordem real (`envio_s`, `resposta_s`, `tipo`, `ia_s`, `humano_s`, em segundos),
  `fila_humana_de_fundo_por_minuto` (1.440), `atendentes_ativos_por_meia_hora` (48). Sem identificador de contato,
  sem texto, sem nome de atendente. **Não commitar rodada.** A de 17/11 é parcial (até 12h45).
- Token opcional para ler o CRM ao vivo (só GET): `EDUIT_CRM_TOKEN` em `.motor-local/env` (modelo em `.env.motor-local.example`).
- O ambiente local não recebe o webhook de respostas (precisaria de endereço público).

Testes:
```bash
python -m unittest discover -s tests -p "test_motor_*.py"     # 211 testes, sem rede e sem banco
MOTOR_TEST_DSN="host=127.0.0.1 port=55440 user=motor dbname=dcz_sync_teste" python -m unittest tests.test_motor_pg
```
(o banco de teste precisa terminar em `_teste`; crie com `createdb -h 127.0.0.1 -p 55440 -U motor dcz_sync_teste`)

## 9. Decisões já tomadas (não reabrir sem o Marcelo)

- Expediente 9h–22h, último lote às 21h, corte configurável (`MOTOR_JANELA_*`, `MOTOR_CORTE_MIN`).
- **Trava de fila mede a subida desde antes do 1º lote do dia** (`MOTOR_FILA_BASE=dia`, decisão de 17/11); `lote` existe por configuração.
- Uma campanha por dia sobre a etiqueta do dia; o motor só etiqueta o grupo de controle; cancela no fim do dia.
- Prazo efetivo: dia 10/25 em fim de semana ou feriado vale no primeiro dia útil seguinte (25/10/2026 → 26/10). Vale para corte de "em dia", lembretes, recuperação e a data no texto.
- 60 chamadas/min ao CRM; nenhum teste provocando 429; 403 registrado no diário, sem nova tentativa.
- Só graduação no piloto; negócio em dois níveis fica fora; etiquetas "Sem lembretes de mensalidade" e "Sem mensagens automáticas" excluem.
- Motor não pode ser "enviesado pelo produto"; atendimento vai até as 22h.

## 10. Decisões em aberto

1. Textos: aprovar ou trocar as 9 propostas; como comparar variantes A/B (um template por dia → alternar por dia ou por ciclo).
2. Dia do lembrete do dia 10 tem três mensagens (calouro, tardio, reconhecimento): a campanha do dia manda só a principal.
3. Quem pede para parar pelo motor precisa receber a etiqueta de exclusão no CRM (o motor hoje não aplica).
4. No envio real o limite de 2 mensagens por ciclo e "uma por dia" não são garantidos pelo motor (perda consciente do desenho de campanha por etiqueta).
5. Guardar o texto mascarado por 30 dias (`MOTOR_GUARDAR_TEXTO`, desligado).
6. Limites da fila (+30/+60, mínimo de 5 atendentes) a confirmar com dados reais.
7. 24/12 e 31/12 contam como dia útil; se a equipe não atende, incluir em `MOTOR_FERIADOS`.
8. Conferir se há automação com gatilho "etiqueta adicionada" para qualquer etiqueta (rodaria em cada aluno do controle).

## 11. Frentes relacionadas (outras sessões, outros branches)

- **Motor de rematrícula:** branch `feat/motor-rematricula-exec` no mesmo repositório, com formato genérico de "régua"
  (`motor/regua*.py`, `motor/reguas/<id>.json`). Ainda não juntado ao Motor de Ativações; a consolidação é um passo futuro.
- **NPS do aluno:** PR dcz-crm-sync #132 (sem merge). **Feedback com Jev:** auditoria de atendimento ao aluno.
- Documentos de planejamento (fora do git, vão no pacote): `motor-ativacoes-plano.html`, `motor-piloto-2411.html`,
  `motor-rodada-simulada.html`, `simulador-motor-financeiro.html`.

## 12. Primeira mensagem sugerida para o seu Claude

> Vou continuar o projeto "Motor de Ativações" no repositório `mfpi1209/dcz-crm-sync`, branch `feat/motor-ativacoes`.
> Leia `docs/motor-ativacoes-passagem.md` e as entradas "Motor de Ativações" do `AGENTS.md` (12/11 a 17/11) antes de
> qualquer coisa. Respeite as regras da seção 5 (nada específico no código do CRM, nada de envio real, nada de merge
> na master sem o Marcelo pedir, nunca expor token). Primeiro: faça merge da `master` no branch, rode os testes do
> motor e suba o ambiente local com `python scripts/motor_local.py preparar` e `python scripts/motor_local.py`.
> Depois me diga o que falta da seção 7 e qual é o próximo passo que depende só de nós.
