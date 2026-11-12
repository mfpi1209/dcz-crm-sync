-- Motor de Ativações — tabelas do livro-razão no dcz_sync (prefixo motor_).
-- Idempotente: pode rodar mais de uma vez.
-- Aplicar: python scripts/apply_motor_sql.py
-- NÃO roda no boot do painel nem do worker; aplicar em produção só com autorização.
--
-- O motor guarda só identificadores do CRM, datas e status.
-- Nome, telefone, CPF, RGM e e-mail ficam no CRM.

-- Chaves que a página do painel grava (liga/desliga e parada geral).
CREATE TABLE IF NOT EXISTS motor_controle (
    id             SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    ligado         BOOLEAN NOT NULL DEFAULT FALSE,
    parada_geral   BOOLEAN NOT NULL DEFAULT FALSE,
    parada_motivo  TEXT NOT NULL DEFAULT '',
    atualizado_por TEXT NOT NULL DEFAULT '',
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO motor_controle (id) VALUES (1) ON CONFLICT (id) DO NOTHING;

-- Batimento do worker (um único motor ativo, garantido por advisory lock).
CREATE TABLE IF NOT EXISTS motor_worker (
    id             SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    host           TEXT,
    pid            INTEGER,
    versao         TEXT,
    iniciado_em    TIMESTAMPTZ,
    ultimo_tick_em TIMESTAMPTZ,
    modo           TEXT,
    enabled        BOOLEAN,
    dry_run        BOOLEAN,
    ultimo_erro    TEXT,
    config         JSONB
);

-- Cada leitura da base de negócios do CRM.
CREATE TABLE IF NOT EXISTS motor_cargas (
    id                 BIGSERIAL PRIMARY KEY,
    dia                DATE NOT NULL,
    ciclo              TEXT NOT NULL,
    iniciada_em        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    concluida_em       TIMESTAMPTZ,
    completa           BOOLEAN NOT NULL DEFAULT FALSE,
    ciclo_aberto       BOOLEAN NOT NULL DEFAULT FALSE,
    negocios           INTEGER NOT NULL DEFAULT 0,
    alunos             INTEGER NOT NULL DEFAULT 0,
    abertos_dia10      INTEGER NOT NULL DEFAULT 0,
    abertos_financeira INTEGER NOT NULL DEFAULT 0,
    novas_baixas       INTEGER NOT NULL DEFAULT 0,
    erro               TEXT
);
CREATE INDEX IF NOT EXISTS idx_motor_cargas_dia ON motor_cargas (dia DESC, id DESC);

-- Retrato atual do aluno (contato do CRM). Dado financeiro é sempre D-1.
CREATE TABLE IF NOT EXISTS motor_alunos (
    contact_id           TEXT PRIMARY KEY,
    deal_id              TEXT NOT NULL DEFAULT '',
    situacao             TEXT NOT NULL DEFAULT '',
    nivel                TEXT NOT NULL DEFAULT '',
    calouro              BOOLEAN NOT NULL DEFAULT FALSE,
    dia10_aberto         BOOLEAN NOT NULL DEFAULT FALSE,
    financeira_aberta    BOOLEAN NOT NULL DEFAULT FALSE,
    tem_telefone         BOOLEAN NOT NULL DEFAULT TRUE,
    bloqueio             TEXT,
    pediu_silencio       BOOLEAN NOT NULL DEFAULT FALSE,
    lembretes_sem_reacao INTEGER NOT NULL DEFAULT 0,
    carga_id             BIGINT,
    visto_em             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    criado_em            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_motor_alunos_carga ON motor_alunos (carga_id);

-- Livro-razão por aluno e ciclo: abertura, baixa e sorteio do controle.
-- A classe do ciclo (em dia, tardio, não pagou) é derivada destas datas.
CREATE TABLE IF NOT EXISTS motor_aluno_ciclo (
    contact_id            TEXT NOT NULL,
    ciclo                 TEXT NOT NULL,              -- AAAA-MM
    deal_id               TEXT NOT NULL DEFAULT '',
    aberto_em             DATE NOT NULL,
    baixa_dia10_em        DATE,                       -- dia em que o Dia 10 virou Não (baixa D-1)
    controle              BOOLEAN NOT NULL DEFAULT FALSE,
    declarou_pagamento_em DATE,
    atualizado_em         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (contact_id, ciclo)
);
CREATE INDEX IF NOT EXISTS idx_motor_aluno_ciclo_ciclo ON motor_aluno_ciclo (ciclo);

-- Plano do dia (um por dia e por modo).
CREATE TABLE IF NOT EXISTS motor_planos (
    id        BIGSERIAL PRIMARY KEY,
    dia       DATE NOT NULL,
    simulado  BOOLEAN NOT NULL DEFAULT TRUE,
    ciclo     TEXT NOT NULL,
    piloto    TEXT NOT NULL DEFAULT 'formula',
    status    TEXT NOT NULL DEFAULT 'proposto',       -- proposto | aprovado | encerrado
    resumo    JSONB NOT NULL DEFAULT '{}'::jsonb,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dia, simulado)
);

-- Lotes do plano. Em simulação o lote "liberado" fica com status 'simulado'.
CREATE TABLE IF NOT EXISTS motor_lotes (
    id               BIGSERIAL PRIMARY KEY,
    plano_id         BIGINT NOT NULL REFERENCES motor_planos(id) ON DELETE CASCADE,
    dia              DATE NOT NULL,
    simulado         BOOLEAN NOT NULL DEFAULT TRUE,
    ordem            INTEGER NOT NULL,
    frente           TEXT NOT NULL,
    mensagem         TEXT NOT NULL,
    tamanho_previsto INTEGER NOT NULL DEFAULT 0,
    tamanho          INTEGER NOT NULL DEFAULT 0,
    status           TEXT NOT NULL DEFAULT 'planejado', -- planejado | simulado | liberado | pausado | concluido | adiado | recusado
    motivo           TEXT NOT NULL DEFAULT '',
    fila             JSONB,
    crm_campaign_id  TEXT,
    crm_tag_id       TEXT,
    criado_em        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    liberado_em      TIMESTAMPTZ,
    atualizado_em    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dia, simulado, ordem)
);

-- O que cada aluno recebeu (ou receberia, em simulação).
-- UNIQUE (contact_id, dia, simulado): nunca duas campanhas no mesmo dia para o mesmo aluno.
CREATE TABLE IF NOT EXISTS motor_mensagens (
    id            BIGSERIAL PRIMARY KEY,
    contact_id    TEXT NOT NULL,
    ciclo         TEXT NOT NULL,
    dia           DATE NOT NULL,
    simulado      BOOLEAN NOT NULL DEFAULT TRUE,
    frente        TEXT NOT NULL,
    mensagem      TEXT NOT NULL,
    estado        TEXT NOT NULL DEFAULT '',           -- estado do aluno na hora do lote
    lote_id       BIGINT REFERENCES motor_lotes(id) ON DELETE CASCADE,
    status        TEXT NOT NULL DEFAULT 'simulado',   -- simulado | enviado | entregue | lido | falhou
    erro          TEXT,
    respondeu_em  TIMESTAMPTZ,
    classificacao TEXT,
    confianca     NUMERIC(4,3),
    criado_em     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (contact_id, dia, simulado)
);
CREATE INDEX IF NOT EXISTS idx_motor_mensagens_ciclo ON motor_mensagens (ciclo, simulado, contact_id);
CREATE INDEX IF NOT EXISTS idx_motor_mensagens_lote ON motor_mensagens (lote_id);

-- Leituras da fila de atendimento e a decisão do governador em cada uma.
CREATE TABLE IF NOT EXISTS motor_fila_leituras (
    id                BIGSERIAL PRIMARY KEY,
    lida_em           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ok                BOOLEAN NOT NULL DEFAULT TRUE,
    aguardando        INTEGER,
    maior_espera_s    INTEGER,
    atendentes_online INTEGER,
    sla_estourado     INTEGER,
    decisao           TEXT,
    motivo            TEXT,
    erro              TEXT
);
CREATE INDEX IF NOT EXISTS idx_motor_fila_lida_em ON motor_fila_leituras (lida_em DESC);

-- Diário de decisões (quem, quando, o quê e por quê).
CREATE TABLE IF NOT EXISTS motor_diario (
    id        BIGSERIAL PRIMARY KEY,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tipo      TEXT NOT NULL,                          -- controle | carga | plano | lote | pausa | fila | erro | worker
    autor     TEXT NOT NULL DEFAULT 'trilhos',        -- trilhos | piloto:formula | piloto:agente | usuario:<login> | worker
    texto     TEXT NOT NULL,
    dados     JSONB
);
CREATE INDEX IF NOT EXISTS idx_motor_diario_criado ON motor_diario (criado_em DESC);

-- Toda ação no CRM, com chave de idempotência. Em simulação fica o que seria chamado.
CREATE TABLE IF NOT EXISTS motor_acoes (
    id         BIGSERIAL PRIMARY KEY,
    chave      TEXT NOT NULL UNIQUE,
    tipo       TEXT NOT NULL,
    lote_id    BIGINT REFERENCES motor_lotes(id) ON DELETE CASCADE,
    simulado   BOOLEAN NOT NULL DEFAULT TRUE,
    status     TEXT NOT NULL DEFAULT 'simulado',      -- simulado | feito | falhou
    requisicao JSONB,
    resposta   JSONB,
    criado_em  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
