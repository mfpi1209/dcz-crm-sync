-- NPS do aluno: tabelas nps_ no dcz_sync.
-- NÃO roda no boot do painel. Aplicar com: python scripts/apply_nps_sql.py
-- Idempotente. Só cria objetos com prefixo nps_.

-- Uma linha por pesquisa enviada. A resposta entra na mesma linha quando chega.
-- Sem nome, telefone, CPF, RGM nem e-mail do aluno: só os ids do CRM.
CREATE TABLE IF NOT EXISTS nps_pesquisas (
    id                   BIGSERIAL PRIMARY KEY,
    tipo                 TEXT        NOT NULL DEFAULT 'atendimento',
    deal_id              TEXT        NOT NULL,
    contact_id           TEXT        NOT NULL,
    enviado_em           TIMESTAMPTZ NOT NULL,
    conversa_origem      TEXT        NOT NULL DEFAULT '',
    depto                TEXT        NOT NULL DEFAULT '',
    -- Nome de quem atendeu, como aparece nas mensagens do CRM. Só é mostrado
    -- a quem tem a permissão nps_aluno_atendentes.
    atendente            TEXT,
    atendente_buscado_em TIMESTAMPTZ,
    nota                 SMALLINT    CHECK (nota BETWEEN 0 AND 10),
    resolvido            TEXT        CHECK (resolvido IN ('sim', 'parte', 'nao')),
    resolvido_bruto      TEXT        NOT NULL DEFAULT '',
    motivo               TEXT        NOT NULL DEFAULT '',
    respondido_em        TIMESTAMPTZ,
    -- TRUE quando o CRM não trouxe a hora da resposta e vale a hora da coleta.
    respondido_estimado  BOOLEAN     NOT NULL DEFAULT FALSE,
    criado_em            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT nps_pesquisas_tipo_ck CHECK (tipo IN ('atendimento', 'calouro', 'veterano', 'rematricula')),
    CONSTRAINT nps_pesquisas_uk UNIQUE (tipo, deal_id, enviado_em)
);
CREATE INDEX IF NOT EXISTS nps_pesquisas_enviado_idx ON nps_pesquisas (tipo, enviado_em);
CREATE INDEX IF NOT EXISTS nps_pesquisas_atendente_idx ON nps_pesquisas (atendente_buscado_em) WHERE nota IS NOT NULL;

-- Saúde da coleta: uma linha por execução.
CREATE TABLE IF NOT EXISTS nps_coletas (
    id              BIGSERIAL PRIMARY KEY,
    inicio          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    fim             TIMESTAMPTZ,
    status          TEXT        NOT NULL DEFAULT 'rodando' CHECK (status IN ('rodando', 'ok', 'erro')),
    disparada_por   TEXT        NOT NULL DEFAULT '',
    negocios_lidos  INTEGER     NOT NULL DEFAULT 0,
    pesquisas_novas INTEGER     NOT NULL DEFAULT 0,
    respostas_novas INTEGER     NOT NULL DEFAULT 0,
    atendentes      INTEGER     NOT NULL DEFAULT 0,
    erro            TEXT
);
