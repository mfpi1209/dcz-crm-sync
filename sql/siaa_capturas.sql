-- Schema das tabelas SIAA (Consulta SIAA)
-- Executar UMA VEZ no SQL Editor do Supabase.
-- Idempotente: usa IF NOT EXISTS em tudo.

-- ---------------------------------------------------------------------------
-- siaa_capturas — cada linha = 1 raspagem do SIAA para 1 RGM.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.siaa_capturas (
    id                  bigserial PRIMARY KEY,
    rgm                 text        NOT NULL,
    nome                text,
    curso               text,
    situacao_academica  text,
    serie               text,
    periodo             text,
    data_matricula      text,
    cod_turma           text,
    status              text,           -- 'sucesso' | 'parcial' | 'vazio'
    erro                text,
    capturado_em        timestamptz,
    fonte               text,
    raw_payload         jsonb,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS siaa_capturas_rgm_idx        ON public.siaa_capturas (rgm);
CREATE INDEX IF NOT EXISTS siaa_capturas_created_at_idx ON public.siaa_capturas (created_at DESC);
CREATE INDEX IF NOT EXISTS siaa_capturas_rgm_created_at_idx
    ON public.siaa_capturas (rgm, created_at DESC);

-- ---------------------------------------------------------------------------
-- siaa_documentos — documentos vinculados a uma captura.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.siaa_documentos (
    id           bigserial PRIMARY KEY,
    captura_id   bigint      NOT NULL REFERENCES public.siaa_capturas(id) ON DELETE CASCADE,
    rgm          text,
    codigo       text,
    descricao    text,
    obrigatorio  text,
    situacao     text,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS siaa_documentos_captura_id_idx ON public.siaa_documentos (captura_id);
CREATE INDEX IF NOT EXISTS siaa_documentos_rgm_idx        ON public.siaa_documentos (rgm);

-- ---------------------------------------------------------------------------
-- siaa_titulos_financeiros — títulos (vencidos, a vencer, pagos).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS public.siaa_titulos_financeiros (
    id              bigserial PRIMARY KEY,
    captura_id      bigint      NOT NULL REFERENCES public.siaa_capturas(id) ON DELETE CASCADE,
    rgm             text,
    categoria       text,           -- 'vencidos' | 'a_vencer' | 'pagos' (livre)
    tipo_titulo     text,
    numero_titulo   text,
    vencimento      text,           -- string (dd/mm/aaaa vindo do SIAA)
    atraso          text,
    valor           numeric,
    desconto        numeric,
    multa_juros     numeric,
    total           numeric,
    data_pagamento  text,
    tipo_pagamento  text,
    valor_pago      numeric,
    created_at      timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS siaa_titulos_captura_id_idx ON public.siaa_titulos_financeiros (captura_id);
CREATE INDEX IF NOT EXISTS siaa_titulos_rgm_idx        ON public.siaa_titulos_financeiros (rgm);
CREATE INDEX IF NOT EXISTS siaa_titulos_categoria_idx  ON public.siaa_titulos_financeiros (categoria);

-- ---------------------------------------------------------------------------
-- RLS: as tabelas são acessadas pelo painel usando a service_role key
-- (bypass de RLS). Se você preferir bloquear o acesso via anon/JWT do público,
-- habilite RLS e não crie policies — o service_role continua tendo acesso.
-- ---------------------------------------------------------------------------
ALTER TABLE public.siaa_capturas             ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.siaa_documentos           ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.siaa_titulos_financeiros  ENABLE ROW LEVEL SECURITY;
