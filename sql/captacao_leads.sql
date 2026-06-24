-- Tabela: captacao_leads
-- Armazena cadastros enviados pelo formulario de Captacao Externa
-- (modos: promotor e candidato).
--
-- Como rodar:
--   1) Abra o Supabase do projeto (o mesmo que .env aponta em SUPABASE_URL).
--   2) SQL Editor -> New Query -> cole este script -> Run.
--
-- Permissoes:
--   - Se a SUPABASE_KEY do backend for service_role, o RLS abaixo eh suficiente
--     (service_role faz bypass).
--   - Se for anon, descomente a policy de insert no final.

create table if not exists public.captacao_leads (
    id              bigserial primary key,
    nome            text        not null,
    contato         text        not null,
    email           text,
    nivel           text,
    curso           text,
    grau            text,
    modalidade      text,
    ingresso        text,
    tipo            text        not null check (tipo in ('promotor','candidato')),
    usuario_logado  text,
    promotor        text,
    ensino_medio    boolean,
    ano_em          smallint    check (ano_em in (1,2,3)),
    created_at      timestamptz not null default now()
);

create index if not exists idx_captacao_leads_created_at
    on public.captacao_leads (created_at desc);

create index if not exists idx_captacao_leads_tipo
    on public.captacao_leads (tipo);

alter table public.captacao_leads enable row level security;

-- Descomente APENAS se o backend usar a chave anon em vez de service_role:
-- create policy "captacao_leads_insert_any"
--   on public.captacao_leads
--   for insert
--   with check (true);
