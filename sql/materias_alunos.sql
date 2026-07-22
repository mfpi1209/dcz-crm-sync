-- Tabelas para a feature "Matérias dos Alunos" (SIAA em lote)
-- Executar no Supabase SQL Editor.

create table if not exists public.materias_alunos (
    id            bigint generated always as identity primary key,
    rgm           text        not null,
    aluno         text,
    sigla         text        not null,
    disciplina    text,
    resultado     text,
    consultado_em timestamptz not null default now(),
    unique (rgm, sigla)
);
create index if not exists idx_materias_alunos_rgm on public.materias_alunos (rgm);

create table if not exists public.materias_alunos_consultas (
    rgm           text        primary key,
    aluno         text,
    status        text        not null,          -- ok | sem_materias | erro
    mensagem      text,
    qtd_materias  integer     not null default 0,
    consultado_em timestamptz not null default now()
);
create index if not exists idx_materias_alunos_consultas_status
    on public.materias_alunos_consultas (status);
