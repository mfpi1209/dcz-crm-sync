/** Respostas das rotas /api/bwipo/painel* (routes/bwipo_sync.py) e auxiliares usadas pela tela. */

export interface Kpis {
  matriculas: number;
  em_curso: number;
  evasao: number;
  aberto?: number;
  perdido?: number;
  sem_data?: number;
  fora_padrao: number;
  media: number;
  dias: number;
  ticket: number;
  prefixo: string;
  vendas_6m: number | null;
  vendas_1a: number | null;
  pct_6m: number | null;
  pct_1a: number | null;
  delta_6m: number | null;
  delta_1a: number | null;
  compare_6m_period: string | null;
  compare_1a_period: string | null;
}

/** Uma matrícula do recorte. `fonte`: manual | bwipo | kommo | sem_dono. */
export interface Linha {
  rgm: string;
  nome: string;
  situacao: string;
  data: string;
  polo: string;
  nivel: string;
  tipo: string;
  curso: string;
  agente: string;
  fonte: string;
  conta: boolean;
  fora: boolean;
}

export interface LinhaRanking {
  agente: string;
  matriculas: number;
  aberto: number;
  perdido: number;
  evasao: number;
  pos: number;
  meta: number;
  intermediaria: number;
  supermeta: number;
  conv: number;
  user_id: number | null;
  bwipo_owner?: string | null;
}

export interface Painel {
  ok: boolean;
  error?: string;
  kpis: Kpis;
  linhas: Linha[];
  prefixos: { pfx: string; n: number }[];
  ranking: LinhaRanking[];
  polos: { polo: string; total: number }[];
  evolucao: { dia: string; total: number }[];
  opcoes: { polos: string[]; agentes: string[] };
}

export interface Extras {
  ok: boolean;
  ytd: number;
  error?: string;
}

export interface Candidato {
  origem: "bwipo" | "kommo" | string;
  id: string | number;
  number?: number | null;
  user_id: number | null;
  agente: string;
  etapa: string;
  ganho?: boolean;
  data?: string;
}

export interface Resolucao {
  user_id?: number | null;
  user_name?: string | null;
  por?: string | null;
}

export interface Conflito {
  rgm: string;
  candidatos: Candidato[];
  resolucao: Resolucao | null;
}

export interface Conflitos {
  ok: boolean;
  conflitos: Conflito[];
  total: number;
  total_nao_resolvidos: number;
}

export interface ConsultaRgm {
  ok: boolean;
  rgm: string;
  agente: string | null;
  user_id: number | null;
  fonte: string | null;
  resolucao: Resolucao | null;
  contar_venda: boolean;
  candidatos: Candidato[];
}

export interface NegocioPick {
  id: string;
  number: number | null;
  title: string;
  etapa: string;
  agente: string;
}

export interface MiniSync {
  ok: boolean;
  msg?: string;
  pick?: NegocioPick[];
  pinned?: boolean;
  rgm?: string;
  number?: number;
}

export interface Contatos {
  ok: boolean;
  contatos: Record<string, { cpf: string; telefone: string }>;
}

export interface Duplicatas {
  ok: boolean;
  total: number;
  duplicatas: {
    rgm: string;
    count: number;
    negocios: { id: string; number: number | null; title: string; agente: string; etapa: string; ganho: boolean; perdido: boolean }[];
  }[];
}

export interface SemData {
  ok: boolean;
  sem_data: { rgm: string; number: number | null; agente: string; etapa: string }[];
  sem_negocio: string[];
  total: number;
}

export interface Diagnostico {
  ok: boolean;
  numeros: Record<string, number>;
  depara_por_tipo: Record<string, number>;
  donos_sem_depara: { agente: string; negocios: number }[];
  syncs: { entidade: string; em: string | null; status: string | null; registros?: number | null; erro: string | null }[];
}

export interface SyncAgentes {
  ok: boolean;
  kommo_users: number | null;
  depara: number;
  bwipo_sem_depara: number;
}

export interface Ciclo {
  id: number;
  nome: string;
  descricao?: string | null;
  ativo: boolean;
  dt_inicio: string;
  dt_fim: string;
}

export interface Turma {
  id: number;
  nome: string;
  nivel: string | null;
  dt_inicio: string;
  dt_fim: string;
}

export interface SnapshotInfo {
  ok: boolean;
  total?: number;
  min_date?: string | null;
  max_date?: string | null;
  mm_inscritos?: number;
  mm_matriculados?: number;
}

/** Filtros que vão para /api/bwipo/painel. Ciclo e turma só preenchem datas e nível. */
export interface Filtros {
  polo: string;
  nivel: string;
  agente: string;
  ini: string;
  fim: string;
}
