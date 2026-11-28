/** Respostas de routes/minha_performance.py usadas pela tela (só leitura do formato). */

export type Tier = "base" | "intermediaria" | "meta" | "supermeta";

export interface Campanha {
  id: number;
  nome: string;
  dt_inicio: string;
  dt_fim: string;
  is_active?: boolean;
}

export interface Campanhas {
  ok: boolean;
  error?: string;
  campanhas: (Campanha & { ativa?: boolean })[];
  default_id: number | null;
}

export interface Agentes {
  ok?: boolean;
  error?: string;
  agentes: { kommo_uid: number; name: string }[];
  suporte_uids: number[];
}

export interface DiaHeatmap {
  data: string;
  dia_semana?: number;
  meta: number;
  realizadas: number | null;
  mat: number;
  aceites: number;
  status: "hit" | "partial" | "miss" | "rest" | "future" | string;
}

export interface DiaBreakdown {
  data: string;
  realizadas: number;
  meta: number;
  total: number;
}

export interface ProgressoTier {
  tier: Tier | string;
  target: number;
  pct: number;
  atingido: boolean;
  valor_por_mat: number;
  ganho: number;
}

export interface Desbloqueie {
  tier: Tier | string;
  target: number;
  falta: number;
  atingido: boolean;
  valor_por_mat: number;
  ganho_total: number;
  ganho_adicional: number;
}

export interface Ranking {
  posicao: number;
  total_agentes: number;
  lider_total?: number;
  diferenca_lider: number;
  meu_total: number;
  minhas_mat: number;
  meus_aceites: number;
  media_time: number;
}

export interface Conquista {
  id: string;
  nome: string;
  icone: string;
  desc?: string;
}

export interface MetaEquipeSuporte {
  meta: number;
  total_matriculas: number;
  pct?: number;
  meta_batida?: boolean;
  agentes_count?: number;
  is_suporte?: boolean;
  matriculas_fonte?: string;
}

export interface Insights {
  ok?: boolean;
  error?: string;
  campanha: Campanha | null;
  agent_name?: string;
  total_matriculas?: number;
  metas?: { intermediaria?: number; meta?: number; supermeta?: number };
  tier?: Tier | string;
  pct?: number;
  pace_atual?: number;
  pace_meta?: number;
  projecao?: number;
  projecao_tier?: string;
  projecao_financeira?: number;
  dias_restantes?: number;
  hoje?: {
    meta?: number;
    realizadas?: number;
    aceites_fila?: number;
    aceites_hoje?: number;
    bonus_fixo?: number;
    bonus_extra?: number;
    ontem_realizadas?: number;
  };
  sequencia?: number;
  streak_nivel?: "aquecendo" | "em_chamas" | "imparavel" | null;
  heatmap?: DiaHeatmap[];
  mensagem?: string;
  tier_progress?: ProgressoTier[];
  premiacao?: {
    tier_bonus?: number;
    daily_bonus?: number;
    receb_bonus?: number;
    total?: number;
    daily_breakdown?: DiaBreakdown[];
    desbloqueie?: Desbloqueie[];
  };
  ranking?: Ranking | null;
  conquistas?: Conquista[];
  unificado?: { campanhas?: string[]; ganho_extra?: number } | null;
  meta_equipe_suporte?: MetaEquipeSuporte | null;
  pix_suporte_equipe?: boolean;
  pix_fonte_fila?: boolean;
  painel_suporte?: boolean;
  suporte_equipe_view?: boolean;
  media_vendas?: number;
  chart_matriculas_dia?: { data: string; matriculas: number }[];
  total_matriculas_time?: number;
}

export interface Historico {
  ok?: boolean;
  historico: {
    campanha_id: number;
    nome: string;
    dt_inicio: string;
    dt_fim: string;
    ativa: boolean;
    total_matriculas: number;
    tier: string | null;
    total_premiacao: number;
  }[];
}

/** /api/comercial-rgm/data (só os campos que a antiga lia). */
export interface DadosComercial {
  ok?: boolean;
  kpis?: { vendas?: number; media_diaria?: number };
  evolucao?: { data: string; count?: number }[];
}

export interface Periodo {
  nome?: string;
  dt_inicio: string;
  dt_fim: string;
}

export interface MatriculaOficial {
  nome?: string;
  rgm?: string;
  curso?: string;
  nivel?: string;
  polo?: string;
  data_matricula?: string;
  situacao?: string;
  outlier?: boolean;
  conta_para_meta?: boolean;
}

export interface MatriculasOficiais {
  ok?: boolean;
  error?: string;
  matriculas: MatriculaOficial[];
  total_contando?: number;
  total_contavel?: number;
  ciclos_painel?: Record<string, string>;
}

export interface MinhaMatricula {
  id: number;
  nome?: string;
  rgm?: string;
  curso?: string;
  polo?: string;
  data_matricula?: string;
  ciclo?: string;
  nivel?: string;
  kommo_lead_id?: string | number;
  observacao?: string;
}

export interface MinhasMatriculas {
  ok?: boolean;
  error?: string;
  matriculas: MinhaMatricula[];
}

export interface Ajuste {
  id: number;
  tipo: string;
  status: string;
  nome_aluno?: string;
  rgm?: string;
  kommo_lead_id?: string;
  descricao?: string;
  resposta_admin?: string;
  created_at?: string;
}

export interface Ajustes {
  ok?: boolean;
  error?: string;
  ajustes: Ajuste[];
}

/** Quem a tela mostra: a equipe de suporte, um consultor (kommo_uid) ou ninguém escolhido. */
export type Selecao = "suporte_equipe" | number | null;
