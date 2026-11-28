/** Respostas de routes/inadimplencia.py (só leitura; não mudam com a migração). */

export interface Competencias {
  competencias: { value: string; label: string }[];
}

export interface Snapshot {
  id: number;
  snapshot_id: number;
  effective_date: string | null;
  uploaded_at: string | null;
  filename: string | null;
  row_count_total?: number;
  inadimplentes: number;
  em_curso: number;
  taxa_pct: number;
  nivel: string | null;
  competencia_label: string | null;
}

export interface Lista {
  snapshots: Snapshot[];
  total_em_curso_atual?: number;
  total_em_curso_source_date?: string | null;
}

/** /atual e cada lado do /comparar. */
export interface Lado {
  snapshot_id: number | null;
  snapshot_date: string | null;
  effective_date?: string | null;
  uploaded_at?: string | null;
  filename?: string | null;
  inadimplentes: number;
  em_curso: number;
  taxa_pct: number;
  is_fallback: boolean;
  requested_date: string | null;
  fallback_date: string | null;
  dias_diferenca: number;
}

export interface Comparacao {
  a: Lado;
  b: Lado;
  delta_taxa_pct: number;
  delta_inadimplentes: number;
}

interface UploadPeriodo {
  uploaded_at: string | null;
  inadimplentes: number;
  taxa_pct: number;
}

export type ComparacaoPeriodo =
  | { insuficiente: true; snapshots_count: number }
  | {
      insuficiente?: false;
      competencia: string;
      competencia_label: string;
      snapshots_count: number;
      primeiro: UploadPeriodo;
      ultimo: UploadPeriodo;
      variacao_pp: number;
    };

export interface PontoEvolucao {
  date: string | null;
  effective_date?: string | null;
  uploaded_at?: string | null;
  snapshot_id?: number;
  inadimplentes: number;
  em_curso: number;
  taxa_pct: number;
}

export interface Evolucao {
  points: PontoEvolucao[];
  em_curso_constant?: number;
}

export interface PontoMes {
  day: number;
  date: string;
  taxa_pct: number;
  inadimplentes: number;
  em_curso: number;
}

export interface EvolucaoPorMes {
  competencias: { nivel: string; label: string; points: PontoMes[] }[];
  em_curso_constant?: number;
}

export interface Reincidencia {
  buckets: { "1": number; "2": number; "3": number; "4_plus": number };
  competencias_usadas: string[];
  snapshots_usados?: { snapshot_id: number; nivel: string; uploaded_at: string | null }[];
  rgms_analisados: number;
}

/** Filtro de cima aplicado (window._inadFilters na antiga). Só um vale por vez. */
export interface Filtros {
  competencia: string;
  date_a: string | null;
  date_b: string | null;
  recent_months: string | null;
}

/** Botões 7d/30d/90d/Tudo do gráfico de evolução (null = vale o filtro de cima). */
export type Faixa = null | "7" | "30" | "90" | "all";
