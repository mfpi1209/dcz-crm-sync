/** Respostas das APIs da home (routes/dashboard.py, routes/kommo_sync.py, routes/upload.py). */

/** GET /api/dashboard */
export interface Fonte {
  snapshot: { id: string; filename: string; row_count: number; uploaded_at: string } | null;
  sync_running?: boolean;
  update_running?: boolean;
  error?: string;
}

/** GET /api/dashboard/ciclos-distinct */
export interface CicloDistinto {
  nome: string;
  total: number | null;
}

export interface TotaisTipo {
  novos?: number;
  regresso?: number;
  recompra?: number;
  rematricula?: number;
  outros?: number;
}

/** GET /api/dashboard/students */
export interface Alunos {
  totals: TotaisTipo;
  by_situacao: Record<string, number>;
  by_nivel: Record<string, number>;
  by_polo: Record<string, number>;
  by_turma: Record<string, number>;
  by_ciclo: Record<string, number>;
  grand_total?: number;
  inbound_transfers?: number;
  card_filters?: { novos?: string; rematricula?: string };
  error?: string;
}

/** GET /api/dashboard/inbound-transfers */
export interface Transferencias {
  ok: boolean;
  error?: string;
  alunos?: { rgm: string; nome: string; polo: string; tipo: string; ciclo: string }[];
}

/** GET /api/dashboard/timeline */
export interface Evolucao {
  periods?: string[];
  series?: { novos?: number[]; rematricula?: number[]; regresso?: number[]; recompra?: number[]; total?: number[] };
  range?: { from: string; to: string };
  error?: string;
}

export interface Agregado {
  grand_total?: number;
  totals?: TotaisTipo;
}

export interface Comparacao {
  label?: string;
  period?: string;
  current?: Agregado;
}

export interface CicloResumo {
  nome: string;
  nivel: string;
  totals: TotaisTipo;
  by_situacao: Record<string, number>;
  by_polo: Record<string, number>;
  grand_total: number;
}

/** GET /api/dashboard/ciclos */
export interface Ciclos {
  ciclos?: CicloResumo[];
  config?: { nome: string; nivel: string }[];
  distinct_nivels?: Record<string, number>;
  comparisons?: { ytd?: Comparacao; ytd_prev?: Comparacao; m6?: Comparacao; m6_prev?: Comparacao };
  error?: string;
}

/** GET /api/lista-alunos/latest */
export interface SaudeFinanceira {
  ok: boolean;
  has_data?: boolean;
  error?: string;
  snapshot?: { uploaded_at: string };
  total_alunos?: number;
  adimplentes?: number;
  inadimplentes?: number;
  pct_inadimplencia?: number;
  filtered_tipo?: string | null;
  filtered_situacao?: string | null;
  filtered_nivel?: string | null;
}

export interface Etapa {
  key: string;
  label: string;
  count: number;
  pct?: number | null;
  delta?: number;
  delta_pct?: number;
  highlight?: boolean;
}

export interface ResumoOntem {
  date?: string;
  vendas?: number;
  leads?: number;
  leads_prev?: number;
  leads_delta_pct?: number;
}

export interface DadosFunil {
  total?: number;
  new_today?: number;
  stages?: Etapa[];
  source?: string;
  synced_at?: string;
  fetched_at?: string;
  stale?: boolean;
  live_error?: string;
  yesterday_summary?: ResumoOntem;
}

/** GET /api/kommo/funnel-live */
export interface Funil {
  ok: boolean;
  data?: DadosFunil;
  warming?: boolean;
  error?: string;
  detail?: string;
}

/** GET /api/kommo/new-leads-today */
export interface LeadsHoje {
  ok: boolean;
  count?: number | null;
}

/** GET /api/kommo/yesterday-summary e /api/dashboard/funnel-yesterday */
export interface Ontem {
  ok: boolean;
  data?: ResumoOntem;
}

/** Filtros aplicados da página (os mesmos do _dashActiveFilters antigo). */
export interface FiltrosDash {
  ciclo: string;
  nivel: string;
  de: string;
  ate: string;
  situacao: string;
  tipo: string;
  polo: string;
}

export type Tipo = "novos_agg" | "novos" | "rematricula" | "regresso" | "recompra";
