/** Respostas das APIs do Repasse (routes/repasse.py). */

export interface Filtros {
  ok: boolean;
  ciclos: string[];
  tipos: string[];
  turmas_por_ciclo: Record<string, string[]>;
}

export interface Taxa {
  taxa: number;
}

/** Id do agente: kommo_user_id (número) ou "bw:<nome>" para dono do Bwipo sem depara. */
export type IdAgente = number | string;

export interface Agente {
  id: IdAgente;
  nome: string;
  owner_id?: string | number | null;
  qtd_alunos: number;
  total_valor: number;
  media_por_aluno?: number;
}

export interface Agentes {
  ok: boolean;
  is_admin?: boolean;
  agentes: Agente[];
  totais: { valor: number; alunos: number; agentes?: number };
}

export interface AlunoDetalhe {
  rgm: string;
  valor: number;
  tipo_pagamento: string;
  turma: string;
  ciclo: string;
}

export interface Detalhe {
  ok: boolean;
  alunos: AlunoDetalhe[];
  total: number;
}

export interface Planilha {
  ok: boolean;
  page: number;
  pages: number;
  total: number;
  valor: number;
  rows: { rgm: string; valor: number; turma: string; beleza: string; ciclo: string }[];
}

export interface LinhaMatriculado {
  rgm: string;
  polo: string;
  nivel: string;
  modalidade: string;
  data_matricula: string;
  ciclo: string;
  turma: string;
  valor: number;
  tipo_pagamento: string;
  pagou: boolean;
}

export interface Matriculados {
  ok: boolean;
  page: number;
  pages: number;
  total: number;
  pagos: number;
  valor: number;
  filtros: {
    ciclos: string[];
    niveis: string[];
    meses: { id: string; label: string }[];
    ciclo: string;
  };
  rows: LinhaMatriculado[];
}

/** Filtros de cima (valem para agentes, detalhe e planilha). */
export interface FiltrosTela {
  ciclo: string;
  tipo: string;
  turma: string;
}
