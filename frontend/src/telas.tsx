import { lazy, type ComponentType } from "react";
import { Repasse } from "./paginas/Repasse";
import { UsoDoDashboard } from "./paginas/UsoDoDashboard";

// Carregada sob demanda: leva o chart.js para um pedaço separado do bundle.
const Dashboard = lazy(() => import("./paginas/Dashboard").then((m) => ({ default: m.Dashboard })));

/**
 * Telas já migradas para o front novo: chave = mesmo nome da página no painel
 * (helpers.ALL_PAGES). Enquanto uma tela não está aqui, o menu leva ao painel atual.
 *
 * Para migrar uma tela: criar o componente em src/paginas/ e registrar aqui.
 * A permissão continua no servidor; a rota só aparece para quem tem a página no /api/me.
 */
export const TELAS_NOVAS: Record<string, ComponentType> = {
  page_views: UsoDoDashboard, // etapa 3 (27/11): piloto
  repasse: Repasse, // 28/11: no menu antigo, o link só troca depois da aprovação
  dashboard: Dashboard, // 28/11: idem (supervisores ainda abrem o painel atual)
};

export const NOMES_TELAS_NOVAS: ReadonlySet<string> = new Set(Object.keys(TELAS_NOVAS));
