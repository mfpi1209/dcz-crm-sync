import { lazy, type ComponentType, type LazyExoticComponent } from "react";
import { Repasse } from "./paginas/Repasse";
import { UsoDoDashboard } from "./paginas/UsoDoDashboard";

// Minha Performance traz o chart.js: carrega só quando a tela abre (fora do pacote principal).
// O App.tsx põe a tela num Suspense e num ErroTela.
const MinhaPerformance = lazy(() => import("./paginas/MinhaPerformance").then((m) => ({ default: m.MinhaPerformance })));

/**
 * Telas já migradas para o front novo: chave = mesmo nome da página no painel
 * (helpers.ALL_PAGES). Enquanto uma tela não está aqui, o menu leva ao painel atual.
 *
 * Para migrar uma tela: criar o componente em src/paginas/ e registrar aqui.
 * A permissão continua no servidor; a rota só aparece para quem tem a página no /api/me.
 */
export const TELAS_NOVAS: Record<string, ComponentType | LazyExoticComponent<ComponentType>> = {
  page_views: UsoDoDashboard, // etapa 3 (27/11): piloto
  repasse: Repasse, // 28/11: no menu antigo, o link só troca depois da aprovação
  minha_performance: MinhaPerformance, // 28/11: idem
};

export const NOMES_TELAS_NOVAS: ReadonlySet<string> = new Set(Object.keys(TELAS_NOVAS));
