import { type ComponentType, lazy } from "react";
import { BwipoComercial } from "./paginas/BwipoComercial";
import { CompararCursos, InfoCursos, LocalizacaoPolos, RecomendacaoCursos } from "./paginas/Ferramentas";
import { Repasse } from "./paginas/Repasse";
import { UsoDoDashboard } from "./paginas/UsoDoDashboard";

// Telas com gráfico carregam sob demanda: levam o chart.js, que fica num pedaço separado
// do pacote principal. O App.tsx põe cada tela num Suspense e num ErroTela.
const Inadimplencia = lazy(() => import("./paginas/Inadimplencia").then((m) => ({ default: m.Inadimplencia })));
const Dashboard = lazy(() => import("./paginas/Dashboard").then((m) => ({ default: m.Dashboard })));
const MinhaPerformance = lazy(() => import("./paginas/MinhaPerformance").then((m) => ({ default: m.MinhaPerformance })));

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
  bwipo_comercial: BwipoComercial, // 28/11: idem, aguarda aprovação
  minha_performance: MinhaPerformance, // 28/11: idem
  dashboard: Dashboard, // 28/11: idem (supervisores ainda abrem o painel atual)
  inadimplencia: Inadimplencia, // 28/11: idem, aguarda aprovação
  comparar_cursos: CompararCursos, // 28/11: ferramentas em iframe (static/eduit/app.html)
  recomendacao_cursos: RecomendacaoCursos,
  localizacao_polos: LocalizacaoPolos,
  info_cursos: InfoCursos,
};

/** Telas que ocupam a área de conteúdo inteira, sem padding (ex.: ferramentas em iframe). */
export const TELAS_CHEIAS: ReadonlySet<string> = new Set([
  "comparar_cursos",
  "recomendacao_cursos",
  "localizacao_polos",
  "info_cursos",
]);

export const NOMES_TELAS_NOVAS: ReadonlySet<string> = new Set(Object.keys(TELAS_NOVAS));
