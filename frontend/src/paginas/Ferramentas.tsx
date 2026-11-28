import { FerramentaIframe } from "./ferramentas/FerramentaIframe";

/** Mesma página estática e mesmos hashes de static/js/ferramentas.js. */
export const FERRAMENTA_BASE = "/static/eduit/app.html";

export function CompararCursos() {
  return <FerramentaIframe titulo="Comparar cursos" url={`${FERRAMENTA_BASE}#comparar`} />;
}
export function RecomendacaoCursos() {
  return <FerramentaIframe titulo="Recomendação de cursos" url={`${FERRAMENTA_BASE}#recomendacao`} />;
}
export function LocalizacaoPolos() {
  return <FerramentaIframe titulo="Localização dos polos" url={`${FERRAMENTA_BASE}#localizacao`} />;
}
export function InfoCursos() {
  return <FerramentaIframe titulo="Informações de cursos" url={`${FERRAMENTA_BASE}#informacoes-cursos`} />;
}
