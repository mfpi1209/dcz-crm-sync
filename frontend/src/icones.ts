/**
 * Ícone (Material Symbols) das telas que aparecem soltas no menu — seções
 * Fixadas, Mais usadas e Geral. Subitens de grupo não levam ícone: ficam
 * presos à linha-guia do grupo. Tela sem ícone aqui usa o padrão.
 */
const ICONES: Record<string, string> = {
  dashboard: "space_dashboard",
  search: "search",
  sync: "sync",
  avisos: "notifications",
  profile: "person",
  siaa_consulta: "manage_search",
  siaa_sessao: "key",
  minha_performance: "insights",
  meus_atendimentos: "support_agent",
  bwipo_comercial: "monitoring",
  comercial_rgm: "leaderboard",
  repasse: "payments",
  page_views: "query_stats",
  ia_comercial: "smart_toy",
  disparador_whatsapp: "chat",
  inadimplencia: "account_balance_wallet",
  rematricula: "autorenew",
  solicitacoes_ti: "add_circle",
  meus_chamados_ti: "confirmation_number",
  chamados_ti: "view_kanban",
  config: "tune",
  logs: "receipt_long",
  match_merge: "merge",
  precos_bwipo: "sell",
  localizacao_polos: "location_on",
  cadastro_leads: "person_add",
  subir_blog: "edit_note",
};

export const ICONE_PADRAO = "article";

export function iconeDaTela(pagina: string): string {
  return ICONES[pagina] ?? ICONE_PADRAO;
}
