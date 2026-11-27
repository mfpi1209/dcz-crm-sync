/**
 * Registra a abertura de uma tela em /api/track-page-view, como o painel atual
 * (trackPageView em static/js/utils.js). Sem isso, as telas migradas sumiriam
 * do relatório "Uso do Dashboard". Nunca atrapalha a navegação.
 */
export function registrarAcesso(pagina: string): void {
  if (!pagina) return;
  try {
    const corpo = JSON.stringify({ page: pagina });
    if (typeof navigator !== "undefined" && navigator.sendBeacon) {
      navigator.sendBeacon("/api/track-page-view", new Blob([corpo], { type: "application/json" }));
      return;
    }
    fetch("/api/track-page-view", {
      method: "POST",
      credentials: "same-origin",
      headers: { "Content-Type": "application/json" },
      body: corpo,
      keepalive: true,
    }).catch(() => {});
  } catch {
    /* registro de uso nunca derruba a tela */
  }
}
