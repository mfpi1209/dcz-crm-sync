/* Disparador WhatsApp — embed do app externo tool_whatsapp_alunos via iframe.
 *
 * A página é uma página Flask protegida por permissão ('disparador_whatsapp')
 * que apenas exibe o app externo dentro de um iframe full-height. A hierarquia
 * de acesso é herdada do dcz-crm-sync (nav_can() na sidebar + before_app_request
 * de autenticação global). Toda a lógica de negócio (CSV, templates, jornadas,
 * relatórios, conversão, regras) roda no app externo.
 */

function _dwIframeUrl() {
    const iframe = document.getElementById('dw-iframe');
    if (!iframe) return '';
    return iframe.dataset.src || iframe.getAttribute('data-src') || iframe.src || '';
}

/** Só carrega o embed quando a aba fica visível (evita lazy + parent hidden). */
function _dwEnsureIframeLoaded() {
    const iframe = document.getElementById('dw-iframe');
    if (!iframe) return;
    const url = _dwIframeUrl();
    if (!url || url === 'about:blank') return;
    const cur = iframe.getAttribute('src') || '';
    if (!cur || cur === 'about:blank' || cur !== url) {
        iframe.setAttribute('src', url);
    }
}

function loadDisparadorWhatsapp() {
    _dwEnsureIframeLoaded();
}

function dwReloadIframe() {
    const iframe = document.getElementById('dw-iframe');
    if (!iframe) return;
    const url = _dwIframeUrl();
    if (!url || url === 'about:blank') return;
    iframe.setAttribute('src', 'about:blank');
    setTimeout(() => iframe.setAttribute('src', url), 30);
}
