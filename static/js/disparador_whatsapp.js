/* Disparador WhatsApp — embed do app externo tool_whatsapp_alunos via iframe.
 *
 * A página é uma página Flask protegida por permissão ('disparador_whatsapp')
 * que apenas exibe o app externo dentro de um iframe full-height. A hierarquia
 * de acesso é herdada do dcz-crm-sync (nav_can() na sidebar + before_app_request
 * de autenticação global). Toda a lógica de negócio (CSV, templates, jornadas,
 * relatórios, conversão, regras) roda no app externo.
 */

function loadDisparadorWhatsapp() {
    const iframe = document.getElementById('dw-iframe');
    if (!iframe) return;
    if (!iframe.dataset.dwLoaded) {
        iframe.dataset.dwLoaded = '1';
    }
}

function dwReloadIframe() {
    const iframe = document.getElementById('dw-iframe');
    if (!iframe) return;
    const url = iframe.getAttribute('src');
    iframe.setAttribute('src', 'about:blank');
    setTimeout(() => iframe.setAttribute('src', url), 30);
}
