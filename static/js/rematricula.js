/* Rematrícula — embed do painel tool_whatsapp_alunos (/rematricula) via iframe. */

function loadRematricula() {
    const iframe = document.getElementById('remat-iframe');
    if (!iframe) return;
    if (!iframe.dataset.rematLoaded) {
        iframe.dataset.rematLoaded = '1';
    }
}

function rematReloadIframe() {
    const iframe = document.getElementById('remat-iframe');
    if (!iframe) return;
    const url = iframe.getAttribute('src');
    iframe.setAttribute('src', 'about:blank');
    setTimeout(() => iframe.setAttribute('src', url), 30);
}
