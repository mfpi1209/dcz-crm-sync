// ---------------------------------------------------------------------------
// Ferramentas — eduit (iframes com navegação direta por seção)
// ---------------------------------------------------------------------------
const FERRAMENTA_BASE = '/static/eduit/app.html';

const FERRAMENTA_MAP = {
    comparar_cursos:    { iframe: 'iframe-comparar',     hash: 'comparar' },
    recomendacao_cursos:{ iframe: 'iframe-recomendacao',  hash: 'recomendacao' },
    localizacao_polos:  { iframe: 'iframe-localizacao',   hash: 'localizacao' },
    info_cursos:        { iframe: 'iframe-infocursos',    hash: 'informacoes-cursos' },
};

function loadFerramenta(page) {
    const cfg = FERRAMENTA_MAP[page];
    if (!cfg) return;
    const iframe = document.getElementById(cfg.iframe);
    if (!iframe) return;
    const target = FERRAMENTA_BASE + '#' + cfg.hash;
    if (!iframe.src || !iframe.src.includes(cfg.hash)) {
        iframe.src = target;
    }
}
