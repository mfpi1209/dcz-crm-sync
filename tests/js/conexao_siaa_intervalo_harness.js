// Harness sem rede (DOM e fetch simulados): intervalo aleatório e exportação de diagnóstico.
// Uso: node tests/js/conexao_siaa_intervalo_harness.js
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

class El {
    constructor(id) { this.id = id; this._html = ''; this.textContent = ''; this.value = ''; this.checked = false;
        this.disabled = false; this.style = {}; this.children = []; this.className = '';
        this.classList = { add() {}, remove() {}, contains: () => false }; }
    set innerHTML(v) { this._html = String(v); }
    get innerHTML() { return this._html; }
    querySelectorAll() { return []; }
}
const els = {};
const downloads = [];
const document = {
    hidden: false,
    body: { appendChild() {}, removeChild() {} },
    getElementById(id) { return els[id] || (els[id] = new El(id)); },
    createElement(tag) {
        if (tag === 'a') return { click() { downloads.push({ href: this.href, nome: this.download }); } };
        const d = { _t: '' };
        Object.defineProperty(d, 'textContent', { set(v) { d._t = v; }, get() { return d._t; } });
        Object.defineProperty(d, 'innerHTML', { get() { return String(d._t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); } });
        return d;
    },
    querySelectorAll() { return []; },
};

const calls = [];
const sv = { agenda: null, worker: null, modulos: null, diag: 200 };
const SORT = 412;
function novoStatus() {
    sv.agenda = { ativa: false, dias: [0, 1], inicio: '07:00', fim: '22:00', fuso: 'America/Sao_Paulo', intervalo_s: 180,
        versao: 3, intervalo_modo: 'fixo', intervalo_min_min: null, intervalo_max_min: null, aberta_agora: true, proxima_mudanca: '2026-11-16T22:00:00Z' };
    sv.worker = { visto_em: new Date().toISOString(), iniciado_em: '2026-11-16T10:00:00Z', modo: 'jsf', intervalo_s: 180,
        intervalo_modo: 'fixo', intervalo_min_s: null, intervalo_max_s: null, intervalo_sorteado_s: null, proxima_execucao: null };
    const mod = { estado: 'valida', proxima_execucao: '2099-01-01T00:00:00Z', modo_usado: 'jsf', precisa_renovacao: false, ultimo_resultado: null };
    sv.modulos = { academico: mod, financeiro: mod };
}
novoStatus();
const J = (status, obj) => Promise.resolve({ status, json: () => Promise.resolve(obj), headers: { get: () => 'attachment; filename="conexao-siaa-diagnostico-20261116T180000Z.json"' }, blob: () => Promise.resolve({}) });
function fetchFake(url, opts) {
    opts = opts || {};
    const method = opts.method || 'GET';
    const body = opts.body ? JSON.parse(opts.body) : undefined;
    calls.push({ method, url, body });
    const p = new URL(url, 'http://x').pathname.replace('/api/conexao-siaa', '');
    if (p === '/status') return J(200, { ok: true, modulos: sv.modulos, agenda: sv.agenda, pausa_manual: {}, worker: sv.worker });
    if (p === '/historico') return J(200, { ok: true, itens: [], proximo_antes: null });
    if (p === '/renovacao/status') return J(200, { ok: true, config: {}, execucao_atual: null, ultima_execucao: null });
    if (p === '/renovacao/execucoes') return J(200, { ok: true, itens: [], proximo_antes: null });
    if (p === '/agenda' && method === 'PUT') { sv.agenda = Object.assign({}, sv.agenda, body, { versao: body.versao + 1 }); return J(200, { ok: true, agenda: sv.agenda }); }
    if (p === '/diagnostico') return sv.diag === 200 ? J(200, {}) : J(sv.diag, { ok: false, error: 'A session-api demorou demais para responder.' });
    return J(404, { ok: false, error: 'nao' });
}
const ctx = vm.createContext({ document, console, URLSearchParams, URL, Date, Math, JSON, Promise, Uint8Array, parseInt, isNaN,
    setInterval, clearInterval, setTimeout, encodeURIComponent, fetch: fetchFake });
ctx.URL = Object.assign(function (...a) { return new URL(...a); }, { createObjectURL: () => 'blob:x', revokeObjectURL() {} });
ctx.window = { confirm: () => true, crypto: require('crypto').webcrypto };
ctx.crypto = ctx.window.crypto;
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', '..', 'static', 'js', 'conexao_siaa.js'), 'utf8'), ctx);
const run = (c) => vm.runInContext(c, ctx);
const settle = () => new Promise(r => setTimeout(r, 5));
const escritas = () => calls.filter(c => c.method !== 'GET');

(async () => {
    const ok = (n) => console.log('ok -', n);

    // 1) polling não escreve; editor reflete o modo salvo (fixo) e propõe 5 e 8 sem trocar o modo
    run('csPoll()'); await settle();
    assert.deepStrictEqual(escritas(), []);
    assert.strictEqual(els['cs-ag-tipo-iv'].value, 'fixo');
    assert.strictEqual(String(els['cs-ag-min'].value), '5');
    assert.strictEqual(String(els['cs-ag-max'].value), '8');
    assert.strictEqual(els['cs-ag-min'].disabled, true);
    calls.length = 0; run('csPoll()'); await settle(); run('csPoll()'); await settle();
    assert.deepStrictEqual(escritas(), []);
    assert.strictEqual(els['cs-ag-tipo-iv'].value, 'fixo', 'polling não troca o modo');
    ok('polling só faz GET; editor não troca o modo sozinho (propõe 5 e 8)');

    // 2) salvar sem mexer no modo envia fixo
    calls.length = 0; await run('conexaoSiaaSalvarAgenda()'); await settle();
    let w = escritas(); assert.strictEqual(w.length, 1);
    assert.strictEqual(w[0].method, 'PUT');
    assert.strictEqual(w[0].body.intervalo_modo, 'fixo');
    assert.strictEqual(w[0].body.intervalo_s, 180);
    // Fixo: a proposta 5–8 do formulário não é gravada; mantém o que estava salvo (null).
    assert.strictEqual(w[0].body.intervalo_min_min, null);
    assert.strictEqual(w[0].body.intervalo_max_min, null);
    ok('modo fixo salvo continua fixo');

    // 3) validação do cliente no aleatório: mín > máx, fora de 1..30, vazios
    for (const [mn, mx, re] of [['9', '8', /mínimo/], ['0', '8', /1 a 30/], ['5', '31', /1 a 30/], ['', '8', /informe/]]) {
        els['cs-ag-tipo-iv'].value = 'aleatorio'; els['cs-ag-min'].value = mn; els['cs-ag-max'].value = mx;
        calls.length = 0; await run('conexaoSiaaSalvarAgenda()'); await settle();
        assert.deepStrictEqual(escritas(), [], mn + '/' + mx);
        assert(re.test(els['cs-agenda-msg'].textContent), els['cs-agenda-msg'].textContent);
    }
    els['cs-ag-min'].value = '3'; els['cs-ag-max'].value = '9';
    calls.length = 0; await run('conexaoSiaaSalvarAgenda()'); await settle();
    w = escritas(); assert.strictEqual(w.length, 1);
    assert.deepStrictEqual([w[0].body.intervalo_modo, w[0].body.intervalo_min_min, w[0].body.intervalo_max_min], ['aleatorio', 3, 9]);
    assert.strictEqual(w[0].body.versao, 4);
    ok('aleatório validado no cliente e enviado só após o admin escolher e salvar');

    // 4) exibição: sorteado e próxima execução (worker e módulos), com aviso
    novoStatus();
    sv.agenda.ativa = true; sv.agenda.intervalo_modo = 'aleatorio'; sv.agenda.intervalo_min_min = 5; sv.agenda.intervalo_max_min = 8;
    sv.worker = Object.assign(sv.worker, { intervalo_modo: 'aleatorio', intervalo_min_s: 300, intervalo_max_s: 480,
        intervalo_sorteado_s: SORT, intervalo_s: SORT, proxima_execucao: '2099-01-01T00:07:00Z' });
    await run('conexaoSiaaLoadStatus()'); await settle();
    const wk = els['cs-worker'].innerHTML;
    assert(/Aleatório: 5 a 8 min/.test(wk), wk);
    assert(/Sorteado agora/.test(wk) && /412 s/.test(wk));
    assert(/Próxima execução agendada/.test(wk) && /não evita detecção/.test(wk));
    assert(/Próxima execução agendada/.test(els['cs-modulos'].innerHTML));
    assert(/Aleatório: 5 a 8 min/.test(els['cs-agenda-resumo'].innerHTML));
    ok('exibe modo, configuração, sorteado e próxima execução agendada');

    // 5) o editor só passa a mostrar Aleatório depois de recarregar a agenda salva
    run('conexaoSiaaDescartarAgenda()');
    assert.strictEqual(els['cs-ag-tipo-iv'].value, 'aleatorio');
    assert.strictEqual(String(els['cs-ag-min'].value), '5');
    ok('editor reflete o modo aleatório salvo');

    // 6) exportação: GET com período, sem escrita, baixa o arquivo; erro mostra mensagem
    document.getElementById('cs-diag-periodo').value = '3d';
    calls.length = 0; await run('conexaoSiaaExportarDiagnostico()'); await settle();
    assert.strictEqual(calls.length, 1);
    assert.deepStrictEqual([calls[0].method, calls[0].url], ['GET', '/api/conexao-siaa/diagnostico?periodo=3d']);
    assert.strictEqual(downloads.length, 1);
    assert.strictEqual(downloads[0].nome, 'conexao-siaa-diagnostico-20261116T180000Z.json');
    sv.diag = 504; calls.length = 0; await run('conexaoSiaaExportarDiagnostico()'); await settle();
    assert(/demorou/.test(els['cs-diag-msg'].textContent)); assert.strictEqual(downloads.length, 1);
    assert.strictEqual(els['cs-diag-btn'].disabled, false);
    ok('exportação faz um GET, baixa o arquivo e trata erro');
})().catch(e => { console.error('FALHOU:', e); process.exit(1); });
