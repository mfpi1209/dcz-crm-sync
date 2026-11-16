// Harness sem rede: DOM e fetch simulados. Roda static/js/conexao_siaa.js num vm.
// Uso: node tests/js/conexao_siaa_renovacao_harness.js   (sai com código != 0 se falhar)
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

class El {
    constructor(id) { this.id = id; this._html = ''; this.textContent = ''; this.value = '50'; this.checked = false;
        this.disabled = false; this.style = {}; this.children = []; this.className = '';
        this.classList = { add() {}, remove() {}, contains: () => false }; }
    set innerHTML(v) { this._html = String(v); }
    get innerHTML() { return this._html; }
    querySelectorAll() { return []; }
}
const els = {};
const document = {
    hidden: false,
    getElementById(id) { return els[id] || (els[id] = new El(id)); },
    createElement() {
        const d = { _t: '' };
        Object.defineProperty(d, 'textContent', { set(v) { d._t = v; }, get() { return d._t; } });
        Object.defineProperty(d, 'innerHTML', { get() { return String(d._t).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;'); } });
        return d;
    },
    querySelectorAll() { return []; },
};

// ---- servidor simulado ----------------------------------------------------
const calls = [];               // { method, url, body }
const sv = { exec: [], status: null, falhaSolicitar: null, confirmar: true, confirms: [] };
function execFake(i) {
    return { id: 'rn-' + i, tipo: i % 2 ? 'manual' : 'automatica', estado: 'concluida',
        gatilho: { modulos: ['financeiro'], motivo: 'sessao_expirada' },
        criada_em: new Date(Date.UTC(2026, 10, 16, 10, 0, 0) - i * 3600e3).toISOString(),
        inicio: new Date(Date.UTC(2026, 10, 16, 10, 0, 5) - i * 3600e3).toISOString(), fim: null, duracao_ms: 170000,
        ultimo_progresso: { etapa: 'finalizando', em: '2026-11-16T10:03:00Z' }, janela_fechou_durante: false, login: 'ok',
        modulos: { academico: { coleta: 'ok', http: 200, publicacao: 'ok', vinculo: null, sessao_validada: true },
                   financeiro: { coleta: 'ok', http: 200, publicacao: 'incerto', vinculo: null, sessao_validada: false } },
        erro: null, proxima_acao: 'nenhuma' };
}
for (let i = 0; i < 30; i++) sv.exec.push(execFake(i));
sv.status = {
    ok: true, gerado_em: '2026-11-16T10:05:00Z', estado: 'monitorando', motivo_estado: 'Agenda aberta.',
    config: { ativa: true, suspensa: false, versao: 4, atualizada_em: null },
    executor: { visto_em: '2026-11-16T10:04:50Z', ocupado: false }, episodio: null,
    execucao_atual: null, ultima_execucao: execFake(0), ultima_renovacao_ok: '2026-11-16T10:03:00Z', proxima_acao: 'nenhuma',
};
const J = (status, obj) => Promise.resolve({ status, json: () => Promise.resolve(obj) });
function fetchFake(url, opts) {
    opts = opts || {};
    const method = opts.method || 'GET';
    const body = opts.body ? JSON.parse(opts.body) : undefined;
    calls.push({ method, url, body });
    const u = new URL(url, 'http://x');
    const p = u.pathname.replace('/api/conexao-siaa', '');
    if (p === '/status') return J(200, { ok: true, modulos: {}, agenda: {}, pausa_manual: {}, worker: {} });
    if (p === '/historico') return J(200, { ok: true, itens: [], proximo_antes: null });
    if (p === '/renovacao/status') return J(200, sv.status);
    if (p === '/renovacao/execucoes') {
        const lim = parseInt(u.searchParams.get('limite'), 10), ant = u.searchParams.get('antes');
        let l = sv.exec; if (ant) l = l.filter(x => x.criada_em < ant);
        const it = l.slice(0, lim);
        return J(200, { ok: true, itens: it, proximo_antes: l.length > lim ? it[it.length - 1].criada_em : null });
    }
    if (p === '/renovacao/solicitar') {
        if (sv.falhaSolicitar) return J(sv.falhaSolicitar, { ok: false, error: 'x' });
        return J(201, { ok: true, execucao: execFake(99) });
    }
    if (p === '/renovacao/config' || p === '/renovacao/suspensao') return J(200, { ok: true, config: sv.status.config });
    if (p === '/renovacao/liberar') return J(409, { ok: false, error: 'versao', atual: 7, message: 'x' });
    return J(404, { ok: false, error: 'nao' });
}

const ctx = vm.createContext({
    document, console, URLSearchParams, URL, Date, Math, JSON, Promise, Uint8Array, parseInt, isNaN, setInterval, clearInterval,
    fetch: fetchFake,
});
ctx.window = { confirm: (m) => { sv.confirms.push(m); return sv.confirmar; }, crypto: require('crypto').webcrypto };
ctx.crypto = ctx.window.crypto;
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', '..', 'static', 'js', 'conexao_siaa.js'), 'utf8'), ctx);
document.getElementById('cs-rn-limite').value = '25';
const run = (code) => vm.runInContext(code, ctx);
const settle = () => new Promise(r => setTimeout(r, 5));
const escritas = () => calls.filter(c => c.method !== 'GET');
const body = (id) => els[id].innerHTML;

(async () => {
    let n = 0; const ok = (nome) => { n++; console.log('ok -', nome); };

    // 1) polling: só GET; 1ª página; nenhuma escrita
    calls.length = 0;
    run('csPoll()'); await settle();
    assert.deepStrictEqual(escritas(), [], 'polling não pode escrever');
    assert(calls.some(c => c.url.includes('/renovacao/status')));
    assert(calls.some(c => c.url.includes('/renovacao/execucoes')));
    assert.strictEqual(els['cs-rn-body'].innerHTML.split('<tr>').length - 1, 25);
    assert(/Publicação: Confirmada/.test(body('cs-rn-body')) && /Sessão validada: Sim/.test(body('cs-rn-body')));
    assert(/Publicação confirmada/.test(body('cs-renov-corpo')) && /Sessão validada/.test(body('cs-renov-corpo')));
    ok('polling só faz GET e preenche a 1ª página');

    // 2) paginação preservada: Carregar mais, depois polling não recarrega a 1ª página
    run('conexaoSiaaRenovExecucoesMais()'); await settle();
    assert.strictEqual(run('csRenov.exec.itens.length'), 30);
    assert.strictEqual(run('csRenov.exec.proximo'), null);
    calls.length = 0;
    run('csPoll()'); await settle();
    assert(!calls.some(c => c.url.includes('/renovacao/execucoes')), 'polling não recarrega execuções após Carregar mais');
    assert(calls.some(c => c.url.includes('/renovacao/status')));
    assert.strictEqual(run('csRenov.exec.itens.length'), 30);
    assert.deepStrictEqual(escritas(), []);
    ok('paginação das execuções preservada no polling');

    // 3) Atualizar volta à 1ª página
    run('conexaoSiaaRefresh()'); await settle();
    assert.strictEqual(run('csRenov.exec.itens.length'), 25);
    assert.strictEqual(run('csRenov.exec.paginou'), false);
    calls.length = 0; run('csPoll()'); await settle();
    assert(calls.some(c => c.url.includes('/renovacao/execucoes')));
    ok('Atualizar volta à 1ª página e o polling volta a recarregar');

    // 4) Renovar agora: um único pedido por confirmação (duplo clique)
    calls.length = 0; sv.confirms.length = 0;
    const a = run('conexaoSiaaRenovSolicitar()'); const b = run('conexaoSiaaRenovSolicitar()');
    await Promise.all([a, b]); await settle();
    let w = escritas();
    assert.strictEqual(w.length, 1, 'duplo clique = 1 POST');
    assert.strictEqual(w[0].url, '/api/conexao-siaa/renovacao/solicitar');
    assert(/^[0-9a-f]{32}$/.test(w[0].body.pedido_id));
    assert.strictEqual(w[0].body.confirmacao, 'substituir_sessoes');
    assert.strictEqual(sv.confirms.length, 1);
    assert(/credenciais/i.test(sv.confirms[0]) && /substitu/i.test(sv.confirms[0]));
    const id1 = w[0].body.pedido_id;
    ok('Renovar agora envia um único pedido por confirmação');

    // 5) nova confirmação = novo pedido_id; cancelar no confirm não envia
    calls.length = 0; await run('conexaoSiaaRenovSolicitar()'); await settle();
    w = escritas(); assert.strictEqual(w.length, 1); assert.notStrictEqual(w[0].body.pedido_id, id1);
    sv.confirmar = false; calls.length = 0; await run('conexaoSiaaRenovSolicitar()');
    assert.deepStrictEqual(escritas(), []); sv.confirmar = true;
    ok('nova confirmação gera novo pedido_id; cancelar não envia');

    // 6) falha ambígua reaproveita o pedido_id (idempotência)
    sv.falhaSolicitar = 504; calls.length = 0;
    await run('conexaoSiaaRenovSolicitar()'); await settle();
    const pid = escritas()[0].body.pedido_id;
    sv.falhaSolicitar = null; calls.length = 0;
    await run('conexaoSiaaRenovSolicitar()'); await settle();
    assert.strictEqual(escritas()[0].body.pedido_id, pid);
    ok('timeout reenvia o mesmo pedido_id');

    // 7) botão desabilitado com execução ativa e não envia
    sv.status = Object.assign({}, sv.status, { estado: 'em_execucao', execucao_atual: Object.assign(execFake(50), { estado: 'em_execucao', duracao_ms: null, fim: null }) });
    await run('csRenovCarregarStatus()');
    assert(/id="cs-rn-btn-renovar"[^>]*disabled/.test(body('cs-renov-corpo')), 'Renovar agora desabilitado');
    calls.length = 0; await run('conexaoSiaaRenovSolicitar()');
    assert.deepStrictEqual(escritas(), []);
    ok('Renovar agora desabilitado e inerte com execução ativa');

    // 8) liberar só com episódio bloqueado; 409 mostra mensagem e recarrega
    sv.status = Object.assign({}, sv.status, { estado: 'bloqueada', execucao_atual: null,
        episodio: { id: 'ep-1', desde: '2026-11-16T10:00:00Z', modulos: ['financeiro'], motivo: 'sessao_expirada', tentativas_automaticas: 1, bloqueado: true, bloqueio_motivo: 'incerta', liberado_em: null } });
    await run('csRenovCarregarStatus()');
    assert(/Liberar nova tentativa/.test(body('cs-renov-corpo')));
    calls.length = 0; await run('conexaoSiaaRenovLiberar()'); await settle();
    assert.strictEqual(escritas().length, 1);
    assert.deepStrictEqual(escritas()[0].body, { episodio_id: 'ep-1', versao: 4 });
    assert(/Outra pessoa alterou/.test(els['cs-renov-msg'].textContent));
    assert(calls.some(c => c.method === 'GET' && c.url.includes('/renovacao/status')), 'recarrega após 409');
    sv.status = Object.assign({}, sv.status, { estado: 'monitorando', episodio: null });
    await run('csRenovCarregarStatus()');
    assert(!/Liberar nova tentativa/.test(body('cs-renov-corpo')));
    ok('Liberar visível só com bloqueio; 409 recarrega e avisa');

    // 9) ativar envia versão; confirmação explícita
    calls.length = 0; sv.confirms.length = 0;
    await run('conexaoSiaaRenovAtivar(false)'); await settle();
    assert.deepStrictEqual(escritas()[0].body, { ativa: false, versao: 4 });
    assert.strictEqual(escritas()[0].method, 'PUT');
    await run('conexaoSiaaRenovSuspender(true)'); await settle();
    assert.deepStrictEqual(escritas()[1].body, { suspensa: true, versao: 4 });
    ok('ativar/suspender enviam versao');

    console.log('\n' + n + ' verificações passaram.');
})().catch(e => { console.error('FALHOU:', e); process.exit(1); });
