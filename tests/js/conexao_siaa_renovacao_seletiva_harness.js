// Harness sem rede da renovação seletiva por módulo: DOM e fetch simulados.
// Uso: node tests/js/conexao_siaa_renovacao_seletiva_harness.js   (sai com código != 0 se falhar)
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

class El {
    constructor(id) { this.id = id; this._html = ''; this.textContent = ''; this.value = '25'; this.checked = false;
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

const calls = [];
const sv = { status: null, exec: [], liberar: null, confirmar: true, confirms: [] };
const MOD_OK = { coleta: 'ok', http: 200, publicacao: 'ok', vinculo: null, sessao_validada: true };
const MOD_FALHOU = { coleta: 'falhou', http: null, publicacao: 'nao_executada', vinculo: null, sessao_validada: false };
const MOD_ZERO = { coleta: 'nao_executada', http: null, publicacao: 'nao_executada', vinculo: null, sessao_validada: null };
function exec(over) {
    return Object.assign({ id: 'rn-1', tipo: 'automatica', estado: 'concluida',
        gatilho: { modulos: ['academico'], motivo: 'sessao_expirada' },
        criada_em: '2026-11-19T10:00:00Z', inicio: '2026-11-19T10:00:05Z', fim: '2026-11-19T10:03:00Z', duracao_ms: 170000,
        ultimo_progresso: null, janela_fechou_durante: false, login: 'ok', erro: null, proxima_acao: 'nenhuma',
        modulos: { academico: Object.assign({}, MOD_OK), financeiro: Object.assign({}, MOD_OK) } }, over);
}
function status(over) {
    return Object.assign({ ok: true, gerado_em: '2026-11-19T10:05:00Z', estado: 'monitorando', motivo_estado: null,
        config: { ativa: true, suspensa: false, versao: 4, atualizada_em: null },
        executor: { visto_em: '2026-11-19T10:04:50Z', ocupado: false }, episodio: null,
        execucao_atual: null, ultima_execucao: null, ultima_renovacao_ok: null, proxima_acao: 'nenhuma' }, over);
}
const J = (st, obj) => Promise.resolve({ status: st, json: () => Promise.resolve(obj) });
function fetchFake(url, opts) {
    opts = opts || {};
    const method = opts.method || 'GET';
    const body = opts.body ? JSON.parse(opts.body) : undefined;
    calls.push({ method, url, body });
    const p = new URL(url, 'http://x').pathname.replace('/api/conexao-siaa', '');
    if (p === '/status') return J(200, { ok: true, modulos: {}, agenda: {}, pausa_manual: {}, worker: {} });
    if (p === '/historico') return J(200, { ok: true, itens: [], proximo_antes: null });
    if (p === '/renovacao/status') return J(200, sv.status);
    if (p === '/renovacao/execucoes') return J(200, { ok: true, itens: sv.exec, proximo_antes: null });
    if (p === '/renovacao/solicitar') return J(201, { ok: true, execucao: exec({ estado: 'pendente' }) });
    if (p === '/renovacao/liberar') return J(200, sv.liberar);
    return J(404, { ok: false, error: 'nao' });
}

const ctx = vm.createContext({
    document, console, URLSearchParams, URL, Date, Math, JSON, Promise, Uint8Array, parseInt, isNaN, setInterval, clearInterval,
    fetch: fetchFake,
});
ctx.window = { confirm: (m) => { sv.confirms.push(m); return sv.confirmar; }, crypto: require('crypto').webcrypto };
ctx.crypto = ctx.window.crypto;
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', '..', 'static', 'js', 'conexao_siaa.js'), 'utf8'), ctx);
const run = (code) => vm.runInContext(code, ctx);
const settle = () => new Promise(r => setTimeout(r, 5));
const escritas = () => calls.filter(c => c.method !== 'GET');
const html = (id) => els[id].innerHTML;
// Linha <tr> da tabela que começa com o nome do módulo.
function linha(h, nome) {
    const m = h.split('<tr>').filter(t => t.indexOf('<td>' + nome + '</td>') === 0);
    assert(m.length >= 1, 'linha de ' + nome + ' não encontrada');
    return m[0];
}
async function carregar(st, execs) {
    sv.status = st; sv.exec = execs || [];
    await run('csRenovCarregarStatus()');
    await run('csRenovCarregarExecucoes(false)');
    await settle();
}

(async () => {
    let n = 0; const ok = (nome) => { n++; console.log('ok -', nome); };

    // 1) só acadêmico solicitado: financeiro "Não solicitado" (execução e histórico), nunca falha
    let e = exec({ modulos_solicitados: ['academico'], resultado: 'completo',
        modulos: { academico: Object.assign({ situacao: 'ok' }, MOD_OK), financeiro: Object.assign({ situacao: 'nao_solicitado' }, MOD_ZERO) } });
    await carregar(status({ ultima_execucao: e }), [e]);
    let c = html('cs-renov-corpo');
    assert(/Sucesso completo/.test(c));
    assert(/Módulos solicitados<\/dt><dd>Acadêmico<\/dd>/.test(c));
    assert(/Não solicitado/.test(linha(c, 'Financeiro')) && !/Falhou/.test(linha(c, 'Financeiro')));
    assert(/Renovado/.test(linha(c, 'Acadêmico')));
    let h = html('cs-rn-body');
    assert(/Não solicitado/.test(h) && !/Falhou/.test(h) && /Sucesso completo/.test(h));
    ok('só acadêmico: financeiro aparece como Não solicitado');

    // 2) só financeiro solicitado, mesmo sem `situacao` (API não manda) e com dados zerados
    e = exec({ modulos_solicitados: ['financeiro'], resultado: 'completo',
        modulos: { academico: Object.assign({}, MOD_ZERO), financeiro: Object.assign({}, MOD_OK) } });
    await carregar(status({ ultima_execucao: e }), [e]);
    c = html('cs-renov-corpo');
    assert(/Módulos solicitados<\/dt><dd>Financeiro<\/dd>/.test(c));
    assert(/Não solicitado/.test(linha(c, 'Acadêmico')));
    assert(/Renovado/.test(linha(c, 'Financeiro')));
    ok('só financeiro: acadêmico aparece como Não solicitado');

    // 3) ambos solicitados, completo
    e = exec({ modulos_solicitados: ['academico', 'financeiro'], resultado: 'completo',
        modulos: { academico: Object.assign({ situacao: 'ok' }, MOD_OK), financeiro: Object.assign({ situacao: 'ok' }, MOD_OK) } });
    await carregar(status({ ultima_execucao: e }), [e]);
    c = html('cs-renov-corpo');
    assert(/Módulos solicitados<\/dt><dd>Acadêmico e Financeiro<\/dd>/.test(c));
    assert(!/Não solicitado/.test(c) && /Sucesso completo/.test(c));
    ok('ambos solicitados: sucesso completo');

    // 4) parcial: um ok, outro falhou -> "Parcial", nem sucesso nem falha total
    e = exec({ estado: 'falhou', erro: 'coleta_falhou', modulos_solicitados: ['academico', 'financeiro'], resultado: 'parcial',
        modulos: { academico: Object.assign({ situacao: 'ok' }, MOD_OK), financeiro: Object.assign({ situacao: 'falhou' }, MOD_FALHOU) } });
    await carregar(status({ ultima_execucao: e }), [e]);
    c = html('cs-renov-corpo');
    const res = c.split('<dt>Resultado</dt>')[1].split('</dd>')[0];
    assert(/Parcial/.test(res), 'resultado parcial');
    assert(!/Sucesso|Falhou|Concluída|t-ok|t-bad/.test(res.replace(/Renovado: Acadêmico · não renovado: Financeiro/, '')), 'parcial não é sucesso nem falha: ' + res);
    assert(/Renovado: Acadêmico · não renovado: Financeiro/.test(res));
    assert(/Falhou/.test(linha(c, 'Financeiro')) && /Renovado/.test(linha(c, 'Acadêmico')));
    h = html('cs-rn-body');
    const colRes = h.split('</td>')[2];
    assert(/Parcial/.test(colRes) && !/Falhou/.test(colRes), 'histórico: coluna Resultado parcial');
    ok('parcial aparece como Parcial (execução e histórico)');

    // 5) bloqueio individual do episódio + alvos nos controles e nas confirmações
    const ep = { id: 'ep-1', desde: '2026-11-19T09:00:00Z', modulos: ['academico', 'financeiro'], motivo: 'sessao_expirada',
        tentativas_automaticas: 1, bloqueado: true, bloqueio_motivo: 'falha_execucao', liberado_em: null,
        modulos_estado: {
            academico: { tentativas_automaticas: 1, bloqueado: false, bloqueio_motivo: null, resolvido: true, resolvido_em: '2026-11-19T10:03:00Z', liberado_em: null },
            financeiro: { tentativas_automaticas: 1, bloqueado: true, bloqueio_motivo: 'resultado_incerto', resolvido: false, resolvido_em: null, liberado_em: null } } };
    await carregar(status({ estado: 'bloqueada', episodio: ep, ultima_execucao: e,
        alvos: { renovar_agora: ['financeiro'], liberar: ['financeiro'], automatica: [] } }), [e]);
    c = html('cs-renov-corpo');
    const tabEp = c.split('cs-rn-ep-modulos')[1];
    assert(tabEp, 'tabela de módulos do episódio');
    assert(/Bloqueado<\/span> resultado incerto/.test(linha(tabEp, 'Financeiro')));
    assert(/Resolvido/.test(linha(tabEp, 'Acadêmico')) && !/Bloqueado/.test(linha(tabEp, 'Acadêmico')));
    assert(/<td>1<\/td>/.test(linha(tabEp, 'Financeiro')), 'tentativas por módulo');
    assert(/Liberar nova tentativa \(Financeiro\)/.test(c));
    assert(/"Renovar agora" atinge: Financeiro\./.test(c));
    assert(/Renovação automática atingiria agora<\/dt><dd>Nenhum módulo elegível/.test(c));
    ok('bloqueio individual do episódio exibido por módulo');

    sv.confirms.length = 0; calls.length = 0; sv.confirmar = false;
    await run('conexaoSiaaRenovSolicitar()');
    await run('conexaoSiaaRenovLiberar()');
    assert.deepStrictEqual(escritas(), [], 'cancelar na confirmação não envia');
    assert(/Módulos atingidos: Financeiro\./.test(sv.confirms[0]) && !/Acadêmico/.test(sv.confirms[0]));
    assert(/Liberar nova tentativa para: Financeiro\?/.test(sv.confirms[1]) && !/Acadêmico/.test(sv.confirms[1]));
    sv.confirmar = true;
    sv.liberar = { ok: true, episodio: Object.assign({}, ep, { bloqueado: false }), modulos_liberados: ['financeiro'] };
    calls.length = 0;
    await run('conexaoSiaaRenovLiberar()'); await settle();
    assert.deepStrictEqual(escritas().map(x => x.body), [{ episodio_id: 'ep-1', versao: 4 }], 'corpo do liberar inalterado');
    assert(/Nova tentativa liberada: Financeiro\./.test(els['cs-renov-msg'].textContent));
    calls.length = 0;
    await run('conexaoSiaaRenovSolicitar()'); await settle();
    const w = escritas();
    assert.strictEqual(w.length, 1);
    assert.deepStrictEqual(Object.keys(w[0].body).sort(), ['confirmacao', 'pedido_id'], 'corpo do solicitar inalterado');
    ok('confirmações listam os módulos certos; corpos dos POSTs inalterados');

    // 6) alvos com os dois módulos
    await carregar(status({ estado: 'bloqueada', episodio: Object.assign({}, ep, { modulos_estado: {
            academico: Object.assign({}, ep.modulos_estado.financeiro), financeiro: Object.assign({}, ep.modulos_estado.financeiro) } }),
        alvos: { renovar_agora: ['academico', 'financeiro'], liberar: ['academico', 'financeiro'], automatica: ['academico'] } }), []);
    sv.confirms.length = 0; sv.confirmar = false;
    await run('conexaoSiaaRenovSolicitar()');
    await run('conexaoSiaaRenovLiberar()');
    sv.confirmar = true;
    assert(/Módulos atingidos: Acadêmico e Financeiro\./.test(sv.confirms[0]));
    assert(/Liberar nova tentativa para: Acadêmico e Financeiro\?/.test(sv.confirms[1]));
    assert(/Renovação automática atingiria agora<\/dt><dd>Acadêmico</.test(html('cs-renov-corpo')));
    ok('confirmações com os dois módulos');

    // 7) alvos.liberar vazio: Liberar não é oferecido nem envia
    await carregar(status({ estado: 'bloqueada', episodio: ep,
        alvos: { renovar_agora: ['financeiro'], liberar: [], automatica: [] } }), []);
    c = html('cs-renov-corpo');
    assert(!/id="cs-rn-btn-liberar"/.test(c), 'sem botão Liberar');
    assert(/Nenhum módulo bloqueado para liberar/.test(c));
    sv.confirms.length = 0; calls.length = 0;
    await run('conexaoSiaaRenovLiberar()');
    assert.strictEqual(sv.confirms.length, 0);
    assert.deepStrictEqual(escritas(), []);
    ok('alvos.liberar vazio não oferece liberação');

    // 8) backend antigo: sem alvos, sem modulos_estado, sem resultado/situacao/modulos_solicitados
    const epAntigo = { id: 'ep-2', desde: '2026-11-19T09:00:00Z', modulos: ['financeiro'], motivo: 'sessao_expirada',
        tentativas_automaticas: 1, bloqueado: true, bloqueio_motivo: 'incerta', liberado_em: null };
    const eAntiga = exec({ estado: 'incerta', erro: 'publicacao_incerta',
        modulos: { academico: Object.assign({}, MOD_OK), financeiro: Object.assign({}, MOD_OK, { publicacao: 'incerto', sessao_validada: false }) } });
    await carregar(status({ estado: 'bloqueada', episodio: epAntigo, ultima_execucao: eAntiga }), [eAntiga]);
    c = html('cs-renov-corpo');
    assert(/Incerta/.test(c.split('<dt>Resultado</dt>')[1].split('</dd>')[0]), 'resultado derivado do estado');
    assert(/Módulos solicitados<\/dt><dd>Acadêmico e Financeiro<\/dd>/.test(c));
    assert(!/Não solicitado/.test(c) && !/Não solicitado/.test(html('cs-rn-body')));
    assert(!/cs-rn-ep-modulos/.test(c) && /Bloqueado<\/span> publicação incerta/.test(c));
    assert(!/Renovação automática atingiria agora/.test(c));
    assert(/id="cs-rn-btn-liberar"[^>]*>[\s\S]*?Liberar nova tentativa<\/button>/.test(c));
    assert(/"Renovar agora" atinge: os dois módulos \(Acadêmico e Financeiro\)\./.test(c));
    sv.confirms.length = 0; sv.confirmar = false;
    await run('conexaoSiaaRenovSolicitar()');
    await run('conexaoSiaaRenovLiberar()');
    sv.confirmar = true;
    assert(/os dois módulos/.test(sv.confirms[0]) && /credenciais/i.test(sv.confirms[0]));
    assert(/^Liberar nova tentativa\? O bloqueio e as tentativas deste episódio são zerados/.test(sv.confirms[1]));
    sv.liberar = { ok: true, episodio: Object.assign({}, epAntigo, { bloqueado: false }), modulos_liberados: null };
    await run('conexaoSiaaRenovLiberar()'); await settle();
    assert.strictEqual(els['cs-renov-msg'].textContent, 'Nova tentativa liberada.');
    ok('backend antigo: textos atuais e nada quebra');

    // 9) polling continua só lendo
    calls.length = 0;
    run('csPoll()'); await settle();
    assert.deepStrictEqual(escritas(), []);
    ok('polling não dispara escrita');

    // 10) incerta continua visível junto do resultado; parcial sem módulo pendente não diz "nenhum"
    e = exec({ estado: 'incerta', erro: 'publicacao_incerta', modulos_solicitados: ['academico'], resultado: 'parcial',
        modulos: { academico: Object.assign({ situacao: 'ok' }, MOD_OK), financeiro: Object.assign({ situacao: 'nao_solicitado' }, MOD_OK) } });
    await carregar(status({ ultima_execucao: e }), [e]);
    c = html('cs-renov-corpo');
    assert(/Parcial/.test(c) && /Incerta/.test(c), 'estado incerta some atrás do resultado');
    assert(!/não renovado: nenhum/.test(c));
    ok('estado incerta aparece junto do resultado');

    // 11) solicitado e não tentado: "Não executado" com motivo, nunca "Falhou"
    e = exec({ estado: 'falhou', erro: 'coleta_falhou', modulos_solicitados: ['academico', 'financeiro'], resultado: 'falha',
        modulos: { academico: Object.assign({ situacao: 'falhou' }, MOD_FALHOU),
                   financeiro: Object.assign({ situacao: 'nao_executado', motivo_situacao: 'falha_anterior' }, MOD_ZERO) } });
    await carregar(status({ ultima_execucao: e }), [e]);
    c = html('cs-renov-corpo');
    assert(/Não executado/.test(linha(c, 'Financeiro')) && !/Falhou/.test(linha(c, 'Financeiro')));
    assert(/parou no módulo anterior/.test(linha(c, 'Financeiro')));
    h = html('cs-rn-body');
    assert(/Não executado/.test(h));
    ok('não tentado aparece como Não executado com o motivo');

    console.log('\n' + n + ' verificações passaram.');
})().catch(e => { console.error('FALHOU:', e); process.exit(1); });
