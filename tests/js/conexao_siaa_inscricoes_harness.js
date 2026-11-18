// Harness sem rede: roda static/js/conexao_siaa.js num vm e confere o módulo inscricoes.
// Uso: node tests/js/conexao_siaa_inscricoes_harness.js   (sai com código != 0 se falhar)
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

class El {
    constructor(id) { this.id = id; this._html = ''; this.textContent = ''; this.value = ''; this.style = {};
        this.className = ''; this.classList = { add() {}, remove() {}, contains: () => false }; }
    set innerHTML(v) { this._html = String(v); }
    get innerHTML() { return this._html; }
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
const ctx = vm.createContext({
    document, console, URLSearchParams, URL, Date, Math, JSON, Promise, Uint8Array, parseInt, isNaN, setInterval, clearInterval,
    fetch: () => Promise.reject(new Error('sem rede')),
});
ctx.window = { confirm: () => true, crypto: require('crypto').webcrypto };
ctx.crypto = ctx.window.crypto;
vm.runInContext(fs.readFileSync(path.join(__dirname, '..', '..', 'static', 'js', 'conexao_siaa.js'), 'utf8'), ctx);
const run = (code) => vm.runInContext(code, ctx);

// Isola o HTML do card de um módulo.
function card(html, modulo) {
    const partes = html.split('<div class="cs-card" data-modulo="').slice(1);
    const p = partes.filter((x) => x.startsWith(modulo + '"'))[0];
    assert(p, 'card ' + modulo + ' ausente');
    return p;
}
const mod = (o) => Object.assign({ ultima_confirmacao: null, ultima_verificacao: null, proxima_execucao: null, modo_usado: null, ultimo_resultado: null }, o);

(function () {
    let n = 0; const ok = (nome) => { n++; console.log('ok -', nome); };
    const AVISO_ANTIGO = 'Precisa renovar a sessão';

    // 1) três cards, na ordem
    run('csRenderModulos(' + JSON.stringify({ modulos: { academico: mod({ estado: 'valida' }), financeiro: mod({ estado: 'valida' }), inscricoes: mod({ estado: 'valida' }) } }) + ')');
    let h = els['cs-modulos'].innerHTML;
    const ordem = [...h.matchAll(/data-modulo="([a-z]+)"/g)].map((x) => x[1]);
    assert.deepStrictEqual(ordem, ['academico', 'financeiro', 'inscricoes']);
    assert(!h.includes('cs-manual') && !h.includes(AVISO_ANTIGO));
    ok('três cards; inscricoes válida sem aviso');

    // 2) nao_configurada
    run('csRenderModulos(' + JSON.stringify({ modulos: { inscricoes: mod({ estado: 'nao_configurada' }) } }) + ')');
    let c = card(els['cs-modulos'].innerHTML, 'inscricoes');
    assert(c.includes('Não configurada'), 'badge');
    assert(c.includes('Sessão ainda não cadastrada') && c.includes('Cadastre em Atualizar sessão SIAA'), 'aviso de cadastro');
    assert(c.includes('t-neutral'));
    assert(!c.includes(AVISO_ANTIGO) && !c.includes('cs-alert-err'));
    ok('nao_configurada: badge e aviso de cadastro');

    // 3) atualizacao_manual
    run('csRenderModulos(' + JSON.stringify({ modulos: { inscricoes: mod({ estado: 'atualizacao_manual', precisa_renovacao: true }) } }) + ')');
    c = card(els['cs-modulos'].innerHTML, 'inscricoes');
    assert(c.includes('Atualização manual necessária'), 'badge');
    assert(c.includes('t-bad'));
    assert(c.includes('Cole um cURL novo') && c.includes('não cobre este módulo'), 'aviso de cURL novo');
    assert(!c.includes(AVISO_ANTIGO), 'precisa_renovacao não dispara o aviso antigo em inscricoes');
    ok('atualizacao_manual: badge e aviso de cURL novo (sem aviso de renovação)');

    // 4) acadêmico e financeiro continuam com o aviso antigo
    run('csRenderModulos(' + JSON.stringify({ modulos: { academico: mod({ estado: 'expirada', precisa_renovacao: true }), financeiro: mod({ estado: 'valida' }), inscricoes: mod({ estado: 'nao_configurada' }) } }) + ')');
    h = els['cs-modulos'].innerHTML;
    c = card(h, 'academico');
    assert(c.includes(AVISO_ANTIGO) && c.includes('cs-renovar') && c.includes('MFA'));
    assert(!card(h, 'financeiro').includes(AVISO_ANTIGO));
    assert(!card(h, 'inscricoes').includes(AVISO_ANTIGO));
    ok('acadêmico com precisa_renovacao mantém o aviso antigo');

    // 5) estados desconhecidos e sem estado seguem tratados
    run('csRenderModulos({modulos:{inscricoes:{}}})');
    assert(card(els['cs-modulos'].innerHTML, 'inscricoes').includes('Sem dados'));
    ok('inscricoes sem estado mostra Sem dados');

    // 6) tabela de renovação só tem Acadêmico e Financeiro
    const e = { modulos: { academico: { coleta: 'ok', http: 200, publicacao: 'ok', sessao_validada: true },
        financeiro: { coleta: 'ok', http: 200, publicacao: 'ok', sessao_validada: true },
        inscricoes: { coleta: 'ok', http: 200, publicacao: 'ok', sessao_validada: true } } };
    ctx.__e = e;
    const t = run('csRnModulosTabela(__e)');
    const linhas = [...t.matchAll(/<tr><td>([^<]+)<\/td>/g)].map((x) => x[1]);
    assert.deepStrictEqual(linhas, ['Acadêmico', 'Financeiro']);
    assert(!t.includes('Inscrições'));
    ok('tabela de módulos da renovação: só Acadêmico e Financeiro');

    // 7) histórico rotula os módulos
    for (const [k, rot] of [['inscricoes', 'Inscrições'], ['academico', 'Acadêmico'], ['financeiro', 'Financeiro'], ['x', 'x']]) {
        ctx.__it = { em: '2026-11-16T10:00:00Z', modulo: k, classe: 'disponivel', motivo: 'ping_ok', etapa: 'ping', modo: 'jsf', status_http: 200, duracao_ms: 1, origem: 'agendada' };
        const linha = run('csHistoricoRow(__it)');
        assert(linha.includes('<td>' + rot + '</td>'), k + ' => ' + rot + ': ' + linha);
    }
    ctx.__it = { em: null, modulo: null };
    assert(run('csHistoricoRow(__it)').includes('<td>—</td>'));
    ok('histórico rotula inscricoes como Inscrições');

    console.log(n + ' verificações ok');
})();
