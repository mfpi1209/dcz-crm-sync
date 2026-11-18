// Harness sem rede: DOM e fetch simulados. Roda o <script> inline de templates/partials/_siaa_sessao.html num vm.
// Uso: node tests/js/siaa_sessao_inscricoes_harness.js   (sai com código != 0 se falhar)
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

const html = fs.readFileSync(path.join(__dirname, '..', '..', 'templates', 'partials', '_siaa_sessao.html'), 'utf8');
const m = html.match(/<script>([\s\S]*?)<\/script>/);
assert(m, 'script inline não encontrado');

const COOKIE = 'JSESSIONID=jsid-ficticio; TS01abcd=ts-ficticio';
const CURL = "curl 'https://siaa.test/wsvcon01.jsf' -H 'Cookie: " + COOKIE + "' --data-raw 'cpf=12345678900'";

const inputs = {}, msgs = {}, status = {};
const mk = (o, k) => o[k] || (o[k] = { value: '', textContent: '', className: '', dataset: {} });
const document = {
    getElementById(id) {
        let r = id.match(/^siaa-sessao-input-(.+)$/); if (r) return mk(inputs, r[1]);
        r = id.match(/^siaa-sessao-msg-(.+)$/); if (r) return mk(msgs, r[1]);
        return null;
    },
    querySelector(sel) {
        const r = sel.match(/data-module="([^"]+)"\] \.siaa-sessao-status/);
        return r ? mk(status, r[1]) : null;
    },
};
const calls = [];
let resposta = { status: 200, corpo: {} };
async function fetchFake(url, opts) {
    calls.push({ url, method: opts.method, body: JSON.parse(opts.body) });
    return { status: resposta.status, json: async () => { if (resposta.corpo === null) throw new Error('x'); return resposta.corpo; } };
}
const window = {};
const ctx = vm.createContext({ document, window, fetch: fetchFake, console, JSON, Promise });
vm.runInContext(m[1], ctx);

const msgTxt = () => msgs.inscricoes.textContent;
const reset = (corpo, st) => { calls.length = 0; resposta = { status: st || 200, corpo }; inputs.inscricoes.value = '  ' + CURL + '  '; msgs.inscricoes.textContent = ''; };
mk(inputs, 'inscricoes'); mk(msgs, 'inscricoes'); mk(status, 'inscricoes');

(async () => {
    const vistos = []; // todas as mensagens exibidas
    const registrar = () => vistos.push(msgTxt() + '|' + status.inscricoes.textContent);
    let n = 0; const ok = (nome) => { n++; console.log('ok -', nome); };

    // 1) Testar: POST com module e cookie (texto do textarea, aparado)
    reset({ ok: true, alive: true, status: 200, formulario: 'form' });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert.strictEqual(calls.length, 1);
    assert.strictEqual(calls[0].url, '/siaa-sessao/testar');
    assert.strictEqual(calls[0].method, 'POST');
    assert.deepStrictEqual(calls[0].body, { module: 'inscricoes', cookie: CURL });
    assert(/formul[aá]rio form/.test(msgTxt()), msgTxt());
    assert.strictEqual(status.inscricoes.dataset.status, 'alive');
    assert.strictEqual(status.inscricoes.textContent, 'VIVO');
    assert.strictEqual(inputs.inscricoes.value.trim(), CURL, 'Testar não limpa o campo');
    ok('Testar envia module+cookie e mostra VIVO citando o formulário');

    // 2) alive=true sem formulário
    reset({ ok: true, alive: true, status: 200 });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert(/Sess[aã]o v[aá]lida/.test(msgTxt()) && !/formul[aá]rio/.test(msgTxt()), msgTxt());
    ok('alive sem formulário não cita formulário');

    // 3) sessao_expirada
    reset({ ok: true, alive: false, classe: 'sessao_expirada', status: 302 });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert.strictEqual(status.inscricoes.textContent, 'EXPIRADA');
    assert(/expirada/i.test(msgTxt()) && /cURL novo/.test(msgTxt()), msgTxt());
    ok('sessao_expirada mostra EXPIRADA e pede cURL novo');

    // 4) indisponivel_siaa
    reset({ ok: true, alive: false, classe: 'indisponivel_siaa', motivo: 'timeout' });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert.strictEqual(status.inscricoes.textContent, 'SIAA FORA');
    assert(/timeout/.test(msgTxt()), msgTxt());
    ok('indisponivel_siaa mostra SIAA FORA');

    // 5) etapa=formulario lista os ids
    reset({ ok: true, alive: false, etapa: 'formulario', motivo: 'form_nao_achado', formularios_encontrados: ['formA', 'j_idt5:f'] });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert.strictEqual(status.inscricoes.textContent, 'INCONCLUSIVO');
    assert(/formA, j_idt5:f/.test(msgTxt()) && /form_nao_achado/.test(msgTxt()), msgTxt());
    ok('etapa formulario lista os ids encontrados');

    // 6) etapa=formulario sem ids e inconclusivo genérico
    reset({ ok: true, alive: false, etapa: 'formulario', motivo: 'form_nao_achado' });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert(!/Formul[aá]rios na p[aá]gina/.test(msgTxt()), msgTxt());
    reset({ ok: true, alive: false, classe: 'outra', motivo: 'x' });
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert(/inconclusivo/i.test(msgTxt()), msgTxt());
    ok('inconclusivos sem ids');

    // 7) erro 400 com error
    reset({ ok: false, error: 'cURL sem cookie de sessão', codigo: 'curl_sem_cookie' }, 400);
    await window.siaaSessaoTestar('inscricoes'); registrar();
    assert.strictEqual(msgTxt(), 'cURL sem cookie de sessão');
    assert.strictEqual(status.inscricoes.textContent, 'ERRO');
    assert.strictEqual(msgs.inscricoes.className, 'siaa-sessao-msg error');
    ok('erro 400 mostra error');

    // 8) campo vazio não chama a API
    calls.length = 0; inputs.inscricoes.value = '   ';
    await window.siaaSessaoTestar('inscricoes'); await window.siaaSessaoSalvar('inscricoes');
    assert.strictEqual(calls.length, 0);
    ok('campo vazio não envia');

    // 9) Salvar com sonda_validou => campo vazio e SALVO
    reset({ ok: true, publicacao: { gravada: true, sonda_validou: true, vinculo: 'vinculado' }, sonda: { alive: true } });
    await window.siaaSessaoSalvar('inscricoes'); registrar();
    assert.strictEqual(calls[0].url, '/siaa-sessao/salvar');
    assert.deepStrictEqual(calls[0].body, { module: 'inscricoes', cookie: CURL });
    assert.strictEqual(inputs.inscricoes.value, '');
    assert.strictEqual(status.inscricoes.textContent, 'SALVO');
    assert.strictEqual(status.inscricoes.dataset.status, 'saved');
    assert(/confirmada/.test(msgTxt()) && /vinculado/.test(msgTxt()), msgTxt());
    ok('Salvar validado limpa o campo e mostra SALVO');

    // 10) Salvar sem validação => campo vazio e aviso
    reset({ ok: true, publicacao: { gravada: true, sonda_validou: false }, sonda: { classe: 'sessao_expirada', motivo: 'redirect_login' } });
    await window.siaaSessaoSalvar('inscricoes'); registrar();
    assert.strictEqual(inputs.inscricoes.value, '');
    assert.strictEqual(status.inscricoes.textContent, 'SALVO SEM CONFIRMAR');
    assert(/sessao_expirada/.test(msgTxt()) && /redirect_login/.test(msgTxt()), msgTxt());
    assert(/warn/.test(msgs.inscricoes.className));
    reset({ ok: true });
    await window.siaaSessaoSalvar('inscricoes'); registrar();
    assert.strictEqual(inputs.inscricoes.value, '');
    assert(/sem sonda/.test(msgTxt()), msgTxt());
    ok('Salvar sem confirmação limpa o campo e avisa');

    // 11) Salvar com erro => campo preservado
    reset({ ok: false, error: 'cURL inválido', codigo: 'curl_invalido' }, 400);
    await window.siaaSessaoSalvar('inscricoes'); registrar();
    assert.strictEqual(inputs.inscricoes.value.trim(), CURL);
    assert.strictEqual(msgTxt(), 'cURL inválido');
    assert.strictEqual(status.inscricoes.textContent, 'ERRO');
    ok('Salvar com erro preserva o campo');

    // 12) falha de rede preserva o campo
    reset({}); const fOrig = ctx.fetch; ctx.fetch = async () => { throw new Error('rede ' + COOKIE); };
    await window.siaaSessaoSalvar('inscricoes'); registrar();
    ctx.fetch = fOrig;
    assert.strictEqual(inputs.inscricoes.value.trim(), CURL);
    assert.strictEqual(msgTxt(), 'Falha de rede.');
    ok('falha de rede preserva o campo e não exibe a exceção');

    // 13) nenhuma mensagem contém o cookie/CPF
    for (const v of vistos) for (const p of ['jsid-ficticio', 'ts-ficticio', 'JSESSIONID', '12345678900', 'curl ']) {
        assert(!v.includes(p), 'mensagem vaza ' + p + ': ' + v);
    }
    ok('nenhuma mensagem exibida contém cookie, CPF ou cURL (' + vistos.length + ' mensagens)');

    // 14) lotes incluem inscricoes
    mk(inputs, 'academico'); mk(inputs, 'financeiro'); inputs.academico.value = ''; inputs.financeiro.value = '';
    reset({ ok: true, alive: true });
    await window.siaaSessaoTestarTodos();
    assert.strictEqual(calls.length, 1); assert.strictEqual(calls[0].body.module, 'inscricoes');
    reset({ ok: true, publicacao: { sonda_validou: true } });
    await window.siaaSessaoSalvarTodos();
    assert.strictEqual(calls.length, 1); assert.strictEqual(calls[0].url, '/siaa-sessao/salvar');
    ok('Testar/Salvar todos incluem inscricoes');

    console.log(n + ' verificações ok');
})().catch((e) => { console.error(e); process.exit(1); });
