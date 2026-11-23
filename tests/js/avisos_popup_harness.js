// Harness do popup de avisos (utils.js): funções reais extraídas do arquivo, DOM/fetch/storage
// simulados, sem rede. Sai com código 1 se alguma verificação falhar.
'use strict';
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const assert = require('assert');

const src = fs.readFileSync(path.join(__dirname, '..', '..', 'static', 'js', 'utils.js'), 'utf8');

function trecho(re) {
    const m = src.match(re);
    if (!m) throw new Error('trecho não encontrado: ' + re);
    return m[0];
}
function fn(nome) {
    return trecho(new RegExp('^(async )?function ' + nome + '\\(.*?^}\\n', 'ms'));
}
const codigo = [
    trecho(/^const AVISOS_POLL_MS = .*$/m),
    'let _avisosPollTimer = null;',
    fn('_avChaveApresentados'), fn('_avApresentados'), fn('_avMarcarApresentados'),
    fn('_avisosParaPopup'), fn('_iniciarPollingAvisos'), fn('checkAvisosNaoLidos'),
].join('\n');

class Storage {
    constructor(quebrado) { this.d = {}; this.quebrado = quebrado; }
    getItem(k) { if (this.quebrado) throw new Error('bloqueado'); return k in this.d ? this.d[k] : null; }
    setItem(k, v) { if (this.quebrado) throw new Error('bloqueado'); this.d[k] = String(v); }
}

// "Servidor": não lidos do usuário (como /api/avisos/nao-lidos devolve).
function novoAmbiente({ session, local, usuario = 'fabio@eduit.com.br', resposta }) {
    const popups = [];
    const ctx = {
        sessionStorage: session, localStorage: local, popups, overlayAberto: false,
        intervalos: [], redirecionou: false, console,
        window: { _avUsuario: usuario },
        document: {
            hidden: false,
            listeners: {},
            addEventListener(ev, f) { this.listeners[ev] = f; },
            getElementById(id) { return id === 'avisos-popup-overlay' && ctx.overlayAberto ? {} : null; },
        },
        setInterval(f, ms) { ctx.intervalos.push(f); return ctx.intervalos.length; },
        clearInterval() { ctx.intervalos = []; },
        renderNotifPanel() {},
        _showAvisosPopup(av) { popups.push(av.map(a => a.id)); },
        async fetch() { return resposta(); },
        async api() { return resposta(); },
    };
    vm.createContext(ctx);
    vm.runInContext(codigo + '\nthis.checkAvisosNaoLidos = checkAvisosNaoLidos; this._iniciarPollingAvisos = _iniciarPollingAvisos;'
        + 'this.timer = () => _avisosPollTimer;', ctx);
    return ctx;
}

function ok(avisos) {
    return async () => ({ ok: true, status: 200, json: async () => ({ count: avisos.length, avisos }) });
}
const M1 = { id: 1, titulo: 'Manual', prioridade: 'normal', siaa_estado: null };
const S = (id, estado) => ({ id, titulo: 'SIAA ' + id, prioridade: estado === 'aberto' ? 'urgente' : 'normal', siaa_estado: estado });

const tick = () => new Promise(r => setImmediate(r));
async function rodar(amb) { amb.intervalos[0](); for (let k = 0; k < 5; k++) await tick(); }

(async () => {
    const session = new Storage(), local = new Storage();
    let avisos = [M1];
    const servidor = async () => ok(avisos)();

    // 1. carga: regra de sempre (manual aparece)
    let a = novoAmbiente({ session, local, resposta: servidor });
    await a.checkAvisosNaoLidos();
    assert.deepStrictEqual(a.popups, [[1]], 'carga mostra o manual');
    a._iniciarPollingAvisos();
    assert.strictEqual(a.intervalos.length, 1, 'polling iniciado');

    // 2. incidente novo durante o uso -> popup só dele, sem recarregar
    avisos = [S(10, 'aberto'), M1];
    await rodar(a);
    assert.deepStrictEqual(a.popups, [[1], [10]], 'incidente aparece durante o uso');

    // 3. polling seguinte: não repete
    await rodar(a);
    assert.strictEqual(a.popups.length, 2, 'não repete no polling');

    // 4. manual novo durante o uso: regra de sempre (não abre popup no meio do uso)
    avisos = [S(10, 'aberto'), { id: 2, siaa_estado: null }, M1];
    await rodar(a);
    assert.strictEqual(a.popups.length, 2, 'manual segue a regra da carga');

    // 5. recarregar na mesma sessão: nada repete
    let b = novoAmbiente({ session, local, resposta: servidor });
    await b.checkAvisosNaoLidos();
    assert.deepStrictEqual(b.popups, [], 'recarga não repete');

    // 6. nova sessão do navegador: manuais não lidos voltam (regra de sempre); o SIAA apresentado não
    const session2 = new Storage();
    let c = novoAmbiente({ session: session2, local, resposta: servidor });
    await c.checkAvisosNaoLidos();
    assert.deepStrictEqual(c.popups, [[2, 1]], 'nova sessão: só manuais');

    // 7. incidente recuperado antes de ser apresentado: nunca abre popup (carga ou polling)
    avisos = [S(11, 'recuperado'), S(12, 'recuperado_sem_aviso')];
    c._iniciarPollingAvisos();
    await rodar(c);
    let d = novoAmbiente({ session: new Storage(), local, resposta: servidor });
    await d.checkAvisosNaoLidos();
    assert.strictEqual(c.popups.length, 1, 'recuperado não abre no polling');
    assert.deepStrictEqual(d.popups, [], 'recuperado não abre na carga');

    // 8. aviso apresentado aberto e depois recuperado: não reabre
    avisos = [S(10, 'recuperado')];
    await rodar(c);
    assert.strictEqual(c.popups.length, 1, 'recuperação não reabre popup');

    // 9. popup aberto: não substitui; aparece na verificação seguinte
    avisos = [S(13, 'aberto')];
    c.overlayAberto = true;
    await rodar(c);
    assert.strictEqual(c.popups.length, 1, 'não substitui popup aberto');
    c.overlayAberto = false;
    await rodar(c);
    assert.deepStrictEqual(c.popups[1], [13], 'aparece depois de fechar');

    // 10. aba oculta: o polling não consulta
    let consultas = 0;
    let e = novoAmbiente({ session: new Storage(), local: new Storage(), resposta: async () => { consultas++; return ok([])(); } });
    e._iniciarPollingAvisos();
    e.document.hidden = true;
    await rodar(e);
    assert.strictEqual(consultas, 0, 'aba oculta não consulta');
    e.document.hidden = false;
    e.document.listeners.visibilitychange();
    for (let k = 0; k < 5; k++) await tick();
    assert.strictEqual(consultas, 1, 'volta à aba consulta na hora');

    // 11. outro usuário no mesmo navegador: o "apresentado" é por usuário
    avisos = [S(13, 'aberto')];
    let f = novoAmbiente({ session: new Storage(), local, usuario: 'marcelo@eduit.com.br', resposta: servidor });
    await f.checkAvisosNaoLidos();
    assert.deepStrictEqual(f.popups, [[13]], 'Marcelo vê o seu');

    // 12. sessão expirada no polling: para o polling, sem redirecionar
    let g = novoAmbiente({ session: new Storage(), local: new Storage(),
        resposta: async () => ({ ok: false, status: 401, json: async () => ({}) }) });
    g._iniciarPollingAvisos();
    await g.checkAvisosNaoLidos({ polling: true });
    assert.strictEqual(g.timer(), null, 'polling parado');
    assert.deepStrictEqual(g.popups, []);

    // 13. erro 500 no polling: ignora e continua
    let h = novoAmbiente({ session: new Storage(), local: new Storage(),
        resposta: async () => ({ ok: false, status: 500, json: async () => ({}) }) });
    h._iniciarPollingAvisos();
    await h.checkAvisosNaoLidos({ polling: true });
    assert.notStrictEqual(h.timer(), null, 'polling segue');

    // 14. storage bloqueado: não quebra, mostra o incidente
    let i = novoAmbiente({ session: new Storage(), local: new Storage(true), resposta: ok([S(20, 'aberto')]) });
    i._iniciarPollingAvisos();
    await rodar(i);
    assert.deepStrictEqual(i.popups, [[20]], 'funciona sem localStorage');

    console.log('avisos_popup_harness: ok');
})().catch(e => { console.error(e); process.exit(1); });
