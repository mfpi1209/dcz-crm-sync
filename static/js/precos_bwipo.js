let _bpPreview = null;
let _bpLinhas = [];
let _bpGlobais = [];
let _bpPorCurso = {};
let _bpMenuFor = null;
let _bpMenuBtn = null;
let _bpSort = { col: 'final', dir: 'desc' };
let _bpCursosAbertos = false;

function loadPrecosBwipo() {
    bpPainel();
    if (!window._bpMenuBound) {
        document.addEventListener('click', bpFecharMenu);
        window.addEventListener('scroll', bpPosicionarMenu, true);
        window.addEventListener('resize', bpPosicionarMenu);
        window._bpMenuBound = true;
    }
}

function _bpEsc(s) {
    return String(s ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}

function _bpMoney(n) {
    if (n == null || n === '') return '—';
    return Number(n).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function _bpFile() {
    const input = document.getElementById('bp-file');
    return input && input.files && input.files[0];
}

function bpArquivoPos() {
    const input = document.getElementById('bp-file-pos');
    const file = input && input.files && input.files[0];
    const name = document.getElementById('bp-file-pos-name');
    const nota = document.getElementById('bp-pos-nota');
    if (!file || !name || !nota) return;
    name.textContent = file.name;
    nota.textContent = 'Planilha escolhida. A Pós ainda não é lida: falta a amostra do arquivo de preço.';
}

function _bpCanal(nome) {
    return (_bpPreview && _bpPreview.canais || []).find(c => c.canal === nome);
}

function _bpEhBase(canal) {
    return _bpFold(canal).includes('estrateg');
}

function _bpEhAprovados(canal) {
    const f = _bpFold(canal);
    return f === 'aprovados' || f === 'mar aberto';
}

function _bpCanalMarAberto() {
    return (_bpPreview && _bpPreview.canais || []).find(c => _bpEhAprovados(c.canal));
}

function _bpFixas() {
    return (_bpPreview && _bpPreview.canais || []).filter(c => _bpEhBase(c.canal) || _bpEhAprovados(c.canal));
}

function _bpFold(s) {
    return String(s || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLowerCase().trim();
}

function _bpOpcionais() {
    return (_bpPreview && _bpPreview.canais || []).filter(c => !(_bpEhBase(c.canal) || _bpEhAprovados(c.canal)));
}

const _bpSites = {
    wix: ['Site Cruzeiro', '/api/atualizar_wix_canal'],
    wix2: ['Eduit-ead', '/api/atualizar_wix2_canal'],
    wix3: ['Eduit New', '/api/atualizar_wix3_canal'],
};

function _bpBotoes(travar) {
    ['bp-btn-aplicar', 'bp-btn-hist', 'bp-btn-ia', 'bp-btn-site', 'bp-site'].forEach(id => {
        const el = document.getElementById(id);
        if (el) el.disabled = travar;
    });
}

function bpLog(alvo, texto, tipo) {
    const box = document.getElementById('bp-log');
    if (!box) return;
    const vazio = document.getElementById('bp-log-vazio');
    if (vazio) vazio.remove();
    const hora = new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    const linha = document.createElement('div');
    linha.className = 'bp-log-linha' + (tipo === 'ok' ? ' is-ok' : tipo === 'erro' ? ' is-erro' : '');
    linha.innerHTML = `<span class="bp-log-hora">${hora}</span><span class="bp-log-alvo">${_bpEsc(alvo)}</span><span class="bp-log-txt">${_bpEsc(texto)}</span>`;
    box.appendChild(linha);
    box.scrollTop = box.scrollHeight;
}

async function bpPreview() {
    const file = _bpFile();
    const status = document.getElementById('bp-status');
    if (!file) return;
    document.getElementById('bp-file-name').textContent = file.name;
    status.textContent = 'Lendo planilha e cruzando com o CRM…';
    _bpBotoes(true);
    const body = new FormData();
    body.append('file', file);
    const res = await fetch('/api/bwipo-precos/preview', { method: 'POST', body });
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
        status.textContent = data.error || 'Falha ao ler a planilha.';
        _bpBotoes(false);
        return;
    }
    _bpPreview = data;
    _bpGlobais = [];
    _bpPorCurso = {};
    bpRenderCanais();
    bpRenderCursos();
    bpInsights();
    document.getElementById('bp-preview-wrap').classList.remove('hidden');
    status.textContent = '';
    _bpBotoes(false);
}

function bpRenderCanais() {
    const box = document.getElementById('bp-canais');
    if (!box) return;
    const fixas = _bpFixas();
    const chips = fixas.map(c => {
        const nome = _bpEhBase(c.canal) ? 'Balcão Estratégico' : 'Mar Aberto';
        return `<span class="bp-pill">${_bpEsc(nome)}</span>`;
    }).join('');
    const extras = _bpGlobais.map(canal => {
        const c = _bpCanal(canal);
        const nome = (c && c.label) || canal;
        return `<span class="bp-pill">${_bpEsc(nome)}<button type="button" onclick="bpTirarGlobal('${_bpEsc(canal)}')" aria-label="Remover">×</button></span>`;
    }).join('');
    box.innerHTML = chips + extras + `<button type="button" class="bp-plus" onclick="bpAbrirMenu(event, null)" aria-label="Adicionar cota para todos">+</button>`;
}

function bpTirarGlobal(canal) {
    _bpGlobais = _bpGlobais.filter(c => c !== canal);
    bpRenderCanais();
    bpRenderCursos();
}

function bpAbrirMenu(ev, chave) {
    ev.stopPropagation();
    const menu = document.getElementById('bp-menu');
    const usados = new Set(_bpGlobais);
    if (chave && _bpPorCurso[chave]) _bpPorCurso[chave].forEach(c => usados.add(c));
    const opcoes = _bpOpcionais().filter(c => !usados.has(c.canal));
    if (!opcoes.length) {
        menu.innerHTML = `<div class="px-3 py-2 text-sm text-[var(--text-muted)]">Nenhuma outra cota na planilha.</div>`;
    } else {
        menu.innerHTML = opcoes.map(c =>
            `<button type="button" data-canal="${_bpEsc(c.canal)}">${_bpEsc(c.label)}</button>`
        ).join('');
        menu.querySelectorAll('button').forEach(btn => {
            btn.onclick = (e) => {
                e.stopPropagation();
                bpEscolherCota(btn.dataset.canal);
            };
        });
    }
    _bpMenuFor = chave;
    _bpMenuBtn = ev.currentTarget;
    menu.onclick = (e) => e.stopPropagation();
    if (menu.parentElement !== document.body) document.body.appendChild(menu);
    menu.classList.remove('hidden');
    bpPosicionarMenu();
}

function bpPosicionarMenu(ev) {
    if (ev && ev.target && ev.target.closest && ev.target.closest('#bp-menu')) return;
    const menu = document.getElementById('bp-menu');
    if (!menu || menu.classList.contains('hidden') || !_bpMenuBtn) return;
    if (!document.body.contains(_bpMenuBtn)) {
        bpFecharMenu();
        return;
    }
    const r = _bpMenuBtn.getBoundingClientRect();
    const margem = 8;
    const mw = menu.offsetWidth;
    const mh = menu.offsetHeight;
    let left = r.left;
    if (left + mw > window.innerWidth - margem) left = r.right - mw;
    left = Math.max(margem, Math.min(left, window.innerWidth - mw - margem));
    let top = r.bottom + 6;
    if (top + mh > window.innerHeight - margem) top = r.top - mh - 6;
    if (top < margem) top = margem;
    menu.style.left = left + 'px';
    menu.style.top = top + 'px';
}

function bpEscolherCota(canal) {
    if (_bpMenuFor) {
        const lista = _bpPorCurso[_bpMenuFor] || [];
        if (!lista.includes(canal)) lista.push(canal);
        _bpPorCurso[_bpMenuFor] = lista;
    } else if (!_bpGlobais.includes(canal)) {
        _bpGlobais.push(canal);
        bpRenderCanais();
    }
    bpFecharMenu();
    bpRenderCursos();
}

function bpFecharMenu() {
    const menu = document.getElementById('bp-menu');
    if (menu) menu.classList.add('hidden');
    _bpMenuFor = null;
    _bpMenuBtn = null;
}

function bpTirarCurso(chave, canal) {
    _bpPorCurso[chave] = (_bpPorCurso[chave] || []).filter(c => c !== canal);
    bpRenderCursos();
}

function _bpCotaDoCurso(linha, canal) {
    return (linha.cotas || []).find(c => c.canal === canal);
}

function bpToggleCursos(abrir) {
    _bpCursosAbertos = typeof abrir === 'boolean' ? abrir : !_bpCursosAbertos;
    const lista = document.getElementById('bp-preview');
    const seta = document.getElementById('bp-seta-cursos');
    if (lista) lista.classList.toggle('hidden', !_bpCursosAbertos);
    if (seta) seta.textContent = _bpCursosAbertos ? 'expand_less' : 'expand_more';
}

function bpBuscarCursos() {
    const q = (document.getElementById('bp-busca-cursos').value || '').trim();
    if (q) bpToggleCursos(true);
    bpRenderCursos();
}

function _bpMedia(nums) {
    if (!nums.length) return null;
    return nums.reduce((s, n) => s + n, 0) / nums.length;
}

function _bpCard(titulo, valor, detalhe) {
    return `<div class="bp-card p-4">
        <div class="text-[11px] uppercase tracking-wider text-[var(--text-muted)]">${_bpEsc(titulo)}</div>
        <div class="text-xl font-bold mt-1">${_bpEsc(valor)}</div>
        <div class="text-xs text-[var(--text-muted)] mt-1">${_bpEsc(detalhe)}</div>
    </div>`;
}

function bpInsights() {
    const box = document.getElementById('bp-insights');
    const cards = document.getElementById('bp-insights-cards');
    const notas = document.getElementById('bp-insights-notas');
    if (!box || !cards || !notas || !_bpPreview) return;
    const linhas = _bpPreview.linhas || [];
    const comBase = linhas.map(l => ({ l, c: (l.cotas || []).find(c => _bpEhBase(c.canal)) })).filter(x => x.c && x.c.final != null);
    if (!comBase.length) {
        box.classList.add('hidden');
        return;
    }
    const menor = comBase.reduce((a, b) => a.c.final < b.c.final ? a : b);
    const maior = comBase.reduce((a, b) => a.c.final > b.c.final ? a : b);
    const media = _bpMedia(comBase.map(x => x.c.final));
    const pares = linhas.map(l => {
        const b = (l.cotas || []).find(c => _bpEhBase(c.canal));
        const a = (l.cotas || []).find(c => _bpEhAprovados(c.canal));
        if (!b || !a || b.final == null || a.final == null || a.final === 0) return null;
        return { nome: l.curso, dif: a.final - b.final, pct: (a.final - b.final) / a.final * 100 };
    }).filter(Boolean);
    const econ = _bpMedia(pares.map(p => p.dif));
    const econPct = _bpMedia(pares.map(p => p.pct));
    const criar = linhas.filter(l => l.acao === 'criar');
    const atualizar = linhas.length - criar.length;
    cards.innerHTML = [
        _bpCard('Cursos', String(linhas.length), `${atualizar} para atualizar · ${criar.length} para criar`),
        _bpCard('Mais barato', _bpMoney(menor.c.final), `${menor.l.curso} · ${Number(menor.c.desconto).toLocaleString('pt-BR')}%`),
        _bpCard('Mais caro', _bpMoney(maior.c.final), `${maior.l.curso} · ${Number(maior.c.desconto).toLocaleString('pt-BR')}%`),
        _bpCard('Estratégico vs Mar Aberto', econ == null ? '—' : _bpMoney(econ), econPct == null ? '' : `em média ${econPct.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}% abaixo · média ${_bpMoney(media)}`),
    ].join('');

    const frases = [];
    const desc = new Map();
    comBase.forEach(x => {
        const k = Number(x.c.desconto);
        desc.set(k, (desc.get(k) || 0) + 1);
    });
    const moda = [...desc.entries()].sort((a, b) => b[1] - a[1])[0];
    if (moda && moda[1] < comBase.length) {
        const outros = comBase.filter(x => Number(x.c.desconto) !== moda[0]).slice(0, 3).map(x => `${x.l.curso} (${Number(x.c.desconto).toLocaleString('pt-BR')}%)`);
        frases.push(`O desconto de ${moda[0].toLocaleString('pt-BR')}% cobre ${moda[1]} cursos. Fora dele: ${outros.join(', ')}${comBase.length - moda[1] > 3 ? '…' : ''}.`);
    }
    const digital = comBase.filter(x => _bpFold(x.l.sub).includes('digital'));
    const semi = comBase.filter(x => _bpFold(x.l.sub).includes('semi'));
    if (digital.length >= 5 && semi.length >= 5) {
        const md = _bpMedia(digital.map(x => x.c.final));
        const ms = _bpMedia(semi.map(x => x.c.final));
        const diff = ms - md;
        frases.push(diff > 1
            ? `Semipresencial custa em média ${_bpMoney(diff)} a mais que o Digital (${_bpMoney(ms)} contra ${_bpMoney(md)}).`
            : `Digital e Semipresencial ficam perto: ${_bpMoney(md)} e ${_bpMoney(ms)} em média.`);
    }
    if (criar.length) {
        frases.push(`${criar.length === 1 ? 'Ainda não existe no CRM' : 'Ainda não existem no CRM'}: ${criar.slice(0, 4).map(l => l.curso).join(', ')}.`);
    }
    const sel = document.getElementById('bp-canal');
    if (sel && _bpEhBase(sel.value) && _bpLinhas.length && pares.length) {
        const map = new Map(_bpLinhas.map(h => [`${_bpFold(h.curso)}|${_bpFold(h.grau)}`, h]));
        let caiu = 0, subiu = 0, igual = 0;
        let queda = null, alta = null;
        comBase.forEach(x => {
            const h = map.get(x.l.chave);
            if (!h || h.final == null) return;
            const d = x.c.final - Number(h.final);
            const pct = Number(h.final) ? d / Number(h.final) * 100 : 0;
            if (d < -0.05) {
                caiu += 1;
                if (!queda || d < queda.d) queda = { nome: x.l.curso, d, pct };
            } else if (d > 0.05) {
                subiu += 1;
                if (!alta || d > alta.d) alta = { nome: x.l.curso, d, pct };
            } else igual += 1;
        });
        if (caiu + subiu + igual) {
            let txt = `Contra o último preço gravado do Balcão Estratégico: ${caiu} ficam mais baratos, ${subiu} mais caros e ${igual} iguais.`;
            if (queda) txt += ` Maior queda: ${queda.nome} (${queda.pct.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%).`;
            if (alta) txt += ` Maior alta: ${alta.nome} (${alta.pct.toLocaleString('pt-BR', { maximumFractionDigits: 1 })}%).`;
            frases.push(txt);
        }
    }
    notas.innerHTML = frases.map(f => `<p class="bp-nota">${_bpEsc(f)}</p>`).join('');
    notas.classList.toggle('hidden', !frases.length);
    box.classList.remove('hidden');
}

function bpRenderCursos() {
    const box = document.getElementById('bp-preview');
    if (!box || !_bpPreview) return;
    const q = (document.getElementById('bp-busca-cursos').value || '').toLocaleLowerCase('pt-BR');
    const linhas = (_bpPreview.linhas || []).filter(l => !q || (l.curso + ' ' + l.grau).toLocaleLowerCase('pt-BR').includes(q));
    const contagem = document.getElementById('bp-contagem');
    if (contagem) contagem.textContent = q ? `${linhas.length} de ${_bpPreview.linhas.length}` : `${_bpPreview.linhas.length} cursos`;
    box.innerHTML = linhas.map(l => {
        const fixas = (l.cotas || []).filter(c => c.fixa).sort((a, b) => (_bpEhBase(a.canal) ? 0 : 1) - (_bpEhBase(b.canal) ? 0 : 1));
        const extras = []
            .concat(_bpGlobais)
            .concat(_bpPorCurso[l.chave] || [])
            .filter((c, i, arr) => arr.indexOf(c) === i && !(_bpEhBase(c) || _bpEhAprovados(c)));
        const blocos = fixas.map(c => `
            <div class="bp-cota">
                <span>${_bpEsc(c.label)}</span>
                <b>${_bpMoney(c.final)}</b>
                <span>${Number(c.desconto).toLocaleString('pt-BR')}%</span>
            </div>`).join('');
        const mais = extras.map(canal => {
            const c = _bpCotaDoCurso(l, canal) || { label: canal, final: null, desconto: null };
            const soDeste = (_bpPorCurso[l.chave] || []).includes(canal);
            const x = soDeste ? `<button type="button" onclick="bpTirarCurso('${_bpEsc(l.chave)}','${_bpEsc(canal)}')">×</button>` : '';
            return `<div class="bp-cota"><span>${_bpEsc(c.label)} ${x}</span><b>${_bpMoney(c.final)}</b><span>${c.desconto == null ? '' : Number(c.desconto).toLocaleString('pt-BR') + '%'}</span></div>`;
        }).join('');
        const aviso = l.aviso ? `<div class="text-xs text-amber-500 mt-1">Parecido com ${_bpEsc(l.aviso)}</div>` : '';
        const badge = l.acao === 'criar'
            ? '<span class="text-xs font-semibold text-sky-500">Criar</span>'
            : '<span class="text-xs font-semibold text-emerald-500">Atualizar</span>';
        return `<div class="bp-row">
            <div>
                <div class="font-semibold">${_bpEsc(l.curso)}</div>
                <div class="text-xs text-[var(--text-muted)]">${_bpEsc(l.grau)}${l.sub ? ' · ' + _bpEsc(l.sub) : ''}</div>
                ${aviso}
            </div>
            <div class="flex flex-wrap items-center gap-2">${blocos}${mais}
                <button type="button" class="bp-plus" onclick="bpAbrirMenu(event, '${_bpEsc(l.chave)}')" aria-label="Adicionar bolsa só neste curso">+</button>
            </div>
            <div>${badge}</div>
        </div>`;
    }).join('') || `<p class="px-4 py-6 text-sm text-[var(--text-muted)]">Nenhum curso com esse nome.</p>`;
}

async function bpAplicar(somenteHistorico) {
    const file = _bpFile();
    if (!file || !_bpPreview) return;
    const status = document.getElementById('bp-status');
    const n = _bpPreview.cursos || 0;
    const msg = somenteHistorico
        ? `Gravar o histórico de ${n} cursos, sem mexer no CRM?`
        : `Aplicar ${n} cursos no CRM? Sobem Balcão Estratégico e Mar Aberto. Grade e imagem ficam.`;
    if (!window.confirm(msg)) return;
    const alvo = somenteHistorico ? 'Histórico' : 'CRM';
    status.textContent = somenteHistorico ? 'Gravando histórico…' : 'Atualizando o CRM… pode levar alguns minutos.';
    bpLog(alvo, somenteHistorico ? `Gravando ${n} cursos, sem mexer no CRM.` : `Começou. ${n} cursos. Balcão Estratégico e Mar Aberto.`, 'info');
    _bpBotoes(true);
    const body = new FormData();
    body.append('file', file);
    body.append('somente_historico', somenteHistorico ? '1' : '0');
    body.append('canais', JSON.stringify(_bpGlobais));
    body.append('por_curso', JSON.stringify(_bpPorCurso));
    const res = await fetch('/api/bwipo-precos/aplicar', { method: 'POST', body });
    const data = await res.json().catch(() => ({}));
    _bpBotoes(false);
    if (!res.ok || !data.ok) {
        status.textContent = data.error || 'Falha ao aplicar.';
        bpLog(alvo, data.error || 'Falha ao aplicar.', 'erro');
        return;
    }
    (data.resultados || []).forEach(r => {
        const nome = `${r.curso || ''}${r.grau ? ' · ' + r.grau : ''}`;
        bpLog('CRM', r.ok ? `${nome} — ${r.acao || 'ok'}` : `${nome} — ${r.erro || 'erro'}`, r.ok ? 'ok' : 'erro');
    });
    const r = data.resumo || {};
    status.textContent = somenteHistorico
        ? `Histórico ${data.snapshot_id} gravado.`
        : `Aplicado. ${r.ok || 0} ok, ${r.erro || 0} erro.`;
    bpLog(alvo, somenteHistorico ? `Gravado. Snapshot ${data.snapshot_id}.` : `Fechou. ${r.ok || 0} ok, ${r.erro || 0} erro.`, (r.erro ? 'erro' : 'ok'));
    bpPainel();
}

async function bpAtualizarSite() {
    const file = _bpFile();
    const sel = document.getElementById('bp-site');
    const site = _bpSites[sel && sel.value];
    if (!file || !_bpPreview || !site) return;
    const status = document.getElementById('bp-status');
    const [nome, url] = site;
    const aberto = _bpCanalMarAberto();
    if (!aberto) {
        status.textContent = 'A planilha não tem o canal Mar Aberto.';
        bpLog(nome, 'A planilha não tem o canal Mar Aberto.', 'erro');
        return;
    }
    if (!window.confirm(`Atualizar ${nome} com o valor do Mar Aberto?`)) return;
    status.textContent = `Atualizando ${nome}…`;
    bpLog(nome, 'Começou. O valor é o do Mar Aberto.', 'info');
    _bpBotoes(true);
    const body = new FormData();
    body.append('file', file);
    body.append('canal', aberto.canal);
    try {
        const res = await fetch(url, { method: 'POST', body });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.error) {
            status.textContent = `${nome}: ${data.error || 'falhou'}`;
            bpLog(nome, data.error || 'Falhou.', 'erro');
            return;
        }
        (data.linhas || []).forEach(l => {
            const mudou = `${l.chave}: ${l.de == null ? '—' : l.de} → ${l.para}`;
            bpLog(nome, l.ok ? mudou : `${mudou}. ${l.erro || 'erro'}`, l.ok ? 'ok' : 'erro');
        });
        const erros = data.erros ? `, ${data.erros} erro` : '';
        const iguais = data.iguais ? `, ${data.iguais} iguais` : '';
        status.textContent = `${nome}: ${data.atualizados || 0} atualizados${iguais}${erros}`;
        bpLog(nome, `Fechou. ${data.atualizados || 0} atualizados${iguais}${erros}.`, data.erros ? 'erro' : 'ok');
        if (data.ultimo_erro) bpLog(nome, data.ultimo_erro, 'erro');
    } catch (e) {
        status.textContent = `${nome}: ${e.message || 'falhou'}`;
        bpLog(nome, e.message || 'Falhou.', 'erro');
    } finally {
        _bpBotoes(false);
    }
}

async function bpAtualizarIa() {
    const file = _bpFile();
    if (!file || !_bpPreview) return;
    const status = document.getElementById('bp-status');
    const aberto = _bpCanalMarAberto();
    if (!aberto) {
        status.textContent = 'A planilha não tem o canal Mar Aberto.';
        bpLog('IA', 'A planilha não tem o canal Mar Aberto.', 'erro');
        return;
    }
    if (!window.confirm('Atualizar a base da IA com o valor do Mar Aberto? O curso que já existe troca o preço em documents_precos e em cursos_salesbot, no Postgres e no Supabase.')) return;
    status.textContent = 'Atualizando a base da IA…';
    bpLog('IA', 'Começou. O valor é o do Mar Aberto.', 'info');
    _bpBotoes(true);
    const body = () => {
        const fd = new FormData();
        fd.append('file', file);
        fd.append('canal', aberto.canal);
        return fd;
    };
    const fechar = (alvo, data) => {
        (data.linhas || []).forEach(l => {
            const mudou = `${l.chave}: ${l.de == null ? '—' : l.de} → ${l.para}`;
            bpLog(alvo, l.ok ? mudou : `${mudou}. ${l.erro || 'erro'}`, l.ok ? 'ok' : 'erro');
        });
        const erros = data.erros ? `, ${data.erros} erro` : '';
        const iguais = data.iguais ? `, ${data.iguais} iguais` : '';
        bpLog(alvo, `Fechou. ${data.atualizados || 0} atualizados${iguais}${erros}.`, data.erros ? 'erro' : 'ok');
        if (data.ultimo_erro) bpLog(alvo, data.ultimo_erro, 'erro');
        return `${data.atualizados || 0} atualizados${iguais}${erros}`;
    };
    try {
        const res = await fetch('/api/atualizar_grad_canal', { method: 'POST', body: body() });
        const data = await res.json().catch(() => ({}));
        if (!res.ok || data.error) {
            status.textContent = `IA: ${data.error || 'falhou'}`;
            bpLog('IA', data.error || 'Falhou.', 'erro');
        } else {
            const resumo = fechar('IA', data);
            status.textContent = `IA: ${resumo}`;
        }
        bpLog('Salesbot', 'Começou. Preço eduit 1 do curso que já existe.', 'info');
        const res2 = await fetch('/api/atualizar_salesbot_canal', { method: 'POST', body: body() });
        const data2 = await res2.json().catch(() => ({}));
        if (!res2.ok || data2.error) {
            status.textContent = `Salesbot: ${data2.error || 'falhou'}`;
            bpLog('Salesbot', data2.error || 'Falhou.', 'erro');
        } else {
            const resumo2 = fechar('Salesbot', data2);
            status.textContent = `Salesbot: ${resumo2}`;
        }
    } catch (e) {
        status.textContent = `IA: ${e.message || 'falhou'}`;
        bpLog('IA', e.message || 'Falhou.', 'erro');
    } finally {
        _bpBotoes(false);
    }
}

async function bpPainel() {
    const sel = document.getElementById('bp-canal');
    if (!sel) return;
    const canal = sel.value || '';
    const res = await fetch('/api/bwipo-precos/painel' + (canal ? ('?canal=' + encodeURIComponent(canal)) : ''));
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) return;
    document.getElementById('bp-kpi-caiu').textContent = data.caiu;
    document.getElementById('bp-kpi-subiu').textContent = data.subiu;
    document.getElementById('bp-kpi-igual').textContent = data.igual;
    const atual = sel.value;
    const opts = data.canais || [];
    if (sel.options.length !== opts.length) {
        const ordenados = opts.slice().sort((a, b) => {
            const pa = _bpEhBase(a.canal) ? 0 : (_bpEhAprovados(a.canal) ? 1 : 2);
            const pb = _bpEhBase(b.canal) ? 0 : (_bpEhAprovados(b.canal) ? 1 : 2);
            return pa - pb || a.label.localeCompare(b.label, 'pt-BR');
        });
        sel.innerHTML = ordenados.map(c => {
            const nome = _bpEhBase(c.canal) ? 'Balcão Estratégico' : (_bpEhAprovados(c.canal) ? 'Mar Aberto' : c.label);
            return `<option value="${_bpEsc(c.canal)}">${_bpEsc(nome)}</option>`;
        }).join('');
        const preferido = atual && ordenados.some(c => c.canal === atual)
            ? atual
            : (ordenados.find(c => _bpEhBase(c.canal)) || ordenados[0] || {}).canal;
        if (preferido) sel.value = preferido;
    }
    _bpLinhas = data.linhas || [];
    document.getElementById('bp-vazio').classList.toggle('hidden', _bpLinhas.length > 0);
    bpFiltrar();
    if (_bpPreview) bpInsights();
}

function bpOrdenar(col) {
    if (_bpSort.col === col) _bpSort.dir = _bpSort.dir === 'desc' ? 'asc' : 'desc';
    else _bpSort = { col, dir: 'desc' };
    document.querySelectorAll('#page-precos_bwipo .bp-sort').forEach(btn => {
        const on = btn.dataset.col === _bpSort.col;
        btn.classList.toggle('is-on', on);
        const seta = btn.querySelector('.bp-seta');
        if (seta) seta.textContent = on ? (_bpSort.dir === 'desc' ? '▼' : '▲') : '';
    });
    bpFiltrar();
}

function _bpCampo(l, col) {
    if (col === 'curso') return l.curso || '';
    if (col === 'grau') return l.grau || '';
    if (col === 'final') return l.final;
    if (col === 'delta') return l.delta;
    return l.menor_6m;
}

function bpFiltrar() {
    const q = (document.getElementById('bp-busca').value || '').toLocaleLowerCase('pt-BR');
    const filtro = (document.getElementById('bp-filtro') || {}).value || '';
    const rows = _bpLinhas.filter(l => {
        if (q && !(l.curso + ' ' + l.grau).toLocaleLowerCase('pt-BR').includes(q)) return false;
        if (filtro === 'subiu') return l.delta != null && l.delta > 0.05;
        if (filtro === 'caiu') return l.delta != null && l.delta < -0.05;
        if (filtro === 'igual') return l.delta != null && Math.abs(l.delta) <= 0.05;
        if (filtro === 'acima') return l.menor_6m != null && l.final != null && l.menor_6m < l.final - 0.009;
        return true;
    });
    const col = _bpSort.col;
    const desc = _bpSort.dir === 'desc';
    rows.sort((a, b) => {
        const va = _bpCampo(a, col);
        const vb = _bpCampo(b, col);
        const na = va == null || va === '';
        const nb = vb == null || vb === '';
        if (na || nb) return na - nb;
        if (typeof va === 'number' && typeof vb === 'number') return desc ? vb - va : va - vb;
        return desc
            ? String(vb).localeCompare(String(va), 'pt-BR')
            : String(va).localeCompare(String(vb), 'pt-BR');
    });
    document.getElementById('bp-hist').innerHTML = rows.map(l => {
        let delta = '—';
        if (l.delta != null) {
            const cls = l.delta < 0 ? 'text-emerald-500' : (l.delta > 0 ? 'text-rose-500' : '');
            const sinal = l.delta > 0 ? '+' : '';
            delta = `<span class="${cls}">${sinal}${l.delta.toLocaleString('pt-BR')}%</span>`;
        }
        const menor = l.menor_6m != null && l.final != null && l.menor_6m < l.final - 0.009
            ? _bpMoney(l.menor_6m) : 'é o menor';
        return `<tr class="border-t border-[var(--border)]">
            <td class="px-4 py-2">${_bpEsc(l.curso)}</td>
            <td class="py-2">${_bpEsc(l.grau)}</td>
            <td class="py-2">${_bpMoney(l.final)}</td>
            <td class="py-2">${delta}</td>
            <td class="py-2 pr-4">${menor}</td>
        </tr>`;
    }).join('');
}
