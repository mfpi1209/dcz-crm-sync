// Motor de Ativações — resultado do ciclo (motor × controle), comportamento, previsão e textos. Só leitura.

let _mtzPronto = false;

const MTZ_ESTADOS = {
    sem_historico: 'Sem histórico', em_dia: 'Em dia', silenciado: 'Silenciado', tardio: 'Tardio',
    primeiro_atraso: 'Primeiro atraso', nao_reage: 'Não reage',
};

function _mtzPct(v) { return v == null ? '—' : (v * 100).toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + '%'; }
function _mtzPp(v) { return v == null ? '—' : (v > 0 ? '+' : '') + v.toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' p.p.'; }
function _mtzCiclo(c) { const [a, m] = c.split('-'); return m + '/' + a; }
function _mtzTexto(id, t) { const el = document.getElementById(id); if (el) el.textContent = t; }

function mtzIniciar() {
    if (_mtzPronto) return;
    _mtzPronto = true;
    mtzCarregar();
}

async function mtzCarregar() {
    try {
        const sel = document.getElementById('mtz-ciclo');
        const res = await api('/api/motor/resultados' + (sel.value ? '?ciclo=' + encodeURIComponent(sel.value) : ''));
        const d = await res.json();
        if (!res.ok || !d.ok) return;
        _mtzTextos(d.textos || []);
        if (!d.instalado) return;
        sel.innerHTML = (d.ciclos.length ? d.ciclos : [d.resultado.ciclo]).map(c => `<option value="${c}"${c === d.resultado.ciclo ? ' selected' : ''}>${_mtzCiclo(c)}</option>`).join('');
        _mtzResultado(d.resultado);
        _mtzComportamento(d.comportamento);
    } catch (e) { /* a página segue sem estes blocos */ }
}

function _mtzResultado(r) {
    const m = r.motor, c = r.controle, e = r.em_dia;
    _mtzTexto('mtz-k-motor', _mtzPct(m.taxa_em_dia));
    _mtzTexto('mtz-k-motor-sub', _mtNum(m.em_dia) + ' de ' + _mtNum(m.alunos) + ' · até o dia 25: ' + _mtzPct(m.taxa_ate_dia25));
    _mtzTexto('mtz-k-controle', _mtzPct(c.taxa_em_dia));
    _mtzTexto('mtz-k-controle-sub', _mtNum(c.em_dia) + ' de ' + _mtNum(c.alunos) + ' · até o dia 25: ' + _mtzPct(c.taxa_ate_dia25));
    _mtzTexto('mtz-k-efeito', _mtzPp(e.diferenca_pp));
    _mtzTexto('mtz-k-efeito-sub', e.diferenca_pp == null ? 'sem grupo de controle neste ciclo'
        : 'entre ' + _mtzPp(e.de_pp) + ' e ' + _mtzPp(e.ate_pp) + (e.conclusivo ? ' · ' + _mtNum(e.a_mais) + ' alunos a mais em dia' : ' · ainda não dá para afirmar'));
    _mtzTexto('mtz-k-cobertura', _mtzPct(r.cobertura.taxa));
    _mtzTexto('mtz-k-cobertura-sub', _mtNum(r.cobertura.receberam) + ' alunos fora do controle');

    const avisos = [];
    if (!r.dia10_fechado) avisos.push('Ciclo em andamento: "pagou em dia" só fecha depois de ' + _mtDia(r.corte_dia10) + ' (prazo mais a folga da baixa). Até lá os números sobem.');
    else if (!r.dia25_fechado) avisos.push('"Até o dia 25" só fecha depois de ' + _mtDia(r.corte_dia25) + '.');
    if (c.alunos && c.alunos < r.amostra_minima_controle) avisos.push('Grupo de controle com ' + _mtNum(c.alunos) + ' alunos: abaixo de ' + r.amostra_minima_controle + ' a diferença não é dada como conclusiva.');
    if (r.cobertura.controle_que_recebeu) avisos.push(_mtNum(r.cobertura.controle_que_recebeu) + ' alunos do controle receberam mensagem: o controle está contaminado e a diferença fica menor do que o efeito real.');
    if (!r.por_mensagem.some(x => x.sairam)) avisos.push('Nenhum envio real neste ciclo: motor e controle receberam o mesmo tratamento, então a diferença esperada é zero.');
    const faixa = document.getElementById('mtz-faixa');
    faixa.innerHTML = avisos.map(a => `<p>${_mtEsc(a)}</p>`).join('');
    _mtMostrar('mtz-faixa', avisos.length > 0);

    document.getElementById('mtz-estados').innerHTML = r.por_estado.map(x => `<tr><td>${MTZ_ESTADOS[x.estado] || x.estado}</td>`
        + `<td class="mt-num">${_mtNum(x.motor.alunos)}</td><td class="mt-num">${_mtzPct(x.motor.taxa_em_dia)}</td><td class="mt-num">${_mtzPct(x.motor.taxa_ate_dia25)}</td>`
        + `<td class="mt-num">${_mtNum(x.controle.alunos)}</td><td class="mt-num">${_mtzPct(x.controle.taxa_em_dia)}</td>`
        + `<td class="mt-num">${_mtzPp(x.em_dia.diferenca_pp)}${x.em_dia.conclusivo ? '' : (x.em_dia.diferenca_pp == null ? '' : ' <span class="mt-neutro">(incerto)</span>')}</td></tr>`).join('')
        || '<tr><td colspan="7">Sem alunos neste ciclo.</td></tr>';

    const msgs = r.por_mensagem.filter(x => x.sairam);
    _mtMostrar('mtz-msgs-vazio', !msgs.length);
    _mtMostrar('mtz-msgs-tabela', msgs.length > 0);
    document.getElementById('mtz-msgs').innerHTML = msgs.map(x => `<tr><td>${_mtEsc(MT_MENSAGENS[x.mensagem] || x.mensagem)}</td><td>${_mtEsc(x.variante)}</td>`
        + `<td class="mt-num">${_mtNum(x.sairam)}</td><td class="mt-num">${_mtzPct(x.taxa_entrega)}</td><td class="mt-num">${_mtzPct(x.taxa_leitura)}</td>`
        + `<td class="mt-num">${_mtzPct(x.taxa_resposta)}</td><td class="mt-num">${x.parar_por_mil == null ? '—' : x.parar_por_mil.toLocaleString('pt-BR')}</td><td class="mt-num">${_mtzPct(x.taxa_em_dia)}</td></tr>`).join('');
    const rot = Object.entries(r.respostas);
    _mtzTexto('mtz-respostas', rot.length ? 'Respostas: ' + rot.map(([k, v]) => (MT_ROTULOS[k] || k) + ' ' + _mtNum(v)).join(' · ') : '');
    _mtMostrar('mtz-bloco', true);
}

function _mtzComportamento(c) {
    const p = c.previsao;
    const caixa = (id, chave) => {
        _mtzTexto(id, p ? _mtNum(p[chave].esperado) : '—');
        _mtzTexto(id + '-sub', p ? 'entre ' + _mtNum(Math.max(0, p[chave].esperado - p[chave].margem)) + ' e ' + _mtNum(p[chave].esperado + p[chave].margem) : 'sem histórico suficiente');
    };
    caixa('mtz-p-emdia', 'em_dia'); caixa('mtz-p-tardio', 'tardio'); caixa('mtz-p-nao', 'nao_pagou');
    const r = c.ritmo;
    _mtzTexto('mtz-p-ritmo', r ? (r.diferenca > 0 ? '+' : '') + _mtNum(r.diferenca) : '—');
    _mtzTexto('mtz-p-ritmo-sub', r ? _mtNum(r.pagaram_ate_hoje) + ' pagaram; o esperado para hoje era ' + _mtNum(r.esperado_ate_hoje) : 'sem ciclo fechado para comparar');
    _mtzTexto('mtz-p-nota', c.ciclos_de_base.length
        ? 'Base: ' + c.ciclos_de_base.map(_mtzCiclo).join(', ') + '. ' + _mtNum(c.alunos_no_ciclo) + ' alunos em ' + _mtzCiclo(c.ciclo) + ', ' + _mtNum(c.pagaram_ate_hoje) + ' já com baixa. Estado com menos de ' + c.amostra_minima + ' casos usa a taxa geral.'
        : 'Ainda não há ciclo fechado no livro-razão do motor: a previsão aparece depois do primeiro ciclo completo (ou da importação do histórico).');
    document.getElementById('mtz-comportamento').innerHTML = c.por_estado.map(x => `<tr><td>${MTZ_ESTADOS[x.estado] || x.estado}</td><td class="mt-num">${_mtNum(x.alunos)}</td>`
        + `<td class="mt-num">${_mtzPct(x.em_dia)}</td><td class="mt-num">${_mtzPct(x.tardio)}</td><td class="mt-num">${_mtzPct(x.nao_pagou)}</td>`
        + `<td class="mt-num">${_mtNum(c.por_estado_atual[x.estado] || 0)}</td></tr>`).join('');
    _mtMostrar('mtz-bloco-previsao', true);
}

function _mtzTextos(textos) {
    document.getElementById('mtz-textos').innerHTML = textos.map(t => `<tr><td>${_mtEsc(MT_MENSAGENS[t.mensagem] || t.mensagem)}</td><td>${_mtEsc(t.variante)}</td><td>${_mtEsc(t.angulo)}</td>`
        + `<td style="white-space: pre-line; min-width: 18rem">${_mtEsc(t.corpo)}</td><td>${t.botoes.map(_mtEsc).join('<br>')}</td>`
        + `<td>${t.problemas.length ? '<span class="mt-aviso">' + t.problemas.map(_mtEsc).join('<br>') + '</span>' : '<span class="mt-ok">Sem problemas</span>'}`
        + `<br><span class="mt-neutro">${t.aprovado ? 'Aprovado' : 'Proposta: falta aprovar e cadastrar'}</span></td></tr>`).join('');
    _mtMostrar('mtz-bloco-textos', textos.length > 0);
}
