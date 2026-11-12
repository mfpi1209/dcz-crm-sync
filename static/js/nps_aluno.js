// NPS do Aluno — página de leitura. Os números vêm de /api/nps/resumo (tabelas nps_ do painel).

const NP_LEITURA = {
    insuficiente: ['menos de 30 respostas', 'np-neutro'],
    indicativo: ['indicativo', 'np-aviso'],
    comparavel: ['comparável', 'np-ok'],
};

function _npEsc(s) {
    return String(s ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function _npNum(n) { return n == null ? '—' : Number(n).toLocaleString('pt-BR'); }
function _npPct(n) { return n == null ? '—' : Number(n).toLocaleString('pt-BR', { maximumFractionDigits: 1 }) + '%'; }
function _npNps(n) { return n == null ? '—' : (n > 0 ? '+' : '') + Number(n).toLocaleString('pt-BR', { maximumFractionDigits: 1 }); }
function _npMargem(n) { return n == null ? '—' : '±' + Number(n).toLocaleString('pt-BR', { maximumFractionDigits: 1 }); }
function _npSet(id, texto) { const el = document.getElementById(id); if (el) el.textContent = texto; }
function _npMostrar(id, sim) { const el = document.getElementById(id); if (el) el.classList.toggle('hidden', !sim); }
function _npLeitura(chave) {
    const [texto, classe] = NP_LEITURA[chave] || NP_LEITURA.insuficiente;
    return `<span class="np-sel ${classe}">${texto}</span>`;
}
function _npDiaHora(iso) {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', timeZone: 'America/Sao_Paulo' });
}
function _npPeriodo() {
    const de = document.getElementById('np-de')?.value || '';
    const ate = document.getElementById('np-ate')?.value || '';
    const p = new URLSearchParams();
    if (de) p.set('de', de);
    if (ate) p.set('ate', ate);
    return p;
}

function loadNpsAluno() { npAtualizar(); }

async function npAtualizar() {
    let d;
    try {
        const r = await api('/api/nps/resumo?' + _npPeriodo().toString());
        d = await r.json();
        if (!r.ok || !d.ok) throw new Error(d.error || 'Falha ao carregar');
    } catch (e) {
        _npFaixa('Não consegui carregar o NPS: ' + e.message);
        return;
    }
    const de = document.getElementById('np-de');
    const ate = document.getElementById('np-ate');
    if (de && !de.value) de.value = d.de;
    if (ate && !ate.value) ate.value = d.ate;
    _npMostrar('np-btn-coletar', !!d.admin && !!d.coleta_habilitada && d.instalado !== false);

    if (d.instalado === false) {
        _npFaixa('As tabelas do NPS ainda não foram criadas neste banco. Aplicar com: python scripts/apply_nps_sql.py');
        return;
    }
    const avisos = [];
    if (!d.coleta_habilitada) avisos.push('A coleta automática está desligada (NPS_COLETA_ENABLED=0): os números só mudam quando ela for ligada.');
    if (d.coleta && d.coleta.status === 'erro') avisos.push('A última coleta falhou: ' + (d.coleta.erro || 'erro não informado') + '.');
    if (d.resolvido_nao_lido) avisos.push(_npNum(d.resolvido_nao_lido) + ' respostas vieram com o "resolvido" num formato que a coleta não reconheceu; conferir as opções do formulário.');
    _npFaixa(avisos.join(' '));

    const g = d.geral;
    _npSet('np-nps', _npNps(g.nps));
    _npSet('np-nps-sub', g.nps == null ? `precisa de ${d.regras.n_min_publicar} respostas` : `margem ${_npMargem(g.margem)} · ${_npPct(g.promotores_pct)} promotores, ${_npPct(g.detratores_pct)} detratores`);
    _npSet('np-resp', _npNum(g.respostas));
    _npSet('np-resp-sub', `${_npNum(g.envios)} enviadas · taxa ${_npPct(g.taxa_resposta)}`);
    _npSet('np-res', _npPct(g.resolvido_sim_pct));
    _npSet('np-res-sub', g.resolvido_sim_pct == null ? 'sem respostas suficientes' : `${_npPct(g.resolvido_nao_pct)} disseram que não`);
    _npSet('np-coleta', d.coleta ? _npDiaHora(d.coleta.fim || d.coleta.inicio) : 'nunca rodou');
    _npSet('np-coleta-sub', d.coleta ? `${_npNum(d.coleta.negocios_lidos)} negócios lidos · ${_npNum(d.coleta.respostas_novas)} respostas novas` : '');

    _npMostrar('np-dist', !!d.distribuicao);
    _npMostrar('np-dist-vazio', !d.distribuicao);
    if (d.distribuicao) {
        const max = Math.max(...d.distribuicao, 1);
        document.getElementById('np-barras').innerHTML = d.distribuicao.map((q, nota) => {
            const classe = nota >= 9 ? 'pro' : nota >= 7 ? 'neu' : 'det';
            return `<div class="np-barra ${classe}" style="height:${Math.round(100 * q / max)}%" title="Nota ${nota}: ${_npNum(q)}"></div>`;
        }).join('');
    }

    const linha = (r, comTaxa) => `<tr><td>${_npEsc(r.recorte)}</td>`
        + (comTaxa ? `<td class="np-num">${_npNum(r.envios)}</td>` : '')
        + `<td class="np-num">${_npNum(r.respostas)}</td>`
        + (comTaxa ? `<td class="np-num">${_npPct(r.taxa_resposta)}</td>` : '')
        + `<td class="np-num">${_npNps(r.nps)}</td><td class="np-num">${_npMargem(r.margem)}</td>`;
    const vazio = (colunas) => `<tr><td colspan="${colunas}" class="text-[var(--text-muted)]">Nenhuma pesquisa no período.</td></tr>`;

    document.getElementById('np-deptos').innerHTML = d.por_depto.length ? d.por_depto.map(r =>
        linha(r, true) + `<td class="np-num">${_npPct(r.detratores_pct)}</td><td class="np-num">${_npPct(r.resolvido_sim_pct)}</td><td>${_npLeitura(r.leitura)}</td></tr>`
    ).join('') : vazio(9);
    document.getElementById('np-semanas').innerHTML = d.por_semana.length ? d.por_semana.map(r =>
        linha(r, true) + `<td class="np-num">${_npPct(r.resolvido_sim_pct)}</td><td>${_npLeitura(r.leitura)}</td></tr>`
    ).join('') : vazio(8);

    _npMostrar('np-bloco-atendentes', !!d.ve_atendentes);
    if (d.ve_atendentes) {
        const at = d.por_atendente || [];
        document.getElementById('np-atendentes').innerHTML = at.length ? at.map(r =>
            `<tr><td>${_npEsc(r.recorte)}</td><td>${_npEsc(r.depto)}</td><td class="np-num">${_npNum(r.respostas)}</td>`
            + `<td class="np-num">${_npNps(r.nps)}</td><td class="np-num">${_npMargem(r.margem)}</td>`
            + `<td class="np-num">${_npPct(r.detratores_pct)}</td><td class="np-num">${_npPct(r.resolvido_sim_pct)}</td><td>${_npLeitura(r.leitura)}</td></tr>`
        ).join('') : vazio(8);
    }
    npMotivos();
}

function _npFaixa(texto) {
    const el = document.getElementById('np-faixa');
    if (!el) return;
    el.textContent = texto || '';
    el.classList.toggle('hidden', !texto);
}

async function npMotivos() {
    const alvo = document.getElementById('np-motivos');
    if (!alvo) return;
    const p = _npPeriodo();
    const classe = document.getElementById('np-classe')?.value || '';
    if (classe) p.set('classe', classe);
    try {
        const r = await api('/api/nps/motivos?' + p.toString());
        const d = await r.json();
        const lista = d.motivos || [];
        alvo.innerHTML = lista.length ? lista.map(m =>
            `<div class="np-motivo"><div><b>${_npEsc(m.nota)}</b></div><div>${_npEsc(m.motivo)}<div class="np-sub">${_npEsc(m.depto || 'sem departamento')}${m.resolvido ? ' · resolvido: ' + _npEsc({ sim: 'sim', parte: 'em parte', nao: 'não' }[m.resolvido]) : ''}</div></div></div>`
        ).join('') + (d.total > lista.length ? `<div class="np-sub mt-2">Mostrando ${lista.length} de ${_npNum(d.total)}.</div>` : '')
            : 'Nenhum comentário nesse recorte.';
    } catch (e) {
        alvo.textContent = 'Não consegui carregar os comentários.';
    }
}

async function npColetar() {
    const botao = document.getElementById('np-btn-coletar');
    if (botao) botao.disabled = true;
    try {
        const r = await api('/api/nps/coletar', { method: 'POST' });
        const d = await r.json();
        _npFaixa(d.ok ? 'Coleta iniciada. Leva alguns minutos; toque em Atualizar depois.' : (d.error || 'Não foi possível iniciar a coleta.'));
    } catch (e) {
        _npFaixa('Não foi possível iniciar a coleta.');
    } finally {
        if (botao) botao.disabled = false;
    }
}
