// ===========================================================================
// INTELLIGENCE PAGE
// ===========================================================================

const INTEL_TIPO_LABELS = {
    matriculados: 'Matriculados', inadimplentes: 'Inadimplentes',
    concluintes: 'Concluintes', acesso_ava: 'Acesso AVA',
    sem_rematricula: 'Sem Rematrícula',
};
const INTEL_TIPO_COLORS = {
    matriculados: {bg:'from-emerald-500/20 to-green-500/20', border:'border-emerald-500/40', text:'text-emerald-400', bar:'bg-emerald-500'},
    inadimplentes: {bg:'from-amber-500/20 to-orange-500/20', border:'border-amber-500/40', text:'text-amber-400', bar:'bg-amber-500'},
    concluintes: {bg:'from-purple-500/20 to-violet-500/20', border:'border-purple-500/40', text:'text-purple-400', bar:'bg-purple-500'},
    acesso_ava: {bg:'from-sky-500/20 to-blue-500/20', border:'border-sky-500/40', text:'text-sky-400', bar:'bg-sky-500'},
    sem_rematricula: {bg:'from-rose-500/20 to-pink-500/20', border:'border-rose-500/40', text:'text-rose-400', bar:'bg-rose-500'},
};

async function loadIntelligence() {
    await Promise.all([
        _loadIntelOverview(), _loadIntelCompare(), _loadIntelCrossref(),
        _loadIntelAlerts(),
        _loadEngScores(), _loadEngCharts(), _loadCommLog(),
    ]);
}

async function _loadIntelOverview() {
    const container = document.getElementById('intel-overview-cards');
    try {
        const res = await fetch('/api/upload/info');
        const data = await res.json();
        const snaps = data.snapshots || {};
        let html = '';
        for (const tipo of ['matriculados','inadimplentes','concluintes','acesso_ava','sem_rematricula']) {
            const s = snaps[tipo];
            const c = INTEL_TIPO_COLORS[tipo];
            const label = INTEL_TIPO_LABELS[tipo];
            html += `<div class="glass-card p-4 border ${c.border} bg-gradient-to-br ${c.bg}">
                <p class="text-xs font-medium text-gray-400 mb-1">${label}</p>
                <p class="text-2xl font-bold text-[var(--text-primary)] font-display">${s ? s.row_count.toLocaleString('pt-BR') : '—'}</p>
                <p class="text-[10px] text-gray-500 mt-1">${s ? s.uploaded_at : 'Nenhum snapshot'}</p>
            </div>`;
        }
        container.innerHTML = html;
    } catch(e) { container.innerHTML = '<p class="text-red-400 text-sm col-span-5">Erro ao carregar visão geral</p>'; }
}

async function _loadIntelCompare() {
    const tipo = document.getElementById('intel-tipo').value;
    const periodo = document.getElementById('intel-periodo').value;
    const container = document.getElementById('intel-compare-cards');
    const detail = document.getElementById('intel-compare-detail');
    try {
        const res = await fetch(`/api/snapshots/compare?tipo=${tipo}&periodo=${periodo}`);
        const d = await res.json();
        if (d.error) { container.innerHTML = `<p class="text-gray-500 text-sm col-span-3">${d.error}</p>`; detail.innerHTML=''; return; }
        const sa = d.snap_a, sb = d.snap_b;
        const deltaP = sb && sb.row_count > 0 ? ((sa.row_count - sb.row_count) / sb.row_count * 100).toFixed(1) : '—';
        const deltaClass = d.delta_total > 0 ? 'text-emerald-400' : d.delta_total < 0 ? 'text-rose-400' : 'text-gray-400';
        const deltaSign = d.delta_total > 0 ? '+' : '';
        container.innerHTML = `
            <div class="glass-card p-4">
                <p class="text-xs text-gray-500 mb-1">Snapshot Atual</p>
                <p class="text-xl font-bold text-[var(--text-primary)]">${sa.row_count.toLocaleString('pt-BR')}</p>
                <p class="text-[10px] text-gray-500">${sa.uploaded_at}</p>
            </div>
            <div class="glass-card p-4">
                <p class="text-xs text-gray-500 mb-1">Snapshot Anterior</p>
                <p class="text-xl font-bold text-[var(--text-primary)]">${sb ? sb.row_count.toLocaleString('pt-BR') : 'N/D'}</p>
                <p class="text-[10px] text-gray-500">${sb ? sb.uploaded_at : '—'}</p>
            </div>
            <div class="glass-card p-4">
                <p class="text-xs text-gray-500 mb-1">Variação</p>
                <p class="text-xl font-bold ${deltaClass}">${deltaSign}${d.delta_total.toLocaleString('pt-BR')} <span class="text-sm">(${deltaP}%)</span></p>
                <div class="flex gap-3 mt-2 text-[10px]">
                    <span class="text-emerald-400">+${d.novos} novos</span>
                    <span class="text-rose-400">-${d.removidos} saíram</span>
                    <span class="text-gray-400">${d.mantidos} mantidos</span>
                </div>
            </div>`;

        let detailHtml = '';
        const statsA = d.stats_a || {};
        const statsB = d.stats_b || {};
        const metricsToShow = Object.keys(statsA).filter(k => typeof statsA[k] === 'number');
        if (metricsToShow.length > 0) {
            detailHtml += '<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 mt-3">';
            for (const m of metricsToShow) {
                const va = statsA[m], vb = statsB[m];
                const delta = vb != null ? va - vb : 0;
                const cls = delta > 0 ? 'text-emerald-400' : delta < 0 ? 'text-rose-400' : 'text-gray-500';
                detailHtml += `<div class="bg-gray-100 dark:bg-gray-800/40 rounded-lg p-3"><p class="text-[10px] text-gray-500 uppercase">${m.replace(/_/g,' ')}</p><p class="text-sm font-bold text-[var(--text-primary)]">${typeof va==='number'?va.toLocaleString('pt-BR'):va}</p>${vb!=null?`<p class="text-[10px] ${cls}">${delta>0?'+':''}${delta.toLocaleString('pt-BR')}</p>`:''}
                </div>`;
            }
            detailHtml += '</div>';
        }
        detail.innerHTML = detailHtml;
    } catch(e) { container.innerHTML = `<p class="text-red-400 text-sm col-span-3">Erro: ${e.message}</p>`; detail.innerHTML=''; }
}

async function _loadIntelCrossref() {
    const container = document.getElementById('intel-crossref-cards');
    const pairs = [
        ['matriculados','inadimplentes','Matriculados com dívida'],
        ['matriculados','acesso_ava','Matriculados no AVA'],
        ['matriculados','sem_rematricula','Matr. sem rematrícula'],
        ['matriculados','concluintes','Matr. concluintes'],
    ];
    try {
        const results = await Promise.all(pairs.map(([a,b]) => fetch(`/api/snapshots/crossref?tipo_a=${a}&tipo_b=${b}`).then(r=>r.json())));
        let html = '';
        results.forEach((d,i) => {
            const [,, label] = pairs[i];
            if (d.error || d.total_a === 0) {
                html += `<div class="glass-card p-4"><p class="text-xs text-gray-500">${label}</p><p class="text-sm text-gray-600 mt-1">Sem dados</p></div>`;
                return;
            }
            const pct = d.total_a > 0 ? (d.em_ambos / d.total_a * 100).toFixed(1) : '0';
            html += `<div class="glass-card p-4">
                <p class="text-xs text-gray-500 mb-2">${label}</p>
                <p class="text-xl font-bold text-[var(--text-primary)]">${d.em_ambos.toLocaleString('pt-BR')} <span class="text-sm text-indigo-400">(${pct}%)</span></p>
                <div class="w-full bg-gray-200 dark:bg-gray-800 rounded-full h-1.5 mt-2">
                    <div class="bg-indigo-500 h-1.5 rounded-full" style="width:${Math.min(pct,100)}%"></div>
                </div>
                <div class="flex justify-between text-[10px] text-gray-500 mt-1">
                    <span>${INTEL_TIPO_LABELS[pairs[i][0]]}: ${d.total_a.toLocaleString('pt-BR')}</span>
                    <span>${INTEL_TIPO_LABELS[pairs[i][1]]}: ${d.total_b.toLocaleString('pt-BR')}</span>
                </div>
            </div>`;
        });
        container.innerHTML = html || '<p class="text-gray-500 text-sm col-span-4">Nenhum dado disponível para cruzamento</p>';
    } catch(e) { container.innerHTML = `<p class="text-red-400 text-sm col-span-4">Erro: ${e.message}</p>`; }
}

async function _loadIntelTimeline() {
    // Removido a pedido do usuário
}

async function _loadIntelAlerts() {
    const container = document.getElementById('intel-alerts-list');
    try {
        const [crossInadAll, crossInadGrad, crossInadPos, crossMatrAva, crossMatrSem] = await Promise.all([
            fetch('/api/snapshots/crossref?tipo_a=matriculados&tipo_b=inadimplentes').then(r=>r.json()).catch(()=>({})),
            fetch('/api/snapshots/crossref?tipo_a=matriculados&tipo_b=inadimplentes&nivel=Gradua%C3%A7%C3%A3o').then(r=>r.json()).catch(()=>({})),
            fetch('/api/snapshots/crossref?tipo_a=matriculados&tipo_b=inadimplentes&nivel=P%C3%B3s-Gradua%C3%A7%C3%A3o').then(r=>r.json()).catch(()=>({})),
            fetch('/api/snapshots/crossref?tipo_a=matriculados&tipo_b=acesso_ava').then(r=>r.json()).catch(()=>({})),
            fetch('/api/snapshots/crossref?tipo_a=matriculados&tipo_b=sem_rematricula').then(r=>r.json()).catch(()=>({})),
        ]);
        let alerts = [];
        const inadIcon = 'M12 8c-1.657 0-3 .895-3 2s1.343 2 3 2 3 .895 3 2-1.343 2-3 2m0-8c1.11 0 2.08.402 2.599 1M12 8V7m0 1v8m0 0v1m0-1c-1.11 0-2.08-.402-2.599-1M21 12a9 9 0 11-18 0 9 9 0 0118 0z';

        if (crossInadGrad.em_ambos > 0) {
            const pct = (crossInadGrad.em_ambos / Math.max(crossInadGrad.total_a, 1) * 100).toFixed(1);
            alerts.push({
                level: pct > 20 ? 'high' : pct > 10 ? 'medium' : 'low',
                icon: inadIcon,
                title: `Graduação: ${crossInadGrad.em_ambos.toLocaleString('pt-BR')} inadimplentes (${pct}%)`,
                desc: `De ${crossInadGrad.total_a.toLocaleString('pt-BR')} matriculados de graduação, ${crossInadGrad.em_ambos.toLocaleString('pt-BR')} possuem títulos em aberto.`,
                exportParams: 'tipo_a=matriculados&tipo_b=inadimplentes&subset=em_ambos&nivel=Gradua%C3%A7%C3%A3o',
            });
        }
        if (crossInadPos.em_ambos > 0) {
            const pct = (crossInadPos.em_ambos / Math.max(crossInadPos.total_a, 1) * 100).toFixed(1);
            alerts.push({
                level: pct > 20 ? 'high' : pct > 10 ? 'medium' : 'low',
                icon: inadIcon,
                title: `Pós-Graduação: ${crossInadPos.em_ambos.toLocaleString('pt-BR')} inadimplentes (${pct}%)`,
                desc: `De ${crossInadPos.total_a.toLocaleString('pt-BR')} matriculados de pós, ${crossInadPos.em_ambos.toLocaleString('pt-BR')} possuem títulos em aberto.`,
                exportParams: 'tipo_a=matriculados&tipo_b=inadimplentes&subset=em_ambos&nivel=P%C3%B3s-Gradua%C3%A7%C3%A3o',
            });
        } else if (crossInadAll.em_ambos > 0 && !crossInadGrad.em_ambos) {
            const pct = (crossInadAll.em_ambos / Math.max(crossInadAll.total_a, 1) * 100).toFixed(1);
            alerts.push({
                level: pct > 20 ? 'high' : pct > 10 ? 'medium' : 'low',
                icon: inadIcon,
                title: `${crossInadAll.em_ambos.toLocaleString('pt-BR')} alunos inadimplentes (${pct}%)`,
                desc: `De ${crossInadAll.total_a.toLocaleString('pt-BR')} matriculados, ${crossInadAll.em_ambos.toLocaleString('pt-BR')} possuem títulos em aberto.`,
                exportParams: 'tipo_a=matriculados&tipo_b=inadimplentes&subset=em_ambos',
            });
        }

        if (crossInadAll.apenas_b > 0) {
            alerts.push({
                level: 'medium',
                icon: 'M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z',
                title: `${crossInadAll.apenas_b.toLocaleString('pt-BR')} inadimplentes não encontrados como matriculados`,
                desc: `Alunos na base de inadimplentes que não constam na base de matriculados ativos.`,
                exportParams: 'tipo_a=matriculados&tipo_b=inadimplentes&subset=apenas_b',
            });
        }

        if (crossMatrAva.total_a > 0 && crossMatrAva.apenas_a > 0) {
            const pct = (crossMatrAva.apenas_a / Math.max(crossMatrAva.total_a, 1) * 100).toFixed(1);
            alerts.push({
                level: pct > 30 ? 'high' : pct > 15 ? 'medium' : 'low',
                icon: 'M9.75 17L9 20l-1 1h8l-1-1-.75-3M3 13h18M5 17h14a2 2 0 002-2V5a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z',
                title: `${crossMatrAva.apenas_a.toLocaleString('pt-BR')} matriculados sem registro no AVA (${pct}%)`,
                desc: `Esses alunos não possuem dados de acesso ao ambiente virtual de aprendizagem.`,
                exportParams: 'tipo_a=matriculados&tipo_b=acesso_ava&subset=apenas_a',
            });
        }
        if (crossMatrSem.em_ambos > 0) {
            alerts.push({
                level: 'high',
                icon: 'M18.364 18.364A9 9 0 005.636 5.636m12.728 12.728A9 9 0 015.636 5.636m12.728 12.728L5.636 5.636',
                title: `${crossMatrSem.em_ambos.toLocaleString('pt-BR')} alunos matriculados constam como sem rematrícula`,
                desc: `Inconsistência: presentes na base de matriculados e também na base de sem rematrícula.`,
                exportParams: 'tipo_a=matriculados&tipo_b=sem_rematricula&subset=em_ambos',
            });
        }
        if (alerts.length === 0) {
            alerts.push({level:'low', icon:'M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z', title:'Nenhum alerta no momento', desc:'Faça upload dos snapshots para gerar alertas automáticos.'});
        }
        const levelColors = {high:'border-rose-500/60 bg-rose-500/5',medium:'border-amber-500/60 bg-amber-500/5',low:'border-[var(--border)] bg-gray-100 dark:bg-gray-800/20'};
        const levelIcons = {high:'text-rose-400',medium:'text-amber-400',low:'text-gray-500'};
        container.innerHTML = alerts.map(a => `
            <div class="border rounded-xl p-4 flex items-start gap-3 ${levelColors[a.level]}">
                <svg class="w-5 h-5 flex-shrink-0 mt-0.5 ${levelIcons[a.level]}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="${a.icon}"/></svg>
                <div class="flex-1">
                    <p class="text-sm font-semibold text-[var(--text-primary)]">${a.title}</p>
                    <p class="text-xs text-gray-400 mt-0.5">${a.desc}</p>
                </div>
                ${a.exportParams ? `<button onclick="window.open('/api/snapshots/crossref/export?${a.exportParams}','_blank')" class="flex-shrink-0 btn-secondary text-[10px] px-2.5 py-1 rounded-lg flex items-center gap-1" title="Exportar CSV">
                    <svg class="w-3 h-3" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 10v6m0 0l-3-3m3 3l3-3m2 8H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z"/></svg>
                    CSV</button>` : ''}
            </div>`).join('');
    } catch(e) { container.innerHTML = `<p class="text-red-400 text-sm">Erro: ${e.message}</p>`; }
}

// ===========================================================================
// ENGAGEMENT AVA + COMMUNICATION MONITOR
// ===========================================================================
let _engPage_current = 1;

const _riskLabels = {engajado:'Engajado', atencao:'Atenção', em_risco:'Em Risco', critico:'Crítico'};
const _riskColors = {engajado:'#10b981', atencao:'#f59e0b', em_risco:'#f97316', critico:'#ef4444'};
const _riskBadge = {
    engajado: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    atencao: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
    em_risco: 'bg-orange-500/15 text-orange-400 border-orange-500/30',
    critico: 'bg-rose-500/15 text-rose-400 border-rose-500/30',
};
const _channelIcons = {
    email: '<svg class="w-3.5 h-3.5 text-sky-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3 8l7.89 5.26a2 2 0 002.22 0L21 8M5 19h14a2 2 0 002-2V7a2 2 0 00-2-2H5a2 2 0 00-2 2v10a2 2 0 002 2z"/></svg>',
    whatsapp: '<svg class="w-3.5 h-3.5 text-green-400" fill="currentColor" viewBox="0 0 24 24"><path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347z"/></svg>',
    ambos: '<svg class="w-3.5 h-3.5 text-violet-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 12h.01M12 12h.01M16 12h.01M21 12c0 4.418-4.03 8-9 8a9.863 9.863 0 01-4.255-.949L3 20l1.395-3.72C3.512 15.042 3 13.574 3 12c0-4.418 4.03-8 9-8s9 3.582 9 8z"/></svg>',
};
const _statusBadge = {
    enviado: 'bg-emerald-500/15 text-emerald-400',
    pendente: 'bg-sky-500/15 text-sky-400',
    falha: 'bg-rose-500/15 text-rose-400',
    entregue: 'bg-indigo-500/15 text-indigo-400',
    lido: 'bg-violet-500/15 text-violet-400',
    respondido: 'bg-indigo-500/15 text-indigo-400',
    cancelado: 'bg-gray-500/15 text-gray-400',
};

async function _recalcEngagement() {
    const btn = document.getElementById('btn-recalc-eng');
    const originalHtml = btn ? btn.innerHTML : '';
    try {
        if (btn) {
            btn.disabled = true;
            btn.innerHTML = '<svg class="w-4 h-4 animate-spin inline-block mr-1" fill="none" viewBox="0 0 24 24"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"/><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"/></svg> Recalculando...';
        }
        const res = await api('/api/engagement/recalculate', {method:'POST'});
        const d = await res.json();
        if (d.error) { _showEngAlert('error', 'Erro: ' + d.error); return; }

        let msg = `${d.processed.toLocaleString('pt-BR')} alunos processados.`;
        if (!d.has_ava_snapshot) {
            _showEngAlert('warning', `${msg} Nenhum snapshot de Acesso AVA encontrado — todos os scores ficam baixos. Faça upload do relatório AVA na aba Atualização.`);
        } else if (d.without_ava > 0 && d.with_ava === 0) {
            _showEngAlert('warning', `${msg} Nenhum aluno teve match com dados AVA (${d.ava_rows_total} registros AVA, ${d.mat_rows_total} matriculados). Verifique se o RGM bate entre os arquivos.`);
        } else if (d.without_ava > d.with_ava) {
            _showEngAlert('info', `${msg} ${d.with_ava.toLocaleString('pt-BR')} com dados AVA, ${d.without_ava.toLocaleString('pt-BR')} sem match.`);
        } else {
            _showEngAlert('success', `${msg} ${d.with_ava.toLocaleString('pt-BR')} com dados AVA.`);
        }
        await _loadEngScores();
        await _loadEngCharts();
    } catch(e) {
        _showEngAlert('error', 'Erro: ' + e.message);
    } finally {
        if (btn) { btn.disabled = false; btn.innerHTML = originalHtml; }
    }
}

function _showEngAlert(type, message) {
    const colors = {
        success: 'bg-emerald-900/50 border-emerald-500/40 text-emerald-300',
        warning: 'bg-amber-900/50 border-amber-500/40 text-amber-300',
        error: 'bg-rose-900/50 border-rose-500/40 text-rose-300',
        info: 'bg-sky-900/50 border-sky-500/40 text-sky-300',
    };
    const icons = { success: '✓', warning: '⚠', error: '✕', info: 'ℹ' };
    const container = document.getElementById('eng-alert-container');
    if (!container) { toast(message, 'info'); return; }
    container.innerHTML = `<div class="rounded-lg border px-4 py-3 text-sm mb-4 flex items-start gap-2 ${colors[type] || colors.info}">
        <span class="font-bold text-base leading-none mt-0.5">${icons[type] || 'ℹ'}</span>
        <span>${message}</span>
        <button onclick="this.parentElement.remove()" class="ml-auto opacity-60 hover:opacity-100 text-lg leading-none">&times;</button>
    </div>`;
}

async function _triggerEvaluation() {
    try {
        const res = await api('/api/comm/evaluate', {method:'POST'});
        const d = await res.json();
        toast(d.message || 'Avaliação iniciada', 'success');
        setTimeout(() => _loadCommLog(), 3000);
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

function _engNivel() {
    const el = document.getElementById('eng-nivel');
    return el ? el.value : '';
}

async function _loadEngScores() {
    try {
        const nivel = _engNivel();
        const qs = nivel ? `per_page=1&nivel=${encodeURIComponent(nivel)}` : 'per_page=1';
        const res = await api(`/api/engagement/scores?${qs}`);
        const d = await res.json();
        const s = d.summary || {};
        const total = Object.values(s).reduce((a,b)=>a+b, 0);
        const pct = (v) => total > 0 ? ` (${(v / total * 100).toFixed(1)}%)` : '';

        document.getElementById('eng-total').textContent = total.toLocaleString('pt-BR');
        document.getElementById('eng-engajados').innerHTML = `${(s.engajado || 0).toLocaleString('pt-BR')}<span class="text-xs text-emerald-400/70 ml-1">${pct(s.engajado || 0)}</span>`;
        document.getElementById('eng-atencao').innerHTML = `${(s.atencao || 0).toLocaleString('pt-BR')}<span class="text-xs text-amber-400/70 ml-1">${pct(s.atencao || 0)}</span>`;
        document.getElementById('eng-risco').innerHTML = `${(s.em_risco || 0).toLocaleString('pt-BR')}<span class="text-xs text-orange-400/70 ml-1">${pct(s.em_risco || 0)}</span>`;
        document.getElementById('eng-criticos').innerHTML = `${(s.critico || 0).toLocaleString('pt-BR')}<span class="text-xs text-rose-400/70 ml-1">${pct(s.critico || 0)}</span>`;

        if (d.has_ava_snapshot === false && total > 0) {
            _showEngAlert('warning', 'Nenhum snapshot de Acesso AVA encontrado. Faça upload do relatório AVA na aba Atualização e depois clique em Recalcular.');
        } else if (total > 0 && (s.engajado || 0) === 0 && (s.atencao || 0) === 0 && (s.em_risco || 0) === 0) {
            _showEngAlert('warning', 'Todos os alunos estão como Crítico. Verifique se o snapshot AVA foi carregado e se o RGM bate com os matriculados. Tente Recalcular.');
        }

        const semAvaEl = document.getElementById('eng-sem-ava-count');
        if (semAvaEl && d.sem_ava_count != null) {
            semAvaEl.textContent = d.sem_ava_count.toLocaleString('pt-BR');
        }
    } catch(e) {
        console.error('Erro engagement scores:', e);
    }
}

function _exportSemAva() {
    const nivel = _engNivel();
    const qs = nivel ? `?nivel=${encodeURIComponent(nivel)}` : '';
    window.open(`/api/engagement/export-sem-ava${qs}`, '_blank');
}

async function _loadEngCharts() {
    try {
        const nivel = _engNivel();
        const qs = nivel ? `per_page=1&nivel=${encodeURIComponent(nivel)}` : 'per_page=1';
        const res = await api(`/api/engagement/scores?${qs}`);
        const d = await res.json();
        const s = d.summary || {};
        const total = Object.values(s).reduce((a,b)=>a+b, 0);

        const chart = eInit('eng-risk-chart');
        if (!chart) return;
        const labels = ['Engajado','Atenção','Em Risco','Crítico'];
        const values = [s.engajado||0, s.atencao||0, s.em_risco||0, s.critico||0];
        const colors = ['#10b981','#f59e0b','#f97316','#ef4444'];
        chart.setOption({
            backgroundColor: 'transparent',
            tooltip: { ...eTooltip('item'), formatter: p => `${p.name}: ${p.value.toLocaleString('pt-BR')} (${p.percent}%)` },
            legend: { bottom: 0, textStyle: { color: eThemeColors().textColor, fontSize: 11 } },
            series: [{
                type: 'pie', radius: ['55%', '80%'],
                label: { show: false },
                emphasis: { label: { show: true, fontSize: 13, fontWeight: 'bold' } },
                data: labels.map((l, i) => ({ name: l, value: values[i], itemStyle: { color: colors[i] } })),
                itemStyle: { borderRadius: 4 },
            }],
            animationDuration: 600,
        }, true);
    } catch(e) { console.error('Risk chart error:', e); }
}

async function _loadCommLog() {
    const channel = (document.getElementById('comm-log-filter') || {}).value || '';
    try {
        const [logRes, queueRes] = await Promise.all([
            api(`/api/comm/log?limit=30&channel=${encodeURIComponent(channel)}`),
            api('/api/comm/queue?limit=100'),
        ]);
        const logData = await logRes.json();
        const queueData = await queueRes.json();

        const queue = queueData.queue || [];
        const sentCount = queue.filter(q => q.status === 'enviado').length;
        const pendingCount = queue.filter(q => q.status === 'pendente').length;
        const failedCount = queue.filter(q => q.status === 'falha').length;

        document.getElementById('comm-total-sent').textContent = sentCount.toLocaleString('pt-BR');
        document.getElementById('comm-total-pending').textContent = pendingCount.toLocaleString('pt-BR');
        document.getElementById('comm-total-failed').textContent = failedCount.toLocaleString('pt-BR');

        const items = logData.log || [];
        const tbody = document.getElementById('comm-log-tbody');
        if (!items.length) {
            tbody.innerHTML = '<tr><td colspan="6" class="py-4 text-center text-gray-500">Nenhuma comunicação registrada</td></tr>';
            return;
        }
        tbody.innerHTML = items.map(i => {
            const chIcon = _channelIcons[i.channel] || _channelIcons.email;
            const stBadge = _statusBadge[i.status] || _statusBadge.pendente;
            return `<tr class="border-b border-[var(--border)] hover:bg-gray-100 dark:hover:bg-gray-800/30 transition">
                <td class="py-2 pr-2 text-xs text-gray-400">${i.sent_at || '—'}</td>
                <td class="py-2 pr-2 font-mono text-xs">${esc(i.rgm || '—')}</td>
                <td class="py-2 pr-2 text-xs">${esc(i.rule_name || '—')}</td>
                <td class="py-2 pr-2">${chIcon}</td>
                <td class="py-2 pr-2"><span class="text-[10px] font-bold px-2 py-0.5 rounded-full ${stBadge}">${i.status}</span></td>
                <td class="py-2 text-xs text-gray-400 max-w-xs truncate">${esc((i.message_preview || '').substring(0, 80))}</td>
            </tr>`;
        }).join('');
    } catch(e) { console.error('Comm log error:', e); }
}


// ===========================================================================
// COMM RULES CRUD (Config > Régua)
// ===========================================================================
let _commRulesCache = [];

async function _loadCommRules() {
    const tbody = document.getElementById('regua-tbody');
    if (!tbody) return;
    try {
        const res = await api('/api/comm/rules');
        const data = await res.json();
        _commRulesCache = data.rules || [];
        if (!_commRulesCache.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="py-4 text-center text-gray-500">Nenhuma regra configurada</td></tr>';
            return;
        }
        const audienceLabels = {todos:'Todos', calouros:'Calouros', veteranos:'Veteranos', risco:'Em Risco'};
        const triggerLabels = {inatividade:'Inatividade', score_baixo:'Score Baixo', primeiro_acesso:'1º Acesso', queda_score:'Queda de Score'};
        tbody.innerHTML = _commRulesCache.map(r => {
            const enCls = r.enabled
                ? 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30'
                : 'bg-gray-500/15 text-gray-400 border-gray-500/30';
            const enLbl = r.enabled ? 'Ativa' : 'Inativa';
            const chIcon = _channelIcons[r.channel] || _channelIcons.email;
            return `<tr class="border-b border-[var(--border)] hover:bg-gray-100 dark:hover:bg-gray-800/30 transition">
                <td class="py-2.5">
                    <button onclick="_toggleRuleEnabled(${r.id}, ${!r.enabled})" title="${r.enabled ? 'Desativar' : 'Ativar'}" class="cursor-pointer">
                        <span class="text-[10px] font-bold px-2 py-0.5 rounded-full border ${enCls}">${enLbl}</span>
                    </button>
                </td>
                <td class="py-2.5 font-medium">${esc(r.name)}</td>
                <td class="py-2.5 text-xs">${audienceLabels[r.audience] || r.audience}</td>
                <td class="py-2.5 text-xs">${triggerLabels[r.trigger_type] || r.trigger_type} (${r.trigger_days}d)</td>
                <td class="py-2.5">${chIcon} <span class="text-xs ml-1">${r.channel}${r.escalation_channel ? ' → '+r.escalation_channel : ''}</span></td>
                <td class="py-2.5 text-center text-xs">${r.cooldown_days}d</td>
                <td class="py-2.5 text-center text-xs">${r.max_per_week}</td>
                <td class="py-2.5">
                    <div class="flex gap-1">
                        <button onclick="_showRuleModal(${r.id})" class="btn-secondary text-xs px-2 py-1 rounded-lg">Editar</button>
                        <button onclick="_deleteCommRule(${r.id})" class="text-xs px-2 py-1 rounded-lg text-rose-400 hover:bg-rose-500/15 transition">Excluir</button>
                    </div>
                </td>
            </tr>`;
        }).join('');
    } catch(e) {
        tbody.innerHTML = `<tr><td colspan="8" class="py-4 text-center text-rose-400">Erro: ${e.message}</td></tr>`;
    }
}

function _showRuleModal(ruleId) {
    const existing = ruleId ? _commRulesCache.find(r => r.id === ruleId) : null;
    const r = existing || {
        name:'', description:'', audience:'todos', trigger_type:'inatividade',
        trigger_days:7, channel:'email', escalation_channel:'', escalation_after_days:'',
        message_template:'', cooldown_days:3, max_per_week:2, priority:0, enabled:true
    };
    const isNew = !existing;
    const old = document.getElementById('rule-modal');
    if (old) old.remove();

    const modal = document.createElement('div');
    modal.id = 'rule-modal';
    modal.className = 'fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm';
    modal.innerHTML = `
        <div class="glass-card p-6 w-full max-w-2xl mx-4 max-h-[90vh] overflow-y-auto" style="background:rgba(15,23,42,0.95)">
            <h3 class="text-lg font-bold text-[var(--text-primary)] font-display mb-5">${isNew ? 'Nova Regra' : 'Editar Regra'}</h3>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div class="md:col-span-2">
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Nome</label>
                    <input id="rule-name" value="${esc(r.name)}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                </div>
                <div class="md:col-span-2">
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Descrição</label>
                    <input id="rule-desc" value="${esc(r.description)}" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Audiência</label>
                    <select id="rule-audience" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                        <option value="todos" ${r.audience==='todos'?'selected':''}>Todos</option>
                        <option value="calouros" ${r.audience==='calouros'?'selected':''}>Calouros</option>
                        <option value="veteranos" ${r.audience==='veteranos'?'selected':''}>Veteranos</option>
                        <option value="risco" ${r.audience==='risco'?'selected':''}>Em Risco</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Tipo de Gatilho</label>
                    <select id="rule-trigger" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                        <option value="inatividade" ${r.trigger_type==='inatividade'?'selected':''}>Inatividade</option>
                        <option value="score_baixo" ${r.trigger_type==='score_baixo'?'selected':''}>Score Baixo</option>
                        <option value="primeiro_acesso" ${r.trigger_type==='primeiro_acesso'?'selected':''}>1º Acesso</option>
                        <option value="queda_score" ${r.trigger_type==='queda_score'?'selected':''}>Queda de Score</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Dias do Gatilho</label>
                    <input id="rule-trigdays" type="number" value="${r.trigger_days}" min="1" max="365" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Canal Principal</label>
                    <select id="rule-channel" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                        <option value="email" ${r.channel==='email'?'selected':''}>E-mail</option>
                        <option value="whatsapp" ${r.channel==='whatsapp'?'selected':''}>WhatsApp</option>
                        <option value="ambos" ${r.channel==='ambos'?'selected':''}>Ambos</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Canal de Escalação</label>
                    <select id="rule-esc-channel" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                        <option value="" ${!r.escalation_channel?'selected':''}>Nenhum</option>
                        <option value="whatsapp" ${r.escalation_channel==='whatsapp'?'selected':''}>WhatsApp</option>
                        <option value="email" ${r.escalation_channel==='email'?'selected':''}>E-mail</option>
                        <option value="ambos" ${r.escalation_channel==='ambos'?'selected':''}>Ambos</option>
                    </select>
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Escalar Após (dias)</label>
                    <input id="rule-esc-days" type="number" value="${r.escalation_after_days || ''}" min="1" max="30" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg" placeholder="—">
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Cooldown (dias)</label>
                    <input id="rule-cooldown" type="number" value="${r.cooldown_days}" min="1" max="30" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Máx por Semana</label>
                    <input id="rule-maxweek" type="number" value="${r.max_per_week}" min="1" max="10" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                </div>
                <div>
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Prioridade</label>
                    <input id="rule-priority" type="number" value="${r.priority}" min="0" max="100" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg">
                </div>
                <div class="md:col-span-2">
                    <label class="block text-xs text-gray-500 mb-1 font-medium">Template da Mensagem</label>
                    <textarea id="rule-template" rows="4" class="input-glass px-3 py-2 text-sm text-gray-200 w-full rounded-lg" placeholder="Olá {{primeiro_nome}}, notamos que...">${esc(r.message_template)}</textarea>
                    <p class="text-[10px] text-gray-600 mt-1">Variáveis: {{nome}}, {{primeiro_nome}}, {{curso}}, {{polo}}, {{email}}, {{rgm}}, {{dias_sem_acesso}}, {{score}}</p>
                </div>
                <div class="flex items-center gap-2">
                    <input type="checkbox" id="rule-enabled" ${r.enabled ? 'checked' : ''} class="rounded border-gray-600">
                    <label for="rule-enabled" class="text-sm text-gray-300">Regra ativa</label>
                </div>
            </div>
            <div class="flex justify-end gap-3 mt-6">
                <button onclick="document.getElementById('rule-modal').remove()" class="btn-secondary px-4 py-2 rounded-xl text-sm">Cancelar</button>
                <button onclick="_saveCommRule(${ruleId || 'null'})" class="btn-primary px-4 py-2 rounded-xl text-sm">Salvar</button>
            </div>
        </div>`;
    document.body.appendChild(modal);
}

async function _saveCommRule(ruleId) {
    const body = {
        name: document.getElementById('rule-name').value.trim(),
        description: document.getElementById('rule-desc').value.trim(),
        audience: document.getElementById('rule-audience').value,
        trigger_type: document.getElementById('rule-trigger').value,
        trigger_days: parseInt(document.getElementById('rule-trigdays').value) || 7,
        channel: document.getElementById('rule-channel').value,
        escalation_channel: document.getElementById('rule-esc-channel').value || null,
        escalation_after_days: parseInt(document.getElementById('rule-esc-days').value) || null,
        message_template: document.getElementById('rule-template').value.trim(),
        cooldown_days: parseInt(document.getElementById('rule-cooldown').value) || 3,
        max_per_week: parseInt(document.getElementById('rule-maxweek').value) || 2,
        priority: parseInt(document.getElementById('rule-priority').value) || 0,
        enabled: document.getElementById('rule-enabled').checked,
    };
    if (!body.name) { toast('Nome é obrigatório', 'warning'); return; }
    if (!body.message_template) { toast('Template é obrigatório', 'warning'); return; }
    try {
        const method = ruleId ? 'PUT' : 'POST';
        const url = ruleId ? `/api/comm/rules/${ruleId}` : '/api/comm/rules';
        const res = await api(url, {method, headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)});
        const d = await res.json();
        if (d.error) { toast('Erro: ' + d.error, 'error'); return; }
        document.getElementById('rule-modal').remove();
        _loadCommRules();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

async function _deleteCommRule(ruleId) {
    if (!confirm('Excluir esta regra permanentemente?')) return;
    try {
        const res = await api(`/api/comm/rules/${ruleId}`, {method:'DELETE'});
        const d = await res.json();
        if (d.error) { toast('Erro: ' + d.error, 'error'); return; }
        _loadCommRules();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}

async function _toggleRuleEnabled(ruleId, newState) {
    try {
        const res = await api(`/api/comm/rules/${ruleId}`, {
            method:'PUT',
            headers:{'Content-Type':'application/json'},
            body: JSON.stringify({enabled: newState})
        });
        const d = await res.json();
        if (d.error) { toast('Erro: ' + d.error, 'error'); return; }
        _loadCommRules();
    } catch(e) { toast('Erro: ' + e.message, 'error'); }
}
