// ---------------------------------------------------------------------------
// Minha Performance — JS
// ---------------------------------------------------------------------------
let _mpRole = '';
let _mpKommoUid = null;
let _mpSelectedUid = null;
let _mpChartDaily = null;
let _mpMatriculas = [];
let _mpAdminOpen = false;

const _mpFmt = v => (v || 0).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
const _mpDias = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];

async function loadMinhaPerformance() {
    const loading = document.getElementById('mp-loading');
    const noLink = document.getElementById('mp-no-link');
    const content = document.getElementById('mp-content');
    const noCamp = document.getElementById('mp-no-campanha');
    loading.classList.remove('hidden');
    noLink.classList.add('hidden');
    content.classList.add('hidden');
    noCamp.classList.add('hidden');

    try {
        const me = await (await api('/api/me')).json();
        _mpRole = me.role || '';
        _mpKommoUid = me.kommo_user_id;

        const adminCtrl = document.getElementById('mp-admin-controls');
        if (_mpRole === 'admin') {
            adminCtrl.classList.remove('hidden');
            adminCtrl.classList.add('flex');
            _mpLoadAgentSelect();
        } else {
            adminCtrl.classList.add('hidden');
        }

        const uid = _mpSelectedUid || _mpKommoUid;
        if (!uid && _mpRole !== 'admin') {
            loading.classList.add('hidden');
            noLink.classList.remove('hidden');
            return;
        }

        await Promise.all([
            _mpLoadData(uid),
            _mpLoadPremiacao(uid),
            _mpLoadHistorico(uid),
        ]);
        loading.classList.add('hidden');
    } catch (e) {
        loading.classList.add('hidden');
        console.error('loadMinhaPerformance error', e);
    }
}

async function _mpLoadAgentSelect() {
    try {
        const res = await api('/api/minha-performance/agentes');
        const d = await res.json();
        if (!d.ok) return;
        const sel = document.getElementById('mp-agent-select');
        const current = sel.value;
        sel.innerHTML = '<option value="">Selecionar agente...</option>' +
            d.agentes.map(a => `<option value="${a.kommo_uid}" ${String(a.kommo_uid) === current ? 'selected' : ''}>${a.name}</option>`).join('');
    } catch (e) { console.error(e); }
}

function mpAgentChanged() {
    const val = document.getElementById('mp-agent-select').value;
    _mpSelectedUid = val ? parseInt(val) : null;
    loadMinhaPerformance();
}

function mpToggleAdmin() {
    _mpAdminOpen = !_mpAdminOpen;
    document.getElementById('mp-admin-panel').classList.toggle('hidden', !_mpAdminOpen);
    if (_mpAdminOpen) {
        _mpLoadCampanhas();
    }
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------
async function _mpLoadData(kommoUid) {
    const qs = kommoUid ? `?kommo_uid=${kommoUid}` : '';
    try {
        const res = await api('/api/minha-performance' + qs);
        const d = await res.json();
        if (!d.ok && d.error) {
            document.getElementById('mp-no-link').classList.remove('hidden');
            return;
        }
        if (!d.campanha) {
            document.getElementById('mp-no-campanha').classList.remove('hidden');
            return;
        }

        document.getElementById('mp-content').classList.remove('hidden');
        document.getElementById('mp-agent-name').textContent = d.agent_name || 'Agente';
        document.getElementById('mp-campanha-nome').textContent = d.campanha.nome;
        document.getElementById('mp-campanha-datas').textContent = `${_fmtDateBR(d.campanha.dt_inicio)} — ${_fmtDateBR(d.campanha.dt_fim)}`;

        _mpMatriculas = d.matriculas || [];

        // Progress ring
        const pct = Math.min(d.pct || 0, 100);
        const circumference = 2 * Math.PI * 52;
        const offset = circumference - (pct / 100) * circumference;
        const ring = document.getElementById('mp-ring');
        requestAnimationFrame(() => { ring.style.strokeDashoffset = offset; });
        document.getElementById('mp-pct').textContent = `${Math.round(d.pct || 0)}%`;
        document.getElementById('mp-total').textContent = (d.total || 0).toLocaleString('pt-BR');

        // Tier badge
        const badge = document.getElementById('mp-tier-badge');
        if (d.tier) {
            const tierMap = {
                supermeta: { label: 'SUPERMETA', cls: 'bg-emerald-500/20 text-emerald-400 border border-emerald-500/30' },
                meta: { label: 'META', cls: 'bg-blue-500/20 text-blue-400 border border-blue-500/30' },
                intermediaria: { label: 'INTERMEDIÁRIA', cls: 'bg-amber-500/20 text-amber-400 border border-amber-500/30' },
            };
            const t = tierMap[d.tier] || tierMap.meta;
            badge.textContent = t.label;
            badge.className = `mt-2 px-3 py-1 rounded-full text-xs font-bold ${t.cls}`;
            badge.classList.remove('hidden');
        } else {
            badge.classList.add('hidden');
        }

        // Gradient based on tier
        const grad = document.querySelector('#mp-gradient stop:first-child');
        const grad2 = document.querySelector('#mp-gradient stop:last-child');
        if (d.tier === 'supermeta') { grad.setAttribute('stop-color', '#10b981'); grad2.setAttribute('stop-color', '#059669'); }
        else if (d.tier === 'meta') { grad.setAttribute('stop-color', '#3b82f6'); grad2.setAttribute('stop-color', '#2563eb'); }
        else if (d.tier === 'intermediaria') { grad.setAttribute('stop-color', '#f59e0b'); grad2.setAttribute('stop-color', '#d97706'); }
        else { grad.setAttribute('stop-color', '#f59e0b'); grad2.setAttribute('stop-color', '#ef4444'); }

        // Falta para...
        const metas = d.metas || {};
        document.getElementById('mp-falta-inter').textContent = d.falta_inter != null ? d.falta_inter : '--';
        document.getElementById('mp-falta-meta').textContent = d.falta_meta != null ? d.falta_meta : '--';
        document.getElementById('mp-falta-super').textContent = d.falta_super != null ? d.falta_super : '--';
        document.getElementById('mp-meta-inter').textContent = metas.intermediaria || '--';
        document.getElementById('mp-meta-val').textContent = metas.meta || '--';
        document.getElementById('mp-meta-super').textContent = metas.supermeta || '--';

        // Detail table
        _mpRenderDetailTable(d.matriculas || []);
    } catch (e) {
        console.error('_mpLoadData', e);
    }
}

async function _mpLoadPremiacao(kommoUid) {
    const qs = kommoUid ? `?kommo_uid=${kommoUid}` : '';
    try {
        const res = await api('/api/minha-performance/premiacao' + qs);
        const d = await res.json();
        if (!d.ok) return;

        document.getElementById('mp-prem-tier').textContent = _mpFmt(d.tier_bonus);
        const tierLabel = d.tier ? d.tier.toUpperCase() : 'Nenhum';
        const vpmat = d.tier_valor_por_mat || 0;
        document.getElementById('mp-prem-tier-detail').textContent =
            d.tier ? `${tierLabel} — ${_mpFmt(vpmat)}/mat x ${d.total_matriculas}` : 'Nenhum tier atingido';

        document.getElementById('mp-prem-daily').textContent = _mpFmt(d.daily_bonus);
        document.getElementById('mp-prem-daily-detail').textContent =
            `${d.daily_dias_batidos || 0}/${d.daily_dias_total || 0} dias batidos`;

        document.getElementById('mp-prem-receb').textContent = _mpFmt(d.receb_bonus);
        document.getElementById('mp-prem-receb-detail').textContent =
            d.receb_total_valor > 0 ? `Sobre ${_mpFmt(d.receb_total_valor)} recebidos` : 'Sem dados de recebimento';

        document.getElementById('mp-prem-total').textContent = _mpFmt(d.total);

        // Daily breakdown table
        _mpRenderDailyBreakdown(d.daily_breakdown || []);

        // Chart
        _mpRenderDailyChart(d.daily_breakdown || []);
    } catch (e) {
        console.error('_mpLoadPremiacao', e);
    }
}

async function _mpLoadHistorico(kommoUid) {
    const qs = kommoUid ? `?kommo_uid=${kommoUid}` : '';
    try {
        const res = await api('/api/minha-performance/historico' + qs);
        const d = await res.json();
        if (!d.ok) return;
        const wrap = document.getElementById('mp-historico');
        const hist = d.historico || [];
        if (!hist.length) {
            wrap.innerHTML = '<p class="text-slate-600 text-xs">Nenhum histórico disponível</p>';
            return;
        }
        wrap.innerHTML = hist.map(h => {
            const tierMap = {
                supermeta: { label: 'SUPERMETA', cls: 'text-emerald-400 bg-emerald-500/10 border-emerald-500/20' },
                meta: { label: 'META', cls: 'text-blue-400 bg-blue-500/10 border-blue-500/20' },
                intermediaria: { label: 'INTERM.', cls: 'text-amber-400 bg-amber-500/10 border-amber-500/20' },
            };
            const t = tierMap[h.tier] || { label: 'Abaixo', cls: 'text-red-400 bg-red-500/10 border-red-500/20' };
            const meta = h.metas?.meta || 0;
            const pctStr = meta > 0 ? `${Math.round(h.total_matriculas / meta * 100)}%` : '--';
            const isActive = h.ativa;
            return `<div class="rounded-xl p-4 border ${t.cls} min-w-[200px] ${isActive ? 'ring-1 ring-amber-500/30' : ''}">
                <div class="flex items-center justify-between mb-2">
                    <span class="text-xs font-bold text-white">${h.nome}</span>
                    ${isActive ? '<span class="text-[9px] bg-amber-500/20 text-amber-400 px-1.5 py-0.5 rounded-full font-bold">ATIVA</span>' : ''}
                </div>
                <p class="text-[10px] text-slate-500">${_fmtDateBR(h.dt_inicio)} — ${_fmtDateBR(h.dt_fim)}</p>
                <div class="flex items-baseline gap-2 mt-2">
                    <span class="text-xl font-black text-white font-display">${h.total_matriculas}</span>
                    <span class="text-xs text-slate-500">/ ${meta}</span>
                    <span class="text-xs font-bold ${t.cls.split(' ')[0]}">${pctStr}</span>
                </div>
                <p class="text-[10px] font-bold mt-1 ${t.cls.split(' ')[0]}">${t.label}</p>
                <div class="mt-2 pt-2 border-t border-slate-700/20 space-y-0.5">
                    <p class="text-[10px] text-slate-400">Tier: <span class="font-bold text-amber-300">${_mpFmt(h.tier_bonus)}</span></p>
                    <p class="text-[10px] text-slate-400">Diária: <span class="font-bold text-cyan-300">${_mpFmt(h.daily_bonus)}</span> <span class="text-slate-600">(${h.dias_batidos}/${h.dias_total}d)</span></p>
                    <p class="text-[10px] font-bold text-emerald-400 mt-1">Total: ${_mpFmt(h.total_premiacao)}</p>
                </div>
            </div>`;
        }).join('');
    } catch (e) {
        console.error('_mpLoadHistorico', e);
    }
}

// ---------------------------------------------------------------------------
// Render helpers
// ---------------------------------------------------------------------------
function _fmtDateBR(s) {
    if (!s) return '';
    const parts = s.split('-');
    if (parts.length === 3) return `${parts[2]}/${parts[1]}/${parts[0]}`;
    return s;
}

function _mpRenderDetailTable(matriculas) {
    const tbody = document.getElementById('mp-detail-tbody');
    if (!matriculas.length) {
        tbody.innerHTML = '<tr><td colspan="4" class="px-5 py-6 text-center text-slate-600">Nenhuma matrícula no período</td></tr>';
        return;
    }
    tbody.innerHTML = matriculas.map(m => {
        const rgm = m.rgm || '--';
        const nivel = m.nivel || '--';
        const mod = m.modalidade || '--';
        const dt = m.data_matricula ? _fmtDateBR(m.data_matricula) : '--';
        return `<tr class="mp-detail-row" data-rgm="${rgm}">
            <td class="px-4 py-2 text-slate-300 font-mono text-xs">${rgm}</td>
            <td class="px-4 py-2 text-slate-400 text-xs">${nivel}</td>
            <td class="px-4 py-2 text-slate-400 text-xs">${mod}</td>
            <td class="px-4 py-2 text-slate-400 text-xs">${dt}</td>
        </tr>`;
    }).join('');
}

function mpFilterTable() {
    const q = (document.getElementById('mp-search-rgm').value || '').trim().toLowerCase();
    document.querySelectorAll('.mp-detail-row').forEach(row => {
        row.style.display = !q || (row.dataset.rgm || '').toLowerCase().includes(q) ? '' : 'none';
    });
}

function _mpRenderDailyBreakdown(breakdown) {
    const tbody = document.getElementById('mp-daily-tbody');
    if (!breakdown.length) {
        tbody.innerHTML = '<tr><td colspan="7" class="px-5 py-6 text-center text-slate-600">Sem dados de premiação diária</td></tr>';
        return;
    }
    let totalFixo = 0, totalExtra = 0, totalGeral = 0;
    const rows = breakdown.map(b => {
        totalFixo += b.bonus_fixo;
        totalExtra += b.bonus_extra;
        totalGeral += b.total;
        const bateu = b.realizadas >= b.meta;
        const cls = bateu ? 'text-emerald-400' : (b.realizadas > 0 ? 'text-amber-400' : 'text-slate-600');
        return `<tr>
            <td class="px-3 py-1.5 text-slate-300 text-xs">${_fmtDateBR(b.data)}</td>
            <td class="px-3 py-1.5 text-center text-slate-400 text-xs">${b.dia_nome}</td>
            <td class="px-3 py-1.5 text-center text-slate-500 text-xs">${b.meta}</td>
            <td class="px-3 py-1.5 text-center font-bold text-xs ${cls}">${b.realizadas}</td>
            <td class="px-3 py-1.5 text-right text-xs ${b.bonus_fixo > 0 ? 'text-emerald-400' : 'text-slate-600'}">${b.bonus_fixo > 0 ? _mpFmt(b.bonus_fixo) : '--'}</td>
            <td class="px-3 py-1.5 text-right text-xs ${b.bonus_extra > 0 ? 'text-cyan-400' : 'text-slate-600'}">${b.bonus_extra > 0 ? _mpFmt(b.bonus_extra) : '--'}</td>
            <td class="px-3 py-1.5 text-right font-bold text-xs ${b.total > 0 ? 'text-white' : 'text-slate-600'}">${b.total > 0 ? _mpFmt(b.total) : '--'}</td>
        </tr>`;
    }).join('');
    tbody.innerHTML = rows + `<tr class="bg-slate-800/30 border-t border-slate-600/30">
        <td colspan="4" class="px-3 py-2 text-xs font-bold text-slate-300 text-right">Total</td>
        <td class="px-3 py-2 text-right text-xs font-bold text-emerald-400">${_mpFmt(totalFixo)}</td>
        <td class="px-3 py-2 text-right text-xs font-bold text-cyan-400">${_mpFmt(totalExtra)}</td>
        <td class="px-3 py-2 text-right text-xs font-bold text-white">${_mpFmt(totalGeral)}</td>
    </tr>`;
}

function _mpRenderDailyChart(breakdown) {
    const canvas = document.getElementById('mp-chart-daily');
    if (!canvas) return;
    if (_mpChartDaily) { _mpChartDaily.destroy(); _mpChartDaily = null; }
    if (!breakdown.length) return;

    const labels = breakdown.map(b => _fmtDateBR(b.data));
    const realizadas = breakdown.map(b => b.realizadas);
    const metas = breakdown.map(b => b.meta);

    _mpChartDaily = new Chart(canvas, {
        type: 'bar',
        data: {
            labels,
            datasets: [
                {
                    label: 'Matrículas',
                    data: realizadas,
                    backgroundColor: breakdown.map(b => b.realizadas >= b.meta ? 'rgba(16,185,129,0.6)' : 'rgba(245,158,11,0.4)'),
                    borderRadius: 4,
                    barPercentage: 0.7,
                },
                {
                    label: 'Meta diária',
                    data: metas,
                    type: 'line',
                    borderColor: 'rgba(99,102,241,0.6)',
                    borderDash: [4, 4],
                    borderWidth: 2,
                    pointRadius: 0,
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, labels: { color: '#94a3b8', font: { size: 10 } } },
            },
            scales: {
                x: { ticks: { color: '#64748b', font: { size: 9 }, maxRotation: 45 }, grid: { display: false } },
                y: { beginAtZero: true, ticks: { color: '#64748b', font: { size: 10 }, stepSize: 1 }, grid: { color: 'rgba(100,116,139,0.1)' } },
            },
        },
    });
}

// ---------------------------------------------------------------------------
// Admin: Campanhas
// ---------------------------------------------------------------------------
async function _mpLoadCampanhas() {
    try {
        const res = await api('/api/premiacao/campanhas');
        const d = await res.json();
        if (!d.ok) return;
        const wrap = document.getElementById('mp-campanhas-list');
        const sel = document.getElementById('mp-daily-camp');
        sel.innerHTML = '<option value="">Selecionar...</option>';

        if (!d.campanhas.length) {
            wrap.innerHTML = '<p class="text-xs text-slate-600">Nenhuma campanha criada</p>';
            return;
        }
        wrap.innerHTML = d.campanhas.map(c => {
            const tiers = c.tiers || {};
            const ativaLabel = c.ativa
                ? '<span class="text-[9px] bg-emerald-500/20 text-emerald-400 px-1.5 py-0.5 rounded-full font-bold">Ativa</span>'
                : '<span class="text-[9px] bg-slate-700 text-slate-400 px-1.5 py-0.5 rounded-full">Inativa</span>';
            return `<div class="bg-slate-800/30 rounded-lg p-3 border border-slate-700/30 flex items-center justify-between">
                <div>
                    <div class="flex items-center gap-2">
                        <span class="text-sm font-bold text-white">${c.nome}</span>
                        ${ativaLabel}
                    </div>
                    <p class="text-[10px] text-slate-500 mt-0.5">${_fmtDateBR(c.dt_inicio)} — ${_fmtDateBR(c.dt_fim)}</p>
                    <p class="text-[10px] text-slate-400 mt-1">
                        Inter: ${_mpFmt(tiers.intermediaria || 0)}/mat
                        | Meta: ${_mpFmt(tiers.meta || 0)}/mat
                        | Super: ${_mpFmt(tiers.supermeta || 0)}/mat
                    </p>
                </div>
                <div class="flex gap-2">
                    <button onclick="mpToggleCampanha(${c.id}, ${c.ativa})" class="text-[10px] text-slate-400 hover:text-white border border-slate-700 px-2 py-1 rounded transition-all">
                        ${c.ativa ? 'Desativar' : 'Ativar'}
                    </button>
                    <button onclick="mpDeleteCampanha(${c.id})" class="text-[10px] text-red-400 hover:text-red-300 border border-red-900/30 px-2 py-1 rounded transition-all">Excluir</button>
                </div>
            </div>`;
        }).join('');

        d.campanhas.forEach(c => {
            sel.innerHTML += `<option value="${c.id}">${c.nome} (${_fmtDateBR(c.dt_inicio)} — ${_fmtDateBR(c.dt_fim)})</option>`;
        });
    } catch (e) { console.error(e); }
}

async function mpSaveCampanha() {
    const nome = document.getElementById('mp-camp-nome').value.trim();
    const dt_inicio = document.getElementById('mp-camp-ini').value;
    const dt_fim = document.getElementById('mp-camp-fim').value;
    if (!nome || !dt_inicio || !dt_fim) { toast('Preencha nome e datas', 'warning'); return; }
    const tiers = {};
    const inter = parseFloat(document.getElementById('mp-camp-tier-inter').value) || 0;
    const meta = parseFloat(document.getElementById('mp-camp-tier-meta').value) || 0;
    const sup = parseFloat(document.getElementById('mp-camp-tier-super').value) || 0;
    if (inter > 0) tiers.intermediaria = inter;
    if (meta > 0) tiers.meta = meta;
    if (sup > 0) tiers.supermeta = sup;

    const receb_regras = [];
    const recebValor = parseFloat(document.getElementById('mp-camp-receb-valor').value) || 0;
    if (recebValor > 0) {
        receb_regras.push({
            tier: 'qualquer',
            modo: document.getElementById('mp-camp-receb-modo').value,
            valor: recebValor,
        });
    }

    try {
        const res = await api('/api/premiacao/campanhas', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ nome, dt_inicio, dt_fim, tiers, receb_regras }),
        });
        const d = await res.json();
        if (d.error) { toast(d.error, 'error'); return; }
        toast('Campanha criada', 'success');
        document.getElementById('mp-camp-nome').value = '';
        _mpLoadCampanhas();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function mpToggleCampanha(cid, currentActive) {
    try {
        await api(`/api/premiacao/campanhas/${cid}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ativa: !currentActive }),
        });
        _mpLoadCampanhas();
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

async function mpDeleteCampanha(cid) {
    if (!confirm('Excluir esta campanha? Todas as metas diárias e regras serão removidas.')) return;
    try {
        await api(`/api/premiacao/campanhas/${cid}`, { method: 'DELETE' });
        _mpLoadCampanhas();
        toast('Campanha excluída', 'success');
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Admin: Metas diárias grid
// ---------------------------------------------------------------------------
let _mpDailyAgents = [];

async function mpLoadDailyGrid() {
    const cid = document.getElementById('mp-daily-camp').value;
    const wrap = document.getElementById('mp-daily-grid-wrap');
    if (!cid) { wrap.innerHTML = '<p class="text-xs text-slate-600">Selecione uma campanha acima</p>'; return; }

    try {
        const [agRes, diRes] = await Promise.all([
            api('/api/minha-performance/agentes'),
            api(`/api/premiacao/campanhas/${cid}/diarias`),
        ]);
        const agents = (await agRes.json()).agentes || [];
        const diarias = (await diRes.json()).diarias || [];
        _mpDailyAgents = agents;

        const lookup = {};
        diarias.forEach(d => {
            const key = `${d.kommo_user_id}_${d.dia_semana}`;
            lookup[key] = d;
        });

        let html = `<table class="w-full text-[11px]">
            <thead>
                <tr class="text-[9px] uppercase tracking-wider text-slate-500 border-b border-slate-700/20">
                    <th class="text-left px-2 py-2 sticky left-0 bg-slate-900/95 z-10">Agente</th>`;
        _mpDias.forEach((d, i) => {
            html += `<th class="text-center px-1 py-2" colspan="3">${d}</th>`;
        });
        html += `</tr><tr class="text-[8px] text-slate-600 border-b border-slate-700/10">
            <th class="sticky left-0 bg-slate-900/95 z-10"></th>`;
        _mpDias.forEach(() => {
            html += '<th class="px-1">Meta</th><th class="px-1">Fixo</th><th class="px-1">Extra</th>';
        });
        html += '</tr></thead><tbody>';

        agents.forEach(a => {
            html += `<tr class="border-b border-slate-700/10"><td class="px-2 py-1.5 text-slate-300 font-medium sticky left-0 bg-slate-900/95 z-10 whitespace-nowrap">${a.name}</td>`;
            for (let dow = 0; dow < 7; dow++) {
                const key = `${a.kommo_uid}_${dow}`;
                const val = lookup[key] || {};
                html += `<td class="px-0.5"><input type="number" min="0" data-uid="${a.kommo_uid}" data-dow="${dow}" data-field="meta" value="${val.meta_diaria || 0}" class="mp-daily-input w-10 input-glass px-1 py-0.5 text-center text-[10px] text-slate-300"></td>`;
                html += `<td class="px-0.5"><input type="number" min="0" step="0.01" data-uid="${a.kommo_uid}" data-dow="${dow}" data-field="fixo" value="${val.bonus_fixo || 0}" class="mp-daily-input w-12 input-glass px-1 py-0.5 text-center text-[10px] text-slate-300"></td>`;
                html += `<td class="px-0.5"><input type="number" min="0" step="0.01" data-uid="${a.kommo_uid}" data-dow="${dow}" data-field="extra" value="${val.bonus_extra || 0}" class="mp-daily-input w-12 input-glass px-1 py-0.5 text-center text-[10px] text-slate-300"></td>`;
            }
            html += '</tr>';
        });
        html += '</tbody></table>';
        wrap.innerHTML = html;
    } catch (e) {
        wrap.innerHTML = `<p class="text-xs text-red-400">${e.message}</p>`;
    }
}

async function mpSaveDailyTargets() {
    const cid = document.getElementById('mp-daily-camp').value;
    if (!cid) { toast('Selecione uma campanha', 'warning'); return; }

    const inputs = document.querySelectorAll('.mp-daily-input');
    const dataMap = {};
    inputs.forEach(inp => {
        const uid = inp.dataset.uid;
        const dow = inp.dataset.dow;
        const field = inp.dataset.field;
        const key = `${uid}_${dow}`;
        if (!dataMap[key]) dataMap[key] = { kommo_user_id: parseInt(uid), dia_semana: parseInt(dow) };
        if (field === 'meta') dataMap[key].meta_diaria = parseInt(inp.value) || 0;
        if (field === 'fixo') dataMap[key].bonus_fixo = parseFloat(inp.value) || 0;
        if (field === 'extra') dataMap[key].bonus_extra = parseFloat(inp.value) || 0;
    });

    const items = Object.values(dataMap).filter(d => d.meta_diaria > 0 || d.bonus_fixo > 0 || d.bonus_extra > 0);
    try {
        const res = await api(`/api/premiacao/campanhas/${cid}/diarias`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ items }),
        });
        const d = await res.json();
        if (d.ok) toast('Metas diárias salvas', 'success');
        else toast(d.error || 'Erro', 'error');
    } catch (e) { toast('Erro: ' + e.message, 'error'); }
}

// ---------------------------------------------------------------------------
// Admin: Upload recebimentos
// ---------------------------------------------------------------------------
async function mpUploadRecebimentos(input) {
    const file = input.files[0];
    if (!file) return;
    const mesRef = document.getElementById('mp-receb-mes').value.trim();
    const fd = new FormData();
    fd.append('file', file);
    fd.append('mes_ref', mesRef);
    const msg = document.getElementById('mp-receb-msg');
    msg.className = 'mt-3 text-xs p-3 rounded-lg bg-blue-500/10 text-blue-400 border border-blue-500/20';
    msg.textContent = 'Enviando...';
    msg.classList.remove('hidden');
    try {
        const res = await api('/api/recebimentos/upload', { method: 'POST', body: fd });
        const d = await res.json();
        if (d.ok) {
            msg.className = 'mt-3 text-xs p-3 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/20';
            msg.textContent = `Upload concluído: ${d.rows} linhas importadas.`;
        } else {
            msg.className = 'mt-3 text-xs p-3 rounded-lg bg-red-500/10 text-red-400 border border-red-500/20';
            msg.textContent = d.error || 'Erro no upload';
        }
    } catch (e) {
        msg.className = 'mt-3 text-xs p-3 rounded-lg bg-red-500/10 text-red-400 border border-red-500/20';
        msg.textContent = 'Erro: ' + e.message;
    }
    input.value = '';
}
