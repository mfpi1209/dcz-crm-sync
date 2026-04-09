// ===========================================================================
// FEEDBACK PAGE
// ===========================================================================
const FB_WEBHOOK = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/feedback';
let _fbData = null, _fbChartDetail = null, _fbChartGlobal = null, _fbInited = false;

function fbInit() {
    if (_fbInited) return;
    _fbInited = true;
    const hoje = new Date(), d16 = new Date();
    d16.setDate(d16.getDate() - 16);
    const pad = d => d.toISOString().slice(0, 10);
    document.getElementById('fbStartDate').value = pad(d16);
    document.getElementById('fbEndDate').value = pad(hoje);
    fbFetch();
}

function _fbFmtNum(n) { return n == null ? '--' : new Intl.NumberFormat('pt-BR').format(n); }
function _fbFmtDec(n, d = 1) { return n == null ? '--' : Number(n).toFixed(d); }
function _fbFmtTime(m) {
    if (m == null) return '--:--';
    const mins = Math.floor(m), secs = Math.round((m - mins) * 60);
    return mins + ':' + String(secs).padStart(2, '0');
}
function _fbBadgeCls(n) { return n == null ? 'medium' : n >= 7 ? 'high' : n >= 5 ? 'medium' : 'low'; }
function _fbInitials(name) { return name ? name.split(' ').map(w => w[0]).slice(0, 2).join('').toUpperCase() : '??'; }

function _fbRemoveAccents(s) { return s.normalize('NFD').replace(/[\u0300-\u036f]/g, ''); }
function _fbNormName(n) { return n ? _fbRemoveAccents(n.trim().toLowerCase()) : ''; }
function _fbTitleCase(s) { return s ? s.trim().split(' ').map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ') : ''; }
const _FB_CANON = {
    'danubia':'Danubia','danúbia':'Danubia',
    'debora mani moreira':'Débora Mani Moreira','débora mani moreira':'Débora Mani Moreira',
    'emanuel felipe':'Emanuel Felipe','emnauel felipe':'Emanuel Felipe',
    'maitê carine da silva':'Maitê Carine da Silva','maite carine da silva':'Maitê Carine da Silva',
};
function _fbCanon(n) { if (!n) return n; const k = _fbNormName(n); return _FB_CANON[k] || _fbTitleCase(n); }
function _fbNormC(c) {
    if (!c) return null;
    const m = c.metricas || {};
    return {
        consultor: c.consultor,
        total_atendimentos: m.total_atendimentos ?? c.total_atendimentos ?? 0,
        notas_informadas: m.notas_informadas ?? c.notas_informadas ?? 0,
        nota_media: m.nota_media ?? c.nota_media ?? null,
        tempo_medio_resposta_min: m.tempo_medio_resposta_min ?? c.tempo_medio_resposta_min ?? null,
        tempo_medio_atendimento_min: m.tempo_medio_atendimento_min ?? c.tempo_medio_atendimento_min ?? null
    };
}
function _fbUnify(arr) {
    if (!arr || !Array.isArray(arr)) return [];
    const u = {};
    arr.forEach(raw => {
        const c = _fbNormC(raw); if (!c || !c.consultor) return;
        const cn = _fbCanon(c.consultor), k = _fbNormName(cn);
        if (!u[k]) {
            u[k] = { consultor: cn, ta: c.total_atendimentos||0, ni: c.notas_informadas||0,
                sn: (c.nota_media||0)*(c.notas_informadas||0),
                str: (c.tempo_medio_resposta_min||0)*(c.total_atendimentos||0),
                atr: c.tempo_medio_resposta_min!=null?(c.total_atendimentos||0):0,
                sta: (c.tempo_medio_atendimento_min||0)*(c.total_atendimentos||0),
                ata: c.tempo_medio_atendimento_min!=null?(c.total_atendimentos||0):0,
                orig: [c.consultor] };
        } else {
            const e = u[k]; e.ta += c.total_atendimentos||0; e.ni += c.notas_informadas||0;
            e.sn += (c.nota_media||0)*(c.notas_informadas||0);
            if (c.tempo_medio_resposta_min!=null) { e.str += (c.tempo_medio_resposta_min||0)*(c.total_atendimentos||0); e.atr += c.total_atendimentos||0; }
            if (c.tempo_medio_atendimento_min!=null) { e.sta += (c.tempo_medio_atendimento_min||0)*(c.total_atendimentos||0); e.ata += c.total_atendimentos||0; }
            if (!e.orig.includes(c.consultor)) e.orig.push(c.consultor);
        }
    });
    return Object.values(u).map(e => ({
        consultor: e.consultor, total_atendimentos: e.ta, notas_informadas: e.ni,
        nota_media: e.ni > 0 ? e.sn / e.ni : null,
        tempo_medio_resposta_min: e.atr > 0 ? e.str / e.atr : null,
        tempo_medio_atendimento_min: e.ata > 0 ? e.sta / e.ata : null,
        nomes_originais: e.orig
    }));
}
function _fbValidC(arr) {
    const unified = _fbUnify(arr).filter(c => c.consultor && c.consultor !== 'NaN' &&
        !['não informado','nan'].includes(c.consultor.toLowerCase()) &&
        !c.consultor.toLowerCase().includes('consta apenas') &&
        !c.consultor.toLowerCase().includes('atendentes automáticos')
    ).sort((a, b) => b.total_atendimentos - a.total_atendimentos);
    return _fbCalcProdutividade(unified);
}

function _fbCalcProdutividade(arr) {
    if (!arr || arr.length === 0) return arr;
    const maxTA = Math.max(...arr.map(c => c.total_atendimentos || 0)) || 1;
    const tempos = arr.filter(c => c.tempo_medio_resposta_min != null).map(c => c.tempo_medio_resposta_min);
    const maxT = tempos.length ? Math.max(...tempos) : null;
    return arr.map(c => {
        const sv = (c.total_atendimentos / maxTA) * 10;
        const st = (maxT > 0 && c.tempo_medio_resposta_min != null)
            ? (1 - c.tempo_medio_resposta_min / maxT) * 10 : null;
        const np = st != null ? (sv + st) / 2 : sv;
        const ng = c.nota_media != null ? (c.nota_media + np) / 2 : null;
        return { ...c, nota_produtividade: np, nota_geral: ng };
    });
}

function _fbShowLoading(v) {
    document.getElementById('fb-loading').classList.toggle('active', v);
    document.getElementById('fbBtnBuscar').disabled = v;
    document.getElementById('fbBtnBuscar').classList.toggle('loading', v);
}
function _fbShowError(msg) {
    const t = document.getElementById('fb-error-toast');
    document.getElementById('fbErrorText').textContent = msg;
    t.classList.add('active'); setTimeout(() => t.classList.remove('active'), 5000);
}
function _fbFmtDate(ds) {
    const d = new Date(ds + 'T00:00:00');
    return d.toLocaleDateString('pt-BR', { day:'2-digit', month:'2-digit', year:'numeric' });
}
function _fbSetText(id, txt) {
    const el = document.getElementById(id); if (!el) return;
    if (!txt || txt.trim() === '' || txt === 'Não informado') { el.innerHTML = '<p style="color:var(--text-muted, #64748b);padding:12px 0;">Nenhum feedback disponível</p>'; return; }
    el.innerHTML = '<p>' + txt.replace(/\n/g, '<br>').replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>') + '</p>';
}
function _fbEscHtml(t) { const d = document.createElement('div'); d.textContent = t; return d.innerHTML; }

function _fbPopulateDD(consultores) {
    const sel = document.getElementById('fbConsultorFilter'), cv = sel.value;
    sel.innerHTML = '<option value="">Todos os consultores</option>';
    _fbValidC(consultores).forEach(c => {
        const o = document.createElement('option');
        o.value = c.nomes_originais ? c.nomes_originais[0] : c.consultor;
        o.textContent = c.consultor + ' (' + c.total_atendimentos + ')';
        sel.appendChild(o);
    });
    if (cv) sel.value = cv;
}

function _fbUpdateKPIs(g, consultores) {
    document.getElementById('fbKpiTotal').textContent = _fbFmtNum(g.total_atendimentos);
    document.getElementById('fbKpiNota').textContent = _fbFmtDec(g.nota_media);
    document.getElementById('fbKpiNotasSub').textContent = _fbFmtNum(g.notas_informadas) + ' notas informadas';
    document.getElementById('fbKpiTempoResposta').textContent = _fbFmtTime(g.tempo_medio_resposta_min);
    document.getElementById('fbKpiTempoAtend').textContent = _fbFmtTime(g.tempo_medio_atendimento_min);
    document.getElementById('fbPeriodBadge').textContent = _fbFmtDate(document.getElementById('fbStartDate').value) + ' - ' + _fbFmtDate(document.getElementById('fbEndDate').value);

    // Calcula médias globais de produtividade e nota geral a partir dos consultores
    if (consultores && consultores.length > 0) {
        const valid = _fbValidC(consultores);
        const withProd = valid.filter(c => c.nota_produtividade != null);
        const avgProd = withProd.length > 0 ? withProd.reduce((s, c) => s + c.nota_produtividade, 0) / withProd.length : null;
        const withGeral = valid.filter(c => c.nota_geral != null);
        const avgGeral = withGeral.length > 0 ? withGeral.reduce((s, c) => s + c.nota_geral, 0) / withGeral.length : null;
        document.getElementById('fbKpiNotaProd').textContent = _fbFmtDec(avgProd);
        document.getElementById('fbKpiNotaGeral').textContent = _fbFmtDec(avgGeral);
    }
}

function _fbRenderTable(consultores) {
    const tb = document.getElementById('fbTableBody');
    const valid = _fbValidC(consultores);
    if (valid.length === 0) { tb.innerHTML = '<tr><td colspan="10" class="px-4 py-12 text-center text-slate-500">Nenhum consultor encontrado</td></tr>'; return; }
    tb.innerHTML = valid.map((c, i) => {
        const nm = c.nomes_originais ? c.nomes_originais[0] : c.consultor;
        return '<tr>' +
            '<td><strong>' + (i+1) + '</strong></td>' +
            '<td><div class="fb-consultor-name"><div class="fb-avatar">' + _fbInitials(c.consultor) + '</div>' + c.consultor + '</div></td>' +
            '<td><strong>' + _fbFmtNum(c.total_atendimentos) + '</strong></td>' +
            '<td><span class="fb-nota-badge ' + _fbBadgeCls(c.nota_media) + '">' + (c.nota_media != null ? _fbFmtDec(c.nota_media) : 'N/A') + '</span></td>' +
            '<td><span class="fb-nota-badge ' + _fbBadgeCls(c.nota_produtividade) + '">' + (c.nota_produtividade != null ? _fbFmtDec(c.nota_produtividade) : 'N/A') + '</span></td>' +
            '<td><span class="fb-nota-badge ' + _fbBadgeCls(c.nota_geral) + '">' + (c.nota_geral != null ? _fbFmtDec(c.nota_geral) : 'N/A') + '</span></td>' +
            '<td>' + _fbFmtNum(c.notas_informadas) + '</td>' +
            '<td>' + (c.tempo_medio_resposta_min != null ? _fbFmtTime(c.tempo_medio_resposta_min) : 'N/A') + '</td>' +
            '<td>' + (c.tempo_medio_atendimento_min != null ? _fbFmtTime(c.tempo_medio_atendimento_min) : 'N/A') + '</td>' +
            '<td><button class="fb-btn-detail" onclick="fbViewDetail(\'' + nm.replace(/'/g, "\\'") + '\')"><svg viewBox="0 0 24 24" width="16" height="16" style="fill:currentColor;margin-right:4px;"><path d="M12 4.5C7 4.5 2.73 7.61 1 12c1.73 4.39 6 7.5 11 7.5s9.27-3.11 11-7.5c-1.73-4.39-6-7.5-11-7.5zM12 17c-2.76 0-5-2.24-5-5s2.24-5 5-5 5 2.24 5 5-2.24 5-5 5zm0-8c-1.66 0-3 1.34-3 3s1.34 3 3 3 3-1.34 3-3-1.34-3-3-3z"/></svg>Ver detalhes</button></td>' +
            '</tr>';
    }).join('');
}

function _fbMakeChart(canvasId, chartRef, data) {
    const ctx = document.getElementById(canvasId).getContext('2d');
    if (chartRef) chartRef.destroy();
    if (!data || data.length === 0) return null;
    const sorted = [...data].sort((a, b) => new Date(a.date) - new Date(b.date));
    const labels = sorted.map(d => { const dt = new Date(d.date + 'T00:00:00'); return dt.toLocaleDateString('pt-BR', { day:'2-digit', month:'2-digit' }); });
    return new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                { label: 'Atendimentos', data: sorted.map(d => d.atendimentos || 0), borderColor: '#2563eb', backgroundColor: 'rgba(37,99,235,.1)', fill: true, tension: .4, yAxisID: 'y' },
                { label: 'Nota Atendimento', data: sorted.map(d => d.nota_media != null ? d.nota_media : null), borderColor: '#059669', backgroundColor: 'transparent', borderDash: [5, 5], tension: .4, yAxisID: 'y1', spanGaps: true }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { position: 'top' } },
            scales: {
                y: { type:'linear', display:true, position:'left', title: { display:true, text:'Atendimentos' } },
                y1: { type:'linear', display:true, position:'right', min:0, max:10, title: { display:true, text:'Nota' }, grid: { drawOnChartArea:false } }
            }
        }
    });
}

async function fbFetch() {
    const sd = document.getElementById('fbStartDate').value, ed = document.getElementById('fbEndDate').value;
    const cons = document.getElementById('fbConsultorFilter').value;
    if (!sd || !ed) { _fbShowError('Selecione as datas'); return; }
    const diff = Math.ceil(Math.abs(new Date(ed) - new Date(sd)) / 864e5);
    if (diff > 16) { _fbShowError('Período máximo: 16 dias. Você selecionou ' + diff + ' dias.'); return; }
    _fbShowLoading(true);
    try {
        let url = FB_WEBHOOK + '?start=' + sd + '&end=' + ed + '&topN=5';
        if (cons) url += '&consultor=' + encodeURIComponent(cons);
        const resp = await fetch(url, { method:'GET', mode:'cors' });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const data = await resp.json();
        _fbData = data;
        if (data.global) _fbUpdateKPIs(data.global, data.consultores);
        if (data.consultores) _fbPopulateDD(data.consultores);
        let det = data.consultor_detalhe || data.detalhe || null;
        if (!det && cons && data.consultores) {
            det = data.consultores.find(c => c.consultor === cons || (c.nomes_originais && c.nomes_originais.includes(cons)));
        }
        if (data.serie_dia && det) det.serie_dia = data.serie_dia;
        if (data.metricas && data.metricas.serie_dia && det) det.serie_dia = data.metricas.serie_dia;

        if (cons && det) {
            document.getElementById('fbRankingSection').style.display = 'none';
            document.getElementById('fbGlobalChartSection').style.display = 'none';
            _fbShowDetail(det, cons);
        } else if (cons) {
            document.getElementById('fbRankingSection').style.display = 'none';
            document.getElementById('fbGlobalChartSection').style.display = 'none';
            _fbShowDetail({ consultor: cons, ...data.global }, cons);
        } else {
            document.getElementById('fbRankingSection').style.display = '';
            document.getElementById('fbGlobalChartSection').style.display = '';
            document.getElementById('fbDetailSection').style.display = 'none';
            if (data.consultores) _fbRenderTable(data.consultores);
            const sg = data.global?.serie_dia_global || data.serie_dia_global || null;
            _fbChartGlobal = _fbMakeChart('fbChartGlobal', _fbChartGlobal, sg);
        }
    } catch (e) {
        _fbShowError('Erro: ' + e.message);
    } finally {
        _fbShowLoading(false);
    }
}

function _fbShowDetail(det, cons) {
    document.getElementById('fbDetailSection').style.display = '';
    const m = det.metricas || {};
    document.getElementById('fbDetailName').textContent = det.consultor || m.consultor || cons;
    _fbSetText('fbFeedGeral', det.feedback_geral || m.feedback_geral);
    _fbSetText('fbFeedPos', det.feedback_positivo || m.feedback_positivo);
    _fbSetText('fbFeedNeg', det.feedback_negativo || m.feedback_negativo);
    const sd = det.serie_dia || m.serie_dia || null;
    _fbChartDetail = _fbMakeChart('fbChartDetail', _fbChartDetail, sd);
}

async function fbViewDetail(name) {
    document.getElementById('fbConsultorFilter').value = name;
    await fbFetch();
}
async function fbVoltarTodos() {
    document.getElementById('fbConsultorFilter').value = '';
    await fbFetch();
}

async function fbVerExemplos(tipo) {
    const cons = document.getElementById('fbConsultorFilter').value;
    const sd = document.getElementById('fbStartDate').value, ed = document.getElementById('fbEndDate').value;
    if (!cons) { _fbShowError('Nenhum consultor selecionado'); return; }
    const hdr = document.getElementById('fbModalHeader');
    hdr.className = 'fb-modal-hdr ' + tipo;
    document.getElementById('fbModalTitle').textContent = tipo === 'positivo' ? '👍 Exemplos Positivos' : '👎 Exemplos de Melhoria';
    document.getElementById('fbModalSubtitle').textContent = cons + ' • Carregando...';
    document.getElementById('fb-modal-body').innerHTML = '<div style="display:flex;flex-direction:column;align-items:center;justify-content:center;padding:60px;color:var(--text-muted, #64748b);"><div class="fb-spinner"></div><p>Carregando exemplos...</p></div>';
    document.getElementById('fb-modal-overlay').classList.add('active');
    document.body.style.overflow = 'hidden';
    try {
        const url = FB_WEBHOOK + '?consultor=' + encodeURIComponent(cons) + '&tipo=' + tipo + '&start=' + sd + '&end=' + ed;
        const resp = await fetch(url, { method:'GET', mode:'cors' });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const data = await resp.json();
        const exs = data.exemplos || [], total = data.total || exs.length;
        document.getElementById('fbModalSubtitle').textContent = total + ' exemplo' + (total !== 1 ? 's' : '') + ' encontrado' + (total !== 1 ? 's' : '');
        if (exs.length === 0) {
            document.getElementById('fb-modal-body').innerHTML = '<div style="text-align:center;padding:60px 20px;color:var(--text-muted, #64748b);"><p>Nenhum exemplo ' + (tipo === 'positivo' ? 'positivo' : 'de melhoria') + ' encontrado.</p></div>';
            return;
        }
        document.getElementById('fb-modal-body').innerHTML = exs.map((ex, i) => _fbRenderEx(ex, i, tipo)).join('');
    } catch (e) {
        document.getElementById('fb-modal-body').innerHTML = '<div style="text-align:center;padding:60px;color:var(--text-muted, #64748b);"><p>Erro: ' + e.message + '</p></div>';
    }
}

function _fbRenderEx(ex, idx, tipo) {
    const nota = ex.nota_atendimento, nc = nota >= 8 ? 'alta' : nota >= 6 ? 'media' : 'baixa';
    const ts = ex.timestamp ? new Date(ex.timestamp).toLocaleString('pt-BR') : '';
    const trHtml = (ex.trechos || []).map(t => '<div class="fb-trecho">"' + t + '"</div>').join('');
    const acHtml = (ex.acoes_recomendadas || []).map(a => '<div class="fb-acao">💡 ' + a + '</div>').join('');
    let html = '<div class="fb-ex-card ' + tipo + '"><div class="fb-ex-header"><div><h3 class="fb-ex-titulo">' + (ex.titulo || ex.feedback_base || 'Exemplo') + '</h3><span style="font-size:12px;color:var(--text-muted, #64748b);">' + ts + '</span></div><div><span class="fb-ex-nota ' + nc + '">⭐ ' + (nota != null ? nota.toFixed(1) : 'N/A') + '</span></div></div>';
    if (ex.explicacao) html += '<div class="fb-ex-explicacao">' + ex.explicacao + '</div>';
    if (trHtml) html += '<div style="margin-bottom:16px;"><div style="font-size:13px;font-weight:600;color:var(--text-muted, #64748b);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;">Trechos da Conversa</div>' + trHtml + '</div>';
    if (acHtml) html += '<div style="margin-bottom:16px;"><div style="font-size:13px;font-weight:600;color:var(--text-muted, #64748b);text-transform:uppercase;letter-spacing:.5px;margin-bottom:10px;">Ações Recomendadas</div>' + acHtml + '</div>';
    if (ex.conversa) html += '<div><button class="fb-conv-toggle" onclick="fbToggleConv(this)"><span>📜 Ver conversa completa</span><svg viewBox="0 0 24 24"><path d="M7.41 8.59L12 13.17l4.59-4.58L18 10l-6 6-6-6 1.41-1.41z"/></svg></button><div class="fb-conv-content"><pre>' + _fbEscHtml(ex.conversa) + '</pre></div></div>';
    html += '</div>';
    return html;
}
function fbToggleConv(btn) {
    btn.classList.toggle('active');
    const c = btn.nextElementSibling; c.classList.toggle('active');
    btn.querySelector('span').textContent = c.classList.contains('active') ? '📜 Ocultar conversa' : '📜 Ver conversa completa';
}
function fbCloseModal() {
    document.getElementById('fb-modal-overlay').classList.remove('active');
    document.body.style.overflow = '';
}
