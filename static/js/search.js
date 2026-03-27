// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------
async function loadXlSnapshots() {
    const tipo = document.getElementById('search-xl-tipo').value;
    const params = tipo ? `?tipo=${tipo}` : '';
    try {
        const res = await fetch('/api/xl-snapshots' + params);
        const data = await res.json();
        const sel = document.getElementById('search-snapshot-select');
        sel.innerHTML = '<option value="">Mais recente</option>';
        (data.snapshots || []).forEach(s => {
            const opt = document.createElement('option');
            opt.value = s.id;
            const tipoLabel = {'matriculados':'Matr','inadimplentes':'Inad','concluintes':'Conc'}[s.tipo] || s.tipo;
            opt.textContent = `[${tipoLabel}] ${s.filename} — ${s.uploaded_at} (${s.row_count.toLocaleString('pt-BR')} linhas)`;
            sel.appendChild(opt);
        });
    } catch(e) { /* silent */ }
}

document.getElementById('search-xl-tipo').addEventListener('change', () => loadXlSnapshots());

document.getElementById('search-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const form = new FormData(e.target);
    const params = new URLSearchParams();
    for (const [k, v] of form) params.set(k, v.trim());

    const snapId = document.getElementById('search-snapshot-select').value;
    const xlTipo = document.getElementById('search-xl-tipo').value;
    const xlParams = new URLSearchParams(params);
    if (snapId) xlParams.set('snapshot_id', snapId);
    if (xlTipo) xlParams.set('tipo', xlTipo);

    document.getElementById('search-error').classList.add('hidden');
    document.getElementById('search-empty').classList.add('hidden');
    document.getElementById('search-results').innerHTML = '';
    document.getElementById('search-xl-results').innerHTML = '';
    document.getElementById('search-crm-section').classList.add('hidden');
    document.getElementById('search-xl-section').classList.add('hidden');
    document.getElementById('search-snapshot-info').textContent = '';
    document.getElementById('search-loading').classList.remove('hidden');

    try {
        const xlFetches = [];
        if (snapId || xlTipo) {
            xlFetches.push(fetch('/api/search-xl?' + xlParams));
        } else {
            for (const t of ['matriculados', 'inadimplentes', 'concluintes']) {
                const p = new URLSearchParams(params);
                p.set('tipo', t);
                xlFetches.push(fetch('/api/search-xl?' + p));
            }
        }

        const [resCrm, ...resXls] = await Promise.all([
            fetch('/api/search?' + params),
            ...xlFetches,
        ]);
        const dataCrm = await resCrm.json();
        const allXlResults = [];
        const snapLabels = [];
        for (const rx of resXls) {
            const dx = await rx.json();
            if (dx.results && dx.results.length) {
                const tipoLabel = dx.snapshot
                    ? ({'matriculados':'Matriculados','inadimplentes':'Inadimplentes','concluintes':'Concluintes'}[dx.snapshot.tipo] || dx.snapshot.tipo)
                    : '';
                dx.results.forEach(r => { r._xl_tipo = tipoLabel; });
                allXlResults.push(...dx.results);
            }
            if (dx.snapshot) snapLabels.push(dx.snapshot);
        }

        document.getElementById('search-loading').classList.add('hidden');

        if (dataCrm.error) {
            document.getElementById('search-error').textContent = dataCrm.error;
            document.getElementById('search-error').classList.remove('hidden');
        }

        if (snapLabels.length) {
            document.getElementById('search-snapshot-info').textContent =
                snapLabels.map(s => {
                    const t = {'matriculados':'Matr','inadimplentes':'Inad','concluintes':'Conc'}[s.tipo] || s.tipo;
                    return `${t}: ${s.row_count} linhas`;
                }).join(' | ');
        }

        const hasCrm = dataCrm.results && dataCrm.results.length > 0;
        const hasXl = allXlResults.length > 0;

        if (!hasCrm && !hasXl) {
            document.getElementById('search-empty').classList.remove('hidden');
            return;
        }

        if (hasXl) {
            document.getElementById('search-xl-section').classList.remove('hidden');
            document.getElementById('search-xl-count').textContent = `(${allXlResults.length} resultado${allXlResults.length > 1 ? 's' : ''})`;
            renderXlResults(allXlResults);
        }

        if (hasCrm) {
            document.getElementById('search-crm-section').classList.remove('hidden');
            document.getElementById('search-crm-count').textContent = `(${dataCrm.results.length} resultado${dataCrm.results.length > 1 ? 's' : ''})`;
            renderResults(dataCrm.results);
        }
    } catch (err) {
        document.getElementById('search-loading').classList.add('hidden');
        document.getElementById('search-error').textContent = 'Erro de conexão: ' + err.message;
        document.getElementById('search-error').classList.remove('hidden');
    }
});

function renderResults(results) {
    const container = document.getElementById('search-results');
    container.innerHTML = '';

    results.forEach(r => {
        const statusColor = {
            'won': 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30',
            'in_process': 'bg-blue-500/15 text-blue-400 border border-blue-500/30',
            'lost': 'bg-red-500/15 text-red-400 border border-red-500/30'
        }[r.negocio_status] || 'bg-slate-800 text-slate-300';

        const statusLabel = {
            'won': 'Ganho', 'in_process': 'Em andamento', 'lost': 'Perdido'
        }[r.negocio_status] || r.negocio_status;

        const stageStyle = r.etapa_cor ? `background:${r.etapa_cor}15; color:${r.etapa_cor}; border:1px solid ${r.etapa_cor}40` : '';

        const campos_neg = r.campos_negocio || {};
        const campos_lead = r.campos_lead || {};

        const card = document.createElement('div');
        card.className = 'glass-card p-5 fade-in';
        card.innerHTML = `
            <div class="flex flex-wrap items-start justify-between gap-3 mb-4">
                <div>
                    <h3 class="text-base font-bold text-white font-display">${esc(r.lead_nome || '—')}</h3>
                    <p class="text-xs text-slate-500 mt-0.5">Lead: ${esc(r.lead_id || '')} &middot; Negócio: ${esc(r.negocio_codigo || r.negocio_id || '')}</p>
                </div>
                <div class="flex gap-2 flex-wrap">
                    <span class="tag-pill ${statusColor}">${statusLabel}</span>
                    ${r.etapa_nome ? `<span class="tag-pill" style="${stageStyle}">${esc(r.etapa_nome)}</span>` : ''}
                    ${r.pipeline_nome ? `<span class="tag-pill bg-slate-800/60 text-slate-300 border border-slate-700/30">${esc(r.pipeline_nome)}</span>` : ''}
                </div>
            </div>

            <div class="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2 text-sm mb-4">
                ${field('CPF', r.lead_cpf)}
                ${field('Telefone', r.lead_telefone)}
                ${field('Email', r.lead_email)}
                ${field('Origem', r.lead_origem)}
                ${field('Cidade', r.lead_cidade)}
                ${field('Estado', r.lead_estado)}
                ${field('Valor', r.negocio_valor ? 'R$ ' + Number(r.negocio_valor).toLocaleString('pt-BR') : null)}
                ${field('Atendente', r.atendente)}
                ${field('Criado em', fmtDate(r.negocio_criado_em))}
                ${field('Última mov.', fmtDate(r.negocio_movido_em))}
            </div>

            ${Object.keys(campos_neg).length ? `
            <details class="group">
                <summary class="text-xs font-bold text-slate-400 cursor-pointer mb-2 select-none font-display">
                    Campos do Negócio <span class="text-slate-600">(${Object.keys(campos_neg).length})</span>
                </summary>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2 text-sm pl-1">
                    ${Object.entries(campos_neg).map(([k,v]) => field(k, v || null)).join('')}
                </div>
            </details>` : ''}

            ${Object.keys(campos_lead).length ? `
            <details class="group mt-3">
                <summary class="text-xs font-bold text-slate-400 cursor-pointer mb-2 select-none font-display">
                    Campos do Lead <span class="text-slate-600">(${Object.keys(campos_lead).length})</span>
                </summary>
                <div class="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2 text-sm pl-1">
                    ${Object.entries(campos_lead).map(([k,v]) => field(k, v || null)).join('')}
                </div>
            </details>` : ''}
        `;
        container.appendChild(card);
    });
}

function renderXlResults(results) {
    const container = document.getElementById('search-xl-results');
    container.innerHTML = '';

    const xlLabels = {
        nome: 'Nome', cpf: 'CPF', rgm: 'RGM', curso: 'Curso', polo: 'Polo',
        serie: 'Série', situacao: 'Situação', tipo_matricula: 'Tipo Matrícula',
        data_mat: 'Data Matrícula', email: 'Email', email_acad: 'Email Acadêmico',
        fone_cel: 'Celular', fone_res: 'Fone Res.', fone_com: 'Fone Com.',
        negocio: 'Negócio', empresa: 'Empresa', bairro: 'Bairro', cidade: 'Cidade',
        sexo: 'Sexo', data_nasc: 'Data Nasc.', ciclo: 'Ciclo',
        valor: 'Valor', parcela: 'Parcela', vencimento: 'Vencimento',
        status_financeiro: 'Status Financeiro',
        data_conclusao: 'Data Conclusão', periodo: 'Período',
        modalidade: 'Modalidade', instituicao: 'Instituição',
    };

    results.forEach(r => {
        const sitColor = (r.situacao || '').toLowerCase().includes('em curso')
            ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30'
            : 'bg-amber-500/15 text-amber-400 border border-amber-500/30';

        const tipoColors = {
            'Matriculados': 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/30',
            'Inadimplentes': 'bg-amber-500/15 text-amber-400 border border-amber-500/30',
            'Concluintes': 'bg-purple-500/15 text-purple-400 border border-purple-500/30',
        };
        const tipoBorder = {
            'Matriculados': 'border-l-emerald-500/60',
            'Inadimplentes': 'border-l-amber-500/60',
            'Concluintes': 'border-l-purple-500/60',
        };
        const xlTipo = r._xl_tipo || '';
        const borderCls = tipoBorder[xlTipo] || 'border-l-amber-500/60';
        const tipoCls = tipoColors[xlTipo] || 'bg-slate-700/30 text-slate-400 border border-slate-600/30';

        const card = document.createElement('div');
        card.className = `glass-card p-5 fade-in border-l-4 ${borderCls}`;
        card.innerHTML = `
            <div class="flex flex-wrap items-start justify-between gap-3 mb-4">
                <div>
                    <h3 class="text-base font-bold text-white font-display">${esc(r.nome || '—')}</h3>
                    <p class="text-xs text-slate-500 mt-0.5">RGM: ${esc(r.rgm || '—')} &middot; CPF: ${esc(r.cpf || '—')}</p>
                </div>
                <div class="flex gap-2 flex-wrap">
                    ${xlTipo ? `<span class="tag-pill ${tipoCls}">${esc(xlTipo)}</span>` : ''}
                    ${r.situacao ? `<span class="tag-pill ${sitColor}">${esc(r.situacao)}</span>` : ''}
                    ${r.tipo_matricula ? `<span class="tag-pill bg-blue-500/15 text-blue-400 border border-blue-500/30">${esc(r.tipo_matricula)}</span>` : ''}
                </div>
            </div>
            <div class="grid grid-cols-2 md:grid-cols-4 gap-x-6 gap-y-2 text-sm">
                ${Object.entries(xlLabels).map(([k, label]) => {
                    const v = r[k];
                    if (!v) return '';
                    return `<div><span class="text-slate-500 text-xs">${label}</span><p class="text-slate-200 truncate">${esc(v)}</p></div>`;
                }).join('')}
            </div>
        `;
        container.appendChild(card);
    });
}
