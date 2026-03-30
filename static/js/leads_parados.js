// ---------------------------------------------------------------------------
// Leads Parados
// ---------------------------------------------------------------------------

let _lpAllLeads = [];
let _lpFilteredLeads = [];

async function loadLeadsParados() {
    const loading = document.getElementById('lp-loading');
    const tableWrap = document.getElementById('lp-table-wrap');
    const empty = document.getElementById('lp-empty');
    const errorDiv = document.getElementById('lp-error');
    const badge = document.getElementById('lp-total-badge');
    const selBadge = document.getElementById('lp-sel-badge');

    loading.classList.remove('hidden');
    tableWrap.classList.add('hidden');
    empty.classList.add('hidden');
    errorDiv.classList.add('hidden');
    badge.classList.add('hidden');
    selBadge.classList.add('hidden');
    document.getElementById('lp-media-card').classList.add('hidden');

    try {
        const horas = document.getElementById('lp-horas').value;
        const res = await api('/api/leads-parados?horas=' + horas);
        if (!res.ok) throw new Error('Erro ao carregar dados');
        const data = await res.json();

        loading.classList.add('hidden');
        _lpAllLeads = data.leads || [];

        if (_lpAllLeads.length === 0) {
            empty.classList.remove('hidden');
            _lpPopulateConsultorFilter([]);
            return;
        }

        _lpPopulateConsultorFilter(_lpAllLeads);
        lpFiltrarConsultor();
    } catch (e) {
        loading.classList.add('hidden');
        errorDiv.classList.remove('hidden');
        document.getElementById('lp-error-msg').textContent = e.message || 'Erro desconhecido';
    }
}

function _lpPopulateConsultorFilter(leads) {
    const sel = document.getElementById('lp-consultor');
    const prev = sel.value;
    const consultores = {};
    for (const l of leads) {
        if (l.responsible_user_id && l.consultor && l.consultor !== '—') {
            consultores[l.responsible_user_id] = l.consultor;
        }
    }
    const sorted = Object.entries(consultores).sort((a, b) => a[1].localeCompare(b[1]));
    sel.innerHTML = '<option value="">Todos</option>';
    for (const [uid, name] of sorted) {
        const opt = document.createElement('option');
        opt.value = uid;
        opt.textContent = name;
        sel.appendChild(opt);
    }
    if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
}

function lpFiltrarConsultor() {
    const filtro = document.getElementById('lp-consultor').value;
    _lpFilteredLeads = filtro
        ? _lpAllLeads.filter(l => String(l.responsible_user_id) === filtro)
        : [..._lpAllLeads];
    _lpRenderTable(_lpFilteredLeads);
}

function _lpRenderTable(leads) {
    const tableWrap = document.getElementById('lp-table-wrap');
    const empty = document.getElementById('lp-empty');
    const badge = document.getElementById('lp-total-badge');
    const selBadge = document.getElementById('lp-sel-badge');
    const selectAll = document.getElementById('lp-select-all');
    const mediaCard = document.getElementById('lp-media-card');

    if (leads.length === 0) {
        tableWrap.classList.add('hidden');
        empty.classList.remove('hidden');
        badge.classList.add('hidden');
        selBadge.classList.add('hidden');
        mediaCard.classList.add('hidden');
        return;
    }

    _lpUpdateMedia(leads);

    empty.classList.add('hidden');
    badge.textContent = leads.length + (leads.length === 1 ? ' lead parado' : ' leads parados');
    badge.classList.remove('hidden');

    const tbody = document.getElementById('lp-tbody');
    tbody.innerHTML = '';
    if (selectAll) selectAll.checked = false;

    for (const lead of leads) {
        const tr = document.createElement('tr');
        tr.className = 'hover:bg-white/5 transition-colors';
        tr.dataset.leadId = lead.id;
        tr.dataset.contactId = lead.contact_id || '';
        tr.dataset.responsibleUserId = lead.responsible_user_id || '';

        const corTempo = lead.segundos_parado >= 7200
            ? 'text-red-400 font-bold'
            : lead.segundos_parado >= 3600
                ? 'text-amber-400'
                : 'text-slate-300';

        const kommoUrl = 'https://admamoeduitcombr.kommo.com/leads/detail/' + lead.id;
        tr.innerHTML = `
            <td class="px-4 py-3"><input type="checkbox" class="lp-check accent-blue-500 w-4 h-4 cursor-pointer" value="${lead.id}" onchange="lpUpdateSelBadge()"></td>
            <td class="px-4 py-3 font-mono text-xs"><a href="${kommoUrl}" target="_blank" class="text-blue-400 hover:text-blue-300 underline">${lead.id}</a></td>
            <td class="px-4 py-3 text-slate-200">${_lpEscape(lead.name)}</td>
            <td class="px-4 py-3 text-slate-300">${_lpEscape(lead.consultor)}</td>
            <td class="px-4 py-3 text-slate-400 text-xs">${lead.updated_at}</td>
            <td class="px-4 py-3 ${corTempo}">${lead.tempo_parado}</td>
        `;
        tbody.appendChild(tr);
    }

    tableWrap.classList.remove('hidden');
    lpUpdateSelBadge();
}

function lpToggleAll(master) {
    document.querySelectorAll('.lp-check').forEach(cb => { cb.checked = master.checked; });
    lpUpdateSelBadge();
}

function lpUpdateSelBadge() {
    const checked = document.querySelectorAll('.lp-check:checked').length;
    const total = document.querySelectorAll('.lp-check').length;
    const badge = document.getElementById('lp-sel-badge');
    const selectAll = document.getElementById('lp-select-all');

    if (selectAll) selectAll.checked = checked > 0 && checked === total;

    if (checked > 0) {
        badge.textContent = checked + ' selecionado' + (checked > 1 ? 's' : '');
        badge.classList.remove('hidden');
    } else {
        badge.classList.add('hidden');
    }
}

function refreshLeadsParados() {
    loadLeadsParados();
}

async function lpDistribuir() {
    const checked = document.querySelectorAll('.lp-check:checked');
    if (checked.length === 0) {
        alert('Selecione pelo menos um lead para distribuir.');
        return;
    }

    const selectedLeads = [];
    checked.forEach(cb => {
        const tr = cb.closest('tr');
        selectedLeads.push({
            lead_id: parseInt(tr.dataset.leadId),
            contact_id: tr.dataset.contactId ? parseInt(tr.dataset.contactId) : null,
            responsible_user_id: tr.dataset.responsibleUserId ? parseInt(tr.dataset.responsibleUserId) : null,
        });
    });

    const btn = document.getElementById('lp-btn-distribuir');
    const original = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="inline-block w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></span> Distribuindo... aguarde';

    try {
        const res = await api('/api/leads-parados/distribuir', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ leads: selectedLeads }),
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok) {
            btn.innerHTML = '<span class="material-symbols-outlined text-base">check</span> Concluído! (' + (data.total || 0) + ' leads)';
            btn.classList.replace('bg-emerald-600', 'bg-green-600');
            setTimeout(() => {
                btn.innerHTML = original;
                btn.disabled = false;
                btn.classList.replace('bg-green-600', 'bg-emerald-600');
                loadLeadsParados();
            }, 4000);
        } else {
            throw new Error(data.error || 'Erro ao distribuir');
        }
    } catch (e) {
        btn.innerHTML = original;
        btn.disabled = false;
        alert('Erro: ' + e.message);
    }
}

function _lpUpdateMedia(leads) {
    const card = document.getElementById('lp-media-card');
    const valor = document.getElementById('lp-media-valor');
    if (!leads.length) { card.classList.add('hidden'); return; }

    const soma = leads.reduce((acc, l) => acc + (l.segundos_parado || 0), 0);
    const media = soma / leads.length;
    const h = Math.floor(media / 3600);
    const m = Math.floor((media % 3600) / 60);
    valor.textContent = h > 0 ? `${h}h ${m}min` : `${m}min`;
    card.classList.remove('hidden');
}

function _lpEscape(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}
