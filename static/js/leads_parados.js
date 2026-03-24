// ---------------------------------------------------------------------------
// Leads Parados
// ---------------------------------------------------------------------------

async function loadLeadsParados() {
    const loading = document.getElementById('lp-loading');
    const tableWrap = document.getElementById('lp-table-wrap');
    const empty = document.getElementById('lp-empty');
    const errorDiv = document.getElementById('lp-error');
    const badge = document.getElementById('lp-total-badge');

    loading.classList.remove('hidden');
    tableWrap.classList.add('hidden');
    empty.classList.add('hidden');
    errorDiv.classList.add('hidden');
    badge.classList.add('hidden');

    try {
        const horas = document.getElementById('lp-horas').value;
        const res = await api('/api/leads-parados?horas=' + horas);
        if (!res.ok) throw new Error('Erro ao carregar dados');
        const data = await res.json();

        loading.classList.add('hidden');

        if (!data.leads || data.leads.length === 0) {
            empty.classList.remove('hidden');
            return;
        }

        badge.textContent = data.total + (data.total === 1 ? ' lead parado' : ' leads parados');
        badge.classList.remove('hidden');

        const tbody = document.getElementById('lp-tbody');
        tbody.innerHTML = '';

        for (const lead of data.leads) {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-white/5 transition-colors';

            const corTempo = lead.segundos_parado >= 7200
                ? 'text-red-400 font-bold'
                : lead.segundos_parado >= 3600
                    ? 'text-amber-400'
                    : 'text-slate-300';

            const kommoUrl = 'https://admamoeduitcombr.kommo.com/leads/detail/' + lead.id;
            tr.innerHTML = `
                <td class="px-4 py-3 font-mono text-xs"><a href="${kommoUrl}" target="_blank" class="text-blue-400 hover:text-blue-300 underline">${lead.id}</a></td>
                <td class="px-4 py-3 text-slate-200">${_lpEscape(lead.name)}</td>
                <td class="px-4 py-3 text-slate-300">${_lpEscape(lead.consultor)}</td>
                <td class="px-4 py-3 text-slate-400 text-xs">${lead.updated_at}</td>
                <td class="px-4 py-3 ${corTempo}">${lead.tempo_parado}</td>
            `;
            tbody.appendChild(tr);
        }

        tableWrap.classList.remove('hidden');
    } catch (e) {
        loading.classList.add('hidden');
        errorDiv.classList.remove('hidden');
        document.getElementById('lp-error-msg').textContent = e.message || 'Erro desconhecido';
    }
}

function refreshLeadsParados() {
    loadLeadsParados();
}

async function lpDistribuir() {
    const btn = document.getElementById('lp-btn-distribuir');
    const original = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = '<span class="inline-block w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></span> Distribuindo... aguarde';

    try {
        const horas = document.getElementById('lp-horas').value;
        const res = await api('/api/leads-parados/distribuir', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ horas: parseInt(horas) }),
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

function _lpEscape(str) {
    if (!str) return '';
    const d = document.createElement('div');
    d.textContent = str;
    return d.innerHTML;
}
