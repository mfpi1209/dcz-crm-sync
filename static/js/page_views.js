/* Uso do Dashboard — admin only. */
(function () {
    let _pvLoaded = false;
    const PAGE_LABELS = (typeof PAGE_TITLES === 'object' && PAGE_TITLES) || {};

    function fmt(n) { return Number(n || 0).toLocaleString('pt-BR'); }
    function todayIso() {
        const d = new Date();
        return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
    }
    function addDaysIso(iso, delta) {
        const [y,m,d] = iso.split('-').map(Number);
        const dt = new Date(y, m-1, d);
        dt.setDate(dt.getDate() + delta);
        return dt.getFullYear() + '-' + String(dt.getMonth()+1).padStart(2,'0') + '-' + String(dt.getDate()).padStart(2,'0');
    }
    function escapeHtml(s) {
        return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
    }
    function pageLabel(p) { return PAGE_LABELS[p] || p; }

    function setPreset(days) {
        document.querySelectorAll('#pv-presets .pv-preset-btn').forEach(b => b.classList.remove('active'));
        const btn = document.querySelector(`#pv-presets .pv-preset-btn[onclick="pvPreset(${days})"]`);
        if (btn) btn.classList.add('active');
        const end = todayIso();
        const start = addDaysIso(end, -days);
        document.getElementById('pv-dt-ini').value = start;
        document.getElementById('pv-dt-fim').value = end;
    }

    window.pvPreset = function (days) {
        setPreset(days);
        pvLoad();
    };

    window.loadPageViews = function () {
        if (!_pvLoaded) {
            setPreset(29);
            _pvLoaded = true;
        }
        pvLoad();
    };

    window.pvLoad = async function () {
        const start = document.getElementById('pv-dt-ini').value;
        const end   = document.getElementById('pv-dt-fim').value;
        const qs = 'start_date=' + encodeURIComponent(start) + '&end_date=' + encodeURIComponent(end) + '&_=' + Date.now();
        try {
            const resp = await fetch('/api/page-views/stats?' + qs, { cache: 'no-store' });
            if (resp.status === 403) {
                document.querySelector('#page-page_views main').innerHTML =
                    '<div class="glass-card border border-[var(--border)] rounded-2xl p-8 text-center text-slate-500">Acesso negado. Apenas administrador.</div>';
                return;
            }
            const data = await resp.json();
            if (data.ok) render(data);
        } catch (e) {
            console.error('pvLoad', e);
        }
    };

    function userBadge(u) {
        const isAdmin = u.role === 'admin';
        const cls = isAdmin
            ? 'bg-violet-900/30 text-violet-300 border border-violet-800/50'
            : 'bg-slate-700/40 text-slate-300 border border-slate-600/50';
        return `<span class="inline-flex items-center text-[10px] font-medium px-2 py-0.5 rounded-full ${cls}">${escapeHtml(u.username || '—')}${isAdmin ? ' · admin' : ''}</span>`;
    }

    function render(data) {
        const t = data.totais || {};
        document.getElementById('pv-k-total').textContent = fmt(t.total_hits);
        document.getElementById('pv-k-acessadas').textContent = fmt((data.por_pagina || []).length);
        document.getElementById('pv-k-zeradas').textContent = fmt((data.nao_acessadas || []).length);

        const bodyA = document.getElementById('pv-tbody-acessadas');
        bodyA.innerHTML = (data.por_pagina || []).map(r => `
            <tr class="border-b border-[var(--border)] hover:bg-[var(--bg-elevated)]">
                <td class="px-3 py-2">
                    <div class="text-[var(--text-primary)] font-medium">${escapeHtml(pageLabel(r.page))}</div>
                    <div class="text-[10px] text-slate-500">${escapeHtml(r.page)}</div>
                </td>
                <td class="px-3 py-2 text-right tabular-nums font-semibold text-emerald-400">${fmt(r.hits)}</td>
                <td class="px-3 py-2 text-right tabular-nums text-slate-400">${fmt(r.users)}</td>
            </tr>`).join('') || '<tr><td colspan="3" class="px-3 py-6 text-center text-slate-500">Nenhuma página acessada no período.</td></tr>';

        const bodyZ = document.getElementById('pv-tbody-zeradas');
        bodyZ.innerHTML = (data.nao_acessadas || []).map(r => {
            const users = r.users_with_access || [];
            const badges = users.length
                ? users.map(userBadge).join(' ')
                : '<span class="text-[10px] text-amber-500 italic">Ninguém tem permissão · página órfã</span>';
            return `
                <tr class="border-b border-[var(--border)] hover:bg-[var(--bg-elevated)] align-top">
                    <td class="px-3 py-2 whitespace-nowrap">
                        <div class="text-[var(--text-primary)] font-medium">${escapeHtml(pageLabel(r.page))}</div>
                        <div class="text-[10px] text-slate-500">${escapeHtml(r.page)}</div>
                    </td>
                    <td class="px-3 py-2">
                        <div class="flex flex-wrap gap-1">${badges}</div>
                        <div class="text-[10px] text-slate-500 mt-1">${users.length} usuário(s) com acesso</div>
                    </td>
                </tr>`;
        }).join('') || '<tr><td colspan="2" class="px-3 py-6 text-center text-slate-500">Todas as páginas foram acessadas no período.</td></tr>';
    }
})();
