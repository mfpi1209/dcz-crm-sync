/* Meus chamados TI — tickets do usuário logado. */
(function () {
    let _status = 'abertos';
    let _qTimer = null;

    function $(id) { return document.getElementById(id); }
    function escapeHtml(s) {
        return String(s ?? '').replace(/[&<>"']/g, c => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
        ));
    }
    function statusClass(st) {
        if (st === 'Concluído') return 'sti-st-Concluído';
        if (st === 'Em andamento') return 'sti-st-Em';
        return 'sti-st-Pendente';
    }
    function fmtTs(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return escapeHtml(iso);
            return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
        } catch (e) {
            return escapeHtml(iso);
        }
    }

    function setKpis(kpis) {
        kpis = kpis || {};
        const p = $('mct-kpi-pendente');
        const a = $('mct-kpi-andamento');
        const c = $('mct-kpi-concluido');
        if (p) p.textContent = String(kpis['Pendente'] || 0);
        if (a) a.textContent = String(kpis['Em andamento'] || 0);
        if (c) c.textContent = String(kpis['Concluído'] || 0);
    }

    async function loadList() {
        const q = ($('mct-q')?.value || '').trim();
        const params = new URLSearchParams();
        if (_status) params.set('status', _status);
        if (q) params.set('q', q);
        params.set('limit', '80');
        const resp = await fetch('/api/solicitacoes_ti/meus?' + params.toString(), { cache: 'no-store' });
        const data = await resp.json().catch(() => ({}));
        const items = Array.isArray(data.items) ? data.items : [];
        setKpis(data.kpis);
        const list = $('mct-list');
        const empty = $('mct-empty');
        if (!list) return;
        if (!items.length) {
            list.innerHTML = '';
            if (empty) empty.classList.remove('hidden');
            return;
        }
        if (empty) empty.classList.add('hidden');
        list.innerHTML = items.map(t => `
            <button type="button" onclick="mctOpen(${t.id})"
                    class="w-full text-left glass-card border border-[var(--border)] rounded-xl px-4 py-3 flex flex-wrap items-center gap-3 hover:border-[var(--primary)]">
                <span class="font-mono text-[11px] font-bold px-2 py-0.5 rounded" style="background: var(--bg-elevated);">${escapeHtml(t.protocolo)}</span>
                <span class="flex-1 min-w-[140px] font-semibold text-sm text-[var(--text-primary)] truncate">${escapeHtml(t.titulo)}</span>
                <span class="sti-badge ${statusClass(t.status)}">${escapeHtml(t.status)}</span>
                <span class="sti-badge sti-badge-${escapeHtml(t.urgencia || 'Média')}">${escapeHtml(t.urgencia || '')}</span>
                <span class="text-[11px] font-mono text-slate-400">${fmtTs(t.created_at)}</span>
            </button>
        `).join('');
    }

    function renderTimeline(eventos) {
        if (!eventos || !eventos.length) return '<p class="text-xs text-slate-500">Sem histórico ainda.</p>';
        return `<ol class="space-y-2 border-l border-[var(--border)] pl-4">` + eventos.map(ev => `
            <li>
                <p class="text-xs font-semibold">${escapeHtml(ev.status_novo || '')}
                    <span class="font-normal text-slate-500">· ${escapeHtml(ev.autor_nome || '')}</span>
                </p>
                <p class="text-[11px] text-slate-400 font-mono">${fmtTs(ev.created_at)}</p>
                ${ev.nota ? `<p class="text-xs text-slate-500 mt-0.5">${escapeHtml(ev.nota)}</p>` : ''}
            </li>
        `).join('') + '</ol>';
    }

    window.mctOpen = async function (id) {
        const resp = await fetch('/api/solicitacoes_ti/meus/' + id, { cache: 'no-store' });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.ticket) {
            if (typeof toast === 'function') toast(data.message || 'Não foi possível abrir o chamado.', 'error');
            return;
        }
        const t = data.ticket;
        $('mct-modal-proto').textContent = t.protocolo || '';
        $('mct-modal-title').textContent = t.titulo || '';
        $('mct-modal-body').innerHTML = `
            <div class="flex flex-wrap gap-2">
                <span class="sti-badge ${statusClass(t.status)}">${escapeHtml(t.status)}</span>
                <span class="sti-badge sti-badge-${escapeHtml(t.urgencia)}">${escapeHtml(t.urgencia)}</span>
            </div>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Setor</span><br>${escapeHtml(t.setor)}</p>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Categoria</span><br>${escapeHtml(t.categoria)}</p>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Descrição</span><br>${escapeHtml(t.descricao).replace(/\n/g, '<br>')}</p>
            ${t.observacoes ? `<p><span class="text-slate-500 text-xs uppercase font-bold">Observações</span><br>${escapeHtml(t.observacoes)}</p>` : ''}
            <div>
                <p class="text-slate-500 text-xs uppercase font-bold mb-2">Andamento</p>
                ${renderTimeline(data.eventos)}
            </div>
        `;
        const modal = $('mct-modal');
        if (typeof dczPortalToBody === 'function') dczPortalToBody(modal);
        modal.classList.remove('hidden');
        if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(true);
    };

    window.mctCloseModal = function () {
        const m = $('mct-modal');
        if (m) m.classList.add('hidden');
        if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(false);
    };

    function bind() {
        document.querySelectorAll('#mct-tabs .mct-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                _status = btn.dataset.status || '';
                document.querySelectorAll('#mct-tabs .mct-tab').forEach(b => b.classList.toggle('is-active', b === btn));
                loadList();
            });
        });
        const q = $('mct-q');
        if (q) {
            q.addEventListener('input', () => {
                clearTimeout(_qTimer);
                _qTimer = setTimeout(loadList, 250);
            });
        }
        const modal = $('mct-modal');
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) mctCloseModal();
            });
        }
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') mctCloseModal();
        });
    }

    let _bound = false;
    window.loadMeusChamadosTi = function () {
        if (!_bound) { bind(); _bound = true; }
        loadList().catch(err => console.error(err));
    };
})();
