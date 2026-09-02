/* Fila de chamados TI — listagem + alteração de status. */
(function () {
    let _status = 'abertos';
    let _qTimer = null;
    let _openId = null;
    let _pickedStatus = null;

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
        const p = $('cti-kpi-pendente');
        const a = $('cti-kpi-andamento');
        const c = $('cti-kpi-concluido');
        if (p) p.textContent = String(kpis['Pendente'] || 0);
        if (a) a.textContent = String(kpis['Em andamento'] || 0);
        if (c) c.textContent = String(kpis['Concluído'] || 0);
    }

    function qs() {
        const params = new URLSearchParams();
        if (_status) params.set('status', _status);
        const urg = $('cti-urgencia')?.value || '';
        const setor = $('cti-setor')?.value || '';
        const q = ($('cti-q')?.value || '').trim();
        if (urg) params.set('urgencia', urg);
        if (setor) params.set('setor', setor);
        if (q) params.set('q', q);
        params.set('limit', '150');
        return params.toString();
    }

    async function loadList() {
        const resp = await fetch('/api/solicitacoes_ti/chamados?' + qs(), { cache: 'no-store' });
        const data = await resp.json().catch(() => ({}));
        if (resp.status === 403) {
            const tbody = $('cti-tbody');
            if (tbody) tbody.innerHTML = `<tr><td colspan="8" class="px-4 py-8 text-center text-sm text-slate-500">${escapeHtml(data.message || 'Sem permissão.')}</td></tr>`;
            return;
        }
        const items = Array.isArray(data.items) ? data.items : [];
        setKpis(data.kpis);
        const empty = $('cti-empty');
        const tbody = $('cti-tbody');
        if (!tbody) return;
        if (!items.length) {
            tbody.innerHTML = '';
            if (empty) empty.classList.remove('hidden');
            return;
        }
        if (empty) empty.classList.add('hidden');
        tbody.innerHTML = items.map(t => `
            <tr class="border-b border-[var(--border)] text-sm">
                <td class="px-4 py-3 font-mono text-[11px] font-bold">${escapeHtml(t.protocolo)}</td>
                <td class="px-4 py-3 max-w-[240px] truncate font-medium">${escapeHtml(t.titulo)}</td>
                <td class="px-4 py-3">${escapeHtml(t.solicitante)}</td>
                <td class="px-4 py-3">${escapeHtml(t.setor)}</td>
                <td class="px-4 py-3"><span class="sti-badge sti-badge-${escapeHtml(t.urgencia)}">${escapeHtml(t.urgencia)}</span></td>
                <td class="px-4 py-3"><span class="sti-badge ${statusClass(t.status)}">${escapeHtml(t.status)}</span></td>
                <td class="px-4 py-3 font-mono text-[11px] text-slate-400 whitespace-nowrap">${fmtTs(t.created_at)}</td>
                <td class="px-4 py-3">
                    <button type="button" onclick="ctiOpen(${t.id})" class="text-xs font-bold" style="color: var(--primary);">Abrir</button>
                </td>
            </tr>
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

    function highlightStatus(st) {
        _pickedStatus = st;
        document.querySelectorAll('#cti-status-btns .cti-st-btn').forEach(btn => {
            btn.classList.toggle('is-active', btn.dataset.st === st);
        });
    }

    window.ctiOpen = async function (id) {
        const resp = await fetch('/api/solicitacoes_ti/chamados/' + id, { cache: 'no-store' });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.ticket) {
            if (typeof toast === 'function') toast(data.message || 'Não foi possível abrir o chamado.', 'error');
            return;
        }
        const t = data.ticket;
        _openId = t.id;
        $('cti-modal-proto').textContent = t.protocolo || '';
        $('cti-modal-title').textContent = t.titulo || '';
        $('cti-nota').value = '';
        highlightStatus(t.status);
        $('cti-modal-body').innerHTML = `
            <div class="flex flex-wrap gap-2">
                <span class="sti-badge ${statusClass(t.status)}">${escapeHtml(t.status)}</span>
                <span class="sti-badge sti-badge-${escapeHtml(t.urgencia)}">${escapeHtml(t.urgencia)}</span>
            </div>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Solicitante</span><br>${escapeHtml(t.solicitante)} · ${escapeHtml(t.setor)}</p>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Categoria</span><br>${escapeHtml(t.categoria)}</p>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Descrição</span><br>${escapeHtml(t.descricao).replace(/\n/g, '<br>')}</p>
            ${t.observacoes ? `<p><span class="text-slate-500 text-xs uppercase font-bold">Observações</span><br>${escapeHtml(t.observacoes)}</p>` : ''}
            <div>
                <p class="text-slate-500 text-xs uppercase font-bold mb-2">Histórico</p>
                ${renderTimeline(data.eventos)}
            </div>
        `;
        $('cti-modal').classList.remove('hidden');
    };

    window.ctiCloseModal = function () {
        const m = $('cti-modal');
        if (m) m.classList.add('hidden');
        _openId = null;
    };

    window.ctiSaveStatus = async function () {
        if (!_openId || !_pickedStatus) return;
        const nota = ($('cti-nota')?.value || '').trim();
        const btn = $('cti-save-status');
        if (btn) btn.disabled = true;
        try {
            const resp = await fetch('/api/solicitacoes_ti/chamados/' + _openId + '/status', {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: _pickedStatus, nota }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || data.ok === false) {
                if (typeof toast === 'function') toast(data.message || 'Falha ao salvar.', 'error');
                return;
            }
            if (typeof toast === 'function') toast(data.message || 'Status atualizado.', 'success');
            await ctiOpen(_openId);
            await loadList();
        } catch (e) {
            console.error(e);
            if (typeof toast === 'function') toast('Falha de rede.', 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    };

    window.ctiSetStatus = function (st) {
        _status = st || '';
        document.querySelectorAll('#cti-tabs .cti-tab').forEach(b => {
            b.classList.toggle('is-active', (b.dataset.status || '') === _status);
        });
        loadList();
    };

    function bind() {
        document.querySelectorAll('#cti-tabs .cti-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                _status = btn.dataset.status || '';
                document.querySelectorAll('#cti-tabs .cti-tab').forEach(b => b.classList.toggle('is-active', b === btn));
                loadList();
            });
        });
        document.querySelectorAll('#cti-status-btns .cti-st-btn').forEach(btn => {
            btn.addEventListener('click', () => highlightStatus(btn.dataset.st));
        });
        ['cti-urgencia', 'cti-setor'].forEach(id => {
            const el = $(id);
            if (el) el.addEventListener('change', loadList);
        });
        const q = $('cti-q');
        if (q) {
            q.addEventListener('input', () => {
                clearTimeout(_qTimer);
                _qTimer = setTimeout(loadList, 250);
            });
        }
        const modal = $('cti-modal');
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) ctiCloseModal();
            });
        }
    }

    let _bound = false;
    window.loadChamadosTi = function () {
        if (!_bound) { bind(); _bound = true; }
        loadList().catch(err => console.error(err));
    };
})();
