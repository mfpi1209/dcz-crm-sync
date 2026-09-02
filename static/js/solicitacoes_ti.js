/* Solicitações TI — formulário que grava chamados no Postgres. */
(function () {
    let _stiInited = false;

    function $(id) { return document.getElementById(id); }
    function show(id) { const el = $(id); if (el) el.classList.remove('hidden'); }
    function hide(id) { const el = $(id); if (el) el.classList.add('hidden'); }
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
        if (!iso) return '';
        try {
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return escapeHtml(iso);
            return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
        } catch (e) {
            return escapeHtml(iso);
        }
    }

    async function renderRecent() {
        const list = $('sti-local-list');
        const empty = $('sti-local-empty');
        const count = $('sti-local-count');
        if (!list) return;
        try {
            const resp = await fetch('/api/solicitacoes_ti/meus?limit=8', { cache: 'no-store' });
            const data = await resp.json().catch(() => ({}));
            const tickets = Array.isArray(data.items) ? data.items : [];
            if (count) count.textContent = String(data.total != null ? data.total : tickets.length);
            if (!tickets.length) {
                list.classList.add('hidden');
                list.innerHTML = '';
                if (empty) empty.classList.remove('hidden');
                return;
            }
            if (empty) empty.classList.add('hidden');
            list.classList.remove('hidden');
            list.innerHTML = tickets.map(t => {
                const titulo = escapeHtml(t.titulo || '—');
                const solic = escapeHtml(t.solicitante || '—');
                const setor = escapeHtml(t.setor || '');
                const ts = fmtTs(t.created_at);
                const urg = escapeHtml(t.urgencia || 'Média');
                const id = escapeHtml(t.protocolo || '');
                const st = t.status || 'Pendente';
                const setorHtml = setor ? ` (${escapeHtml(setor)})` : '';
                return `
                <div class="sti-local-card space-y-2 cursor-pointer" onclick="navigate('meus_chamados_ti')">
                    <div class="flex justify-between items-center gap-2">
                        <span class="font-mono font-bold text-[10px] px-2 py-0.5 rounded"
                              style="background: var(--bg-elevated); color: var(--text-primary);">${id}</span>
                        <span class="sti-badge ${statusClass(st)}">${escapeHtml(st)}</span>
                    </div>
                    <div class="space-y-0.5">
                        <h5 class="font-bold text-xs text-[var(--text-primary)] dark:text-white truncate">${titulo}</h5>
                    </div>
                    <div class="flex justify-between items-center text-[10px] text-slate-400 dark:text-slate-500 pt-1.5 border-t border-[var(--border)]">
                        <span class="truncate max-w-[140px] font-medium">${solic}${setorHtml}</span>
                        <span class="sti-badge sti-badge-${urg}">${urg}</span>
                    </div>
                    <div class="text-[10px] font-mono text-slate-400">${ts}</div>
                </div>`;
            }).join('');
        } catch (e) {
            console.warn('sti renderRecent', e);
        }
    }

    function setUrgencia(value) {
        const group = $('sti-urgencia-group');
        if (!group) return;
        group.dataset.value = value;
        group.querySelectorAll('.sti-urg-btn').forEach(btn => {
            btn.classList.toggle('is-active', btn.dataset.urgencia === value);
        });
    }

    function getUrgencia() {
        const group = $('sti-urgencia-group');
        return (group && group.dataset.value) || 'Média';
    }

    function showError(msg) {
        const box = $('sti-error');
        const txt = $('sti-error-text');
        if (txt) txt.textContent = msg || 'Erro desconhecido.';
        if (box) box.classList.remove('hidden');
    }

    function clearError() { hide('sti-error'); }

    function setBusy(busy) {
        const btn = $('sti-submit-btn');
        if (btn) {
            btn.disabled = !!busy;
            btn.style.opacity = busy ? '0.6' : '1';
            btn.style.cursor = busy ? 'wait' : 'pointer';
        }
        if (busy) { show('sti-loading'); } else { hide('sti-loading'); }
    }

    function showSuccess(ticket) {
        const id = $('sti-success-id');
        if (id) id.textContent = ticket && ticket.protocolo
            ? `Protocolo: ${ticket.protocolo}`
            : '';
        hide('sti-form');
        show('sti-success');
    }

    window.stiResetForm = function () {
        const titulo = $('sti-titulo');
        const desc = $('sti-descricao');
        const obs = $('sti-observacoes');
        if (titulo) titulo.value = '';
        if (desc) desc.value = '';
        if (obs) obs.value = '';
        clearError();
        hide('sti-success');
        show('sti-form');
        const t = $('sti-titulo');
        if (t) t.focus();
    };

    window.stiSubmit = async function () {
        clearError();
        const solicitante = ($('sti-solicitante')?.value || '').trim();
        const setor = ($('sti-setor')?.value || '').trim();
        const categoria = ($('sti-categoria')?.value || '').trim();
        const titulo = ($('sti-titulo')?.value || '').trim();
        const descricao = ($('sti-descricao')?.value || '').trim();
        const observacoes = ($('sti-observacoes')?.value || '').trim();
        const urgencia = getUrgencia();

        if (!solicitante || !setor || !categoria || !titulo || !descricao) {
            showError('Preencha todos os campos obrigatórios (marcados com *).');
            return;
        }

        setBusy(true);
        try {
            const resp = await fetch('/api/solicitacoes_ti/submit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    solicitante, setor, categoria, urgencia,
                    titulo, descricao, observacoes,
                }),
            });
            let data = {};
            try { data = await resp.json(); } catch (_) { data = {}; }

            if (!resp.ok || data.status === 'error' || data.ok === false) {
                showError(data.message || `Erro HTTP ${resp.status}.`);
                return;
            }

            const ticket = data.ticket || {};
            await renderRecent();
            showSuccess(ticket);

            try {
                if (typeof toast === 'function') {
                    toast(`Chamado ${ticket.protocolo || ''} enviado!`.trim(), 'success');
                }
            } catch (_) { /* utils opcional */ }
        } catch (e) {
            console.error('stiSubmit', e);
            showError('Falha de rede ao enviar. Tente novamente.');
        } finally {
            setBusy(false);
        }
    };

    async function loadConfig() {
        try {
            const resp = await fetch('/api/solicitacoes_ti/config', { cache: 'no-store' });
            if (!resp.ok) return;
            const data = await resp.json();
            if (!data || !data.ok) return;
            const sol = $('sti-solicitante');
            if (sol && !sol.value && data.default_solicitante) {
                sol.value = data.default_solicitante;
            }
        } catch (e) {
            console.warn('sti loadConfig falhou', e);
        }
    }

    function bindUrgenciaButtons() {
        const group = $('sti-urgencia-group');
        if (!group) return;
        group.querySelectorAll('.sti-urg-btn').forEach(btn => {
            btn.addEventListener('click', () => setUrgencia(btn.dataset.urgencia));
        });
    }

    window.loadSolicitacoesTi = function () {
        if (!_stiInited) {
            _stiInited = true;
            bindUrgenciaButtons();
            setUrgencia('Média');
        }
        loadConfig();
        renderRecent();
        clearError();
        hide('sti-success');
        show('sti-form');
        hide('sti-loading');
    };
})();
