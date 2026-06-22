/* Solicitações TI — formulário que grava chamados no Google Sheets.
 *
 * Conversa apenas com o backend Flask (`/api/solicitacoes_ti/*`); o Flask atua
 * como proxy pro Google Apps Script. Esse modelo evita CORS e dispensa OAuth
 * do usuário final (a permissão de escrita fica no proprietário do Apps Script).
 *
 * Histórico recente fica em `localStorage` (chave `sti_local_tickets_v1`) só
 * pra referência rápida do próprio usuário — a fonte de verdade é a planilha.
 */
(function () {
    const STORAGE_KEY = 'sti_local_tickets_v1';
    const MAX_LOCAL = 25;
    let _stiInited = false;
    let _stiConfig = null;

    function $(id) { return document.getElementById(id); }
    function show(id) { const el = $(id); if (el) el.classList.remove('hidden'); }
    function hide(id) { const el = $(id); if (el) el.classList.add('hidden'); }
    function escapeHtml(s) {
        return String(s ?? '').replace(/[&<>"']/g, c => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
        ));
    }

    function getLocalTickets() {
        try {
            const raw = localStorage.getItem(STORAGE_KEY);
            if (!raw) return [];
            const arr = JSON.parse(raw);
            return Array.isArray(arr) ? arr : [];
        } catch (e) {
            return [];
        }
    }

    function saveLocalTickets(arr) {
        try {
            localStorage.setItem(STORAGE_KEY, JSON.stringify(arr.slice(0, MAX_LOCAL)));
        } catch (e) { /* quota cheia: ignora */ }
    }

    function renderLocalHistory() {
        const tickets = getLocalTickets();
        const list = $('sti-local-list');
        const empty = $('sti-local-empty');
        const clear = $('sti-clear-local');
        const count = $('sti-local-count');
        if (!list) return;
        if (count) count.textContent = String(tickets.length);
        if (!tickets.length) {
            list.classList.add('hidden');
            list.innerHTML = '';
            if (empty) empty.classList.remove('hidden');
            if (clear) clear.classList.add('hidden');
            return;
        }
        if (empty) empty.classList.add('hidden');
        if (clear) clear.classList.remove('hidden');
        list.classList.remove('hidden');
        list.innerHTML = tickets.map(t => {
            const titulo = escapeHtml(t.titulo || '—');
            const desc = escapeHtml(t.descricao || '');
            const solic = escapeHtml(t.solicitante || '—');
            const setor = escapeHtml((t.setor || '').split(' ')[0] || '');
            const ts = escapeHtml(t.timestamp || '');
            const urg = escapeHtml(t.urgencia || 'Média');
            const id = escapeHtml(t.id || '');
            const setorHtml = setor ? ` (${setor})` : '';
            return `
                <div class="sti-local-card space-y-2">
                    <div class="flex justify-between items-center gap-2">
                        <span class="font-mono font-bold text-[10px] px-2 py-0.5 rounded"
                              style="background: var(--bg-elevated); color: var(--text-primary);">${id}</span>
                        <span class="sti-badge sti-badge-${urg}">${urg}</span>
                    </div>
                    <div class="space-y-0.5">
                        <h5 class="font-bold text-xs text-[var(--text-primary)] dark:text-white truncate">${titulo}</h5>
                        <p class="text-[11px] text-slate-500 dark:text-slate-400 leading-snug line-clamp-2">${desc}</p>
                    </div>
                    <div class="flex justify-between items-center text-[10px] text-slate-400 dark:text-slate-500 pt-1.5 border-t border-[var(--border)]">
                        <span class="truncate max-w-[140px] font-medium">${solic}${setorHtml}</span>
                        <span class="font-mono">${ts}</span>
                    </div>
                </div>`;
        }).join('');
    }

    window.stiClearLocal = function () {
        if (!window.confirm('Limpar o histórico local de chamados deste navegador? Os registros no Google Sheets NÃO serão afetados.')) {
            return;
        }
        try { localStorage.removeItem(STORAGE_KEY); } catch (e) { /* ignora */ }
        renderLocalHistory();
    };

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
        const aba = $('sti-success-aba');
        if (id) id.textContent = ticket && ticket.id ? `Protocolo: ${ticket.id} · ${ticket.timestamp || ''}` : '';
        if (aba && _stiConfig && _stiConfig.sheet_name) aba.textContent = _stiConfig.sheet_name;
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

            if (!resp.ok || data.status === 'error') {
                showError(data.message || `Erro HTTP ${resp.status}.`);
                return;
            }

            const ticket = data.ticket || {
                id: '', timestamp: new Date().toLocaleString('pt-BR'),
                solicitante, setor, categoria, urgencia, titulo, descricao, observacoes,
            };
            const arr = [ticket, ...getLocalTickets()];
            saveLocalTickets(arr);
            renderLocalHistory();
            showSuccess(ticket);

            try {
                if (typeof toast === 'function') {
                    toast(`Chamado ${ticket.id || ''} enviado!`.trim(), 'success');
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
            _stiConfig = data;

            const pill = $('sti-config-pill');
            const pillText = $('sti-config-pill-text');
            if (pill && pillText) {
                pillText.textContent = `${data.sheet_name || 'Solicitações'} · ${data.sheet_id_masked || ''}`;
                pill.classList.remove('hidden');
                pill.classList.add('flex');
            }
            const aba = $('sti-aba-label');
            if (aba) aba.textContent = data.sheet_name || 'Solicitações';
            const sucAba = $('sti-success-aba');
            if (sucAba) sucAba.textContent = data.sheet_name || 'Solicitações';

            if (!data.webhook_configured) {
                show('sti-no-webhook');
            } else {
                hide('sti-no-webhook');
            }
            // Pré-preenche o solicitante com o username, se vazio
            const sol = $('sti-solicitante');
            if (sol && !sol.value && data.default_solicitante) {
                const friendly = data.default_solicitante.split('@')[0]
                    .replace(/[._-]+/g, ' ')
                    .replace(/\b\w/g, c => c.toUpperCase())
                    .trim();
                sol.value = friendly;
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
        renderLocalHistory();
        clearError();
        hide('sti-success');
        show('sti-form');
    };
})();
