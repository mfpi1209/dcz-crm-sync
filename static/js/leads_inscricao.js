// ---------------------------------------------------------------------------
// Leads em Inscrição Automática (ferramenta)
// Consulta um webhook do n8n para listar os leads atualmente em inscrição
// automática para o consultor selecionado.
// ---------------------------------------------------------------------------

const LI_WEBHOOK_URL = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/leads_inscricao_automatica";

// Mapeamento id -> nome do consultor (mantido no front por enquanto; pode ser
// movido para o backend caso a lista cresca).
const LI_CONSULTORES = [
    { id: "8239958",  nome: "Fran" },
    { id: "8240189",  nome: "Juliana" },
    { id: "8240438",  nome: "Claudia" },
    { id: "10329248", nome: "Andreina" },
    { id: "10729260", nome: "Jessica" },
    { id: "12158628", nome: "Hugo" },
    { id: "11741316", nome: "Bruno" },
    { id: "12908868", nome: "Diogo" },
    { id: "13018348", nome: "Kamilly" },
    { id: "13018360", nome: "Thais" },
    { id: "14205944", nome: "Thaina" },
    { id: "12209212", nome: "Gabriela" },
    { id: "14482884", nome: "Eduardo" },
    { id: "14464488", nome: "Tamires" },
];

const LI_KOMMO_BASE = "https://admamoeduitcombr.kommo.com/leads/detail/";

let _liLoaded = false;

function loadLeadsInscricao(force) {
    if (_liLoaded && !force) return;
    _liLoaded = true;
    _liPopulateConsultores();
    _liResetUI();
    _liTryAutoSelect();
}

function _liPopulateConsultores() {
    const sel = document.getElementById('li-consultor');
    if (!sel) return;
    if (sel.dataset.populated === '1') return;
    const sorted = LI_CONSULTORES
        .slice()
        .sort((a, b) => a.nome.localeCompare(b.nome, 'pt-BR'));
    const opts = ['<option value="">— Selecione —</option>'].concat(
        sorted.map(c => `<option value="${c.id}">${_liEsc(c.nome)}</option>`)
    );
    sel.innerHTML = opts.join('');
    sel.dataset.populated = '1';
}

// Tenta pré-selecionar o consultor com base no kommo_user_id do usuário logado
// (vindo do /api/me). Se houver match, dispara a listagem automaticamente.
async function _liTryAutoSelect() {
    const sel = document.getElementById('li-consultor');
    if (!sel) return;
    try {
        const res = await api('/api/me');
        const me = await res.json();
        const kid = me && me.kommo_user_id != null ? String(me.kommo_user_id) : '';
        if (!kid) return;
        const match = LI_CONSULTORES.find(c => c.id === kid);
        if (!match) return;
        sel.value = kid;
    } catch (_) {
        // Silenciosamente ignora — a UI permanece manual.
    }
}

function _liResetUI() {
    _liHideStatus();
    const result = document.getElementById('li-result');
    if (result) result.classList.add('hidden');
    const tbody = document.getElementById('li-tbody');
    if (tbody) tbody.innerHTML = '';
    const meta = document.getElementById('li-meta-bruto');
    if (meta) meta.textContent = '';
}

function _liShowStatus(message, kind) {
    const el = document.getElementById('li-status');
    if (!el) return;
    el.textContent = message;
    el.classList.remove('hidden');
    let border = 'var(--primary)';
    let color = 'var(--text-secondary)';
    if (kind === 'error') {
        border = '#ef4444';
        color = '#b91c1c';
    } else if (kind === 'warn') {
        border = '#f59e0b';
        color = '#92400e';
    } else if (kind === 'info') {
        border = 'var(--primary)';
    }
    el.style.borderLeftColor = border;
    el.style.color = color;
}

function _liHideStatus() {
    const el = document.getElementById('li-status');
    if (el) el.classList.add('hidden');
}

function _liSetLoading(isLoading) {
    const btn = document.getElementById('li-listar');
    const label = document.getElementById('li-listar-label');
    const icon = document.getElementById('li-listar-icon');
    if (!btn) return;
    btn.disabled = !!isLoading;
    btn.classList.toggle('opacity-70', !!isLoading);
    btn.classList.toggle('cursor-not-allowed', !!isLoading);
    if (label) label.textContent = isLoading ? 'Carregando...' : 'Listar leads';
    if (icon) icon.textContent = isLoading ? 'progress_activity' : 'search';
    if (icon) icon.classList.toggle('animate-spin', !!isLoading);
}

async function listarLeadsInscricao() {
    const sel = document.getElementById('li-consultor');
    const id = sel ? sel.value : '';
    if (!id) {
        _liShowStatus('Por favor, selecione seu nome antes de listar os leads.', 'warn');
        const result = document.getElementById('li-result');
        if (result) result.classList.add('hidden');
        return;
    }

    const consultor = LI_CONSULTORES.find(c => c.id === id);
    const consultorNome = consultor ? consultor.nome : '';

    _liShowStatus('Consultando webhook...', 'info');
    _liSetLoading(true);
    const result = document.getElementById('li-result');
    if (result) result.classList.add('hidden');

    try {
        const resp = await fetch(LI_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                origem: 'painel_eduit',
                consultor_nome: consultorNome,
                responsible_user_id: Number(id),
            }),
        });
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const json = await resp.json();
        const payload = Array.isArray(json) ? (json[0] || {}) : (json || {});
        const filtro = payload.filtro || {};
        const data = {
            consultor_nome: filtro.consultor_nome_resolvido
                || filtro.responsible_user
                || consultorNome
                || '',
            consultor_status: filtro.consultor_status_resolvido || 'ATIVO',
            qtd_leads: payload.qtd_leads_do_consultor != null
                ? Number(payload.qtd_leads_do_consultor)
                : 0,
            lead_ids: Array.isArray(payload.lead_ids_do_consultor)
                ? payload.lead_ids_do_consultor
                : [],
            total_bruto: payload.total_leads_bruto != null
                ? Number(payload.total_leads_bruto)
                : null,
        };

        if (!data.qtd_leads || !data.lead_ids.length) {
            _liShowStatus('Nenhum lead encontrado para você neste momento.', 'info');
            return;
        }

        _liHideStatus();
        _liRenderResult(data);
    } catch (err) {
        console.error('listarLeadsInscricao', err);
        _liShowStatus('Ocorreu um erro ao listar os leads. Veja o console para detalhes.', 'error');
    } finally {
        _liSetLoading(false);
    }
}

function _liRenderResult(data) {
    const tbody = document.getElementById('li-tbody');
    const result = document.getElementById('li-result');
    const meta = document.getElementById('li-meta-bruto');
    if (!tbody || !result) return;

    const statusPill = `<span class="inline-flex items-center gap-1 px-2.5 py-0.5 rounded-full text-[10px] font-semibold"
                              style="background: rgba(16,185,129,0.12); color: #047857; border: 1px solid rgba(16,185,129,0.25);">
                              <span class="material-symbols-outlined text-[12px]">check_circle</span>
                              ${_liEsc(data.consultor_status)}
                          </span>`;

    const qtdBadge = `<span class="inline-flex items-center justify-center min-w-[28px] h-7 px-2.5 rounded-full text-xs font-bold"
                            style="background: rgba(0,52,111,0.10); color: var(--primary); border: 1px solid rgba(0,52,111,0.20);">
                            ${data.qtd_leads}
                      </span>`;

    const ids = data.lead_ids.map(id => `<a href="${LI_KOMMO_BASE}${encodeURIComponent(id)}"
            target="_blank" rel="noopener noreferrer"
            class="li-lead-pill"
            title="Abrir lead ${_liEsc(String(id))} no Kommo">
            <span class="material-symbols-outlined text-[13px]">open_in_new</span>
            ${_liEsc(String(id))}
        </a>`).join('');

    tbody.innerHTML = `<tr class="border-t border-[var(--border)]" style="transition: background var(--transition-fast);">
        <td class="px-5 py-4 align-top font-medium" style="color: var(--text-primary)">${_liEsc(data.consultor_nome)}</td>
        <td class="px-5 py-4 align-top">${statusPill}</td>
        <td class="px-5 py-4 align-top">${qtdBadge}</td>
        <td class="px-5 py-4 align-top">
            <div class="flex flex-wrap gap-2 max-w-2xl">${ids}</div>
        </td>
    </tr>`;
    result.classList.remove('hidden');

    if (meta) {
        meta.textContent = data.total_bruto != null
            ? `Total bruto: ${data.total_bruto}`
            : '';
    }
}

function _liEsc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}
