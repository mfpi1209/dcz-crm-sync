// ---------------------------------------------------------------------------
// Leads · Promotores
// ---------------------------------------------------------------------------
const LP_WEBHOOK_URL = "https://n8n-new-n8n.ca31ey.easypanel.host/webhook/count_promotores";

const lpState = {
    initialized: false,
    dados: [],
    filtro: ""
};

function lpSetBadge(text, kind) {
    const el = document.getElementById('lp-badge');
    if (!el) return;
    el.textContent = text;
    el.className = 'lp-badge lp-badge-' + (kind || 'idle');
}

function lpShowStatus(text, kind) {
    const el = document.getElementById('lp-status-msg');
    if (!el) return;
    if (!text) {
        el.className = 'lp-status-msg';
        el.textContent = '';
        el.style.display = 'none';
        return;
    }
    el.className = 'lp-status-msg ' + (kind || 'success');
    el.textContent = text;
}

function lpDataHojeBrasilISO() {
    const agora = new Date();
    const brasil = new Date(agora.toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
    const ano = brasil.getFullYear();
    const mes = String(brasil.getMonth() + 1).padStart(2, '0');
    const dia = String(brasil.getDate()).padStart(2, '0');
    return `${ano}-${mes}-${dia}`;
}

function lpPrimeiroDiaMesBrasilISO() {
    const agora = new Date();
    const brasil = new Date(agora.toLocaleString('en-US', { timeZone: 'America/Sao_Paulo' }));
    const ano = brasil.getFullYear();
    const mes = String(brasil.getMonth() + 1).padStart(2, '0');
    return `${ano}-${mes}-01`;
}

function lpFormatarDataBR(iso) {
    if (!iso) return '';
    const [ano, mes, dia] = iso.split('-');
    return `${dia}/${mes}/${ano}`;
}

function lpZerarCards() {
    document.getElementById('lp-total-leads').innerText = '0';
    document.getElementById('lp-total-sim').innerText = '0';
    document.getElementById('lp-total-nao').innerText = '0';
}

function lpNormalizarRespostaN8N(resposta) {
    if (Array.isArray(resposta)) {
        if (resposta.length > 0 && resposta[0].json) {
            return resposta.map(item => item.json);
        }
        return resposta;
    }
    if (resposta && resposta.data && Array.isArray(resposta.data)) return resposta.data;
    if (resposta && resposta.result && Array.isArray(resposta.result)) return resposta.result;
    if (resposta && resposta.rows && Array.isArray(resposta.rows)) return resposta.rows;
    return [];
}

function lpRenderizarCards(dados) {
    const totalLeads = dados.reduce((acc, item) => acc + Number(item.total_leads || 0), 0);
    const totalSim = dados.reduce((acc, item) => acc + Number(item.recadastro_sim || 0), 0);
    const totalNao = dados.reduce((acc, item) => acc + Number(item.recadastro_nao || 0), 0);
    document.getElementById('lp-total-leads').innerText = totalLeads.toLocaleString('pt-BR');
    document.getElementById('lp-total-sim').innerText = totalSim.toLocaleString('pt-BR');
    document.getElementById('lp-total-nao').innerText = totalNao.toLocaleString('pt-BR');
}

function lpRenderizarTabela(dados) {
    const body = document.getElementById('lp-table-body');
    const empty = document.getElementById('lp-empty-results');
    if (!body) return;

    if (!Array.isArray(dados) || dados.length === 0) {
        body.innerHTML = '';
        if (empty) empty.style.display = 'block';
        return;
    }

    if (empty) empty.style.display = 'none';
    body.innerHTML = dados.map(item => {
        const nome = item.consultor || item.promotor || 'Sem promotor';
        const total = Number(item.total_leads || 0).toLocaleString('pt-BR');
        const sim = Number(item.recadastro_sim || 0).toLocaleString('pt-BR');
        const nao = Number(item.recadastro_nao || 0).toLocaleString('pt-BR');
        return `
            <tr>
                <td>${nome}</td>
                <td class="lp-num">${total}</td>
                <td class="lp-num green">${sim}</td>
                <td class="lp-num red">${nao}</td>
            </tr>
        `;
    }).join('');
}

function lpAtualizarFiltro(dados) {
    const sel = document.getElementById('lp-filter');
    if (!sel) return;
    const atual = sel.value;
    const nomes = [...new Set(dados.map(d => d.consultor || d.promotor || 'Sem promotor'))].sort();
    sel.innerHTML = '<option value="">Todos os promotores</option>' +
        nomes.map(n => `<option value="${n.replace(/"/g, '&quot;')}">${n}</option>`).join('');
    if (nomes.includes(atual)) sel.value = atual;
}

function lpAplicarFiltro() {
    const sel = document.getElementById('lp-filter');
    lpState.filtro = sel ? sel.value : '';
    const filtrados = lpState.filtro
        ? lpState.dados.filter(d => (d.consultor || d.promotor || 'Sem promotor') === lpState.filtro)
        : lpState.dados;
    lpRenderizarTabela(filtrados);
    lpRenderizarCards(filtrados);
}

async function lpBuscarDados() {
    const startInput = document.getElementById('lp-start-date');
    const endInput = document.getElementById('lp-end-date');
    const btn = document.getElementById('lp-btn-submit');
    const subEl = document.getElementById('lp-results-sub');

    const startDate = startInput ? startInput.value : '';
    const endDate = endInput ? endInput.value : '';

    if (!startDate || !endDate) {
        lpShowStatus('Selecione a data inicial e a data final.', 'warn');
        lpSetBadge('Parâmetros incompletos', 'err');
        return;
    }
    if (startDate > endDate) {
        lpShowStatus('A data inicial não pode ser maior que a data final.', 'warn');
        lpSetBadge('Período inválido', 'err');
        return;
    }

    lpShowStatus('', null);
    lpSetBadge('Consultando...', 'loading');
    lpZerarCards();
    document.getElementById('lp-table-body').innerHTML = '';
    document.getElementById('lp-empty-results').style.display = 'none';

    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<span class="material-symbols-outlined text-base animate-spin">progress_activity</span> Consultando...';
    }

    const payload = { start_date: startDate, end_date: endDate };

    try {
        const response = await fetch(LP_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const textoResposta = await response.text();
        if (!response.ok) {
            throw new Error(`Erro HTTP ${response.status}\n\nResposta:\n${textoResposta}`);
        }

        let dados;
        try {
            dados = JSON.parse(textoResposta);
        } catch (e) {
            throw new Error('O webhook respondeu, mas não retornou JSON válido.\n\nResposta recebida:\n' + textoResposta);
        }

        dados = lpNormalizarRespostaN8N(dados);

        if (!Array.isArray(dados) || dados.length === 0) {
            lpState.dados = [];
            lpAtualizarFiltro([]);
            lpRenderizarTabela([]);
            lpZerarCards();
            lpSetBadge('Sem dados', 'idle');
            if (subEl) subEl.textContent = `Período: ${lpFormatarDataBR(startDate)} a ${lpFormatarDataBR(endDate)} · Nenhum registro encontrado.`;
            return;
        }

        lpState.dados = dados;
        lpAtualizarFiltro(dados);
        lpAplicarFiltro();
        lpSetBadge('OK', 'ok');
        if (subEl) subEl.textContent = `Período: ${lpFormatarDataBR(startDate)} a ${lpFormatarDataBR(endDate)} · ${dados.length} promotor(es).`;

    } catch (err) {
        lpShowStatus(err.message || 'Falha ao consultar o webhook.', 'error');
        lpSetBadge('Erro', 'err');
        if (subEl) subEl.textContent = 'Falha na consulta. Verifique o período e tente novamente.';
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<span class="material-symbols-outlined text-base">search</span> Buscar';
        }
    }
}

function loadLeadsPromotores() {
    const startInput = document.getElementById('lp-start-date');
    const endInput = document.getElementById('lp-end-date');
    if (startInput && !startInput.value) startInput.value = lpPrimeiroDiaMesBrasilISO();
    if (endInput && !endInput.value) endInput.value = lpDataHojeBrasilISO();

    if (!lpState.initialized) {
        lpState.initialized = true;
        lpBuscarDados();
    }
}
