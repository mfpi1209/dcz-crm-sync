// ---------------------------------------------------------------------------
// Ativações Acadêmicas
// ---------------------------------------------------------------------------
const ATIV_WEBHOOK = "https://n8n-webhook.0y1csu.easypanel.host/webhook/disparos";

function loadAtivacoes() {
    const hoje = new Date();
    const sete = new Date();
    sete.setDate(hoje.getDate() - 7);

    const elInicio = document.getElementById('ativ-data-inicio');
    const elFim    = document.getElementById('ativ-data-fim');
    if (!elInicio.value) elInicio.value = sete.toISOString().substring(0, 10);
    if (!elFim.value)    elFim.value    = hoje.toISOString().substring(0, 10);

    ativAtualizar();
}

async function ativAtualizar() {
    const dataInicio = document.getElementById('ativ-data-inicio').value;
    const dataFim    = document.getElementById('ativ-data-fim').value;

    if (!dataInicio || !dataFim) {
        _ativErro('Selecione o período.');
        return;
    }

    _ativLoading(true);
    _ativErro('');

    const qs = `data_inicio=${dataInicio}&data_fim=${dataFim}`;

    try {
        const res = await fetch(`${ATIV_WEBHOOK}?${qs}`);
        const data = await res.json();

        if (Array.isArray(data)) {
            _ativRenderEvolucao(data);
            const totais = data.reduce((acc, d) => {
                acc.total += d.total_ids || 0;
                acc.entrou += d.entrou_em_atendimento || 0;
                acc.sem += d.sem_resposta || 0;
                acc.conseguiu += d.conseguiu_fazer || 0;
                return acc;
            }, { total: 0, entrou: 0, sem: 0, conseguiu: 0 });
            _ativRenderCards(totais.total, totais.entrou, totais.sem, totais.conseguiu);
        } else {
            _ativRenderCards(
                data.total_ids || 0,
                data.entrou_em_atendimento || 0,
                data.sem_resposta || 0,
                data.conseguiu_fazer || 0
            );
            _ativRenderEvolucao([]);
        }
    } catch (e) {
        _ativErro('Erro ao buscar dados: ' + e.message);
    } finally {
        _ativLoading(false);
    }
}

function _ativRenderCards(total, entrou, sem, conseguiu) {
    document.getElementById('ativ-total').textContent     = total.toLocaleString('pt-BR');
    document.getElementById('ativ-entrou').textContent     = entrou.toLocaleString('pt-BR');
    document.getElementById('ativ-sem-resp').textContent   = sem.toLocaleString('pt-BR');
    document.getElementById('ativ-conseguiu').textContent  = conseguiu.toLocaleString('pt-BR');
}

function _ativRenderEvolucao(rows) {
    const tbody = document.getElementById('ativ-evolucao-body');
    if (!rows.length) {
        tbody.innerHTML = '<tr><td colspan="2" class="px-5 py-6 text-center text-slate-600">Nenhum dado para o período</td></tr>';
        return;
    }
    tbody.innerHTML = rows.map(d =>
        `<tr class="hover:bg-white/[0.02]">
            <td class="px-5 py-2.5 text-slate-300">${esc(d.data || '')}</td>
            <td class="px-5 py-2.5 text-slate-200 font-semibold">${(d.total_ids || 0).toLocaleString('pt-BR')}</td>
        </tr>`
    ).join('');
}

function _ativLoading(show) {
    document.getElementById('ativ-loading').classList.toggle('hidden', !show);
}

function _ativErro(msg) {
    const el = document.getElementById('ativ-erro');
    el.textContent = msg;
    el.classList.toggle('hidden', !msg);
}
