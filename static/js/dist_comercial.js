// ---------------------------------------------------------------------------
// Distribuição Comercial — Dashboard
// ---------------------------------------------------------------------------

const DIST_API_LOAD = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/distribuicaocomercial';
const DIST_API_SAVE = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/edicao_distrib';
const DIST_API_CREATE = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/criar_consultor';

const dcState = {
    data: [],
    initialData: [],
    loading: false,
    initialized: false,
    // Turno map global (id_lead string -> 'dia' | 'noite'). Carregado do
    // backend em loadDistComercial e sincronizado apos cada toggle. Cache
    // aqui evita ida ao servidor em cada render.
    turnoMap: {},
    // Flag: setada quando o gestor clica TURNO DIA/NOITE. dcSalvar la em
    // baixo detecta e envia o snapshot pro backend depois do save.
    pendingSnapshot: null,   // null | 'dia' | 'noite'
    rules: [],
    snapshots: { dia: null, noite: null }
};

function dcEscapeHtml(str) {
    return String(str ?? '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function dcAbrirModalAdicionar() {
    const modal = document.getElementById('dist-modal-add');
    const form = document.getElementById('dist-form-add');
    if (form) form.reset();
    const qtd = document.getElementById('dist-add-qtd');
    if (qtd) qtd.value = '1';
    const status = document.getElementById('dist-add-status');
    if (status) status.value = 'ATIVO';
    if (modal) {
        if (typeof dczPortalToBody === 'function') dczPortalToBody(modal);
        modal.classList.add('is-open');
        if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(true);
    }
    const nome = document.getElementById('dist-add-nome');
    if (nome) setTimeout(() => nome.focus(), 50);
}

function dcFecharModalAdicionar(event) {
    if (event && event.target !== event.currentTarget) return;
    const modal = document.getElementById('dist-modal-add');
    if (modal) modal.classList.remove('is-open');
    if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(false);
}

function dcIdLeadJaExiste(idLead) {
    const id = Number(idLead);
    return dcState.data.some(p => Number(p.id_lead) === id);
}

async function dcSubmitAdicionar(event) {
    event.preventDefault();

    const nome = (document.getElementById('dist-add-nome')?.value || '').trim();
    const idLeadRaw = document.getElementById('dist-add-id-lead')?.value || '';
    const idLead = parseInt(idLeadRaw, 10);
    const status = document.getElementById('dist-add-status')?.value || 'ATIVO';
    let qtd = parseInt(document.getElementById('dist-add-qtd')?.value, 10) || 1;
    qtd = Math.min(5, Math.max(1, qtd));
    const observacao = (document.getElementById('dist-add-obs')?.value || '').trim();

    if (!nome) {
        dcShowNotification('Informe o nome do consultor', 'error');
        return;
    }
    if (!Number.isFinite(idLead) || idLead <= 0) {
        dcShowNotification('ID Kommo inválido', 'error');
        return;
    }
    if (dcIdLeadJaExiste(idLead)) {
        dcShowNotification('Este ID Kommo já está cadastrado no painel', 'error');
        return;
    }

    const btn = document.getElementById('dist-btn-add-submit');
    if (btn) btn.disabled = true;

    try {
        const response = await fetch(DIST_API_CREATE, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                nome,
                id_lead: idLead,
                status,
                quantidade_leads: qtd,
                observacao,
                timestamp: new Date().toISOString()
            })
        });

        if (!response.ok) {
            const errText = await response.text().catch(() => '');
            throw new Error(errText || 'Erro ao cadastrar');
        }

        dcFecharModalAdicionar();
        dcShowNotification(`${nome} cadastrado com sucesso!`, 'success');
        await dcCarregarDados();
    } catch (error) {
        console.error('Erro ao adicionar consultor:', error);
        dcShowNotification('Erro ao cadastrar. Verifique o webhook criar_consultor no n8n.', 'error');
    } finally {
        if (btn) btn.disabled = false;
    }
}

async function dcCarregarDados() {
    const btnRefresh = document.getElementById('dist-btn-refresh');
    const content = document.getElementById('dist-content');
    
    dcState.loading = true;
    if (btnRefresh) btnRefresh.disabled = true;
    
    content.innerHTML = `
        <div class="dist-loading">
            <div class="dist-spinner"></div>
            <p class="dist-loading-text">Carregando dados...</p>
        </div>
    `;
    
    try {
        const response = await fetch(DIST_API_LOAD, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' }
        });
        
        if (!response.ok) throw new Error('Erro ao carregar dados');
        
        const dados = await response.json();
        dcState.data = dados;
        dcState.initialData = JSON.parse(JSON.stringify(dados));
        
        document.getElementById('dist-count').textContent = `${dados.length} registros`;
        dcRenderTable();
        dcShowNotification(`${dados.length} registros carregados com sucesso!`, 'success');
        
    } catch (error) {
        console.error('Erro ao carregar dados:', error);
        content.innerHTML = `
            <div class="dist-empty">
                <p>Erro ao carregar dados. Tente novamente.</p>
            </div>
        `;
    } finally {
        dcState.loading = false;
        if (btnRefresh) btnRefresh.disabled = false;
    }
}

// ── Turnos (Dia / Noite) ── persistencia global via backend.
//
// O turno de cada consultor (dia/noite) vive em dist_comercial_turno_map no
// banco — todos os gestores compartilham a mesma divisao (decisao AGENTS.md
// 2026-07-07). Cache em memoria (dcState.turnoMap) evita ida ao servidor a
// cada render; qualquer toggle envia PUT com o mapa completo pro backend.
//
// Chave stable = id_lead (responsible_user_id no Kommo).

function dcTurnoKey(pessoa) {
    return String(pessoa && (pessoa.id_lead ?? pessoa.id) || '').trim();
}

function dcTurnoIsNoite(pessoa) {
    const k = dcTurnoKey(pessoa);
    if (!k) return false;
    return (dcState.turnoMap[k] === 'noite');
}

async function dcTurnoMapLoad() {
    try {
        const r = await fetch('/api/dist-comercial/turno-map');
        if (!r.ok) throw new Error('http ' + r.status);
        const data = await r.json();
        dcState.turnoMap = (data && data.map) ? { ...data.map } : {};
    } catch (e) {
        console.warn('Falha ao carregar turno-map:', e);
        dcState.turnoMap = {};
    }
}

async function dcTurnoMapPersist() {
    // Envia o mapa completo (server substitui). Consultores nao presentes
    // sao removidos do banco — semantica intencional.
    try {
        const r = await fetch('/api/dist-comercial/turno-map', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ map: dcState.turnoMap })
        });
        if (!r.ok) throw new Error('http ' + r.status);
    } catch (e) {
        console.warn('Falha ao persistir turno-map:', e);
        dcShowNotification('Falha ao salvar divisao dia/noite no servidor.', 'error');
    }
}

window.dcToggleConsultorTurno = function (idLead) {
    const k = String(idLead || '').trim();
    if (!k) return;
    if (dcState.turnoMap[k] === 'noite') {
        delete dcState.turnoMap[k];   // volta pro Dia (default)
    } else {
        dcState.turnoMap[k] = 'noite';
    }
    // Garante que ids do Dia tambem entrem no mapa quando forem categorizados
    // (mesmo que "dia" seja o default no render). Aqui so guardamos os que
    // estao explicitamente na noite; a rota `apply` no backend precisa saber
    // quem e do dia tambem — vamos garantir isso no dcSalvar (envia mapa
    // completo dia+noite baseado em quem esta em dcState.data).
    dcRenderTable();
    dcTurnoMapPersistCompleto();  // fire-and-forget
};

// Persistir dia+noite: pra cada consultor em dcState.data, define o turno
// (dia ou noite). Isso garante que o backend saiba quem e do dia (nao so
// quem esta marcado como noite).
async function dcTurnoMapPersistCompleto() {
    const complete = {};
    (dcState.data || []).forEach(p => {
        const k = dcTurnoKey(p);
        if (!k) return;
        complete[k] = (dcState.turnoMap[k] === 'noite') ? 'noite' : 'dia';
    });
    // Atualiza cache local pra ficar consistente com o que foi enviado
    dcState.turnoMap = complete;
    try {
        await fetch('/api/dist-comercial/turno-map', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ map: complete })
        });
    } catch (e) {
        console.warn('Falha ao persistir turno-map completo:', e);
    }
}

// Aplica um "modo turno" em massa: define status ATIVO/INATIVO pra cada
// consultor com base em qual turno ele esta (dia/noite).
// - turnoAlvo = 'dia'  -> Dia=ATIVO,   Noite=INATIVO
// - turnoAlvo = 'noite' -> Dia=INATIVO, Noite=ATIVO
// So altera dcState.data em memoria; o usuario ainda precisa clicar em
// SALVAR pra persistir via webhook (mesmo padrao dos outros campos).
window.dcAplicarTurno = async function (turnoAlvo) {
    if (turnoAlvo !== 'dia' && turnoAlvo !== 'noite') return;

    if (!Array.isArray(dcState.data) || dcState.data.length === 0) {
        dcShowNotification('Nenhum consultor carregado. Clique em ATUALIZAR primeiro.', 'error');
        return;
    }

    // Busca snapshot do turno alvo (se existir). Comportamento esperto:
    //  - Se snapshot existe: turno alvo recebe status do snapshot (preserva
    //    quem estava INATIVO por folga/ferias). Novos consultores no turno
    //    alvo mas fora do snapshot entram como ATIVO por default.
    //  - Se snapshot NAO existe (primeira aplicacao): comportamento bruto,
    //    todos do turno alvo ficam ATIVO.
    // Turno OPOSTO sempre vira INATIVO (comportamento constante).
    let snapPayload = null;
    try {
        const r = await fetch('/api/dist-comercial/snapshot');
        if (r.ok) {
            const d = await r.json();
            const snap = (d && d.snapshots && d.snapshots[turnoAlvo]) || null;
            if (snap && snap.payload && Object.keys(snap.payload).length > 0) {
                snapPayload = snap.payload;
                dcState.snapshots = d.snapshots || dcState.snapshots;
            }
        }
    } catch (e) {
        console.warn('Falha ao buscar snapshot pra aplicar turno (segue sem):', e);
    }

    const noiteMap = dcState.turnoMap || {};
    let ativados = 0;
    let inativados = 0;
    let semMudanca = 0;

    const previewChanges = [];
    dcState.data.forEach(p => {
        const k = dcTurnoKey(p);
        const isNoite = (k && noiteMap[k] === 'noite');
        const pertenceAoAlvo = (turnoAlvo === 'noite') ? isNoite : !isNoite;

        let novoStatus;
        if (pertenceAoAlvo) {
            if (snapPayload && k && (k in snapPayload)) {
                const v = String(snapPayload[k] || '').toUpperCase();
                novoStatus = (v === 'INATIVO') ? 'INATIVO' : 'ATIVO';
            } else {
                novoStatus = 'ATIVO';
            }
        } else {
            novoStatus = 'INATIVO';
        }

        if ((p.status || '').toUpperCase() !== novoStatus) {
            previewChanges.push({ pessoa: p, novoStatus });
            if (novoStatus === 'ATIVO') ativados++; else inativados++;
        } else {
            semMudanca++;
        }
    });

    if (previewChanges.length === 0) {
        dcShowNotification('Todos os consultores ja estao no modo ' + turnoAlvo.toUpperCase() + '.', 'info');
        return;
    }

    const nomeModo = turnoAlvo === 'noite' ? 'NOITE' : 'DIA';
    const modoTxt = snapPayload
        ? '(respeitando snapshot anterior — quem estava INATIVO permanece INATIVO)'
        : '(primeira aplicacao — todos do turno alvo ficam ATIVO)';
    const msg =
        'Aplicar Modo ' + nomeModo + '\n' + modoTxt + '\n\n' +
        '  ATIVAR:    ' + ativados + ' consultor(es)\n' +
        '  INATIVAR:  ' + inativados + ' consultor(es)\n' +
        '  Sem mudanca: ' + semMudanca + '\n\n' +
        'As mudancas ficam pendentes ate voce clicar em SALVAR.\n' +
        'Ao SALVAR, o snapshot ' + nomeModo + ' sera atualizado com o estado final.';
    if (!window.confirm(msg)) return;

    previewChanges.forEach(({ pessoa, novoStatus }) => {
        pessoa.status = novoStatus;
    });

    // Marca que o proximo SALVAR deve tirar snapshot deste turno. O backend
    // usa esse snapshot como "quem estava ativo/inativo na ultima vez" nas
    // regras automaticas E na proxima aplicacao manual (via este mesmo path).
    dcState.pendingSnapshot = turnoAlvo;

    dcRenderTable();
    dcShowNotification(
        'Modo ' + nomeModo + ' aplicado: ' + ativados + ' ativado(s), ' +
        inativados + ' inativado(s). Clique em SALVAR pra persistir e ' +
        'atualizar o snapshot do turno.',
        'success'
    );
};

function _dcRowHtml(pessoa, turnoAtual) {
    const key = dcTurnoKey(pessoa);
    const btnLabel = turnoAtual === 'noite' ? '&rarr; Dia' : '&rarr; Noite';
    const btnIcon  = turnoAtual === 'noite'
        ? '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>'
        : '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';
    return `
        <tr data-id="${pessoa.id}" data-turno="${turnoAtual}">
            <td>
                <div class="dist-nome-cell">
                    <span class="dist-nome">${dcEscapeHtml(pessoa.nome) || '—'}</span>
                    <span class="dist-status-badge ${pessoa.status === 'ATIVO' ? 'ativo' : 'inativo'}">
                        ${pessoa.status === 'ATIVO' ? 'Ativo' : 'Inativo'}
                    </span>
                </div>
            </td>
            <td class="center">
                <select class="dist-select" onchange="dcUpdatePessoa(${pessoa.id}, 'status', this.value)">
                    <option value="ATIVO" ${pessoa.status === 'ATIVO' ? 'selected' : ''}>Ativo</option>
                    <option value="INATIVO" ${pessoa.status === 'INATIVO' ? 'selected' : ''}>Inativo</option>
                </select>
            </td>
            <td class="center">
                <input type="number"
                       class="dist-input dist-input-number"
                       value="${pessoa.quantidade_leads || 1}"
                       min="1"
                       max="5"
                       onchange="dcUpdatePessoa(${pessoa.id}, 'quantidade_leads', parseInt(this.value) || 1)">
            </td>
            <td>
                <input type="text"
                       class="dist-input dist-input-obs"
                       value="${dcEscapeHtml(pessoa.observacao)}"
                       placeholder="Digite uma observação..."
                       onchange="dcUpdatePessoa(${pessoa.id}, 'observacao', this.value)">
            </td>
            <td class="center">
                <button type="button" class="dist-btn-turno"
                        title="Mover para o outro turno"
                        onclick="window.dcToggleConsultorTurno('${dcEscapeHtml(key)}')">
                    ${btnIcon}<span>${btnLabel}</span>
                </button>
            </td>
        </tr>
    `;
}

function _dcTableHtml(rows) {
    return `
        <div class="dist-table-wrapper">
            <table class="dist-table">
                <thead>
                    <tr>
                        <th style="min-width: 220px;">Nome</th>
                        <th class="center" style="min-width: 140px;">Status</th>
                        <th class="center" style="min-width: 160px;">Quantidade Leads</th>
                        <th style="min-width: 320px;">Observação</th>
                        <th class="center" style="min-width: 140px;">Turno</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows}
                </tbody>
            </table>
        </div>
    `;
}

function dcRenderTable() {
    const content = document.getElementById('dist-content');
    const filtro = document.getElementById('dist-filtro').value;

    let dados = dcState.data;
    if (filtro !== 'TODOS') {
        dados = dados.filter(p => p.status === filtro);
    }

    if (dados.length === 0) {
        content.innerHTML = `
            <div class="dist-empty">
                <p>Nenhum registro encontrado.</p>
            </div>
        `;
        return;
    }

    const noiteMap = dcState.turnoMap || {};
    const dia = [];
    const noite = [];
    dados.forEach(p => {
        const k = dcTurnoKey(p);
        if (k && noiteMap[k] === 'noite') noite.push(p);
        else dia.push(p);
    });

    const rowsDia = dia.map(p => _dcRowHtml(p, 'dia')).join('');
    const rowsNoite = noite.map(p => _dcRowHtml(p, 'noite')).join('');

    const iconSol = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
    const iconLua = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';

    const diaBlock = `
        <div class="dist-turno-section" data-turno="dia">
            <div class="dist-turno-header">
                <div class="dist-turno-title">
                    <span class="dist-turno-icon">${iconSol}</span>
                    <span>Consultores &mdash; Dia</span>
                    <span class="dist-turno-count">${dia.length}</span>
                </div>
            </div>
            ${dia.length
                ? _dcTableHtml(rowsDia)
                : '<div class="dist-turno-empty">Sem consultores no turno do dia com o filtro atual.</div>'
            }
        </div>
    `;

    const noiteBlock = `
        <div class="dist-turno-section" data-turno="noite">
            <div class="dist-turno-header">
                <div class="dist-turno-title">
                    <span class="dist-turno-icon dist-turno-icon--noite">${iconLua}</span>
                    <span>Consultores &mdash; Noite</span>
                    <span class="dist-turno-count">${noite.length}</span>
                </div>
            </div>
            ${noite.length
                ? _dcTableHtml(rowsNoite)
                : '<div class="dist-turno-empty">Sem consultores no turno da noite. Clique em <strong>&rarr; Noite</strong> em qualquer linha do dia pra mover.</div>'
            }
        </div>
    `;

    content.innerHTML = diaBlock + noiteBlock;
}

function dcUpdatePessoa(id, field, value) {
    const pessoa = dcState.data.find(p => p.id === id);
    if (pessoa) {
        pessoa[field] = value;
        
        // Atualizar badge visual se mudou status
        if (field === 'status') {
            const row = document.querySelector(`tr[data-id="${id}"]`);
            if (row) {
                const badge = row.querySelector('.dist-status-badge');
                if (badge) {
                    badge.className = `dist-status-badge ${value === 'ATIVO' ? 'ativo' : 'inativo'}`;
                    badge.textContent = value === 'ATIVO' ? 'Ativo' : 'Inativo';
                }
            }
        }
    }
}

function dcDetectarAlteracoes() {
    const alteracoes = [];
    
    dcState.data.forEach(pessoaAtual => {
        const pessoaInicial = dcState.initialData.find(p => p.id === pessoaAtual.id);
        if (!pessoaInicial) return;
        
        if (pessoaAtual.status !== pessoaInicial.status) {
            alteracoes.push({
                id_lead: pessoaAtual.id_lead,
                nome: pessoaAtual.nome,
                campo: 'status',
                valorAnterior: pessoaInicial.status,
                valorNovo: pessoaAtual.status,
                status: pessoaAtual.status
            });
        }
        
        if (pessoaAtual.quantidade_leads !== pessoaInicial.quantidade_leads) {
            alteracoes.push({
                id_lead: pessoaAtual.id_lead,
                nome: pessoaAtual.nome,
                campo: 'quantidade_leads',
                valorAnterior: pessoaInicial.quantidade_leads,
                valorNovo: pessoaAtual.quantidade_leads,
                status: pessoaAtual.status
            });
        }
        
        if (pessoaAtual.observacao !== pessoaInicial.observacao) {
            alteracoes.push({
                id_lead: pessoaAtual.id_lead,
                nome: pessoaAtual.nome,
                campo: 'observacao',
                valorAnterior: pessoaInicial.observacao || '(vazio)',
                valorNovo: pessoaAtual.observacao || '(vazio)',
                status: pessoaAtual.status
            });
        }
    });
    
    return alteracoes;
}

async function dcSalvar() {
    const alteracoes = dcDetectarAlteracoes();

    if (alteracoes.length === 0) {
        dcShowNotification('Nenhuma alteração detectada', 'info');
        return;
    }

    const btnSave = document.getElementById('dist-btn-save');
    if (btnSave) btnSave.disabled = true;

    try {
        const response = await fetch(DIST_API_SAVE, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                alteracoes,
                timestamp: new Date().toISOString()
            })
        });

        if (response.ok) {
            dcState.initialData = JSON.parse(JSON.stringify(dcState.data));
            dcShowNotification(`${alteracoes.length} alteração(ões) salva(s) com sucesso!`, 'success');

            // Se o SALVAR foi para persistir aplicacao de um Modo Turno,
            // grava o snapshot correspondente. O snapshot guarda o payload
            // {id_lead -> status} SO dos consultores do turno alvo — e o que
            // vai ser aplicado quando a regra automatica disparar.
            if (dcState.pendingSnapshot === 'dia' || dcState.pendingSnapshot === 'noite') {
                await dcSalvarSnapshotTurno(dcState.pendingSnapshot);
                dcState.pendingSnapshot = null;
            }
        } else {
            throw new Error('Erro ao salvar');
        }
    } catch (error) {
        console.error('Erro ao salvar:', error);
        dcShowNotification('Erro ao salvar alterações. Tente novamente.', 'error');
    } finally {
        if (btnSave) btnSave.disabled = false;
    }
}

async function dcSalvarSnapshotTurno(turnoAlvo) {
    // Monta {id_lead: status} SO dos consultores do turno alvo
    const payload = {};
    (dcState.data || []).forEach(p => {
        const k = dcTurnoKey(p);
        if (!k) return;
        const pessoaTurno = (dcState.turnoMap[k] === 'noite') ? 'noite' : 'dia';
        if (pessoaTurno !== turnoAlvo) return;
        const st = (p.status || '').toUpperCase();
        if (st === 'ATIVO' || st === 'INATIVO') payload[k] = st;
    });

    // Antes de salvar snapshot, garante que o turno-map esta 100% sincronizado
    // com dcState.data (todos os id_leads categorizados como dia ou noite).
    await dcTurnoMapPersistCompleto();

    try {
        const r = await fetch('/api/dist-comercial/snapshot', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ turno: turnoAlvo, payload })
        });
        if (!r.ok) throw new Error('http ' + r.status);
        const data = await r.json();
        dcShowNotification(
            'Snapshot do turno ' + turnoAlvo.toUpperCase() + ' atualizado (' +
            (data.count ?? Object.keys(payload).length) +
            ' consultor[es]). Regra automatica vai usar esta configuracao.',
            'info'
        );
    } catch (e) {
        console.warn('Falha ao salvar snapshot:', e);
        dcShowNotification(
            'As alteracoes foram salvas no CRM, mas o snapshot da regra automatica falhou. Tente aplicar o turno novamente.',
            'error'
        );
    }
}

async function loadDistComercial() {
    if (!dcState.initialized) {
        dcState.initialized = true;
    }
    // Carrega turno_map ANTES dos dados pra render ja sair correto (nao
    // aparece todo mundo no Dia por um piscar de olho ate o map chegar).
    await dcTurnoMapLoad();
    dcCarregarDados();
}

// ─────────────────────────────────────────────────────────────────────
// Modal: Configurar regras automaticas de troca de turno
// ─────────────────────────────────────────────────────────────────────

async function dcAbrirModalRegras() {
    const modal = document.getElementById('dist-modal-regras');
    if (!modal) return;
    if (typeof dczPortalToBody === 'function') dczPortalToBody(modal);
    modal.classList.add('is-open');
    if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(true);
    await Promise.all([dcRegrasCarregar(), dcSnapshotsCarregar(), dcLogCarregar()]);
}

function dcFecharModalRegras(event) {
    if (event && event.target !== event.currentTarget) return;
    const modal = document.getElementById('dist-modal-regras');
    if (modal) modal.classList.remove('is-open');
    if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(false);
}

async function dcRegrasCarregar() {
    try {
        const r = await fetch('/api/dist-comercial/rules');
        if (!r.ok) throw new Error('http ' + r.status);
        const data = await r.json();
        dcState.rules = Array.isArray(data.rules) ? data.rules : [];
    } catch (e) {
        console.warn('Falha ao carregar regras:', e);
        dcState.rules = [];
    }
    dcRegrasRender();
}

function dcTimeToMinutes(hhmm) {
    if (!hhmm) return 0;
    const parts = String(hhmm).split(':');
    const h = parseInt(parts[0], 10) || 0;
    const m = parseInt(parts[1], 10) || 0;
    return (h % 24) * 60 + (m % 60);
}

// Renderiza uma regua horaria de 24h com a janela do turno alvo destacada.
// Cross-midnight (ex: 22:00 as 06:00) e suportado: pinta 2 segmentos.
function dcRegraTimelineHtml(hInicio, hFim, turnoAlvo) {
    const total = 24 * 60;
    const startMin = dcTimeToMinutes(hInicio);
    const endMin = dcTimeToMinutes(hFim);
    const alvoNoite = turnoAlvo === 'noite';
    const colorAlvo = alvoNoite ? '#4c6ef5' : '#f59e0b';     // noite=azul, dia=laranja
    const colorOposto = alvoNoite ? '#f59e0b' : '#4c6ef5';    // fora da janela = oposto
    const labelAlvo = alvoNoite ? 'NOITE' : 'DIA';
    const labelOposto = alvoNoite ? 'DIA' : 'NOITE';

    // Constroi segmentos [start, end, cor, label] (end pode ser 1440 no wrap)
    // Regra: janela alvo = [startMin, endMin) se startMin < endMin,
    //        senao [startMin, 1440) + [0, endMin) (cross-midnight)
    let segs = [];
    if (startMin < endMin) {
        segs.push({ from: 0, to: startMin, kind: 'oposto' });
        segs.push({ from: startMin, to: endMin, kind: 'alvo' });
        segs.push({ from: endMin, to: total, kind: 'oposto' });
    } else {
        segs.push({ from: 0, to: endMin, kind: 'alvo' });
        segs.push({ from: endMin, to: startMin, kind: 'oposto' });
        segs.push({ from: startMin, to: total, kind: 'alvo' });
    }

    const now = new Date();
    const nowMin = now.getHours() * 60 + now.getMinutes();
    const nowPct = (nowMin / total) * 100;

    const segsHtml = segs.filter(s => s.to > s.from).map(s => {
        const left = (s.from / total) * 100;
        const width = ((s.to - s.from) / total) * 100;
        const cor = s.kind === 'alvo' ? colorAlvo : colorOposto;
        const label = s.kind === 'alvo' ? labelAlvo : labelOposto;
        return (
            '<div class="dist-timeline-seg" style="left:' + left + '%;width:' + width + '%;' +
                'background:' + cor + ';" title="' + label + '"></div>'
        );
    }).join('');

    // Marcadores 00 06 12 18 24
    const ticks = [0, 6, 12, 18, 24].map(h => {
        const left = (h / 24) * 100;
        return (
            '<div class="dist-timeline-tick" style="left:' + left + '%">' +
                '<span>' + (h < 10 ? '0' + h : h) + 'h</span>' +
            '</div>'
        );
    }).join('');

    return (
        '<div class="dist-timeline">' +
            '<div class="dist-timeline-bar">' + segsHtml +
                '<div class="dist-timeline-now" style="left:' + nowPct + '%" title="Agora"></div>' +
            '</div>' +
            '<div class="dist-timeline-legend">' +
                '<span class="dist-timeline-legend-item"><span class="sw" style="background:' + colorAlvo + '"></span>' + labelAlvo + '</span>' +
                '<span class="dist-timeline-legend-item"><span class="sw" style="background:' + colorOposto + '"></span>' + labelOposto + '</span>' +
            '</div>' +
            '<div class="dist-timeline-ticks">' + ticks + '</div>' +
        '</div>'
    );
}

function dcRegrasRender() {
    const container = document.getElementById('dist-regras-lista');
    if (!container) return;
    if (!dcState.rules.length) {
        container.innerHTML =
            '<div class="dist-regra-empty">Sem regras configuradas. Clique em <strong>Adicionar regra</strong> ' +
            'para criar sua primeira janela (ex: NOITE das 17:00 até 22:00).</div>';
        return;
    }
    const iconX   = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M18 6L6 18M6 6l12 12"/></svg>';

    container.innerHTML = dcState.rules.map(r => {
        const hi = (r.hora_inicio || '').slice(0, 5) || '00:00';
        const hf = (r.hora_fim || '').slice(0, 5) || '00:00';
        const alvo = r.turno_alvo || 'noite';
        const oposto = alvo === 'noite' ? 'dia' : 'noite';
        const labelAlvo = alvo === 'noite' ? 'NOITE' : 'DIA';
        const labelOposto = oposto === 'noite' ? 'NOITE' : 'DIA';
        const ultima = r.last_run_at
            ? 'Última execução: ' + dcFormatIso(r.last_run_at) + ' (' + (r.last_run_result || '') + ')'
            : 'Nunca executada';
        return (
            '<div class="dist-regra-card" data-rule-id="' + r.id + '">' +
                '<div class="dist-regra-card-head">' +
                    '<div class="dist-regra-card-title">' +
                        '<span class="dist-regra-badge dist-regra-badge-' + alvo + '">' + labelAlvo + '</span>' +
                        '<span class="dist-regra-desc">ativo das ' +
                            '<input type="time" value="' + hi + '" ' +
                                'onchange="dcRegraUpdate(' + r.id + ', {hora_inicio: this.value})">' +
                            ' até ' +
                            '<input type="time" value="' + hf + '" ' +
                                'onchange="dcRegraUpdate(' + r.id + ', {hora_fim: this.value})">' +
                        '</span>' +
                    '</div>' +
                    '<div class="dist-regra-card-actions">' +
                        '<label class="dist-regra-toggle">' +
                            '<input type="checkbox" ' + (r.enabled ? 'checked' : '') +
                                ' onchange="dcRegraUpdate(' + r.id + ', {enabled: this.checked})">' +
                            '<span>' + (r.enabled ? 'Ativa' : 'Pausada') + '</span>' +
                        '</label>' +
                        '<button type="button" class="dist-regra-remove" ' +
                            'title="Remover regra" ' +
                            'onclick="dcRegraRemover(' + r.id + ')">' + iconX + '</button>' +
                    '</div>' +
                '</div>' +
                '<div class="dist-regra-card-sel">' +
                    'Turno da janela: ' +
                    '<select onchange="dcRegraUpdate(' + r.id + ', {turno_alvo: this.value})">' +
                        '<option value="noite" ' + (alvo === 'noite' ? 'selected' : '') + '>Noite</option>' +
                        '<option value="dia" '   + (alvo === 'dia'   ? 'selected' : '') + '>Dia</option>' +
                    '</select>' +
                '</div>' +
                dcRegraTimelineHtml(hi, hf, alvo) +
                '<div class="dist-regra-explica">' +
                    'Das <b>' + hi + '</b> às <b>' + hf + '</b>: aplica <b>Modo ' + labelAlvo + '</b>' +
                    ' (' + labelOposto + ' inativo, ' + labelAlvo + ' conforme snapshot).<br>' +
                    'Às <b>' + hf + '</b>: volta pro <b>Modo ' + labelOposto + '</b>' +
                    ' (' + labelAlvo + ' inativo, ' + labelOposto + ' conforme snapshot).' +
                '</div>' +
                '<div class="dist-regra-ultima" title="' + dcEscapeHtml(ultima) + '">' +
                    (r.last_run_at ? '⏱ ' + dcFormatIso(r.last_run_at) + ' (' + (r.last_run_result || '') + ')' : '⏱ Ainda não rodou') +
                '</div>' +
            '</div>'
        );
    }).join('');
}

async function dcAdicionarRegra() {
    const turno = prompt('Turno da janela (dia ou noite):', 'noite');
    if (!turno) return;
    const turnoNorm = turno.trim().toLowerCase();
    if (turnoNorm !== 'dia' && turnoNorm !== 'noite') {
        dcShowNotification('Turno inválido — use "dia" ou "noite".', 'error');
        return;
    }
    const defInicio = turnoNorm === 'noite' ? '17:00' : '07:00';
    const defFim = turnoNorm === 'noite' ? '22:00' : '17:00';
    const hi = prompt(turnoNorm.toUpperCase() + ' começa às (HH:MM):', defInicio);
    if (!hi) return;
    const hf = prompt(turnoNorm.toUpperCase() + ' termina às (HH:MM):', defFim);
    if (!hf) return;
    try {
        const r = await fetch('/api/dist-comercial/rules', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                hora_inicio: hi.trim(),
                hora_fim: hf.trim(),
                turno_alvo: turnoNorm,
                enabled: true
            })
        });
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.error || 'http ' + r.status);
        }
        await dcRegrasCarregar();
        dcShowNotification('Regra criada.', 'success');
    } catch (e) {
        console.warn(e);
        dcShowNotification('Falha ao criar regra: ' + e.message, 'error');
    }
}

async function dcRegraUpdate(id, patch) {
    try {
        const r = await fetch('/api/dist-comercial/rules/' + id, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(patch)
        });
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.error || 'http ' + r.status);
        }
        await dcRegrasCarregar();
    } catch (e) {
        console.warn(e);
        dcShowNotification('Falha ao atualizar regra: ' + e.message, 'error');
    }
}

async function dcRegraRemover(id) {
    if (!confirm('Remover esta regra? Ela para de disparar imediatamente.')) return;
    try {
        const r = await fetch('/api/dist-comercial/rules/' + id, { method: 'DELETE' });
        if (!r.ok) {
            const err = await r.json().catch(() => ({}));
            throw new Error(err.error || 'http ' + r.status);
        }
        await dcRegrasCarregar();
        dcShowNotification('Regra removida.', 'success');
    } catch (e) {
        console.warn(e);
        dcShowNotification('Falha ao remover: ' + e.message, 'error');
    }
}

async function dcSnapshotsCarregar() {
    try {
        const r = await fetch('/api/dist-comercial/snapshot');
        if (!r.ok) throw new Error('http ' + r.status);
        const data = await r.json();
        dcState.snapshots = data.snapshots || { dia: null, noite: null };
    } catch (e) {
        console.warn('Falha ao carregar snapshots:', e);
        dcState.snapshots = { dia: null, noite: null };
    }
    dcSnapshotsRender();
}

function dcSnapshotsRender() {
    const container = document.getElementById('dist-regras-snapshots');
    if (!container) return;
    const iconSol = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:14px;height:14px"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.93 4.93l1.41 1.41M17.66 17.66l1.41 1.41M2 12h2M20 12h2M4.93 19.07l1.41-1.41M17.66 6.34l1.41-1.41"/></svg>';
    const iconLua = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="width:14px;height:14px"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/></svg>';

    function card(turno, snap) {
        const label = turno === 'noite' ? 'Snapshot NOITE' : 'Snapshot DIA';
        const icone = turno === 'noite' ? iconLua : iconSol;
        if (!snap || !snap.payload || Object.keys(snap.payload).length === 0) {
            return (
                '<div class="dist-snap-card">' +
                    '<h4>' + icone + ' ' + label + '</h4>' +
                    '<div class="dist-snap-info dist-snap-empty">Nunca criado — regra automática deste turno não funciona sem snapshot.</div>' +
                    '<div class="dist-snap-info" style="margin-top:4px">Para criar: clique em <strong>TURNO ' +
                        turno.toUpperCase() + '</strong> na tela principal e depois em <strong>SALVAR</strong>.</div>' +
                '</div>'
            );
        }
        const total = Object.keys(snap.payload).length;
        const ativos = Object.values(snap.payload).filter(v => (v || '').toUpperCase() === 'ATIVO').length;
        const inativos = total - ativos;
        return (
            '<div class="dist-snap-card">' +
                '<h4>' + icone + ' ' + label + '</h4>' +
                '<div class="dist-snap-info">' +
                    '<strong>' + total + '</strong> consultor(es) no snapshot &middot; ' +
                    '<strong style="color:var(--dist-success)">' + ativos + ' ATIVO</strong> &middot; ' +
                    '<strong style="color:var(--dist-danger)">' + inativos + ' INATIVO</strong>' +
                '</div>' +
                '<div class="dist-snap-info" style="margin-top:4px">' +
                    'Tirado em ' + dcFormatIso(snap.taken_at) + (snap.taken_by ? ' por ' + dcEscapeHtml(snap.taken_by) : '') +
                '</div>' +
                '<button class="dist-snap-clear" onclick="dcSnapshotLimpar(\'' + turno + '\')">Limpar snapshot</button>' +
            '</div>'
        );
    }

    container.innerHTML = card('dia', dcState.snapshots.dia) + card('noite', dcState.snapshots.noite);
}

async function dcSnapshotLimpar(turno) {
    if (!confirm(
        'Limpar o snapshot do turno ' + turno.toUpperCase() + '?\n\n' +
        'Regras automaticas para este turno vao FALHAR ate voce criar ' +
        'um novo snapshot (aplicando Modo ' + turno.toUpperCase() + ' + SALVAR).'
    )) return;
    try {
        const r = await fetch('/api/dist-comercial/snapshot/' + turno, { method: 'DELETE' });
        if (!r.ok) throw new Error('http ' + r.status);
        await dcSnapshotsCarregar();
        dcShowNotification('Snapshot removido.', 'info');
    } catch (e) {
        console.warn(e);
        dcShowNotification('Falha ao limpar snapshot: ' + e.message, 'error');
    }
}

async function dcLogCarregar() {
    try {
        const r = await fetch('/api/dist-comercial/apply-log?limit=20');
        if (!r.ok) throw new Error('http ' + r.status);
        const data = await r.json();
        const container = document.getElementById('dist-regras-log');
        if (!container) return;
        if (!data.log || !data.log.length) {
            container.innerHTML = '<div style="color:var(--dist-text-muted);padding:8px">Nenhuma execução registrada ainda.</div>';
            return;
        }
        container.innerHTML = data.log.map(e => {
            const isOk = e.resultado === 'ok';
            const tag = isOk ? 'ok' : 'err';
            return (
                '<div class="dist-log-row">' +
                    '<div style="flex:1;min-width:0">' +
                        '<div style="font-weight:600">' +
                            (e.turno_alvo || '').toUpperCase() + ' &middot; ' + (e.origem || '') +
                            (e.autor ? ' &middot; ' + dcEscapeHtml(e.autor) : '') +
                        '</div>' +
                        '<div style="color:var(--dist-text-muted);font-size:11px">' +
                            dcFormatIso(e.executed_at) + ' &middot; ' + dcEscapeHtml(e.mensagem || '') +
                        '</div>' +
                    '</div>' +
                    '<span class="dist-log-tag ' + tag + '">' + dcEscapeHtml(e.resultado || '') + '</span>' +
                '</div>'
            );
        }).join('');
    } catch (e) {
        console.warn('Falha ao carregar log:', e);
    }
}

function dcFormatIso(iso, mode) {
    if (!iso) return '';
    try {
        const d = new Date(iso);
        if (mode === 'short') {
            return d.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' });
        }
        return d.toLocaleString('pt-BR');
    } catch (e) {
        return iso;
    }
}

function dcShowNotification(message, type = 'success') {
    const existing = document.getElementById('dist-notification');
    if (existing) existing.remove();
    
    const colors = {
        success: { bg: '#dcfce7', border: '#86efac', text: '#16a34a', icon: '✔' },
        error: { bg: '#fee2e2', border: '#fca5a5', text: '#dc2626', icon: '✘' },
        info: { bg: '#dbeafe', border: '#93c5fd', text: '#2563eb', icon: 'ℹ' }
    };
    const c = colors[type] || colors.success;
    
    const notification = document.createElement('div');
    notification.id = 'dist-notification';
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        background: ${c.bg};
        border: 1px solid ${c.border};
        color: ${c.text};
        padding: 14px 20px;
        border-radius: 10px;
        font-size: 14px;
        font-weight: 500;
        display: flex;
        align-items: center;
        gap: 10px;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        z-index: 9999;
        animation: distSlideIn 0.3s ease;
    `;
    notification.innerHTML = `<span style="font-size:18px;">${c.icon}</span> ${message}`;
    
    const style = document.createElement('style');
    style.textContent = `
        @keyframes distSlideIn {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        @keyframes distSlideOut {
            from { transform: translateX(0); opacity: 1; }
            to { transform: translateX(100%); opacity: 0; }
        }
    `;
    document.head.appendChild(style);
    document.body.appendChild(notification);
    
    setTimeout(() => {
        notification.style.animation = 'distSlideOut 0.3s ease';
        setTimeout(() => notification.remove(), 300);
    }, 3000);
}
