// ---------------------------------------------------------------------------
// Distribuição Comercial — Dashboard
// ---------------------------------------------------------------------------

const DIST_API_LOAD = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/distribuicaocomercial';
const DIST_API_SAVE = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/edicao_distrib';

const dcState = {
    data: [],
    initialData: [],
    loading: false,
    initialized: false
};

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
    
    const rows = dados.map(pessoa => `
        <tr data-id="${pessoa.id}">
            <td>
                <div class="dist-nome-cell">
                    <span class="dist-nome">${pessoa.nome || '—'}</span>
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
                       value="${pessoa.observacao || ''}" 
                       placeholder="Digite uma observação..."
                       onchange="dcUpdatePessoa(${pessoa.id}, 'observacao', this.value)">
            </td>
        </tr>
    `).join('');
    
    content.innerHTML = `
        <div class="dist-table-wrapper">
            <table class="dist-table">
                <thead>
                    <tr>
                        <th style="min-width: 220px;">Nome</th>
                        <th class="center" style="min-width: 140px;">Status</th>
                        <th class="center" style="min-width: 160px;">Quantidade Leads</th>
                        <th style="min-width: 320px;">Observação</th>
                    </tr>
                </thead>
                <tbody>
                    ${rows}
                </tbody>
            </table>
        </div>
    `;
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

function loadDistComercial() {
    if (!dcState.initialized) {
        dcState.initialized = true;
    }
    dcCarregarDados();
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
