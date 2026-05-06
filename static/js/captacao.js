// ---------------------------------------------------------------------------
// Captação Externa
// ---------------------------------------------------------------------------
const CAP_SUPABASE_URL = 'https://fcwuhwedretyomtrbgzb.supabase.co';
const CAP_SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjd3Vod2VkcmV0eW9tdHJiZ3piIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA3OTI0NTAsImV4cCI6MjA2NjM2ODQ1MH0.IbDvSLmrg_ihyCZMhpDDeA6-solYN-2RhcY8PCHzc6I';
const CAP_WEBHOOK_URL = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/leads_captacao_externa';

const capState = {
    cursos: [],
    nivelSelecionado: '',
    modalidadeSelecionada: '',
    cursoValue: '',
    leads: [],
    initialized: false
};

async function capCarregarCursos() {
    try {
        const res = await fetch(
            `${CAP_SUPABASE_URL}/rest/v1/cursos_salesbot_nome?select=id,content&order=content.asc`,
            { headers: { apikey: CAP_SUPABASE_KEY, Authorization: `Bearer ${CAP_SUPABASE_KEY}` } }
        );
        const data = await res.json();
        const unique = [...new Set(data.map(c => c.content))].sort();
        capState.cursos = unique;
    } catch (e) {
        console.error('Erro ao carregar cursos:', e);
    }
}

function capFormatContato(el) {
    let v = el.value.replace(/\D/g, '');
    v = v.replace(/^(\d{2})(\d)/g, '($1) $2');
    v = v.replace(/(\d)(\d{4})$/, '$1-$2');
    el.value = v;
}

function capSelectNivel(btn) {
    const nivel = btn.dataset.nivel;
    capState.nivelSelecionado = nivel;
    capState.modalidadeSelecionada = '';
    capState.cursoValue = '';

    document.querySelectorAll('#page-captacao .cap-nivel-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');

    document.getElementById('cap-curso-input').value = '';
    document.getElementById('cap-curso-input').placeholder =
        nivel === 'Pós-Graduação' ? 'Digite o nome do curso de pós-graduação...' : 'Digite para buscar o curso...';
    document.getElementById('cap-ac-list').classList.remove('open');

    const grauWrap = document.getElementById('cap-grau-wrap');
    const ingressoWrap = document.getElementById('cap-ingresso-wrap');
    const modGrid = document.getElementById('cap-mod-grid');

    if (nivel === 'Pós-Graduação') {
        grauWrap.style.display = 'none';
        ingressoWrap.style.display = 'none';
        modGrid.innerHTML = '<button type="button" class="cap-mod-btn" data-mod="EAD" onclick="capSelectMod(this)">EAD</button>';
    } else {
        grauWrap.style.display = '';
        ingressoWrap.style.display = '';
        modGrid.innerHTML = '<button type="button" class="cap-mod-btn" data-mod="EAD" onclick="capSelectMod(this)">EAD</button>' +
            '<button type="button" class="cap-mod-btn" data-mod="Semi" onclick="capSelectMod(this)">Semipresencial</button>';
    }

    document.querySelectorAll('#page-captacao .cap-mod-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('input[name="cap-ingresso"]').forEach(r => r.checked = false);

    capUpdateResumo();
}

function capSelectMod(btn) {
    capState.modalidadeSelecionada = btn.dataset.mod;
    document.querySelectorAll('#page-captacao .cap-mod-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
}

function capCursoInput(el) {
    const val = el.value;
    const acList = document.getElementById('cap-ac-list');

    if (capState.nivelSelecionado === 'Pós-Graduação') {
        capState.cursoValue = val;
        acList.classList.remove('open');
        capUpdateResumo();
        return;
    }

    if (val.length < 1) {
        acList.classList.remove('open');
        capState.cursoValue = '';
        capUpdateResumo();
        return;
    }

    const q = val.toLowerCase().trim();
    const filtered = capState.cursos.filter(c => c.toLowerCase().includes(q)).slice(0, 15);

    if (filtered.length === 0) {
        acList.innerHTML = '<div class="cap-ac-empty">Nenhum curso encontrado</div>';
    } else {
        acList.innerHTML = filtered.map(c =>
            `<div class="cap-ac-item" onmousedown="capSelectCurso('${c.replace(/'/g, "\\'")}')">${c}</div>`
        ).join('');
    }
    acList.classList.add('open');
    capUpdateResumo();
}

function capSelectCurso(nome) {
    document.getElementById('cap-curso-input').value = nome;
    capState.cursoValue = nome;
    document.getElementById('cap-ac-list').classList.remove('open');
    capUpdateResumo();
}

function capUpdateResumo() {
    const nome = document.getElementById('cap-nome').value || '---';
    const nivel = capState.nivelSelecionado || '---';
    const curso = capState.cursoValue || document.getElementById('cap-curso-input').value || '---';
    const grau = document.getElementById('cap-grau').value || '---';
    const ingressoEl = document.querySelector('input[name="cap-ingresso"]:checked');
    const ingresso = capState.nivelSelecionado === 'Pós-Graduação' ? 'Pós-Graduação' : (ingressoEl ? ingressoEl.value : '---');

    document.getElementById('cap-res-nome').textContent = nome;
    document.getElementById('cap-res-nivel').textContent = nivel;
    document.getElementById('cap-res-curso').textContent = curso;
    document.getElementById('cap-res-grau').textContent = grau;
    document.getElementById('cap-res-ingresso').textContent = ingresso;
}

function capReset() {
    document.getElementById('cap-form').reset();
    capState.nivelSelecionado = '';
    capState.modalidadeSelecionada = '';
    capState.cursoValue = '';

    document.querySelectorAll('#page-captacao .cap-nivel-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('#page-captacao .cap-mod-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('cap-ac-list').classList.remove('open');
    document.getElementById('cap-grau-wrap').style.display = '';
    document.getElementById('cap-ingresso-wrap').style.display = '';
    document.getElementById('cap-curso-input').placeholder = 'Digite para buscar o curso...';

    const modGrid = document.getElementById('cap-mod-grid');
    modGrid.innerHTML = '<button type="button" class="cap-mod-btn" data-mod="EAD" onclick="capSelectMod(this)">EAD</button>' +
        '<button type="button" class="cap-mod-btn" data-mod="Semi" onclick="capSelectMod(this)">Semipresencial</button>';

    capUpdateResumo();
}

async function capSubmit(e) {
    e.preventDefault();
    const btn = document.getElementById('cap-btn-submit');
    btn.disabled = true;
    btn.innerHTML = '<span class="material-symbols-outlined text-base animate-spin">progress_activity</span> ENVIANDO...';

    const ingressoEl = document.querySelector('input[name="cap-ingresso"]:checked');
    const ingressoFinal = capState.nivelSelecionado === 'Pós-Graduação' ? 'Pós-Graduação' : (ingressoEl ? ingressoEl.value : '---');

    const leadData = {
        nome: document.getElementById('cap-nome').value,
        contato: document.getElementById('cap-contato').value,
        email: document.getElementById('cap-email').value,
        nivel: capState.nivelSelecionado || '---',
        curso: document.getElementById('cap-curso-input').value || '---',
        grau: document.getElementById('cap-grau').value || '---',
        modalidade: capState.modalidadeSelecionada || '---',
        ingresso: ingressoFinal,
        hora: new Date().toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' })
    };

    try {
        await fetch(CAP_WEBHOOK_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(leadData)
        });
    } catch (err) {
        console.error('Erro webhook:', err);
    }

    capState.leads.push(leadData);
    capRenderRecent();
    capShowToast();
    capReset();

    btn.disabled = false;
    btn.innerHTML = '<span class="material-symbols-outlined text-base">cloud_upload</span> FINALIZAR CADASTRO';
}

function capRenderRecent() {
    const container = document.getElementById('cap-recent-list');
    if (capState.leads.length === 0) {
        container.innerHTML = '<p class="cap-recent-empty">Nenhum registro nesta sessão</p>';
        return;
    }
    container.innerHTML = [...capState.leads].reverse().map(l => `
        <div class="cap-recent-item">
            <div class="cap-recent-avatar">${l.nome.substring(0, 2).toUpperCase()}</div>
            <div>
                <div class="cap-recent-name">${l.nome}</div>
                <div class="cap-recent-curso">${l.curso}</div>
            </div>
        </div>
    `).join('');
}

function capShowToast() {
    const toast = document.getElementById('cap-toast');
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3000);
}

function loadCaptacao() {
    if (!capState.initialized) {
        capState.initialized = true;
        capCarregarCursos();

        const username = (document.body.dataset.username || '').trim();
        if (username) {
            const vocLink = document.getElementById('cap-voc-link');
            if (vocLink) {
                vocLink.href = `https://descubra.cruzeiroead.com.br/?utm_source=${encodeURIComponent(username)}`;
            }
        }
    }
    capUpdateResumo();

    document.getElementById('cap-nome').addEventListener('input', capUpdateResumo);
    document.getElementById('cap-grau').addEventListener('change', capUpdateResumo);
    document.getElementById('cap-curso-input').addEventListener('blur', () => {
        setTimeout(() => document.getElementById('cap-ac-list').classList.remove('open'), 200);
    });
}
