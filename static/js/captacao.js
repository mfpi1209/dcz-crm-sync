// ---------------------------------------------------------------------------
// Captação Externa
// ---------------------------------------------------------------------------
const CAP_SUPABASE_URL = 'https://fcwuhwedretyomtrbgzb.supabase.co';
const CAP_SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZjd3Vod2VkcmV0eW9tdHJiZ3piIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NTA3OTI0NTAsImV4cCI6MjA2NjM2ODQ1MH0.IbDvSLmrg_ihyCZMhpDDeA6-solYN-2RhcY8PCHzc6I';
const CAP_WEBHOOK_URL = 'https://n8n-new-n8n.ca31ey.easypanel.host/webhook/leads_captacao_externa';

const capState = {
    cursos: [],
    cursosPos: [],
    nivelSelecionado: '',
    modalidadeSelecionada: '',
    cursoValue: '',
    leads: [],
    initialized: false,
    modo: 'promotor'
};

async function capCarregarCursos() {
    const headers = { apikey: CAP_SUPABASE_KEY, Authorization: `Bearer ${CAP_SUPABASE_KEY}` };
    const fetchTabela = async (tabela) => {
        try {
            const res = await fetch(
                `${CAP_SUPABASE_URL}/rest/v1/${tabela}?select=content&order=content.asc`,
                { headers }
            );
            if (!res.ok) {
                const errText = await res.text();
                console.error(`[Captação] HTTP ${res.status} em ${tabela}:`, errText);
                return [];
            }
            const data = await res.json();
            if (!Array.isArray(data)) {
                console.error(`[Captação] Resposta inválida de ${tabela}:`, data);
                return [];
            }
            const lista = [...new Set(data.map(c => c.content).filter(Boolean))].sort();
            console.info(`[Captação] ${tabela}: ${lista.length} curso(s) carregado(s)`);
            return lista;
        } catch (e) {
            console.error(`[Captação] Erro ao carregar ${tabela}:`, e);
            return [];
        }
    };

    const [grad, pos] = await Promise.all([
        fetchTabela('cursos_salesbot_nome'),
        fetchTabela('cursos_salesbot_pos_nome')
    ]);
    capState.cursos = grad;
    capState.cursosPos = pos;
}

function capFormatContato(el) {
    let v = el.value.replace(/\D/g, '').slice(0, 11);
    if (v.length === 0) {
        el.value = '';
        return;
    }
    if (v.length <= 2) {
        el.value = '(' + v;
    } else if (v.length <= 6) {
        el.value = '(' + v.slice(0, 2) + ') ' + v.slice(2);
    } else if (v.length <= 10) {
        el.value = '(' + v.slice(0, 2) + ') ' + v.slice(2, 6) + '-' + v.slice(6);
    } else {
        el.value = '(' + v.slice(0, 2) + ') ' + v.slice(2, 7) + '-' + v.slice(7);
    }
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

    const cursoInput = document.getElementById('cap-curso-input');
    if (cursoInput) {
        capRenderAutocomplete('');
        cursoInput.focus();
    }

    capUpdateResumo();
}

function capSelectMod(btn) {
    capState.modalidadeSelecionada = btn.dataset.mod;
    document.querySelectorAll('#page-captacao .cap-mod-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
}

function capRenderAutocomplete(val) {
    const acList = document.getElementById('cap-ac-list');
    const isPos = capState.nivelSelecionado === 'Pós-Graduação';
    const lista = isPos ? capState.cursosPos : capState.cursos;

    if (lista.length === 0) {
        const msg = isPos
            ? 'Lista de pós-graduação ainda não disponível. Você pode digitar livremente.'
            : 'Lista de cursos ainda não carregada. Você pode digitar livremente.';
        acList.innerHTML = `<div class="cap-ac-empty">${msg}</div>`;
        acList.classList.add('open');
        return;
    }

    const q = (val || '').toLowerCase().trim();
    const filtered = q
        ? lista.filter(c => c.toLowerCase().includes(q)).slice(0, 15)
        : lista.slice(0, 15);

    if (filtered.length === 0) {
        acList.innerHTML = '<div class="cap-ac-empty">Nenhum curso encontrado</div>';
    } else {
        acList.innerHTML = filtered.map(c =>
            `<div class="cap-ac-item" onmousedown="capSelectCurso('${c.replace(/'/g, "\\'")}')">${c}</div>`
        ).join('');
    }
    acList.classList.add('open');
}

function capCursoInput(el) {
    const val = el.value;
    capState.cursoValue = val;
    capRenderAutocomplete(val);
    capUpdateResumo();
}

function capCursoFocus(el) {
    if (!capState.nivelSelecionado) return;
    capRenderAutocomplete(el.value || '');
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
    const modo = capState.modo || 'promotor';
    const labelEnviando = modo === 'candidato' ? 'ENVIANDO...' : 'ENVIANDO...';
    btn.disabled = true;
    btn.innerHTML = `<span class="material-symbols-outlined text-base animate-spin">progress_activity</span> ${labelEnviando}`;

    const ingressoEl = document.querySelector('input[name="cap-ingresso"]:checked');
    const ingressoFinal = capState.nivelSelecionado === 'Pós-Graduação' ? 'Pós-Graduação' : (ingressoEl ? ingressoEl.value : '---');
    const usuarioLogado = (document.body.dataset.username || '').trim() || '---';

    const leadData = {
        nome: document.getElementById('cap-nome').value,
        contato: document.getElementById('cap-contato').value,
        email: document.getElementById('cap-email').value,
        nivel: capState.nivelSelecionado || '---',
        curso: document.getElementById('cap-curso-input').value || '---',
        grau: document.getElementById('cap-grau').value || '---',
        modalidade: capState.modalidadeSelecionada || '---',
        ingresso: ingressoFinal,
        tipo: modo,
        usuario_logado: usuarioLogado,
        promotor: modo === 'promotor' ? usuarioLogado : '---',
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
    capAplicarCopyModo();
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
    const modo = capState.modo || 'promotor';
    const titulo = toast.querySelector('p[style*="font-weight:700"]');
    const sub = toast.querySelector('p[style*="font-size:0.75rem"]');
    if (titulo && sub) {
        if (modo === 'candidato') {
            titulo.textContent = 'Cadastro recebido!';
            sub.textContent = 'Em breve entraremos em contato.';
        } else {
            titulo.textContent = 'Lead Cadastrado!';
            sub.textContent = 'Os dados foram enviados.';
        }
    }
    toast.classList.add('show');
    setTimeout(() => toast.classList.remove('show'), 3000);
}

function capAplicarCopyModo() {
    const modo = capState.modo || 'promotor';
    const title = document.getElementById('cap-title');
    const subtitle = document.getElementById('cap-subtitle');
    const submitBtn = document.getElementById('cap-btn-submit');

    if (modo === 'candidato') {
        if (title) title.textContent = 'Bem-vindo(a)!';
        if (subtitle) subtitle.textContent = 'Preencha seus dados para conhecer nossos cursos. Entraremos em contato em breve.';
        if (submitBtn) submitBtn.innerHTML = '<span class="material-symbols-outlined text-base">send</span> QUERO SABER MAIS!';
    } else {
        if (title) title.textContent = 'Novo Cadastro';
        if (subtitle) subtitle.textContent = 'Preencha os dados abaixo para registrar o lead captado externamente.';
        if (submitBtn) submitBtn.innerHTML = '<span class="material-symbols-outlined text-base">cloud_upload</span> FINALIZAR CADASTRO';
    }
}

function capSetModo(modo) {
    if (modo !== 'promotor' && modo !== 'candidato') return;
    capState.modo = modo;

    const root = document.getElementById('page-captacao');
    if (root) root.dataset.modo = modo;

    document.querySelectorAll('#page-captacao .cap-modo-btn').forEach(b => {
        b.classList.toggle('active', b.dataset.modo === modo);
    });

    capAplicarCopyModo();
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

    capSetModo(capState.modo || 'promotor');
    capUpdateResumo();

    document.getElementById('cap-nome').addEventListener('input', capUpdateResumo);
    document.getElementById('cap-grau').addEventListener('change', capUpdateResumo);
    document.getElementById('cap-curso-input').addEventListener('blur', () => {
        setTimeout(() => document.getElementById('cap-ac-list').classList.remove('open'), 200);
    });
}
