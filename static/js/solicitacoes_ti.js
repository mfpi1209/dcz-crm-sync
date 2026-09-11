/* Abertura de chamado (TI / Marketing) — grava no Postgres.
   TI mantém os campos originais; Marketing usa o briefing de design, cujas
   especificações variam conforme o formato da peça. */
(function () {
    let _stiInited = false;
    let _depto = 'TI';
    let _cfg = null;
    let _brief = {};   // payload do briefing de Marketing

    // Espelha BRIEF_SPECS/BRIEF_OPCOES em routes/solicitacoes_ti.py. As listas
    // de opções (`opts`) vêm do config para não duplicar rótulos.
    const SPEC_DEFS = {
        'Imagem': [
            { key: 'formatos', type: 'multi', opts: 'formatos', label: 'Formato / aplicação da imagem' },
            { key: 'dimensao_especifica', type: 'text', label: 'Dimensão específica (opcional)', ph: 'Ex: 1080x1350px' },
            { key: 'observacoes', type: 'area', label: 'Observações de imagem', ph: 'Estilo visual, composição, peso do arquivo…' },
        ],
        'Vídeo': [
            { key: 'tem_material_bruto', type: 'bool', label: 'Existe material bruto disponível?', yes: 'Sim, temos material', no: 'Não (precisa gravar)' },
            { key: 'materiais_disponiveis', type: 'multi', opts: 'materiais_disponiveis', label: 'Materiais disponíveis para a edição', showIf: { tem_material_bruto: true } },
            { key: 'material_outro', type: 'text', label: 'Quais outros materiais?', ph: 'Ex: trilha licenciada, vinheta 3D…', showIf: { tem_material_bruto: true } },
            { key: 'link_material_bruto', type: 'text', label: 'Link da pasta na nuvem', ph: 'https://drive.google.com/…', showIf: { tem_material_bruto: true } },
            { key: 'tem_roteiro', type: 'bool', label: 'Possui roteiro pronto?', yes: 'Sim', no: 'Não' },
            { key: 'link_roteiro', type: 'text', label: 'Link do roteiro', ph: 'https://docs.google.com/…', showIf: { tem_roteiro: true } },
            { key: 'ideia_roteiro', type: 'area', label: 'Conceito / ideia do vídeo', ph: 'Mensagem principal, objetivo, cenas desejadas…', showIf: { tem_roteiro: false } },
            { key: 'formato_video', type: 'single', opts: 'formato_video', label: 'Formato do vídeo' },
            { key: 'formato_video_outro', type: 'text', label: 'Qual formato?', ph: 'Ex: 32:9, 4:5 carrossel…', showIf: { formato_video: 'outro' } },
            { key: 'duracao_estimada', type: 'text', label: 'Duração estimada', ph: 'Ex: 15 a 30 segundos' },
        ],
        'UX / UI': [
            { key: 'o_que_sera_desenvolvido', type: 'text', label: 'O que será desenvolvido?', ph: 'Ex: Landing page de captação' },
            { key: 'quantidade_telas', type: 'text', label: 'Quantidade de telas', ph: 'Ex: 3 telas + modal' },
            { key: 'quais_telas', type: 'text', label: 'Quais telas precisam ser criadas?' },
            { key: 'referencias_links', type: 'text', label: 'Referências / links' },
            { key: 'observacoes', type: 'area', label: 'Observações de UX / UI' },
        ],
        'E-book': [
            { key: 'tema_titulo', type: 'text', label: 'Tema / título do e-book' },
            { key: 'tem_texto', type: 'bool', label: 'Tem texto?', yes: 'Sim', no: 'Não' },
            { key: 'link_conteudo_textual', type: 'text', label: 'Link do documento com os textos', ph: 'https://docs.google.com/…', showIf: { tem_texto: true } },
            { key: 'conceito_texto', type: 'area', label: 'Conceito do e-book', showIf: { tem_texto: false } },
            { key: 'quantidade_paginas', type: 'text', label: 'Quantidade de páginas', ph: 'Ex: 18 a 22 páginas' },
            { key: 'formato_entrega', type: 'multi', opts: 'formato_entrega', label: 'Formato de entrega' },
            { key: 'formato_entrega_outro', type: 'text', label: 'Qual outro formato?' },
            { key: 'observacoes', type: 'area', label: 'Observações de e-book' },
        ],
        'Brinde': [
            { key: 'qual_brinde', type: 'text', label: 'Qual será o brinde?', ph: 'Ex: garrafa térmica inox gravada a laser' },
            { key: 'quantidade', type: 'text', label: 'Quantidade estimada', ph: 'Ex: 250 unidades' },
            { key: 'link_referencia', type: 'text', label: 'Fornecedor / modelo / link de referência' },
            { key: 'observacoes', type: 'area', label: 'Observações de brindes' },
        ],
        'Outro': [
            { key: 'tipo_personalizado', type: 'text', label: 'Nome do formato / material', ph: 'Ex: cenografia de evento, stand 3D…' },
            { key: 'formato_entrega', type: 'text', label: 'Formato de entrega esperado', ph: 'Ex: AI aberto + PDF em curvas' },
            { key: 'especificacoes_tecnicas', type: 'area', label: 'Especificações técnicas (medidas, materiais, regras)' },
            { key: 'observacoes', type: 'area', label: 'Observações adicionais' },
        ],
    };

    // Rótulos do briefing para leitura (Meus chamados / Fila). Estáticos, para
    // as outras telas não dependerem do /config da página de abertura.
    const BRIEF_LABELS = {
        bandeira: 'Bandeira / Empresa',
        publico_alvo: 'Público-alvo',
        assunto_tema: 'Assunto / Tema principal',
        objetivo_peca: 'Objetivo da peça',
        tem_telefone: 'Telefone na peça',
        telefone_contato: 'Telefone de contato',
        outras_informacoes: 'Outras informações',
        formatos: 'Formato / aplicação',
        dimensao_especifica: 'Dimensão específica',
        observacoes: 'Observações',
        tem_material_bruto: 'Material bruto disponível',
        materiais_disponiveis: 'Materiais disponíveis',
        material_outro: 'Outros materiais',
        link_material_bruto: 'Link do material bruto',
        tem_roteiro: 'Roteiro pronto',
        link_roteiro: 'Link do roteiro',
        ideia_roteiro: 'Conceito / ideia do vídeo',
        formato_video: 'Formato do vídeo',
        formato_video_outro: 'Formato do vídeo (outro)',
        duracao_estimada: 'Duração estimada',
        o_que_sera_desenvolvido: 'O que será desenvolvido',
        quantidade_telas: 'Quantidade de telas',
        quais_telas: 'Quais telas',
        referencias_links: 'Referências / links',
        tema_titulo: 'Tema / título',
        tem_texto: 'Tem texto',
        link_conteudo_textual: 'Link dos textos',
        conceito_texto: 'Conceito do e-book',
        quantidade_paginas: 'Quantidade de páginas',
        formato_entrega: 'Formato de entrega',
        formato_entrega_outro: 'Formato de entrega (outro)',
        qual_brinde: 'Qual será o brinde',
        quantidade: 'Quantidade',
        link_referencia: 'Fornecedor / referência',
        tipo_personalizado: 'Formato / material',
        especificacoes_tecnicas: 'Especificações técnicas',
    };
    const BRIEF_VALUE_LABELS = {
        feed: 'Feed (1:1 / 4:5)', stories: 'Stories (9:16)', banner: 'Banner Digital',
        email: 'E-mail Marketing', videos: 'Vídeos / Clipes', fotos: 'Fotos em alta',
        audios: 'Áudios / Locuções', logo: 'Logo vetorial', outros: 'Outros arquivos',
        vertical: 'Vertical (9:16)', horizontal: 'Horizontal (16:9)', quadrado: 'Quadrado (1:1)',
        outro: 'Outro', pdf: 'PDF Interativo', digital: 'Digital Web / Flipbook',
        impressao: 'Fechamento para Gráfica',
    };
    // Ordem de leitura do briefing: gerais primeiro, depois as specs.
    const BRIEF_ORDER = Object.keys(BRIEF_LABELS);

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
                const depto = escapeHtml(t.departamento || 'TI');
                return `
                <div class="sti-local-card space-y-2 cursor-pointer" onclick="navigate('meus_chamados_ti')">
                    <div class="flex justify-between items-center gap-2">
                        <span class="font-mono font-bold text-[10px] px-2 py-0.5 rounded"
                              style="background: var(--bg-elevated); color: var(--text-primary);">${id}</span>
                        <span class="sti-badge sti-dep-${depto}">${depto}</span>
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

    /* Briefing em modo leitura — usado por Meus chamados e pela Fila. */
    window.stiBriefingHtml = function (brief, categoria) {
        if (!brief || typeof brief !== 'object') return '';
        const linhas = BRIEF_ORDER.filter(k => brief[k] !== undefined && brief[k] !== '').map(k => {
            const raw = brief[k];
            let val;
            if (typeof raw === 'boolean') val = raw ? 'Sim' : 'Não';
            else if (Array.isArray(raw)) val = raw.map(v => BRIEF_VALUE_LABELS[v] || v).join(', ');
            else val = BRIEF_VALUE_LABELS[raw] || String(raw);
            if (!String(val).trim()) return '';
            return `<div class="flex flex-col gap-0.5">
                <span class="text-slate-500 text-[10px] uppercase font-bold tracking-wider">${escapeHtml(BRIEF_LABELS[k] || k)}</span>
                <span class="text-sm break-words">${escapeHtml(val).replace(/\n/g, '<br>')}</span>
            </div>`;
        }).filter(Boolean).join('');
        if (!linhas) return '';
        const titulo = categoria ? `Briefing · ${categoria}` : 'Briefing';
        return `<div class="rounded-xl border border-[var(--border)] p-4 space-y-3" style="background: var(--bg-elevated);">
            <p class="text-[11px] font-bold uppercase tracking-wider" style="color: var(--primary);">${escapeHtml(titulo)}</p>
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">${linhas}</div>
        </div>`;
    };

    const isMkt = () => _depto === 'Marketing';

    function setCategorias() {
        const sel = $('sti-categoria');
        if (!sel) return;
        const mapa = (_cfg && _cfg.categorias_por_departamento) || {};
        const lista = mapa[_depto] || [];
        const placeholder = isMkt() ? 'Selecione o formato da peça…' : 'Selecione a categoria técnica…';
        const icones = {
            'Erros/Bugs': '🐛 ', 'Processos Novos': '⚙️ ', 'Ideias Novas': '💡 ',
            'Imagem': '🖼️ ', 'Vídeo': '🎥 ', 'UX / UI': '🖥️ ',
            'E-book': '📖 ', 'Brinde': '🎁 ', 'Outro': '✍️ ',
        };
        sel.innerHTML = `<option value="">${escapeHtml(placeholder)}</option>` + lista.map(c =>
            `<option value="${escapeHtml(c)}">${escapeHtml((icones[c] || '') + c)}</option>`
        ).join('');
    }

    function setBandeiras() {
        const sel = $('sti-bandeira');
        if (!sel) return;
        const lista = (_cfg && _cfg.bandeiras) || [];
        sel.innerHTML = '<option value="">Selecione sua bandeira…</option>' + lista.map(b =>
            `<option value="${escapeHtml(b)}">${escapeHtml(b)}</option>`
        ).join('');
    }

    function specVisible(def) {
        if (!def.showIf) return true;
        return Object.keys(def.showIf).every(k => _brief[k] === def.showIf[k]);
    }

    function renderSpecs() {
        const box = $('sti-specs');
        if (!box) return;
        const categoria = ($('sti-categoria')?.value || '').trim();
        const defs = (isMkt() && SPEC_DEFS[categoria]) || null;
        if (!defs) {
            box.classList.add('hidden');
            box.innerHTML = '';
            return;
        }
        const opcoes = ((_cfg && _cfg.briefing_opcoes) || {})[categoria] || {};
        const campos = defs.filter(specVisible).map(def => {
            const val = _brief[def.key];
            const label = `<label class="sti-spec-label">${escapeHtml(def.label)}</label>`;
            if (def.type === 'text') {
                return `<div>${label}<input type="text" data-spec="${def.key}" maxlength="2000"
                        value="${escapeHtml(val || '')}" placeholder="${escapeHtml(def.ph || '')}"
                        class="input-glass h-11 px-4 rounded-lg w-full text-sm font-medium"></div>`;
            }
            if (def.type === 'area') {
                return `<div>${label}<textarea data-spec="${def.key}" rows="3" maxlength="2000"
                        placeholder="${escapeHtml(def.ph || '')}"
                        class="input-glass p-4 rounded-lg w-full text-sm font-medium resize-none">${escapeHtml(val || '')}</textarea></div>`;
            }
            if (def.type === 'bool') {
                return `<div>${label}<div class="flex flex-wrap gap-2">
                        <button type="button" data-spec-bool="${def.key}" data-v="1"
                                class="sti-chip ${val === true ? 'is-active' : ''}">${escapeHtml(def.yes || 'Sim')}</button>
                        <button type="button" data-spec-bool="${def.key}" data-v="0"
                                class="sti-chip ${val === false ? 'is-active' : ''}">${escapeHtml(def.no || 'Não')}</button>
                    </div></div>`;
            }
            const lista = opcoes[def.opts] || [];
            const sel = def.type === 'multi' ? (Array.isArray(val) ? val : []) : [];
            const botoes = lista.map(([id, txt]) => {
                const on = def.type === 'multi' ? sel.includes(id) : val === id;
                const attr = def.type === 'multi' ? 'data-spec-multi' : 'data-spec-single';
                return `<button type="button" ${attr}="${def.key}" data-v="${escapeHtml(id)}"
                        class="sti-chip ${on ? 'is-active' : ''}">${escapeHtml(txt)}</button>`;
            }).join('');
            return `<div>${label}<div class="grid grid-cols-1 sm:grid-cols-2 gap-2">${botoes}</div></div>`;
        }).join('');

        box.classList.remove('hidden');
        box.innerHTML = `<div class="sti-spec-card space-y-4">
            <p class="text-xs font-bold uppercase tracking-wider" style="color: var(--primary);">
                Especificações · ${escapeHtml(categoria)}
            </p>
            ${campos}
        </div>`;
    }

    // Texto/valor dos campos de spec. Inputs de texto não re-renderizam (para
    // não perder o foco); botões re-renderizam porque podem revelar campos.
    function bindSpecDelegation() {
        const box = $('sti-specs');
        if (!box) return;
        box.addEventListener('input', (e) => {
            const key = e.target.dataset && e.target.dataset.spec;
            if (!key) return;
            const v = (e.target.value || '').trim();
            if (v) _brief[key] = v; else delete _brief[key];
        });
        box.addEventListener('click', (e) => {
            const btn = e.target.closest('button[data-spec-bool], button[data-spec-single], button[data-spec-multi]');
            if (!btn) return;
            const d = btn.dataset;
            if (d.specBool) {
                const novo = d.v === '1';
                if (_brief[d.specBool] === novo) delete _brief[d.specBool];
                else _brief[d.specBool] = novo;
            } else if (d.specSingle) {
                if (_brief[d.specSingle] === d.v) delete _brief[d.specSingle];
                else _brief[d.specSingle] = d.v;
            } else if (d.specMulti) {
                const atual = Array.isArray(_brief[d.specMulti]) ? _brief[d.specMulti] : [];
                _brief[d.specMulti] = atual.includes(d.v)
                    ? atual.filter(x => x !== d.v)
                    : atual.concat([d.v]);
                if (!_brief[d.specMulti].length) delete _brief[d.specMulti];
            }
            renderSpecs();
        });
    }

    function setTelefone(v) {
        const group = $('sti-tel-group');
        if (!group) return;
        group.dataset.value = v;
        group.querySelectorAll('.sti-tel-btn').forEach(b => {
            b.classList.toggle('is-active', b.dataset.tel === v);
        });
        const wrap = $('sti-tel-wrap');
        if (wrap) wrap.classList.toggle('hidden', v !== 'sim');
        if (v !== 'sim') {
            const el = $('sti-telefone');
            if (el) el.value = '';
        }
    }

    window.stiSetDepartamento = function (depto) {
        _depto = depto === 'Marketing' ? 'Marketing' : 'TI';
        const group = $('sti-depto-group');
        if (group) {
            group.dataset.value = _depto;
            group.querySelectorAll('.sti-depto-btn').forEach(b => {
                b.classList.toggle('is-active', b.dataset.depto === _depto);
            });
        }
        const mkt = isMkt();
        document.querySelectorAll('#page-solicitacoes_ti .sti-mkt-only').forEach(el => {
            el.classList.toggle('hidden', !mkt);
        });

        const txt = (id, v) => { const el = $(id); if (el) el.textContent = v; };
        const req = '<span class="text-red-500">*</span>';
        const html = (id, v) => { const el = $(id); if (el) el.innerHTML = v; };
        if (mkt) {
            txt('sti-form-sub', 'Preencha o briefing abaixo para enviar sua solicitação ao setor de Marketing.');
            html('sti-lbl-categoria', 'Formato da Peça ' + req);
            html('sti-lbl-titulo', 'Título Resumido / Assunto ' + req);
            html('sti-lbl-descricao', 'Informações Obrigatórias / Detalhes ' + req);
            txt('sti-lbl-observacoes', 'Outras Informações (opcional)');
            txt('sti-footer-hint', 'O briefing fica registrado e o time de Marketing recebe um aviso.');
            const d = $('sti-descricao');
            if (d) d.placeholder = 'Logo, disclaimer, cupom, datas de vigência, URL do site…';
            const o = $('sti-observacoes');
            if (o) o.placeholder = 'Paleta, tom de voz, referências…';
            const b = $('sti-submit-btn');
            if (b) b.textContent = 'Enviar Briefing de Marketing';
        } else {
            txt('sti-form-sub', 'Preencha os dados abaixo para enviar sua solicitação ao setor de TI.');
            html('sti-lbl-categoria', 'Tipo de Requisição ' + req);
            html('sti-lbl-titulo', 'Título Resumido / Assunto ' + req);
            html('sti-lbl-descricao', 'Descrição Detalhada ' + req);
            txt('sti-lbl-observacoes', 'Observações Extras / Patrimônio (opcional)');
            txt('sti-footer-hint', 'O chamado fica registrado no sistema para o TI acompanhar.');
            const d = $('sti-descricao');
            if (d) d.placeholder = 'Descreva detalhadamente sua solicitação…';
            const o = $('sti-observacoes');
            if (o) o.placeholder = 'Ex: Patrimônio nº 8574-A ou RAMAL 415';
            const b = $('sti-submit-btn');
            if (b) b.textContent = 'Enviar Chamado de TI';
        }

        setCategorias();
        _brief = {};
        renderSpecs();
        clearError();
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
        if (id) id.textContent = ticket && ticket.protocolo
            ? `Protocolo: ${ticket.protocolo}`
            : '';
        hide('sti-form');
        show('sti-success');
    }

    window.stiResetForm = function () {
        ['sti-titulo', 'sti-descricao', 'sti-observacoes', 'sti-publico-alvo',
         'sti-assunto-tema', 'sti-objetivo', 'sti-prazo', 'sti-telefone'].forEach(id => {
            const el = $(id);
            if (el) el.value = '';
        });
        const band = $('sti-bandeira');
        if (band) band.value = '';
        setTelefone('nao');
        _brief = {};
        renderSpecs();
        clearError();
        hide('sti-success');
        show('sti-form');
        const t = $('sti-titulo');
        if (t) t.focus();
    };

    function collectBriefing() {
        const brief = Object.assign({}, _brief);
        const put = (key, id) => {
            const v = ($(id)?.value || '').trim();
            if (v) brief[key] = v;
        };
        put('bandeira', 'sti-bandeira');
        put('publico_alvo', 'sti-publico-alvo');
        put('assunto_tema', 'sti-assunto-tema');
        put('objetivo_peca', 'sti-objetivo');
        const temTel = ($('sti-tel-group')?.dataset.value || 'nao') === 'sim';
        brief.tem_telefone = temTel;
        if (temTel) put('telefone_contato', 'sti-telefone');
        return brief;
    }

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

        const payload = {
            departamento: _depto,
            solicitante, setor, categoria, urgencia,
            titulo, descricao, observacoes,
        };
        if (isMkt()) {
            const brief = collectBriefing();
            if (!brief.bandeira || !brief.publico_alvo) {
                showError('Preencha bandeira / empresa e público-alvo.');
                return;
            }
            payload.briefing = brief;
            const prazo = ($('sti-prazo')?.value || '').trim();
            if (prazo) payload.prazo_desejado = prazo;
        }

        setBusy(true);
        try {
            const resp = await fetch('/api/solicitacoes_ti/submit', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload),
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
            _cfg = data;
            const sol = $('sti-solicitante');
            if (sol && !sol.value && data.default_solicitante) {
                sol.value = data.default_solicitante;
            }
            setBandeiras();
            setCategorias();
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

    function bindDepartamento() {
        const group = $('sti-depto-group');
        if (group) {
            group.querySelectorAll('.sti-depto-btn').forEach(btn => {
                btn.addEventListener('click', () => stiSetDepartamento(btn.dataset.depto));
            });
        }
        const tel = $('sti-tel-group');
        if (tel) {
            tel.querySelectorAll('.sti-tel-btn').forEach(btn => {
                btn.addEventListener('click', () => setTelefone(btn.dataset.tel));
            });
        }
        // Trocar o formato da peça troca o bloco de especificações.
        const cat = $('sti-categoria');
        if (cat) cat.addEventListener('change', () => { _brief = {}; renderSpecs(); });
    }

    window.loadSolicitacoesTi = function () {
        if (!_stiInited) {
            _stiInited = true;
            bindUrgenciaButtons();
            bindDepartamento();
            bindSpecDelegation();
            setUrgencia('Média');
            setTelefone('nao');
            stiSetDepartamento('TI');
        }
        loadConfig();
        renderRecent();
        clearError();
        hide('sti-success');
        show('sti-form');
        hide('sti-loading');
    };
})();
