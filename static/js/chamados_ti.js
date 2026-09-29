/* Fila de chamados (TI e Marketing) — listagem + alteração de status.
   O departamento visível depende da permissão: `chamados_ti` mostra a fila de
   TI, `chamados_marketing` a de Marketing (o backend é quem escopa). */
(function () {
    let _status = 'abertos';
    let _depto = '';          // '' = todas as filas que o usuário pode ver
    let _deptosOk = null;     // preenchido pela 1ª resposta do backend
    let _qTimer = null;
    let _openId = null;
    let _pickedStatus = null;
    let _view = 'lista';      // 'lista' | 'kanban'
    let _items = [];          // última resposta do backend (re-render sem refetch)
    let _meta = {};           // is_admin / user_id / departamentos_exclusivos
    let _dragId = null;       // id do chamado em arraste (dataTransfer não é legível no dragover)
    let _dragFrom = null;
    let _checks = [];
    let _openStatus = '';
    let _canManage = false;
    let _ticketSnap = null;

    const VIEW_KEY = 'cti_view_v1';
    const STATUS_ORDEM = ['Novo Ticket', 'A Fazer', 'Em Produção', 'Pausado', 'Concluído'];
    const STATUS_COR = {
        'Novo Ticket': '#60a5fa', 'A Fazer': '#34d399', 'Em Produção': '#a78bfa',
        'Pausado': '#fb923c', 'Concluído': '#2dd4bf',
    };
    const STATUS_CLS = {
        'Novo Ticket': 'sti-st-novo', 'A Fazer': 'sti-st-fazer', 'Em Produção': 'sti-st-prod',
        'Pausado': 'sti-st-pausado', 'Concluído': 'sti-st-Concluído',
    };
    const TIPO_CLS = {
        'Imagem': 'cti-tipo-imagem', 'Vídeo': 'cti-tipo-video', 'UX / UI': 'cti-tipo-ux',
        'E-book': 'cti-tipo-ebook', 'Brinde': 'cti-tipo-brinde',
        'Erros/Bugs': 'cti-tipo-ti', 'Processos Novos': 'cti-tipo-ti', 'Ideias Novas': 'cti-tipo-ti',
    };

    const CATEGORIAS_POR_DEPTO = {
        'TI': ['Erros/Bugs', 'Processos Novos', 'Ideias Novas'],
        'Marketing': ['Imagem', 'Vídeo', 'UX / UI', 'E-book', 'Brinde'],
    };

    function $(id) { return document.getElementById(id); }
    function escapeHtml(s) {
        return String(s ?? '').replace(/[&<>"']/g, c => (
            { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]
        ));
    }
    function statusClass(st) {
        return STATUS_CLS[st] || 'sti-st-novo';
    }
    function fmtTs(iso) {
        if (!iso) return '—';
        try {
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return escapeHtml(iso);
            return d.toLocaleString('pt-BR', { dateStyle: 'short', timeStyle: 'short' });
        } catch (e) {
            return escapeHtml(iso);
        }
    }

    function setKpis(kpis) {
        kpis = kpis || {};
        const ids = {
            'Novo Ticket': 'cti-kpi-novo', 'A Fazer': 'cti-kpi-fazer',
            'Em Produção': 'cti-kpi-prod', 'Pausado': 'cti-kpi-pausado',
            'Concluído': 'cti-kpi-concluido',
        };
        Object.keys(ids).forEach(st => {
            const el = $(ids[st]);
            if (el) el.textContent = String(kpis[st] || 0);
        });
    }

    function qs() {
        const params = new URLSearchParams();
        if (_status) params.set('status', _status);
        else params.set('status', 'todos');
        const urg = $('cti-urgencia')?.value || '';
        const setor = $('cti-setor')?.value || '';
        const cat = $('cti-categoria')?.value || '';
        const q = ($('cti-q')?.value || '').trim();
        if (urg) params.set('urgencia', urg);
        if (setor) params.set('setor', setor);
        if (cat) params.set('categoria', cat);
        if (_depto) params.set('departamento', _depto);
        if (q) params.set('q', q);
        params.set('limit', '150');
        return params.toString();
    }

    /* Filtro de tipo/formato: as opções dependem do departamento escolhido.
       Sem departamento definido, junta os dois conjuntos. */
    function setCategoriaOptions() {
        const sel = $('cti-categoria');
        if (!sel) return;
        const anterior = sel.value;
        const deptos = _depto ? [_depto] : (_deptosOk || Object.keys(CATEGORIAS_POR_DEPTO));
        const lista = [];
        deptos.forEach(d => (CATEGORIAS_POR_DEPTO[d] || []).forEach(c => {
            if (!lista.includes(c)) lista.push(c);
        }));
        sel.innerHTML = '<option value="">Tipo / formato</option>' + lista.map(c =>
            `<option value="${escapeHtml(c)}">${escapeHtml(c)}</option>`
        ).join('');
        sel.value = lista.includes(anterior) ? anterior : '';
    }

    function renderDeptoTabs(deptos, abertos) {
        const wrap = $('cti-depto-wrap');
        const tabs = $('cti-depto-tabs');
        if (!wrap || !tabs) return;
        // Uma fila só: nada a escolher, mantém a barra escondida.
        if (!deptos || deptos.length < 2) {
            wrap.classList.add('hidden');
            const sub = $('cti-sub');
            if (sub && deptos && deptos.length === 1) {
                sub.textContent = `Fila de ${deptos[0]} · altere o status conforme o andamento`;
            }
            return;
        }
        wrap.classList.remove('hidden');
        const conta = abertos || {};
        const opcoes = [['', 'Todas as filas']].concat(deptos.map(d => [d, d]));
        tabs.innerHTML = opcoes.map(([v, txt]) => {
            const n = v ? conta[v] : deptos.reduce((s, d) => s + (conta[d] || 0), 0);
            const badge = (n || n === 0) ? ` (${n})` : '';
            return `<button type="button" data-depto="${escapeHtml(v)}"
                    class="cti-dep-tab ${v === _depto ? 'is-active' : ''}">${escapeHtml(txt + badge)}</button>`;
        }).join('');
        tabs.querySelectorAll('.cti-dep-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                _depto = btn.dataset.depto || '';
                setCategoriaOptions();
                loadList();
            });
        });
    }

    async function loadList() {
        const resp = await fetch('/api/solicitacoes_ti/chamados?' + qs(), { cache: 'no-store' });
        const data = await resp.json().catch(() => ({}));
        if (resp.status === 403) {
            _items = [];
            const tbody = $('cti-tbody');
            if (tbody) tbody.innerHTML = `<tr><td colspan="10" class="px-4 py-8 text-center text-sm text-slate-500">${escapeHtml(data.message || 'Sem permissão.')}</td></tr>`;
            const kb = $('cti-kanban');
            if (kb) kb.innerHTML = '';
            return;
        }
        _items = Array.isArray(data.items) ? data.items : [];
        _meta = {
            is_admin: !!data.is_admin,
            user_id: data.user_id ?? null,
            exclusivos: Array.isArray(data.departamentos_exclusivos) ? data.departamentos_exclusivos : [],
        };
        setKpis(data.kpis);
        if (Array.isArray(data.fila_departamentos)) {
            const primeiraCarga = _deptosOk === null;
            _deptosOk = data.fila_departamentos;
            if (primeiraCarga) setCategoriaOptions();
            renderDeptoTabs(_deptosOk, data.abertos_por_departamento);
        }
        render();
    }

    function render() {
        const empty = $('cti-empty');
        if (empty) empty.classList.toggle('hidden', _items.length > 0);
        if (_view === 'kanban') renderKanban(_items);
        else renderTable(_items);
    }

    function renderTable(items) {
        const tbody = $('cti-tbody');
        if (!tbody) return;
        tbody.innerHTML = items.map(t => `
            <tr class="border-b border-[var(--border)] text-sm">
                <td class="px-4 py-3 font-mono text-[11px] font-bold">${escapeHtml(t.protocolo)}</td>
                <td class="px-4 py-3"><span class="sti-dep-${escapeHtml(t.departamento || 'TI')}">${escapeHtml(t.departamento || 'TI')}</span></td>
                <td class="px-4 py-3 max-w-[240px] truncate font-medium">${escapeHtml(t.titulo)}</td>
                <td class="px-4 py-3">${escapeHtml(t.solicitante)}</td>
                <td class="px-4 py-3">${escapeHtml(t.setor)}</td>
                <td class="px-4 py-3"><span class="sti-badge sti-badge-${escapeHtml(t.urgencia)}">${escapeHtml(t.urgencia)}</span></td>
                <td class="px-4 py-3"><span class="sti-badge ${statusClass(t.status)}">${escapeHtml(t.status)}</span></td>
                <td class="px-4 py-3 whitespace-nowrap">${respCell(t)}</td>
                <td class="px-4 py-3 font-mono text-[11px] text-slate-400 whitespace-nowrap">${fmtTs(t.created_at)}</td>
                <td class="px-4 py-3 pr-6 whitespace-nowrap text-right">
                    <button type="button" onclick="ctiOpen(${t.id})" class="text-xs font-bold" style="color: var(--primary);">Abrir</button>
                </td>
            </tr>
        `).join('');
    }

    /* ── Kanban ──────────────────────────────────────────────────────────── */

    /* Espelha `_pode_alterar_status` do backend: nas filas exclusivas
       (Marketing) o status é do responsável; chamado livre é de quem pegar. */
    function podeAlterar(t) {
        if (_meta.is_admin) return true;
        const depto = t.departamento || 'TI';
        if (!(_meta.exclusivos || []).includes(depto)) return true;
        if (t.responsavel_user_id == null) return true;
        return _meta.user_id != null && t.responsavel_user_id === _meta.user_id;
    }

    /* Colunas visíveis = o que o chip de status deixa passar. */
    function colunasVisiveis() {
        if (_status === 'abertos') return STATUS_ORDEM.filter(st => st !== 'Concluído');
        if (!_status) return STATUS_ORDEM.slice();
        return STATUS_ORDEM.includes(_status) ? [_status] : STATUS_ORDEM.slice();
    }

    /* Badge de prazo (só Marketing preenche `prazo_desejado`). */
    function prazoBadge(t) {
        const m = /^(\d{4})-(\d{2})-(\d{2})/.exec(t.prazo_desejado || '');
        if (!m) return '';
        const alvo = new Date(+m[1], +m[2] - 1, +m[3]);
        const hoje = new Date();
        hoje.setHours(0, 0, 0, 0);
        const dias = Math.round((alvo - hoje) / 86400000);
        const dataTxt = `${m[3]}/${m[2]}`;
        let cls = 'cti-prazo-ok';
        let txt = `Prazo ${dataTxt}`;
        if (t.status === 'Concluído') {
            cls = 'cti-prazo-done';
            txt = 'Concluído';
        } else if (dias < 0) {
            cls = 'cti-prazo-late';
            txt = `Atrasado há ${-dias} ${-dias === 1 ? 'dia' : 'dias'}`;
        } else if (dias === 0) {
            cls = 'cti-prazo-soon';
            txt = 'Entrega hoje';
        } else if (dias <= 3) {
            cls = 'cti-prazo-soon';
            txt = `Falta${dias === 1 ? '' : 'm'} ${dias} ${dias === 1 ? 'dia' : 'dias'}`;
        }
        return `<span class="cti-prazo ${cls}"><span class="material-symbols-outlined text-[13px]">schedule</span>${escapeHtml(txt)}</span>`;
    }

    function iniciais(nome) {
        const p = String(nome || '').trim().split(/\s+/).filter(Boolean);
        if (!p.length) return '?';
        return ((p[0][0] || '') + (p[1] ? p[1][0] : '')).toUpperCase();
    }

    const PROG_POR_STATUS = {
        'Novo Ticket': 10, 'A Fazer': 25, 'Em Produção': 75, 'Pausado': 30, 'Concluído': 100,
    };
    function temaDe(cat) {
        const c = String(cat || '').toLowerCase();
        if (c.includes('vídeo') || c.includes('video')) return 'video';
        if (c.includes('ux')) return 'ux';
        if (c.includes('e-book') || c.includes('ebook')) return 'ebook';
        if (c.includes('brinde')) return 'brinde';
        if (c.includes('imagem')) return 'imagem';
        return 'outro';
    }
    function progressoDe(t, checks) {
        if (checks && checks.length) {
            return Math.round(checks.filter(c => c.feito).length * 100 / checks.length);
        }
        return PROG_POR_STATUS[t.status] ?? 10;
    }

    function cardHtml(t) {
        const pode = podeAlterar(t);
        const tipo = t.categoria || (t.departamento || 'TI');
        const nota = String(t.nota || t.status_nota || '').trim();
        const respNome = (t.responsavel_nome || '').trim();
        const checks = Array.isArray(t.checklist) ? t.checklist : [];
        const pendentes = checks.filter(c => !c.feito);
        const pct = progressoDe(t, checks);
        const filled = Math.round(pct / 10);
        const dots = Array.from({ length: 10 }, (_, i) =>
            `<span class="cti-dot${i < filled ? ' on' : ''}"></span>`).join('');
        let checkHtml = '';
        if (checks.length && !pendentes.length) {
            checkHtml = '<p class="cti-kb-done">Checklist concluído</p>';
        } else if (pendentes.length) {
            checkHtml = `<ul class="cti-kb-checks">${pendentes.slice(0, 3).map(c =>
                `<li>${escapeHtml(c.texto)}</li>`).join('')}</ul>`
                + (pendentes.length > 3 ? `<p class="cti-kb-more">+${pendentes.length - 3} pendente(s)</p>` : '');
        }
        const prazo = prazoBadge(t) || (t.status === 'Concluído'
            ? '<span class="cti-prazo cti-prazo-done">Concluído</span>'
            : '<span class="cti-prazo cti-prazo-ok">Sem prazo</span>');
        const quem = respNome || 'Livre';
        return `
            <div class="cti-kb-card" data-theme="${temaDe(t.categoria)}" data-id="${t.id}" data-status="${escapeHtml(t.status)}"
                 ${pode ? 'draggable="true"' : ''} role="button" tabindex="0"
                 title="${pode ? 'Arraste para mudar o status ou clique para abrir' : 'Só o responsável altera o status deste chamado'}">
                <div class="cti-kb-top">
                    <span class="cti-kb-tag">${escapeHtml(tipo)}</span>
                </div>
                <p class="cti-kb-title">${escapeHtml(t.titulo)}</p>
                ${nota ? `<p class="cti-kb-note"><strong>Nota: </strong>${escapeHtml(nota)}</p>` : ''}
                ${checkHtml}
                <div class="cti-kb-meta">
                    <span>Prazo de entrega</span>
                    ${prazo}
                </div>
                <div class="cti-kb-prog-row"><span>Progresso</span><strong>${pct}%</strong></div>
                <div class="cti-kb-dots">${dots}</div>
                <div class="cti-kb-foot">
                    <span class="cti-kb-who"><span class="cti-kb-ava">${escapeHtml(iniciais(quem === 'Livre' ? '' : quem))}</span><span class="cti-kb-who-name">${escapeHtml(quem.split(' ')[0])}</span></span>
                    <span class="cti-kb-count">${checks.length}</span>
                </div>
            </div>`;
    }

    function renderKanban(items) {
        const wrap = $('cti-kanban');
        if (!wrap) return;
        const cols = colunasVisiveis();
        const hint = $('cti-kb-hint');
        if (hint) hint.classList.toggle('hidden', cols.length > 1);
        wrap.innerHTML = cols.map(st => {
            const doColuna = items.filter(t => t.status === st);
            const cards = doColuna.length
                ? doColuna.map(cardHtml).join('')
                : '<p class="cti-kb-empty">Nenhum chamado aqui.</p>';
            return `
                <section class="cti-kb-col" data-status="${escapeHtml(st)}">
                    <header class="cti-kb-head">
                        <span class="cti-kb-col-name"><span class="cti-kb-caret">▸</span> ${escapeHtml(st)} <span class="cti-kb-col-n">(${doColuna.length})</span></span>
                        <button type="button" class="cti-col-add" title="Abrir novo chamado">+</button>
                    </header>
                    <div class="cti-kb-body">${cards}</div>
                </section>`;
        }).join('');
        wrap.querySelectorAll('.cti-col-add').forEach(btn => {
            btn.addEventListener('click', () => {
                if (typeof navigate === 'function') navigate('solicitacoes_ti');
            });
        });
        bindKanban(wrap);
    }

    function bindKanban(wrap) {
        wrap.querySelectorAll('.cti-kb-card').forEach(card => {
            card.addEventListener('click', () => {
                if (card.classList.contains('is-saving')) return;
                ctiOpen(Number(card.dataset.id));
            });
            card.addEventListener('keydown', (e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    ctiOpen(Number(card.dataset.id));
                }
            });
            if (card.getAttribute('draggable') !== 'true') return;
            card.addEventListener('dragstart', (e) => {
                _dragId = Number(card.dataset.id);
                _dragFrom = card.dataset.status || '';
                card.classList.add('is-dragging');
                try {
                    e.dataTransfer.effectAllowed = 'move';
                    e.dataTransfer.setData('text/plain', String(_dragId));
                } catch (_) { /* noop */ }
            });
            card.addEventListener('dragend', () => {
                card.classList.remove('is-dragging');
                _dragId = null;
                _dragFrom = null;
                wrap.querySelectorAll('.cti-kb-col.is-over').forEach(c => c.classList.remove('is-over'));
            });
        });
        wrap.querySelectorAll('.cti-kb-col').forEach(col => {
            const alvo = col.dataset.status || '';
            col.addEventListener('dragover', (e) => {
                if (_dragId == null || alvo === _dragFrom) return;
                e.preventDefault();
                e.dataTransfer.dropEffect = 'move';
                col.classList.add('is-over');
            });
            col.addEventListener('dragleave', (e) => {
                if (!col.contains(e.relatedTarget)) col.classList.remove('is-over');
            });
            col.addEventListener('drop', (e) => {
                col.classList.remove('is-over');
                if (_dragId == null || alvo === _dragFrom) return;
                e.preventDefault();
                moverCard(_dragId, alvo);
            });
        });
    }

    /* Move o card para a coluna alvo e grava. Se o backend recusar (403 de quem
       não é responsável, 409, etc.) o card volta para a coluna de origem. */
    async function moverCard(id, novoStatus) {
        const wrap = $('cti-kanban');
        const card = wrap?.querySelector(`.cti-kb-card[data-id="${id}"]`);
        const alvo = wrap?.querySelector(`.cti-kb-col[data-status="${novoStatus}"] .cti-kb-body`);
        if (!card || !alvo) return;
        const origem = card.parentElement;
        const vizinho = card.nextElementSibling;
        const vazio = alvo.querySelector('.cti-kb-empty');
        if (vazio) vazio.remove();
        alvo.appendChild(card);
        card.classList.add('is-saving');
        try {
            const resp = await fetch('/api/solicitacoes_ti/chamados/' + id + '/status', {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: novoStatus, nota: '' }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || data.ok === false) {
                card.classList.remove('is-saving');
                if (vizinho) origem.insertBefore(card, vizinho);
                else origem.appendChild(card);
                if (!alvo.children.length) alvo.innerHTML = '<p class="cti-kb-empty">Nenhum chamado aqui.</p>';
                if (typeof toast === 'function') toast(data.message || 'Não foi possível mudar o status.', 'error');
                return;
            }
            if (typeof toast === 'function') toast(data.message || 'Status atualizado.', 'success');
            await loadList();
        } catch (e) {
            console.error(e);
            card.classList.remove('is-saving');
            if (vizinho) origem.insertBefore(card, vizinho);
            else origem.appendChild(card);
            if (typeof toast === 'function') toast('Falha de rede.', 'error');
        }
    }

    function applyView(v, persist) {
        _view = v === 'kanban' ? 'kanban' : 'lista';
        if (persist) {
            try { localStorage.setItem(VIEW_KEY, _view); } catch (_) { /* noop */ }
        }
        document.querySelectorAll('#cti-view-seg .cti-view-btn').forEach(b => {
            b.classList.toggle('ds-segment__btn--active', (b.dataset.view || '') === _view);
        });
        const list = $('cti-list-wrap');
        const kb = $('cti-kanban');
        const hint = $('cti-kb-hint');
        if (list) list.classList.toggle('hidden', _view === 'kanban');
        if (kb) kb.classList.toggle('hidden', _view !== 'kanban');
        if (hint && _view !== 'kanban') hint.classList.add('hidden');
    }

    /* Quem puxou o chamado. Livre = disponível para qualquer um da fila. */
    function respCell(t) {
        if (!t.responsavel_nome) {
            return '<span class="sti-badge cti-livre">Livre</span>';
        }
        return `<span class="text-xs font-medium">${escapeHtml(t.responsavel_nome)}</span>`;
    }

    function renderTimeline(eventos) {
        if (!eventos || !eventos.length) return '<p class="text-xs text-slate-500">Sem histórico ainda.</p>';
        return `<ol class="space-y-2 border-l border-[var(--border)] pl-4">` + eventos.map(ev => `
            <li>
                <p class="text-xs font-semibold">${escapeHtml(ev.status_novo || '')}
                    <span class="font-normal text-slate-500">· ${escapeHtml(ev.autor_nome || '')}</span>
                </p>
                <p class="text-[11px] text-slate-400 font-mono">${fmtTs(ev.created_at)}</p>
                ${ev.nota ? `<p class="text-xs text-slate-500 mt-0.5">${escapeHtml(ev.nota)}</p>` : ''}
            </li>
        `).join('') + '</ol>';
    }

    function highlightStatus(st) {
        _pickedStatus = st;
        const chip = $('cti-status-chip');
        if (chip) {
            chip.className = 'cti-status-chip ' + statusClass(st);
            chip.textContent = st || '';
        }
        const sel = $('cti-status-sel');
        if (sel && sel.value !== st) sel.value = st;
    }

    function renderChecklist() {
        const list = $('cti-check-list');
        if (!list) return;
        const feitos = _checks.filter(c => c.feito).length;
        const pct = _checks.length ? Math.round(feitos * 100 / _checks.length) : 0;
        const count = $('cti-check-count');
        if (count) count.textContent = `${feitos} de ${_checks.length} concluídos`;
        const bar = $('cti-check-bar');
        if (bar) bar.style.width = pct + '%';
        list.innerHTML = _checks.map((c, i) => `
            <li>
                <label>
                    <input type="checkbox" data-i="${i}" ${_canManage ? '' : 'disabled'} ${c.feito ? 'checked' : ''}>
                    <span class="${c.feito ? 'is-done' : ''}">${escapeHtml(c.texto)}</span>
                </label>
                ${_canManage ? `<button type="button" data-del="${i}" aria-label="Remover">×</button>` : ''}
            </li>`).join('') || '<li class="cti-check-empty">Nenhuma entrega neste checklist.</li>';
        list.querySelectorAll('input[type="checkbox"]').forEach(inp => {
            inp.addEventListener('change', () => {
                const i = Number(inp.dataset.i);
                if (_checks[i]) _checks[i].feito = inp.checked;
                renderChecklist();
            });
        });
        list.querySelectorAll('[data-del]').forEach(btn => {
            btn.addEventListener('click', () => {
                _checks.splice(Number(btn.dataset.del), 1);
                renderChecklist();
            });
        });
    }

    window.ctiOpen = async function (id) {
        const resp = await fetch('/api/solicitacoes_ti/chamados/' + id, { cache: 'no-store' });
        const data = await resp.json().catch(() => ({}));
        if (!resp.ok || !data.ticket) {
            if (typeof toast === 'function') toast(data.message || 'Não foi possível abrir o chamado.', 'error');
            return;
        }
        const t = data.ticket;
        _openId = t.id;
        _openStatus = t.status || '';
        _canManage = !!data.can_manage;
        _ticketSnap = t;
        _checks = (Array.isArray(t.checklist) ? t.checklist : []).map(c => ({
            id: c.id, texto: c.texto, feito: !!c.feito,
        }));
        const depto = t.departamento || 'TI';
        const isMkt = depto === 'Marketing';
        const sheet = document.querySelector('#cti-modal .sti-ticket-card');
        if (sheet) {
            sheet.classList.add('cti-sheet');
            sheet.dataset.theme = temaDe(t.categoria);
        }
        const chips = $('cti-modal-chips');
        if (chips) {
            chips.innerHTML = `
                <span class="cti-sheet-code">${escapeHtml(t.protocolo || '')}</span>
                <span id="cti-status-chip" class="cti-status-chip ${statusClass(t.status)}">${escapeHtml(t.status || '')}</span>
                <span class="cti-sheet-tag">${escapeHtml(t.categoria || depto)}</span>
                ${t.bandeira ? `<span class="cti-sheet-soft">${escapeHtml(t.bandeira)}</span>` : ''}
                ${prazoBadge(t)}
            `;
        }
        highlightStatus(t.status);
        $('cti-modal-title').textContent = t.titulo || '';
        const meta = $('cti-modal-meta');
        if (meta) {
            meta.textContent = `Criado em ${fmtTs(t.created_at)} · Solicitante: ${t.solicitante || '—'}`;
        }
        const dis = _canManage ? '' : 'disabled';
        const briefHtml = (isMkt && typeof stiBriefingHtml === 'function')
            ? stiBriefingHtml(t.briefing, t.categoria) : '';
        const feitos = _checks.filter(c => c.feito).length;
        const pct = _checks.length ? Math.round(feitos * 100 / _checks.length) : 0;
        $('cti-modal-body').innerHTML = `
            ${_canManage ? `<label class="cti-field">
                <span class="cti-field-label">Status</span>
                <select id="cti-status-sel" class="input-glass w-full p-3 rounded-lg text-sm">
                    ${STATUS_ORDEM.map(st => `<option value="${escapeHtml(st)}"${st === t.status ? ' selected' : ''}>${escapeHtml(st)}</option>`).join('')}
                </select>
            </label>` : ''}
            <label class="cti-field">
                <span class="cti-field-label"><span class="material-symbols-outlined text-[16px]">link</span> Link da demanda / arquivos</span>
                <input id="cti-link" type="url" maxlength="500" ${dis} class="input-glass w-full p-3 rounded-lg text-sm"
                       placeholder="https://drive.google.com/… ou Figma, Canva" value="${escapeHtml(t.link_demanda || '')}">
                <span class="cti-field-hint">Link de arquivos, pastas do Drive, Figma ou material de entrega.</span>
            </label>
            <label class="cti-field">
                <span class="cti-field-label">Observação / alinhamento interno</span>
                <textarea id="cti-obs" rows="3" maxlength="300" ${dis} class="input-glass w-full p-3 rounded-lg text-sm"
                          placeholder="Notas de alinhamento para a equipe">${escapeHtml(t.observacoes || '')}</textarea>
            </label>
            <p><span class="text-slate-500 text-xs uppercase font-bold">${isMkt ? 'Informações obrigatórias' : 'Descrição'}</span><br>${escapeHtml(t.descricao || '').replace(/\n/g, '<br>')}</p>
            <section class="cti-check">
                <div class="cti-check-head">
                    <span class="cti-field-label"><span class="material-symbols-outlined text-[16px]">checklist</span> Checklist de entregas</span>
                    <span id="cti-check-count">${feitos} de ${_checks.length} concluídos</span>
                </div>
                <div class="cti-kb-prog"><span id="cti-check-bar" style="width:${pct}%"></span></div>
                <ul id="cti-check-list" class="cti-check-list"></ul>
                ${_canManage ? `<div class="cti-check-add">
                    <input id="cti-check-novo" maxlength="180" class="input-glass flex-1 p-2 rounded-lg text-sm" placeholder="Adicionar nova etapa ou entrega…">
                    <button type="button" id="cti-check-add" class="px-3 h-9 rounded-lg text-xs font-bold border border-[var(--border)]">Adicionar</button>
                </div>` : ''}
            </section>
            ${briefHtml}
            <div>
                <p class="text-slate-500 text-xs uppercase font-bold mb-2">Histórico</p>
                ${renderTimeline(data.eventos)}
            </div>
        `;
        const selSt = $('cti-status-sel');
        if (selSt) selSt.addEventListener('change', () => highlightStatus(selSt.value));
        renderChecklist();
        const addBtn = $('cti-check-add');
        if (addBtn) addBtn.addEventListener('click', () => {
            const inp = $('cti-check-novo');
            const texto = (inp?.value || '').trim();
            if (!texto) return;
            _checks.push({ id: Math.random().toString(36).slice(2, 10), texto, feito: false });
            if (inp) inp.value = '';
            renderChecklist();
        });
        const saveBtn = $('cti-save-status');
        if (saveBtn) saveBtn.classList.toggle('hidden', !_canManage);
        const delBtn = $('cti-excluir');
        if (delBtn) delBtn.classList.toggle('hidden', !data.is_admin);
        renderResponsavel(t, data);
        const modal = $('cti-modal');
        if (typeof dczPortalToBody === 'function') dczPortalToBody(modal);
        modal.classList.remove('hidden');
        if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(true);
    };

    /* Linha de responsável + botões de puxar/devolver do modal. */
    function renderResponsavel(t, data) {
        const txt = $('cti-resp-txt');
        if (txt) {
            txt.innerHTML = t.responsavel_nome
                ? `Responsável: <strong>${escapeHtml(t.responsavel_nome)}</strong>${data.sou_responsavel ? ' (você)' : ''}`
                : 'Sem responsável — puxe o chamado para assumir.';
        }
        const btnA = $('cti-assumir');
        if (btnA) btnA.classList.toggle('hidden', !data.can_assumir);
        const btnL = $('cti-liberar');
        if (btnL) {
            btnL.classList.toggle('hidden', !data.can_liberar);
            btnL.textContent = data.sou_responsavel ? 'Devolver à fila' : 'Liberar (admin)';
        }
        const save = $('cti-save-status');
        if (save) {
            const pode = data.can_manage !== false;
            save.disabled = !pode;
            save.style.opacity = pode ? '1' : '0.5';
            save.title = pode ? '' : 'Só o responsável altera o status.';
        }
    }

    async function postAcao(url, okMsg) {
        if (!_openId) return;
        try {
            const resp = await fetch(url, { method: 'POST' });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || data.ok === false) {
                // 409 = outra pessoa puxou primeiro: fecha e recarrega a fila.
                if (typeof toast === 'function') toast(data.message || 'Não foi possível concluir.', 'error');
                ctiCloseModal();
                await loadList();
                return;
            }
            if (typeof toast === 'function') toast(data.message || okMsg, 'success');
            await ctiOpen(_openId);
            await loadList();
        } catch (e) {
            console.error(e);
            if (typeof toast === 'function') toast('Falha de rede.', 'error');
        }
    }

    window.ctiAssumir = function () {
        return postAcao('/api/solicitacoes_ti/chamados/' + _openId + '/assumir', 'Chamado atribuído a você.');
    };

    window.ctiLiberar = function () {
        return postAcao('/api/solicitacoes_ti/chamados/' + _openId + '/liberar', 'Chamado devolvido à fila.');
    };

    window.ctiCloseModal = function () {
        const m = $('cti-modal');
        if (m) m.classList.add('hidden');
        _openId = null;
        if (typeof dczLockBodyScroll === 'function') dczLockBodyScroll(false);
    };

    window.ctiCopiar = async function () {
        const t = _ticketSnap;
        if (!t) return;
        const linhas = [
            t.protocolo, t.titulo, t.status,
            t.link_demanda ? 'Link: ' + t.link_demanda : '',
            t.observacoes ? 'Obs: ' + t.observacoes : '',
            _checks.map(c => (c.feito ? '[x] ' : '[ ] ') + c.texto).join('\n'),
        ].filter(Boolean).join('\n');
        try {
            await navigator.clipboard.writeText(linhas);
            if (typeof toast === 'function') toast('Copiado.', 'success');
        } catch (e) {
            if (typeof toast === 'function') toast('Não foi possível copiar.', 'error');
        }
    };

    window.ctiExcluir = async function () {
        if (!_openId) return;
        if (!window.confirm('Excluir este chamado? Essa ação não volta.')) return;
        try {
            const resp = await fetch('/api/solicitacoes_ti/chamados/' + _openId, { method: 'DELETE' });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || data.ok === false) {
                if (typeof toast === 'function') toast(data.message || 'Não foi possível excluir.', 'error');
                return;
            }
            if (typeof toast === 'function') toast(data.message || 'Chamado excluído.', 'success');
            ctiCloseModal();
            await loadList();
        } catch (e) {
            if (typeof toast === 'function') toast('Falha de rede.', 'error');
        }
    };

    window.ctiSaveStatus = async function () {
        if (!_openId || !_canManage) return;
        const btn = $('cti-save-status');
        if (btn) btn.disabled = true;
        try {
            const quadro = await fetch('/api/solicitacoes_ti/chamados/' + _openId + '/quadro', {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    link_demanda: ($('cti-link')?.value || '').trim(),
                    observacoes: ($('cti-obs')?.value || '').trim(),
                    checklist: _checks,
                }),
            });
            const quadroData = await quadro.json().catch(() => ({}));
            if (!quadro.ok || quadroData.ok === false) {
                if (typeof toast === 'function') toast(quadroData.message || 'Falha ao salvar o quadro.', 'error');
                return;
            }
            if (_pickedStatus && _pickedStatus !== _openStatus) {
                const resp = await fetch('/api/solicitacoes_ti/chamados/' + _openId + '/status', {
                    method: 'PATCH',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ status: _pickedStatus, nota: '' }),
                });
                const data = await resp.json().catch(() => ({}));
                if (!resp.ok || data.ok === false) {
                    if (typeof toast === 'function') toast(data.message || 'Quadro salvo, mas o status não mudou.', 'error');
                    await ctiOpen(_openId);
                    await loadList();
                    return;
                }
            }
            if (typeof toast === 'function') toast('Alterações salvas.', 'success');
            await ctiOpen(_openId);
            await loadList();
        } catch (e) {
            console.error(e);
            if (typeof toast === 'function') toast('Falha de rede.', 'error');
        } finally {
            if (btn) btn.disabled = false;
        }
    };

    window.ctiSetStatus = function (st) {
        _status = st || '';
        document.querySelectorAll('#cti-tabs .cti-tab').forEach(b => {
            b.classList.toggle('is-active', (b.dataset.status || '') === _status);
        });
        loadList();
    };

    function bind() {
        document.querySelectorAll('#cti-view-seg .cti-view-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                applyView(btn.dataset.view || 'lista', true);
                render();
            });
        });
        document.querySelectorAll('#cti-tabs .cti-tab').forEach(btn => {
            btn.addEventListener('click', () => {
                _status = btn.dataset.status || '';
                document.querySelectorAll('#cti-tabs .cti-tab').forEach(b => b.classList.toggle('is-active', b === btn));
                loadList();
            });
        });
        document.querySelectorAll('#cti-status-btns .cti-st-btn').forEach(btn => {
            btn.addEventListener('click', () => highlightStatus(btn.dataset.st));
        });
        ['cti-urgencia', 'cti-setor', 'cti-categoria'].forEach(id => {
            const el = $(id);
            if (el) el.addEventListener('change', loadList);
        });
        const q = $('cti-q');
        if (q) {
            q.addEventListener('input', () => {
                clearTimeout(_qTimer);
                _qTimer = setTimeout(loadList, 250);
            });
        }
        const modal = $('cti-modal');
        if (modal) {
            modal.addEventListener('click', (e) => {
                if (e.target === modal) ctiCloseModal();
            });
        }
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') ctiCloseModal();
        });
    }

    let _bound = false;
    window.loadChamadosTi = function () {
        if (!_bound) {
            bind();
            setCategoriaOptions();
            let salvo = null;
            try { salvo = localStorage.getItem(VIEW_KEY); } catch (_) { /* noop */ }
            applyView(salvo || 'lista', false);
            _bound = true;
        }
        loadList().catch(err => console.error(err));
    };
})();
