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

    const VIEW_KEY = 'cti_view_v1';
    const STATUS_ORDEM = ['Pendente', 'Em andamento', 'Concluído'];
    const STATUS_COR = { 'Pendente': '#b45309', 'Em andamento': '#1d4ed8', 'Concluído': '#059669' };

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
        if (st === 'Concluído') return 'sti-st-Concluído';
        if (st === 'Em andamento') return 'sti-st-Em';
        return 'sti-st-Pendente';
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
        const p = $('cti-kpi-pendente');
        const a = $('cti-kpi-andamento');
        const c = $('cti-kpi-concluido');
        if (p) p.textContent = String(kpis['Pendente'] || 0);
        if (a) a.textContent = String(kpis['Em andamento'] || 0);
        if (c) c.textContent = String(kpis['Concluído'] || 0);
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
        if (_status === 'abertos') return ['Pendente', 'Em andamento'];
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
        if (t.status !== 'Concluído') {
            if (dias < 0) {
                cls = 'cti-prazo-late';
                txt = `Atrasado há ${-dias} ${-dias === 1 ? 'dia' : 'dias'}`;
            } else if (dias === 0) {
                cls = 'cti-prazo-soon';
                txt = 'Vence hoje';
            } else if (dias <= 2) {
                cls = 'cti-prazo-soon';
                txt = `Falta ${dias} ${dias === 1 ? 'dia' : 'dias'}`;
            }
        }
        return `<span class="cti-prazo ${cls}"><span class="material-symbols-outlined text-[12px]">schedule</span>${escapeHtml(txt)}</span>`;
    }

    function cardHtml(t) {
        const pode = podeAlterar(t);
        const depto = t.departamento || 'TI';
        const resp = t.responsavel_nome
            ? `<span class="text-[11px] font-medium text-[var(--text-secondary)] truncate">${escapeHtml(t.responsavel_nome)}</span>`
            : '<span class="sti-badge cti-livre">Livre</span>';
        return `
            <div class="cti-kb-card" data-id="${t.id}" data-status="${escapeHtml(t.status)}"
                 ${pode ? 'draggable="true"' : ''} role="button" tabindex="0"
                 title="${pode ? 'Arraste para mudar o status ou clique para abrir' : 'Só o responsável altera o status deste chamado'}">
                <div class="flex items-center justify-between gap-2">
                    <span class="font-mono text-[11px] font-bold" style="color: var(--primary);">${escapeHtml(t.protocolo)}</span>
                    <span class="sti-badge sti-badge-${escapeHtml(t.urgencia)}">${escapeHtml(t.urgencia)}</span>
                </div>
                <p class="text-sm font-bold text-[var(--text-primary)] leading-snug break-words">${escapeHtml(t.titulo)}</p>
                <div class="flex flex-wrap items-center gap-1.5">
                    <span class="sti-dep-${escapeHtml(depto)}">${escapeHtml(depto)}</span>
                    <span class="text-[10px] font-bold uppercase tracking-wider text-slate-500">${escapeHtml(t.categoria || '')}</span>
                </div>
                <p class="text-[11px] text-slate-500 truncate">${escapeHtml(t.solicitante)} · ${escapeHtml(t.setor)}</p>
                <div class="flex items-center justify-between gap-2 pt-1">
                    ${prazoBadge(t) || '<span class="text-[10px] text-slate-500">' + fmtTs(t.created_at) + '</span>'}
                    ${resp}
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
                        <span class="cti-kb-dot" style="background: ${STATUS_COR[st]}"></span>
                        <span class="text-xs font-bold text-[var(--text-primary)]">${escapeHtml(st)}</span>
                        <span class="text-xs font-bold text-slate-500">(${doColuna.length})</span>
                    </header>
                    <div class="cti-kb-body">${cards}</div>
                </section>`;
        }).join('');
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
        document.querySelectorAll('#cti-status-btns .cti-st-btn').forEach(btn => {
            btn.classList.toggle('is-active', btn.dataset.st === st);
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
        $('cti-modal-proto').textContent = t.protocolo || '';
        $('cti-modal-title').textContent = t.titulo || '';
        $('cti-nota').value = '';
        highlightStatus(t.status);
        const depto = t.departamento || 'TI';
        const isMkt = depto === 'Marketing';
        const briefHtml = (isMkt && typeof stiBriefingHtml === 'function')
            ? stiBriefingHtml(t.briefing, t.categoria) : '';
        $('cti-modal-body').innerHTML = `
            <div class="flex flex-wrap gap-2">
                <span class="sti-dep-${escapeHtml(depto)}">${escapeHtml(depto)}</span>
                <span class="sti-badge ${statusClass(t.status)}">${escapeHtml(t.status)}</span>
                <span class="sti-badge sti-badge-${escapeHtml(t.urgencia)}">${escapeHtml(t.urgencia)}</span>
            </div>
            <p><span class="text-slate-500 text-xs uppercase font-bold">Solicitante</span><br>${escapeHtml(t.solicitante)} · ${escapeHtml(t.setor)}</p>
            <p><span class="text-slate-500 text-xs uppercase font-bold">${isMkt ? 'Formato da peça' : 'Categoria'}</span><br>${escapeHtml(t.categoria)}</p>
            ${t.prazo_desejado ? `<p><span class="text-slate-500 text-xs uppercase font-bold">Prazo desejado</span><br>${escapeHtml(t.prazo_desejado)}</p>` : ''}
            <p><span class="text-slate-500 text-xs uppercase font-bold">${isMkt ? 'Informações obrigatórias' : 'Descrição'}</span><br>${escapeHtml(t.descricao).replace(/\n/g, '<br>')}</p>
            ${t.observacoes ? `<p><span class="text-slate-500 text-xs uppercase font-bold">Observações</span><br>${escapeHtml(t.observacoes)}</p>` : ''}
            ${briefHtml}
            <div>
                <p class="text-slate-500 text-xs uppercase font-bold mb-2">Histórico</p>
                ${renderTimeline(data.eventos)}
            </div>
        `;
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

    window.ctiSaveStatus = async function () {
        if (!_openId || !_pickedStatus) return;
        const nota = ($('cti-nota')?.value || '').trim();
        const btn = $('cti-save-status');
        if (btn) btn.disabled = true;
        try {
            const resp = await fetch('/api/solicitacoes_ti/chamados/' + _openId + '/status', {
                method: 'PATCH',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ status: _pickedStatus, nota }),
            });
            const data = await resp.json().catch(() => ({}));
            if (!resp.ok || data.ok === false) {
                if (typeof toast === 'function') toast(data.message || 'Falha ao salvar.', 'error');
                return;
            }
            if (typeof toast === 'function') toast(data.message || 'Status atualizado.', 'success');
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
