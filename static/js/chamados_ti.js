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

    const CATEGORIAS_POR_DEPTO = {
        'TI': ['Erros/Bugs', 'Processos Novos', 'Ideias Novas'],
        'Marketing': ['Imagem', 'Vídeo', 'UX / UI', 'E-book', 'Brinde', 'Outro'],
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
            const tbody = $('cti-tbody');
            if (tbody) tbody.innerHTML = `<tr><td colspan="10" class="px-4 py-8 text-center text-sm text-slate-500">${escapeHtml(data.message || 'Sem permissão.')}</td></tr>`;
            return;
        }
        const items = Array.isArray(data.items) ? data.items : [];
        setKpis(data.kpis);
        if (Array.isArray(data.fila_departamentos)) {
            const primeiraCarga = _deptosOk === null;
            _deptosOk = data.fila_departamentos;
            if (primeiraCarga) setCategoriaOptions();
            renderDeptoTabs(_deptosOk, data.abertos_por_departamento);
        }
        const empty = $('cti-empty');
        const tbody = $('cti-tbody');
        if (!tbody) return;
        if (!items.length) {
            tbody.innerHTML = '';
            if (empty) empty.classList.remove('hidden');
            return;
        }
        if (empty) empty.classList.add('hidden');
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
        if (!_bound) { bind(); setCategoriaOptions(); _bound = true; }
        loadList().catch(err => console.error(err));
    };
})();
