// ---------------------------------------------------------------------------
// Conexão SIAA — estado da sessão, agenda do keepalive e pausa manual.
// Só lê o que o keepalive registrou (nunca dispara consulta ao SIAA). O estado
// de cada módulo vem exclusivamente do campo `estado` da API.
// ---------------------------------------------------------------------------
const CS_API = '/api/conexao-siaa';
const CS_TZ = 'America/Sao_Paulo';
const CS_POLL_MS = 30000;
const CS_DIAS = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];
const CS_MODULOS = [['academico', 'Acadêmico', 'school'], ['financeiro', 'Financeiro', 'payments']];

const CS_ESTADOS = {
    valida: ['Válida', 't-ok'],
    expirada: ['Expirada', 't-bad'],
    indisponivel: ['Indisponível', 't-bad'],
    inconclusiva: ['Inconclusiva', 't-warn'],
    fora_da_agenda: ['Fora da agenda', 't-neutral'],
    pausa_manual: ['Pausa manual', 't-neutral'],
    pausa_indisponibilidade: ['Pausa por indisponibilidade', 't-warn'],
    desatualizada: ['Verificação desatualizada', 't-warn'],
    sem_dados: ['Sem dados', 't-neutral'],
};
const CS_MODOS = { get: 'GET', jsf: 'Ping JSF', get_fallback: 'GET (fallback do JSF)' };
const CS_CLASSES = {
    disponivel: 'Disponível', sessao_expirada: 'Sessão expirada', indisponivel_siaa: 'SIAA indisponível',
    inconclusivo: 'Inconclusivo', visao_expirada: 'Visão expirada', http_403: 'HTTP 403', falha: 'Falha',
    fora_da_agenda: 'Fora da agenda', pausa_manual: 'Pausa manual', adiado: 'Adiado',
};
const CS_ORIGENS = { agendada: 'Agendada', reabertura: 'Reabertura da janela', inicio: 'Início do worker' };
const CS_FORA = {
    publicacao_sonda_sincrona: 'Sonda síncrona da publicação (confirma a sessão antes de publicar).',
    teste_manual_session_api: 'Botão Testar da página de semeadura da session-api.',
    consulta_manual_painel: 'Consultas manuais feitas no painel (Consulta SIAA e Matérias dos Alunos).',
    jobs_do_painel: 'Jobs do painel que consultam o SIAA (por exemplo, lotes de Matérias dos Alunos).',
};

const csState = {
    status: null,
    okAt: null,          // Date da última leitura bem-sucedida
    erro: false,
    timer: null,
    tick: null,
    ativo: false,
    carregando: false,
    agendaBase: null,    // agenda usada para preencher o formulário (tem a versao)
    agendaSuja: false,
    historico: { itens: [], proximo: null, carregando: false },
};

function csEsc(s) {
    if (s === null || s === undefined) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
}
function csEl(id) { return document.getElementById(id); }

function csData(iso, comSegundos) {
    if (!iso) return '—';
    const d = new Date(iso);
    if (isNaN(d)) return '—';
    const o = { timeZone: CS_TZ, day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false };
    if (comSegundos) o.second = '2-digit';
    return d.toLocaleString('pt-BR', o);
}

function csDuracao(seg) {
    seg = Math.max(0, Math.round(seg));
    if (seg < 60) return seg + ' s';
    if (seg < 3600) return Math.floor(seg / 60) + ' min';
    if (seg < 86400) return Math.floor(seg / 3600) + ' h ' + Math.floor((seg % 3600) / 60) + ' min';
    return Math.floor(seg / 86400) + ' d';
}
function csHa(iso) {
    if (!iso) return null;
    const d = new Date(iso);
    if (isNaN(d)) return null;
    const diff = (Date.now() - d.getTime()) / 1000;
    return diff < 5 ? 'agora' : 'há ' + csDuracao(diff);
}
function csEmBreve(iso) {
    if (!iso) return '';
    const d = new Date(iso);
    if (isNaN(d)) return '';
    const diff = (d.getTime() - Date.now()) / 1000;
    if (diff <= 0) return ' (agora)';
    return ' (em ' + csDuracao(diff) + ')';
}

async function csFetch(method, path, body) {
    const opts = { method, headers: { 'Accept': 'application/json' }, credentials: 'same-origin' };
    if (body !== undefined) {
        opts.headers['Content-Type'] = 'application/json';
        opts.body = JSON.stringify(body);
    }
    let res;
    try {
        res = await fetch(CS_API + path, opts);
    } catch (e) {
        return { status: 0, data: { ok: false, error: 'Sem conexão com o painel.' } };
    }
    let data = null;
    try { data = await res.json(); } catch (e) { data = { ok: false, error: 'Resposta inesperada do painel.' }; }
    return { status: res.status, data: data || {} };
}

function csMostrarErro(msg) {
    const el = csEl('cs-alert');
    if (!el) return;
    if (!msg) { el.classList.add('hidden'); el.textContent = ''; return; }
    el.textContent = msg;
    el.classList.remove('hidden');
}

// ── Status ───────────────────────────────────────────────────────────────
async function conexaoSiaaLoadStatus() {
    if (csState.carregando) return;
    csState.carregando = true;
    const r = await csFetch('GET', '/status');
    csState.carregando = false;
    if (r.status === 200 && r.data.ok) {
        csState.status = r.data;
        csState.okAt = new Date();
        csState.erro = false;
        csMostrarErro('');
        csRenderStatus();
    } else {
        csState.erro = true;
        csMostrarErro((r.data && r.data.error) || 'Não foi possível ler o status.');
    }
    csRenderFresh();
}

function csRenderFresh() {
    const dot = csEl('cs-dot'), txt = csEl('cs-fresh-txt');
    if (!dot || !txt) return;
    dot.className = 'cs-dot' + (csState.erro ? ' err' : (csState.okAt ? ' ok' : ''));
    if (!csState.okAt) { txt.textContent = csState.erro ? 'Sem leitura' : 'Carregando…'; return; }
    const ha = csHa(csState.okAt.toISOString());
    txt.textContent = (csState.erro ? 'Falha na última leitura. Última boa: ' : 'Atualizado ') + ha;
}

function csBadge(estado) {
    const e = CS_ESTADOS[estado] || [estado ? String(estado) : 'Sem dados', 't-neutral'];
    return '<span class="cs-badge ' + e[1] + '">' + csEsc(e[0]) + '</span>';
}

function csProximaExecucao(m, st) {
    if (m.proxima_execucao) return csData(m.proxima_execucao, true) + csEmBreve(m.proxima_execucao);
    const ag = st.agenda || {};
    if (m.estado === 'pausa_manual') return 'Pausado manualmente';
    if (m.estado === 'fora_da_agenda') {
        return ag.proxima_mudanca ? 'Na abertura da janela: ' + csData(ag.proxima_mudanca) : 'Aguardando a janela da agenda';
    }
    return '—';
}

function csUltimoResultado(r) {
    if (!r || !r.em) return '<span class="cs-muted">Nenhum resultado registrado.</span>';
    const classe = CS_CLASSES[r.classe] || r.classe || '—';
    const partes = [
        '<strong>' + csEsc(classe) + '</strong>',
        r.motivo ? '<span class="cs-mono">' + csEsc(r.motivo) + '</span>' : '',
        r.status_http !== null && r.status_http !== undefined ? 'HTTP ' + csEsc(r.status_http) : '',
        r.duracao_ms !== null && r.duracao_ms !== undefined ? csEsc(r.duracao_ms) + ' ms' : '',
    ].filter(Boolean);
    return partes.join(' · ') + '<div class="cs-muted">' + csEsc(csData(r.em, true)) + (r.etapa ? ' · etapa ' + csEsc(r.etapa) : '') + '</div>';
}

function csRenderModulos(st) {
    const root = csEl('cs-modulos');
    root.innerHTML = CS_MODULOS.map(function (mm) {
        const m = (st.modulos || {})[mm[0]] || {};
        const modo = m.modo_usado ? (CS_MODOS[m.modo_usado] || m.modo_usado) : '—';
        const renovar = m.precisa_renovacao
            ? '<div class="cs-alert cs-alert-err cs-renovar" role="alert">Precisa renovar a sessão: o último resultado foi sessão expirada. A renovação exige login com MFA dentro do RemoteApp e semeadura em Atualizar sessão SIAA.</div>'
            : '';
        return '<div class="cs-card" data-modulo="' + mm[0] + '">' +
            '<div class="cs-card-head"><div class="cs-card-title"><span class="material-symbols-outlined">' + mm[2] + '</span>' + csEsc(mm[1]) + '</div>' + csBadge(m.estado) + '</div>' +
            '<dl class="cs-kv">' +
            '<dt>Última confirmação</dt><dd>' + csEsc(csData(m.ultima_confirmacao, true)) + (m.ultima_confirmacao ? ' <span class="cs-muted">(' + csEsc(csHa(m.ultima_confirmacao)) + ')</span>' : '') + '</dd>' +
            '<dt>Última verificação</dt><dd>' + csEsc(csData(m.ultima_verificacao, true)) + '</dd>' +
            '<dt>Próxima execução</dt><dd>' + csEsc(csProximaExecucao(m, st)) + '</dd>' +
            '<dt>Modo usado</dt><dd>' + csEsc(modo) + '</dd>' +
            '<dt>Último resultado</dt><dd>' + csUltimoResultado(m.ultimo_resultado) + '</dd>' +
            '</dl>' + renovar + '</div>';
    }).join('');
}

function csRenderWorker(st) {
    const w = st.worker || {};
    let vivo = '<span class="cs-badge t-neutral">Sem sinal</span>';
    if (w.visto_em) {
        const idade = (Date.now() - new Date(w.visto_em).getTime()) / 1000;
        const limite = (w.intervalo_s || 300) + 120;
        vivo = idade <= limite ? '<span class="cs-badge t-ok">Ativo</span>' : '<span class="cs-badge t-warn">Sem sinal recente</span>';
    }
    const modo = w.modo ? (CS_MODOS[w.modo] || w.modo) : '—';
    csEl('cs-worker').innerHTML =
        '<div class="cs-card-head"><div class="cs-card-title"><span class="material-symbols-outlined">monitor_heart</span>Worker</div>' + vivo + '</div>' +
        '<dl class="cs-kv" style="grid-template-columns:130px minmax(0,1fr)">' +
        '<dt>Último sinal</dt><dd>' + csEsc(csData(w.visto_em, true)) + (w.visto_em ? ' <span class="cs-muted">(' + csEsc(csHa(w.visto_em)) + ')</span>' : '') + '</dd>' +
        '<dt>Iniciado em</dt><dd>' + csEsc(csData(w.iniciado_em)) + '</dd>' +
        '<dt>Modo configurado</dt><dd>' + csEsc(modo) + '</dd>' +
        '<dt>Intervalo efetivo</dt><dd>' + (w.intervalo_s ? csEsc(w.intervalo_s) + ' s' : '—') + '</dd>' +
        '</dl>';
}

function csDiasTexto(dias) {
    if (!dias || !dias.length) return 'nenhum dia';
    return dias.slice().sort().map(function (d) { return CS_DIAS[d]; }).join(', ');
}

function csRenderAgendaResumo(st) {
    const a = st.agenda || {};
    let badge, corpo;
    if (!a.ativa) {
        badge = '<span class="cs-badge t-neutral">Inativa</span>';
        corpo = '<div class="cs-muted">Sem janela: o keepalive verifica o SIAA o tempo todo.</div>';
    } else {
        badge = a.aberta_agora ? '<span class="cs-badge t-ok">Aberta agora</span>' : '<span class="cs-badge t-warn">Fechada agora</span>';
        corpo = '<dl class="cs-kv" style="grid-template-columns:130px minmax(0,1fr)">' +
            '<dt>Janela</dt><dd>' + csEsc(a.inicio) + ' às ' + csEsc(a.fim) + (csAtravessa(a.inicio, a.fim) ? ' <span class="cs-muted">(atravessa a meia-noite)</span>' : '') + '</dd>' +
            '<dt>Dias</dt><dd>' + csEsc(csDiasTexto(a.dias)) + '</dd>' +
            '<dt>' + (a.aberta_agora ? 'Fecha em' : 'Abre em') + '</dt><dd>' + csEsc(csData(a.proxima_mudanca)) + csEsc(csEmBreve(a.proxima_mudanca)) + '</dd>' +
            '<dt>Intervalo</dt><dd>' + (a.intervalo_s ? csEsc(a.intervalo_s) + ' s' : 'Padrão do serviço') + '</dd></dl>';
    }
    const ilegivel = [a.ilegivel ? 'agenda' : '', (st.pausa_manual || {}).ilegivel ? 'pausa manual' : ''].filter(Boolean);
    if (ilegivel.length) {
        corpo = '<div class="cs-alert cs-alert-err" style="margin-bottom:10px">Arquivo de ' + csEsc(ilegivel.join(' e ')) +
            ' ilegível no servidor: tratado como desativado, o keepalive segue verificando o SIAA. Salve novamente para corrigir.</div>' + corpo;
    }
    csEl('cs-agenda-resumo').innerHTML =
        '<div class="cs-card-head"><div class="cs-card-title"><span class="material-symbols-outlined">schedule</span>Agenda</div>' + badge + '</div>' + corpo;
}

function csRenderPausa(st) {
    const p = st.pausa_manual || {};
    const antigo = csEl('cs-pausa-motivo');
    const motivoDigitado = antigo ? antigo.value : '';
    const badge = p.ativa ? '<span class="cs-badge t-warn">Ativa</span>' : '<span class="cs-badge t-neutral">Desativada</span>';
    let corpo;
    if (p.ativa) {
        corpo = '<dl class="cs-kv" style="grid-template-columns:90px minmax(0,1fr);margin-bottom:12px"><dt>Desde</dt><dd>' + csEsc(csData(p.desde)) + '</dd><dt>Motivo</dt><dd>' + (p.motivo ? csEsc(p.motivo) : '<span class="cs-muted">não informado</span>') + '</dd></dl>' +
            '<button class="cs-btn cs-btn-primary" type="button" onclick="conexaoSiaaPausa(false)"><span class="material-symbols-outlined" style="font-size:16px">play_arrow</span>Desativar pausa</button>';
    } else {
        corpo = '<div class="cs-field" style="margin-bottom:10px"><label for="cs-pausa-motivo">Motivo (opcional)</label><input class="cs-input" id="cs-pausa-motivo" maxlength="200" placeholder="Ex.: manutenção do SIAA"></div>' +
            '<button class="cs-btn cs-btn-danger" type="button" onclick="conexaoSiaaPausa(true)"><span class="material-symbols-outlined" style="font-size:16px">pause</span>Ativar pausa manual</button>';
    }
    csEl('cs-pausa').innerHTML =
        '<div class="cs-card-head"><div class="cs-card-title"><span class="material-symbols-outlined">pause_circle</span>Pausa manual</div>' + badge + '</div>' + corpo +
        '<div class="cs-hint" style="margin-top:10px">Suspende as verificações periódicas. Não apaga cookies nem encerra a sessão.</div>';
    const novo = csEl('cs-pausa-motivo');
    if (novo && motivoDigitado) novo.value = motivoDigitado;
}

function csRenderFora(st) {
    const lista = (st.fora_da_agenda && st.fora_da_agenda.length) ? st.fora_da_agenda : Object.keys(CS_FORA);
    csEl('cs-fora').innerHTML = lista.map(function (k) { return '<li>' + csEsc(CS_FORA[k] || k) + '</li>'; }).join('');
}

function csRenderStatus() {
    const st = csState.status;
    if (!st) return;
    csRenderModulos(st);
    csRenderWorker(st);
    csRenderAgendaResumo(st);
    csRenderPausa(st);
    csRenderFora(st);
    // Preenche o editor só se ainda não há base nem edição em curso.
    if (!csState.agendaBase) csPreencherAgenda(st.agenda);
    else csAvisarAgendaMudou();
    csRenderFresh();
}

// ── Pausa manual ──────────────────────────────────────────────────────────
async function conexaoSiaaPausa(ativar) {
    const st = csState.status;
    if (!st) return;
    const p = st.pausa_manual || {};
    const motivoEl = csEl('cs-pausa-motivo');
    const motivo = motivoEl ? motivoEl.value.trim() : '';
    if (motivo.length > 200) { csMostrarErro('Motivo: no máximo 200 caracteres.'); return; }
    const msg = ativar
        ? 'Ativar a pausa manual? As verificações periódicas do SIAA serão suspensas até você desativar.'
        : 'Desativar a pausa manual? O keepalive faz uma verificação imediata e retoma o ritmo normal.';
    if (!window.confirm(msg)) return;
    const r = await csFetch('POST', '/pausa-manual', { ativa: ativar, motivo: motivo, versao: p.versao });
    if (r.status === 409) {
        csMostrarErro('Outra pessoa alterou a pausa manual. Os dados foram recarregados; confira e tente de novo.');
    } else if (r.status !== 200 || !r.data.ok) {
        csMostrarErro(r.data.error || 'Não foi possível alterar a pausa manual.');
    } else {
        csMostrarErro('');
    }
    await conexaoSiaaLoadStatus();
}

// ── Agenda (editor) ───────────────────────────────────────────────────────
function csAtravessa(ini, fim) {
    return !!(ini && fim && /^\d\d:\d\d$/.test(ini) && /^\d\d:\d\d$/.test(fim) && fim < ini);
}

function csDiasHtml() {
    csEl('cs-ag-dias').innerHTML = CS_DIAS.map(function (n, i) {
        return '<label class="cs-day"><input type="checkbox" class="cs-ag-dia" value="' + i + '" onchange="csAgendaMudou()"> ' + n + '</label>';
    }).join('');
}

function csPreencherAgenda(a) {
    a = a || {};
    csState.agendaBase = a;
    csState.agendaSuja = false;
    if (!csEl('cs-ag-dias').children.length) csDiasHtml();
    csEl('cs-ag-ativa').checked = !!a.ativa;
    document.querySelectorAll('#cs-ag-dias .cs-ag-dia').forEach(function (c) {
        c.checked = (a.dias || []).indexOf(parseInt(c.value, 10)) >= 0;
    });
    csEl('cs-ag-inicio').value = a.inicio || '';
    csEl('cs-ag-fim').value = a.fim || '';
    const custom = a.intervalo_s !== null && a.intervalo_s !== undefined;
    csEl('cs-ag-modo-iv').value = custom ? 'custom' : 'padrao';
    csEl('cs-ag-iv').value = custom ? a.intervalo_s : '';
    csEl('cs-ag-versao').textContent = a.versao !== undefined && a.versao !== null ? 'Versão ' + a.versao : '';
    csAgendaMudou(true);
}

function csAgendaMudou(inicial) {
    if (inicial !== true) csState.agendaSuja = true;
    const ativa = csEl('cs-ag-ativa').checked;
    csEl('cs-ag-ativa-hint').textContent = ativa
        ? 'Ativa: fora da janela o keepalive não consulta o SIAA.'
        : 'Desativada: o keepalive verifica o SIAA o tempo todo.';
    const custom = csEl('cs-ag-modo-iv').value === 'custom';
    csEl('cs-ag-iv').disabled = !custom;
    const at = csAtravessa(csEl('cs-ag-inicio').value, csEl('cs-ag-fim').value);
    csEl('cs-ag-meia-noite').textContent = at ? 'Atravessa a meia-noite: a janela começa no dia marcado e termina na manhã seguinte.' : '';
}

function csAvisarAgendaMudou() {
    const st = csState.status, base = csState.agendaBase;
    const el = csEl('cs-agenda-msg');
    if (!st || !base || !el) return;
    if (st.agenda && st.agenda.versao !== base.versao) {
        el.className = 'cs-alert cs-alert-warn';
        el.innerHTML = 'Outra pessoa alterou a agenda (versão ' + csEsc(st.agenda.versao) + '). <a href="#conexao_siaa" onclick="conexaoSiaaDescartarAgenda();return false" style="text-decoration:underline">Recarregar a agenda</a>';
    } else if (el.classList.contains('cs-alert-warn')) {
        el.className = 'cs-alert cs-alert-ok hidden';
    }
}

function csLerAgenda() {
    const dias = Array.prototype.slice.call(document.querySelectorAll('#cs-ag-dias .cs-ag-dia'))
        .filter(function (c) { return c.checked; }).map(function (c) { return parseInt(c.value, 10); });
    const custom = csEl('cs-ag-modo-iv').value === 'custom';
    return {
        ativa: csEl('cs-ag-ativa').checked,
        dias: dias,
        inicio: csEl('cs-ag-inicio').value,
        fim: csEl('cs-ag-fim').value,
        intervalo_s: custom ? parseInt(csEl('cs-ag-iv').value, 10) : null,
    };
}

function csValidarAgenda(a) {
    if (a.ativa && !a.dias.length) return 'Marque ao menos um dia da semana.';
    if (a.ativa && (!a.inicio || !a.fim)) return 'Informe início e fim.';
    if (a.ativa && a.inicio === a.fim) return 'Início e fim não podem ser iguais.';
    if ((a.inicio && !/^([01]\d|2[0-3]):[0-5]\d$/.test(a.inicio)) || (a.fim && !/^([01]\d|2[0-3]):[0-5]\d$/.test(a.fim))) return 'Horário inválido (use HH:MM).';
    if (a.intervalo_s !== null && (isNaN(a.intervalo_s) || a.intervalo_s < 60 || a.intervalo_s > 1800)) return 'Intervalo: de 60 a 1800 segundos, ou padrão do serviço.';
    return '';
}

function csAgendaMsg(texto, ok) {
    const el = csEl('cs-agenda-msg');
    el.textContent = texto;
    el.className = 'cs-alert ' + (ok ? 'cs-alert-ok' : 'cs-alert-err') + (texto ? '' : ' hidden');
}

async function conexaoSiaaSalvarAgenda() {
    const a = csLerAgenda();
    const erro = csValidarAgenda(a);
    if (erro) { csAgendaMsg(erro, false); return; }
    // Horário vazio com agenda desativada vai como null: nada de horário inventado.
    const base = csState.agendaBase || {};
    if (!a.inicio) a.inicio = null;
    if (!a.fim) a.fim = null;
    a.versao = typeof base.versao === 'number' ? base.versao : 0;
    const btn = csEl('cs-ag-salvar');
    btn.disabled = true;
    const r = await csFetch('PUT', '/agenda', a);
    btn.disabled = false;
    if (r.status === 409) {
        csPreencherAgenda((csState.status && csState.status.agenda) || {});
        await conexaoSiaaLoadStatus();
        csPreencherAgenda((csState.status && csState.status.agenda) || {});
        csAgendaMsg('Outra pessoa alterou a agenda. Recarregamos a versão mais recente; confira e salve de novo.', false);
        return;
    }
    if (r.status !== 200 || !r.data.ok) {
        csAgendaMsg(r.data.error || 'Não foi possível salvar a agenda.', false);
        return;
    }
    csAgendaMsg('Agenda salva. Vale a partir da próxima checagem do keepalive (até ~10 s).', true);
    await conexaoSiaaLoadStatus();
    csPreencherAgenda((csState.status && csState.status.agenda) || r.data.agenda);
}

function conexaoSiaaDescartarAgenda() {
    csAgendaMsg('', true);
    csPreencherAgenda((csState.status && csState.status.agenda) || {});
}

// ── Histórico ─────────────────────────────────────────────────────────────
function csHistoricoRow(it) {
    const mod = it.modulo === 'academico' ? 'Acadêmico' : (it.modulo === 'financeiro' ? 'Financeiro' : (it.modulo || '—'));
    const modo = it.modo ? (CS_MODOS[it.modo] || it.modo) : '—';
    const naoReq = ['fora_da_agenda', 'pausa_manual', 'adiado'].indexOf(it.classe) >= 0;
    const tom = it.classe === 'disponivel' ? 't-ok' : (naoReq ? 't-neutral' : (it.classe === 'sessao_expirada' || it.classe === 'indisponivel_siaa' ? 't-bad' : 't-warn'));
    return '<tr><td style="white-space:nowrap">' + csEsc(csData(it.em, true)) + '</td><td>' + csEsc(mod) + '</td>' +
        '<td><span class="cs-badge ' + tom + '">' + csEsc(CS_CLASSES[it.classe] || it.classe || '—') + '</span></td>' +
        '<td class="cs-mono">' + csEsc(it.motivo || '—') + '</td><td>' + csEsc(it.etapa || '—') + '</td><td>' + csEsc(modo) + '</td>' +
        '<td>' + (it.status_http === null || it.status_http === undefined ? '—' : csEsc(it.status_http)) + '</td>' +
        '<td>' + (it.duracao_ms === null || it.duracao_ms === undefined ? '—' : csEsc(it.duracao_ms) + ' ms') + '</td>' +
        '<td>' + csEsc(CS_ORIGENS[it.origem] || it.origem || '—') + '</td></tr>';
}

function csRenderHistorico() {
    const h = csState.historico;
    const body = csEl('cs-h-body');
    body.innerHTML = h.itens.length ? h.itens.map(csHistoricoRow).join('')
        : '<tr><td colspan="9" class="cs-empty">' + (h.carregando ? 'Carregando…' : 'Nenhum evento registrado.') + '</td></tr>';
    csEl('cs-h-mais').style.display = h.proximo ? '' : 'none';
    csEl('cs-h-mais').disabled = h.carregando;
    csEl('cs-h-info').textContent = h.itens.length ? h.itens.length + ' eventos exibidos' + (h.proximo ? '' : ' (fim do histórico)') : '';
}

async function csCarregarHistorico(anexar, silencioso) {
    const h = csState.historico;
    if (h.carregando) return;
    h.carregando = true;
    const q = new URLSearchParams();
    const mod = csEl('cs-h-modulo').value;
    if (mod) q.set('modulo', mod);
    q.set('limite', csEl('cs-h-limite').value);
    if (anexar && h.proximo) q.set('antes', h.proximo);
    if (anexar) h.paginou = true;
    // Recarga silenciosa (polling): mantém a tabela até a resposta chegar.
    if (!anexar && !silencioso) { h.itens = []; h.proximo = null; h.paginou = false; }
    if (!silencioso) csRenderHistorico();
    const r = await csFetch('GET', '/historico?' + q.toString());
    h.carregando = false;
    if (r.status === 200 && r.data.ok) {
        h.itens = anexar ? h.itens.concat(r.data.itens || []) : (r.data.itens || []);
        h.proximo = r.data.proximo_antes || null;
        csMostrarErro('');
    } else if (!silencioso) {
        csMostrarErro(r.data.error || 'Não foi possível ler o histórico.');
    }
    csRenderHistorico();
}
function conexaoSiaaHistoricoRecarregar() { return csCarregarHistorico(false); }
function conexaoSiaaHistoricoMais() { return csCarregarHistorico(true); }

// Polling: status sempre; histórico só na primeira página (quem clicou em
// "Carregar mais" está lendo eventos antigos e não perde a posição).
function csPoll() {
    conexaoSiaaLoadStatus();
    if (!csState.historico.paginou) csCarregarHistorico(false, true);
}

// ── Ciclo de vida da página ───────────────────────────────────────────────
function conexaoSiaaRefresh() {
    conexaoSiaaLoadStatus();
    conexaoSiaaHistoricoRecarregar();
}

function conexaoSiaaOnPage(ativo) {
    if (ativo === csState.ativo) return;
    csState.ativo = ativo;
    if (csState.timer) { clearInterval(csState.timer); csState.timer = null; }
    if (csState.tick) { clearInterval(csState.tick); csState.tick = null; }
    if (!ativo) return;
    conexaoSiaaRefresh();
    csState.timer = setInterval(function () { if (!document.hidden) csPoll(); }, CS_POLL_MS);
    csState.tick = setInterval(csRenderFresh, 1000);
}

function loadConexaoSiaa() { conexaoSiaaOnPage(true); }
