// ---------------------------------------------------------------------------
// Conexão SIAA — estado da sessão, agenda do keepalive e pausa manual.
// Só lê o que o keepalive registrou (nunca dispara consulta ao SIAA). O estado
// de cada módulo vem exclusivamente do campo `estado` da API.
// ---------------------------------------------------------------------------
const CS_API = '/api/conexao-siaa';
const CS_TZ = 'America/Sao_Paulo';
const CS_POLL_MS = 30000;
const CS_DIAS = ['Seg', 'Ter', 'Qua', 'Qui', 'Sex', 'Sáb', 'Dom'];
const CS_MODULOS = [['academico', 'Acadêmico', 'school'], ['financeiro', 'Financeiro', 'payments'], ['inscricoes', 'Inscrições', 'how_to_reg']];
// A renovação automática cobre só acadêmico e financeiro; inscrições é atualizada à mão (cURL).
const CS_MODULOS_RENOVACAO = CS_MODULOS.slice(0, 2);
const CS_MODULOS_MANUAIS = ['inscricoes'];

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
    nao_configurada: ['Não configurada', 't-neutral'],
    atualizacao_manual: ['Atualização manual necessária', 't-bad'],
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
        const manual = CS_MODULOS_MANUAIS.indexOf(mm[0]) >= 0;
        let renovar = m.precisa_renovacao && !manual
            ? '<div class="cs-alert cs-alert-err cs-renovar" role="alert">Precisa renovar a sessão: o último resultado foi sessão expirada. A renovação exige login com MFA dentro do RemoteApp e semeadura em Atualizar sessão SIAA.</div>'
            : '';
        if (manual && m.estado === 'nao_configurada') {
            renovar = '<div class="cs-hint cs-manual" style="margin-top:10px">Sessão ainda não cadastrada; nada é verificado. Cadastre em Atualizar sessão SIAA, card Inscrições.</div>';
        } else if (manual && m.estado === 'atualizacao_manual') {
            renovar = '<div class="cs-alert cs-alert-err cs-manual" role="alert">Sessão expirada. Cole um cURL novo em Atualizar sessão SIAA, card Inscrições. A renovação automática não cobre este módulo.</div>';
        }
        return '<div class="cs-card" data-modulo="' + mm[0] + '">' +
            '<div class="cs-card-head"><div class="cs-card-title"><span class="material-symbols-outlined">' + mm[2] + '</span>' + csEsc(mm[1]) + '</div>' + csBadge(m.estado) + '</div>' +
            '<dl class="cs-kv">' +
            '<dt>Última confirmação</dt><dd>' + csEsc(csData(m.ultima_confirmacao, true)) + (m.ultima_confirmacao ? ' <span class="cs-muted">(' + csEsc(csHa(m.ultima_confirmacao)) + ')</span>' : '') + '</dd>' +
            '<dt>Última verificação</dt><dd>' + csEsc(csData(m.ultima_verificacao, true)) + '</dd>' +
            '<dt>Próxima execução agendada</dt><dd>' + csEsc(csProximaExecucao(m, st)) + '</dd>' +
            '<dt>Modo usado</dt><dd>' + csEsc(modo) + '</dd>' +
            '<dt>Último resultado</dt><dd>' + csUltimoResultado(m.ultimo_resultado) + '</dd>' +
            '</dl>' + renovar + '</div>';
    }).join('');
}

const CS_ALEATORIO_AVISO = '<div class="cs-hint" style="margin-top:10px">A aleatoriedade só distribui as verificações no tempo; não evita detecção nem aumenta a duração da sessão.</div>';

// Texto do modo e da configuração. `w` = worker (segundos) ou `a` = agenda (minutos).
function csIntervaloTexto(w, a) {
    const modo = (w && w.intervalo_modo) || (a && a.intervalo_modo) || 'fixo';
    if (modo === 'aleatorio') {
        const mn = w && w.intervalo_min_s ? w.intervalo_min_s / 60 : (a && a.intervalo_min_min);
        const mx = w && w.intervalo_max_s ? w.intervalo_max_s / 60 : (a && a.intervalo_max_min);
        return 'Aleatório: ' + (mn || '?') + ' a ' + (mx || '?') + ' min';
    }
    const fixo = (a && a.intervalo_s) || (w && w.intervalo_s);
    return 'Fixo: ' + (fixo ? fixo + ' s' : 'padrão do serviço');
}

function csRenderWorker(st) {
    const w = st.worker || {};
    let vivo = '<span class="cs-badge t-neutral">Sem sinal</span>';
    if (w.visto_em) {
        const idade = (Date.now() - new Date(w.visto_em).getTime()) / 1000;
        // Sinal de vida a cada tick (<=10 s) fora dos ciclos, independente do intervalo
        // das sondas; um ciclo lento pode passar de 2 min. Mesmo limite da API (300 s).
        const limite = 300;
        vivo = idade <= limite ? '<span class="cs-badge t-ok">Ativo</span>' : '<span class="cs-badge t-warn">Sem sinal recente</span>';
    }
    const modo = w.modo ? (CS_MODOS[w.modo] || w.modo) : '—';
    csEl('cs-worker').innerHTML =
        '<div class="cs-card-head"><div class="cs-card-title"><span class="material-symbols-outlined">monitor_heart</span>Worker</div>' + vivo + '</div>' +
        '<dl class="cs-kv" style="grid-template-columns:130px minmax(0,1fr)">' +
        '<dt>Último sinal</dt><dd>' + csEsc(csData(w.visto_em, true)) + (w.visto_em ? ' <span class="cs-muted">(' + csEsc(csHa(w.visto_em)) + ')</span>' : '') + '</dd>' +
        '<dt>Iniciado em</dt><dd>' + csEsc(csData(w.iniciado_em)) + '</dd>' +
        '<dt>Modo configurado</dt><dd>' + csEsc(modo) + '</dd>' +
        '<dt>Intervalo</dt><dd>' + csEsc(csIntervaloTexto(w, null)) + '</dd>' +
        (w.intervalo_modo === 'aleatorio' ? '<dt>Sorteado agora</dt><dd>' + (w.intervalo_sorteado_s ? csEsc(csDuracao(w.intervalo_sorteado_s)) + ' <span class="cs-muted">(' + csEsc(w.intervalo_sorteado_s) + ' s)</span>' : '—') + '</dd>'
            : '<dt>Intervalo efetivo</dt><dd>' + (w.intervalo_s ? csEsc(w.intervalo_s) + ' s' : '—') + '</dd>') +
        '<dt>Próxima execução agendada</dt><dd>' + (w.proxima_execucao ? csEsc(csData(w.proxima_execucao, true)) + csEsc(csEmBreve(w.proxima_execucao)) : '—') + '</dd>' +
        '</dl>' + CS_ALEATORIO_AVISO;
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
            '<dt>Intervalo</dt><dd>' + csEsc(csIntervaloTexto(null, a)) + '</dd></dl>' + CS_ALEATORIO_AVISO;
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
    // O modo exibido é sempre o salvo; 5 e 8 são só a proposta dos campos.
    csEl('cs-ag-tipo-iv').value = a.intervalo_modo === 'aleatorio' ? 'aleatorio' : 'fixo';
    csEl('cs-ag-min').value = typeof a.intervalo_min_min === 'number' ? a.intervalo_min_min : 5;
    csEl('cs-ag-max').value = typeof a.intervalo_max_min === 'number' ? a.intervalo_max_min : 8;
    csEl('cs-ag-versao').textContent = a.versao !== undefined && a.versao !== null ? 'Versão ' + a.versao : '';
    csAgendaMudou(true);
}

function csAgendaMudou(inicial) {
    if (inicial !== true) csState.agendaSuja = true;
    const ativa = csEl('cs-ag-ativa').checked;
    csEl('cs-ag-ativa-hint').textContent = ativa
        ? 'Ativa: fora da janela o keepalive não consulta o SIAA.'
        : 'Desativada: o keepalive verifica o SIAA o tempo todo.';
    const alea = csEl('cs-ag-tipo-iv').value === 'aleatorio';
    const custom = csEl('cs-ag-modo-iv').value === 'custom';
    csEl('cs-ag-iv').disabled = alea || !custom;
    csEl('cs-ag-modo-iv').disabled = alea;
    csEl('cs-ag-min').disabled = !alea;
    csEl('cs-ag-max').disabled = !alea;
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
    const alea = csEl('cs-ag-tipo-iv').value === 'aleatorio';
    const custom = !alea && csEl('cs-ag-modo-iv').value === 'custom';
    return {
        ativa: csEl('cs-ag-ativa').checked,
        dias: dias,
        inicio: csEl('cs-ag-inicio').value,
        fim: csEl('cs-ag-fim').value,
        intervalo_s: custom ? parseInt(csEl('cs-ag-iv').value, 10) : (alea ? ((csState.agendaBase || {}).intervalo_s === undefined ? null : (csState.agendaBase || {}).intervalo_s) : null),
        intervalo_modo: alea ? 'aleatorio' : 'fixo',
        intervalo_min_min: parseInt(csEl('cs-ag-min').value, 10),
        intervalo_max_min: parseInt(csEl('cs-ag-max').value, 10),
    };
}

function csValidarAgenda(a) {
    if (a.ativa && !a.dias.length) return 'Marque ao menos um dia da semana.';
    if (a.ativa && (!a.inicio || !a.fim)) return 'Informe início e fim.';
    if (a.ativa && a.inicio === a.fim) return 'Início e fim não podem ser iguais.';
    if ((a.inicio && !/^([01]\d|2[0-3]):[0-5]\d$/.test(a.inicio)) || (a.fim && !/^([01]\d|2[0-3]):[0-5]\d$/.test(a.fim))) return 'Horário inválido (use HH:MM).';
    if (a.intervalo_modo === 'aleatorio') {
        const mn = a.intervalo_min_min, mx = a.intervalo_max_min;
        if (isNaN(mn) || isNaN(mx)) return 'Intervalo aleatório: informe mínimo e máximo em minutos.';
        if (mn < 1 || mn > 30 || mx < 1 || mx > 30) return 'Intervalo aleatório: de 1 a 30 minutos.';
        if (mn > mx) return 'Intervalo aleatório: o mínimo não pode passar do máximo.';
    } else if (a.intervalo_s !== null && (isNaN(a.intervalo_s) || a.intervalo_s < 60 || a.intervalo_s > 1800)) return 'Intervalo: de 60 a 1800 segundos, ou padrão do serviço.';
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
    if (a.intervalo_modo !== 'aleatorio') {
        // Fixo: o par mín./máx. não vale; mantém só o que já estava salvo (a proposta
        // 5–8 exibida no formulário não é gravada sem o admin escolher Aleatório).
        a.intervalo_min_min = typeof base.intervalo_min_min === 'number' ? base.intervalo_min_min : null;
        a.intervalo_max_min = typeof base.intervalo_max_min === 'number' ? base.intervalo_max_min : null;
    }
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

// ── Exportação de diagnóstico ─────────────────────────────────────────────
// Só GET, por clique do admin. O servidor devolve o arquivo já saneado.
async function conexaoSiaaExportarDiagnostico() {
    const msg = csEl('cs-diag-msg'), btn = csEl('cs-diag-btn');
    const periodo = csEl('cs-diag-periodo').value || '24h';
    const falha = function (t) { msg.textContent = t; msg.className = 'cs-alert cs-alert-err' + (t ? '' : ' hidden'); };
    falha('');
    btn.disabled = true;
    try {
        let res;
        try {
            res = await fetch(CS_API + '/diagnostico?periodo=' + encodeURIComponent(periodo),
                { method: 'GET', headers: { 'Accept': 'application/json' }, credentials: 'same-origin' });
        } catch (e) { falha('Sem conexão com o painel.'); return; }
        if (res.status !== 200) {
            let d = null;
            try { d = await res.json(); } catch (e) { d = null; }
            falha((d && d.error) || 'Não foi possível gerar o diagnóstico.');
            return;
        }
        const cd = (res.headers && res.headers.get && res.headers.get('Content-Disposition')) || '';
        const m = /filename="([^"]+)"/.exec(cd);
        const blob = await res.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url; a.download = m ? m[1] : 'conexao-siaa-diagnostico.json';
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
        setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
    } catch (e) {
        falha('Não foi possível gerar o diagnóstico.');
    } finally {
        btn.disabled = false;
    }
}

// ── Histórico ─────────────────────────────────────────────────────────────
function csHistoricoRow(it) {
    const achado = CS_MODULOS.filter(function (mm) { return mm[0] === it.modulo; })[0];
    const mod = achado ? achado[1] : (it.modulo || '—');
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
    // Renovação: só GET (status e execuções); o polling nunca escreve.
    csRenovCarregarStatus();
    if (!csRenov.exec.paginou) csRenovCarregarExecucoes(false, true);
}

// ── Renovação automática ──────────────────────────────────────────────────
// Leitura (status/execuções) roda no polling; escrita (config, suspensão,
// solicitar, liberar) só acontece por clique do admin, depois de confirmação.
const CS_RN_MOTIVOS = {
    execucao_ativa: 'Execução em andamento', episodio_bloqueado: 'Episódio bloqueado após falha ou resultado incerto',
    ultima_execucao_falhou: 'A última execução falhou', config_inativa: 'Renovação automática desativada',
    suspensa: 'Novas renovações suspensas', janela_fechada: 'Fora da agenda', renovacao_ok: 'Renovação concluída',
    configuracao_ilegivel: 'Configuração de agenda ou pausa ilegível', pausa_manual: 'Pausa manual ativa',
    disponibilidade_pausa: 'SIAA em pausa por indisponibilidade', disponibilidade_desconhecida: 'Disponibilidade desconhecida',
    sem_evidencia: 'Sessões válidas: nada a renovar', tentativa_consumida: 'Tentativa automática deste episódio já usada',
    evidencia_nao_fresca: 'Expiração ainda não confirmada por verificação recente', aguardando_executor: 'Aguardando o executor',
    estado_ilegivel: 'Estado da renovação ilegível: intervenção necessária',
};
const CS_RN_ESTADOS = {
    desativada: ['Desativada', 't-neutral'],
    aguardando_agenda: ['Aguardando agenda', 't-neutral'],
    monitorando: ['Monitorando', 't-ok'],
    em_execucao: ['Em execução', 't-warn'],
    concluida: ['Concluída', 't-ok'],
    bloqueada: ['Bloqueada', 't-bad'],
    falhou: ['Falhou', 't-bad'],
};
const CS_RN_EXEC_ESTADOS = {
    pendente: ['Pendente', 't-warn'], atribuida: ['Atribuída', 't-warn'], em_execucao: ['Em execução', 't-warn'],
    concluida: ['Concluída', 't-ok'], falhou: ['Falhou', 't-bad'], incerta: ['Incerta', 't-bad'],
    cancelada: ['Cancelada', 't-neutral'], interrompida: ['Interrompida', 't-bad'],
};
const CS_RN_ATIVAS = ['pendente', 'atribuida', 'em_execucao'];
const CS_RN_TIPOS = { manual: 'Manual', automatica: 'Automática' };
const CS_RN_ETAPAS = {
    preparacao: 'Preparação', navegador: 'Abrindo o navegador', login_local: 'Login local',
    login_remoto: 'Login remoto', credenciais_enviadas: 'Credenciais enviadas', autenticado: 'Autenticado no SIAA',
    coleta_academico: 'Coleta do Acadêmico', coleta_financeiro: 'Coleta do Financeiro',
    publicacao: 'Publicação das sessões', finalizando: 'Finalizando',
};
const CS_RN_ERROS = {
    prova_ja_ativa: 'Já havia outra execução em andamento no executor.',
    preparacao_recusada: 'A preparação foi recusada antes do login.',
    disponibilidade_bloqueia: 'A disponibilidade do SIAA impediu a renovação.',
    navegador_falhou: 'O navegador do executor falhou.',
    canvas_indisponivel: 'A tela do Windows App não ficou pronta a tempo (não é recusa de credencial).',
    login_falhou: 'O login não foi concluído.',
    mfa_herdado_ou_ambiguo: 'A verificação em duas etapas (MFA) ficou ambígua ou herdada.',
    conta_divergente: 'A conta exibida não é a esperada.',
    autenticacao_recusada: 'O SIAA recusou a autenticação.',
    politica_inesperada: 'Apareceu uma política ou tela inesperada.',
    coleta_falhou: 'A coleta da sessão falhou.',
    publicacao_incerta: 'A publicação das sessões ficou incerta.',
    prazo_excedido: 'O prazo da renovação foi excedido.',
    saida_inesperada: 'O executor terminou de forma inesperada.',
    desconhecido: 'Falha sem causa classificada.',
};
const CS_RN_ACOES = {
    nenhuma: 'Nenhuma ação necessária.',
    aguardar: 'Aguardar a próxima verificação.',
    revisar_e_liberar: 'Revisar e liberar nova tentativa.',
    intervencao_humana: 'Intervenção humana necessária.',
    renovacao_manual: 'Renovação manual disponível.',
};
const CS_RN_BLOQUEIOS = {
    falha_login: 'falha de login', falhou: 'execução falhou', incerta: 'publicação incerta',
    interrompida: 'execução interrompida', publicacao_incerta: 'publicação incerta',
    falha_execucao: 'execução falhou', resultado_incerto: 'resultado incerto',
    execucao_interrompida: 'execução interrompida', cancelamentos_repetidos: 'cancelamentos repetidos',
};
const CS_RN_GATILHOS = { sessao_expirada: 'Sessão expirada', manual: 'Pedido manual' };
// Renovação seletiva (aditivo): resultado da execução e situação por módulo.
// Backend antigo não envia `resultado` nem `situacao`: mostra-se o estado como antes.
const CS_RN_RESULTADOS = {
    completo: ['Sucesso completo', 't-ok'],
    parcial: ['Parcial', 't-warn'],
    falha: ['Falha', 't-bad'],
    cancelada: ['Cancelada', 't-neutral'],
};
const CS_RN_SITUACOES = {
    nao_solicitado: ['Não solicitado', 't-neutral'],
    ok: ['Renovado', 't-ok'],
    falhou: ['Falhou', 't-bad'],
    incerto: ['Incerto', 't-warn'],
    pendente: ['Pendente', 't-warn'],
};
const CS_RN_LOGIN = { ok: 'OK', falhou: 'Falhou', nao_executado: 'Não executado' };
const CS_RN_COLETA = { ok: 'OK', falhou: 'Falhou', nao_executada: 'Não executada' };
const CS_RN_PUB = {
    ok: 'Confirmada', incerto: 'Incerta', divergente: 'Divergente', incompativel: 'Incompatível',
    nao_publicada: 'Não publicada', nao_executada: 'Não executada',
};
const CS_RN_MSG_409 = {
    execucao_ativa: 'Já existe uma renovação em andamento.',
    pausa_manual: 'A pausa manual está ativa; desative-a antes de renovar.',
    versao: 'Outra pessoa alterou a configuração. Os dados foram recarregados; confira e tente de novo.',
};

const csRenov = {
    status: null,
    erro: null,          // mensagem de leitura (ex.: API desatualizada)
    enviando: false,     // qualquer escrita em curso
    carregando: false,
    pedido: null,        // { id, em } do último "Renovar agora" sem resposta definitiva
    exec: { itens: [], proximo: null, carregando: false, paginou: false },
};

function csRnLabel(mapa, v, vazio) {
    if (v === null || v === undefined || v === '') return vazio === undefined ? '—' : vazio;
    return mapa[v] !== undefined ? mapa[v] : String(v).replace(/_/g, ' ');
}
function csRnBadge(mapa, v) {
    const e = mapa[v] || [v ? String(v).replace(/_/g, ' ') : 'Sem dados', 't-neutral'];
    return '<span class="cs-badge ' + e[1] + '">' + csEsc(e[0]) + '</span>';
}
function csRnDuracao(e) {
    if (e.duracao_ms !== null && e.duracao_ms !== undefined) return csDuracao(e.duracao_ms / 1000);
    if (e.inicio && CS_RN_ATIVAS.indexOf(e.estado) >= 0) return csDuracao((Date.now() - new Date(e.inicio).getTime()) / 1000) + ' (em andamento)';
    return '—';
}
function csRnSimNao(v) { return v === true ? 'Sim' : (v === false ? 'Não' : '—'); }
function csRnNomeMod(m) {
    const x = CS_MODULOS_RENOVACAO.filter(function (mm) { return mm[0] === m; })[0];
    return x ? x[1] : String(m);
}
function csRnNomesMods(lista) {
    return (lista || []).map(csRnNomeMod).join(' e ');
}
function csRnValidos(lista) {
    return Array.isArray(lista) ? CS_MODULOS_RENOVACAO.map(function (mm) { return mm[0]; }).filter(function (m) { return lista.indexOf(m) >= 0; }) : null;
}
// Execução antiga sem `modulos_solicitados` = os dois módulos.
function csRnSolicitados(e) {
    const l = csRnValidos(e && e.modulos_solicitados);
    return l === null ? CS_MODULOS_RENOVACAO.map(function (mm) { return mm[0]; }) : l;
}
// Situação de um módulo na execução: a da API; sem ela (backend antigo), derivada sem inventar falha.
function csRnSituacao(e, m) {
    if (csRnSolicitados(e).indexOf(m) < 0) return 'nao_solicitado';
    const x = ((e.modulos || {})[m]) || {};
    if (x.situacao && CS_RN_SITUACOES[x.situacao]) return x.situacao;
    if (x.publicacao === 'ok' && x.sessao_validada === true) return 'ok';
    if (CS_RN_ATIVAS.indexOf(e.estado) >= 0) return 'pendente';
    return null;
}
function csRnResultadoBadge(e) {
    // Resultado (completo/parcial/falha) sempre junto do estado da execução: "Incerta" e
    // "Interrompida" não podem sumir atrás de "Falha" ou "Parcial".
    const estado = csRnBadge(CS_RN_EXEC_ESTADOS, e.estado);
    if (!e.resultado || !CS_RN_RESULTADOS[e.resultado]) return estado;
    const resultado = csRnBadge(CS_RN_RESULTADOS, e.resultado);
    // Só acrescenta o estado quando ele diz algo que o resultado não diz.
    return (e.estado === 'incerta' || e.estado === 'interrompida') ? resultado + ' ' + estado : resultado;
}
// Detalhe de um resultado parcial: quais módulos renovaram e quais não.
function csRnParcialTexto(e) {
    if (e.resultado !== 'parcial') return '';
    const sol = csRnSolicitados(e);
    const ok = sol.filter(function (m) { return csRnSituacao(e, m) === 'ok'; });
    const nao = sol.filter(function (m) { return csRnSituacao(e, m) !== 'ok'; });
    if (!nao.length) return 'Renovado: ' + csRnNomesMods(ok) + ' · resultado incerto (ver erro)';
    return 'Renovado: ' + (csRnNomesMods(ok) || 'nenhum') + ' · não renovado: ' + csRnNomesMods(nao);
}
// Alvos dos controles (status.alvos). null = backend antigo (sem o campo).
function csRnAlvo(s, chave) {
    const a = s && s.alvos;
    if (!a || typeof a !== 'object') return null;
    return csRnValidos(a[chave]);
}
function csRnAlvoRenovarTexto(s) {
    const l = csRnAlvo(s, 'renovar_agora');
    return l && l.length ? csRnNomesMods(l) : 'os dois módulos (Acadêmico e Financeiro)';
}
function csRnPodeLiberar(s) {
    const ep = s && s.episodio;
    if (!ep || !ep.id) return false;
    const l = csRnAlvo(s, 'liberar');
    if (l !== null) return l.length > 0;
    return !!ep.bloqueado;
}
function csRnGatilho(e) {
    const g = e.gatilho || {};
    const mods = csRnNomesMods(g.modulos);
    const motivo = csRnLabel(CS_RN_GATILHOS, g.motivo);
    return (mods ? mods + ' · ' : '') + motivo;
}
function csRenovAtiva() {
    const s = csRenov.status;
    if (!s) return false;
    if (s.estado === 'em_execucao') return true;
    return !!(s.execucao_atual && CS_RN_ATIVAS.indexOf(s.execucao_atual.estado) >= 0);
}

function csRnMsg(texto, tipo) {
    const el = csEl('cs-renov-msg');
    if (!el) return;
    el.textContent = texto || '';
    el.className = 'cs-alert ' + (tipo === 'ok' ? 'cs-alert-ok' : (tipo === 'warn' ? 'cs-alert-warn' : 'cs-alert-err')) + (texto ? '' : ' hidden');
}

function csRnModulosTabela(e) {
    const linhas = CS_MODULOS_RENOVACAO.map(function (mm) {
        const sit = csRnSituacao(e, mm[0]);
        const sitHtml = sit ? csRnBadge(CS_RN_SITUACOES, sit) : '—';
        if (sit === 'nao_solicitado') {
            return '<tr><td>' + csEsc(mm[1]) + '</td><td>' + sitHtml + '</td><td>—</td><td>—</td><td>—</td><td>—</td></tr>';
        }
        const m = ((e.modulos || {})[mm[0]]) || {};
        return '<tr><td>' + csEsc(mm[1]) + '</td><td>' + sitHtml + '</td>' +
            '<td>' + csEsc(csRnLabel(CS_RN_COLETA, m.coleta)) + '</td>' +
            '<td>' + (m.http === null || m.http === undefined ? '—' : csEsc(m.http)) + '</td>' +
            '<td>' + csEsc(csRnLabel(CS_RN_PUB, m.publicacao)) + '</td>' +
            '<td>' + csEsc(csRnSimNao(m.sessao_validada)) + '</td></tr>';
    }).join('');
    return '<div class="cs-table-wrap"><table class="cs-table cs-rn-modulos"><thead><tr>' +
        '<th>Módulo</th><th>Situação</th><th>Coleta</th><th>HTTP</th><th>Publicação confirmada</th><th>Sessão validada</th></tr></thead><tbody>' + linhas + '</tbody></table></div>';
}

function csRnExecucaoHtml(e, titulo) {
    if (!e) return '<div class="cs-muted">Nenhuma execução registrada ainda.</div>';
    const prog = e.ultimo_progresso || {};
    const progTxt = prog.etapa ? csEsc(csRnLabel(CS_RN_ETAPAS, prog.etapa)) + (prog.em ? ' <span class="cs-muted">(' + csEsc(csHa(prog.em)) + ')</span>' : '') : '—';
    const erro = e.erro ? csEsc(csRnLabel(CS_RN_ERROS, e.erro)) : '—';
    return '<div class="cs-rn-sub" style="margin-top:0">' + csEsc(titulo) + '</div>' +
        '<dl class="cs-kv">' +
        '<dt>Resultado</dt><dd>' + csRnResultadoBadge(e) +
        (e.resultado === 'parcial' ? ' <span class="cs-muted">' + csEsc(csRnParcialTexto(e)) + '</span>' : '') + '</dd>' +
        '<dt>Tipo</dt><dd>' + csEsc(csRnLabel(CS_RN_TIPOS, e.tipo)) + '</dd>' +
        '<dt>Módulos solicitados</dt><dd>' + csEsc(csRnNomesMods(csRnSolicitados(e)) || '—') + '</dd>' +
        '<dt>Gatilho</dt><dd>' + csEsc(csRnGatilho(e)) + '</dd>' +
        '<dt>Início</dt><dd>' + csEsc(csData(e.inicio || e.criada_em, true)) + '</dd>' +
        '<dt>Duração</dt><dd>' + csEsc(csRnDuracao(e)) + '</dd>' +
        '<dt>Último progresso</dt><dd>' + progTxt + '</dd>' +
        '<dt>Login</dt><dd>' + csEsc(csRnLabel(CS_RN_LOGIN, e.login)) + '</dd>' +
        '<dt>Erro</dt><dd>' + erro + '</dd>' +
        (e.janela_fechou_durante ? '<dt>Agenda</dt><dd>A janela fechou durante a execução; ela seguiu até concluir.</dd>' : '') +
        '</dl>' +
        '<div class="cs-rn-sub">Resultado por módulo</div>' + csRnModulosTabela(e);
}

// Estado por módulo do episódio (renovação seletiva): bloqueio, resolução e tentativas de cada um.
function csRnEpisodioModulos(est) {
    const linhas = CS_MODULOS_RENOVACAO.filter(function (mm) { return est[mm[0]]; }).map(function (mm) {
        const x = est[mm[0]];
        let sit;
        if (x.bloqueado) sit = '<span class="cs-badge t-bad">Bloqueado</span> ' + csEsc(csRnLabel(CS_RN_BLOQUEIOS, x.bloqueio_motivo, ''));
        else if (x.resolvido) sit = '<span class="cs-badge t-ok">Resolvido</span>' + (x.resolvido_em ? ' <span class="cs-muted">(' + csEsc(csData(x.resolvido_em, true)) + ')</span>' : '');
        else sit = '<span class="cs-badge t-warn">Aguardando renovação</span>';
        return '<tr><td>' + csEsc(mm[1]) + '</td><td>' + sit + '</td>' +
            '<td>' + csEsc(x.tentativas_automaticas === null || x.tentativas_automaticas === undefined ? '—' : x.tentativas_automaticas) + '</td>' +
            '<td>' + csEsc(x.liberado_em ? csData(x.liberado_em, true) : '—') + '</td></tr>';
    }).join('');
    if (!linhas) return '';
    return '<div class="cs-table-wrap"><table class="cs-table cs-rn-ep-modulos"><thead><tr>' +
        '<th>Módulo</th><th>Situação no episódio</th><th>Tentativas automáticas</th><th>Liberado em</th></tr></thead><tbody>' + linhas + '</tbody></table></div>';
}

function csRenderRenov() {
    const corpo = csEl('cs-renov-corpo');
    if (!corpo) return;
    const s = csRenov.status;
    const badge = csEl('cs-renov-badge');
    if (!s) {
        if (badge) badge.innerHTML = '';
        corpo.innerHTML = csRenov.erro
            ? '<div class="cs-alert cs-alert-err" style="margin:0">' + csEsc(csRenov.erro) + '</div>'
            : '<div class="cs-empty">Carregando…</div>';
        return;
    }
    if (badge) badge.innerHTML = csRnBadge(CS_RN_ESTADOS, s.estado);
    const cfg = s.config || {};
    const ep = s.episodio;
    const ativa = csRenovAtiva();
    const estMods = (ep && ep.modulos_estado && typeof ep.modulos_estado === 'object') ? ep.modulos_estado : null;
    const bloq = !!(ep && (ep.bloqueado || (estMods && CS_MODULOS_RENOVACAO.some(function (mm) { return estMods[mm[0]] && estMods[mm[0]].bloqueado; }))));
    const podeLiberar = csRnPodeLiberar(s);
    const alvoLiberar = csRnAlvo(s, 'liberar');
    const alvoAuto = csRnAlvo(s, 'automatica');
    const off = csRenov.enviando ? ' disabled' : '';
    const offRenovar = (csRenov.enviando || ativa) ? ' disabled' : '';

    let kv = '<dl class="cs-kv">' +
        '<dt>Estado</dt><dd>' + csRnBadge(CS_RN_ESTADOS, s.estado) + '</dd>' +
        '<dt>Motivo</dt><dd>' + (s.motivo_estado ? csEsc(CS_RN_MOTIVOS[s.motivo_estado] || s.motivo_estado.replace(/_/g, ' ')) : '—') + '</dd>' +
        '<dt>Renovação automática</dt><dd>' + (cfg.ativa ? 'Ativada' : 'Desativada') + (cfg.suspensa ? ' <span class="cs-badge t-warn">Novas renovações suspensas</span>' : '') +
        ' <span class="cs-muted">(versão ' + csEsc(cfg.versao) + ')</span></dd>' +
        '<dt>Última renovação bem-sucedida</dt><dd>' + csEsc(csData(s.ultima_renovacao_ok, true)) + (s.ultima_renovacao_ok ? ' <span class="cs-muted">(' + csEsc(csHa(s.ultima_renovacao_ok)) + ')</span>' : '') + '</dd>' +
        '<dt>Executor</dt><dd>' + (s.executor && s.executor.visto_em ? 'Visto ' + csEsc(csHa(s.executor.visto_em)) + (s.executor.ocupado ? ' · ocupado' : '') : '<span class="cs-muted">Sem sinal</span>') + '</dd>' +
        '<dt>Próxima ação</dt><dd><strong>' + csEsc(csRnLabel(CS_RN_ACOES, s.proxima_acao)) + '</strong></dd>' +
        (alvoAuto !== null ? '<dt>Renovação automática atingiria agora</dt><dd>' + csEsc(alvoAuto.length ? csRnNomesMods(alvoAuto) : 'Nenhum módulo elegível') + '</dd>' : '') +
        '</dl>';

    let episodio = '';
    if (ep) {
        const mods = csRnNomesMods(ep.modulos) || '—';
        episodio = '<div class="cs-rn-sub">Episódio atual</div><dl class="cs-kv">' +
            '<dt>Módulo(s)</dt><dd>' + csEsc(mods) + '</dd>' +
            '<dt>Motivo do gatilho</dt><dd>' + csEsc(csRnLabel(CS_RN_GATILHOS, ep.motivo)) + '</dd>' +
            '<dt>Desde</dt><dd>' + csEsc(csData(ep.desde, true)) + '</dd>' +
            '<dt>Tentativas automáticas</dt><dd>' + csEsc(ep.tentativas_automaticas === null || ep.tentativas_automaticas === undefined ? '—' : ep.tentativas_automaticas) + '</dd>' +
            (bloq && !estMods ? '<dt>Bloqueio</dt><dd><span class="cs-badge t-bad">Bloqueado</span> ' + csEsc(csRnLabel(CS_RN_BLOQUEIOS, ep.bloqueio_motivo, '')) + '</dd>' : '') +
            '</dl>' + (estMods ? csRnEpisodioModulos(estMods) : '');
    }

    const atual = s.execucao_atual;
    const ultima = s.ultima_execucao;
    const exec = atual ? csRnExecucaoHtml(atual, 'Execução em andamento') : csRnExecucaoHtml(ultima, 'Última execução');

    const bloqAviso = bloq
        ? '<div class="cs-alert cs-alert-err" style="margin-top:12px" role="alert">Renovação automática bloqueada neste episódio' +
            (alvoLiberar && alvoLiberar.length ? ' para ' + csEsc(csRnNomesMods(alvoLiberar)) : '') +
            '. Não há nova tentativa automática: revise o resultado e libere uma nova tentativa quando for seguro.</div>'
        : '';

    const acoes = '<div class="cs-rn-actions">' +
        '<button class="cs-btn ' + (cfg.ativa ? '' : 'cs-btn-primary') + '" type="button" id="cs-rn-btn-ativar" onclick="conexaoSiaaRenovAtivar(' + (cfg.ativa ? 'false' : 'true') + ')"' + off + '>' +
        '<span class="material-symbols-outlined" style="font-size:16px">' + (cfg.ativa ? 'toggle_off' : 'toggle_on') + '</span>' + (cfg.ativa ? 'Desativar renovação automática' : 'Ativar renovação automática') + '</button>' +
        '<button class="cs-btn" type="button" id="cs-rn-btn-suspender" onclick="conexaoSiaaRenovSuspender(' + (cfg.suspensa ? 'false' : 'true') + ')"' + off + '>' +
        '<span class="material-symbols-outlined" style="font-size:16px">' + (cfg.suspensa ? 'play_arrow' : 'pause') + '</span>' + (cfg.suspensa ? 'Retomar novas renovações' : 'Suspender novas renovações') + '</button>' +
        '<button class="cs-btn cs-btn-danger" type="button" id="cs-rn-btn-renovar" onclick="conexaoSiaaRenovSolicitar()"' + offRenovar + '>' +
        '<span class="material-symbols-outlined" style="font-size:16px">autorenew</span>' + (csRenov.enviando ? 'Enviando…' : 'Renovar agora') + '</button>' +
        (podeLiberar ? '<button class="cs-btn cs-btn-primary" type="button" id="cs-rn-btn-liberar" onclick="conexaoSiaaRenovLiberar()"' + off + '>' +
            '<span class="material-symbols-outlined" style="font-size:16px">lock_open</span>Liberar nova tentativa' +
            (alvoLiberar && alvoLiberar.length ? ' (' + csEsc(csRnNomesMods(alvoLiberar)) + ')' : '') + '</button>' : '') +
        '</div>' +
        '<div class="cs-hint" style="margin-top:8px" id="cs-rn-alvos">"Renovar agora" atinge: ' + csEsc(csRnAlvoRenovarTexto(s)) + '.' +
        (podeLiberar && alvoLiberar && alvoLiberar.length ? ' "Liberar nova tentativa" atinge: ' + csEsc(csRnNomesMods(alvoLiberar)) + '.' : '') +
        (bloq && !podeLiberar ? ' Nenhum módulo bloqueado para liberar.' : '') + '</div>' +
        '<div class="cs-hint" style="margin-top:4px">Suspender não interrompe uma execução em andamento; só impede que novas comecem.' +
        (ativa ? ' "Renovar agora" fica indisponível enquanto houver execução ativa.' : '') + '</div>';

    corpo.innerHTML = (csRenov.erro ? '<div class="cs-alert cs-alert-warn">' + csEsc(csRenov.erro) + '</div>' : '') +
        kv + bloqAviso + episodio + '<div class="cs-rn-sep"></div>' + exec + acoes;
}

async function csRenovCarregarStatus() {
    if (csRenov.carregando) return;
    csRenov.carregando = true;
    const r = await csFetch('GET', '/renovacao/status');
    csRenov.carregando = false;
    if (r.status === 200 && r.data.ok) {
        csRenov.status = r.data;
        csRenov.erro = null;
    } else {
        csRenov.erro = (r.data && r.data.error) || 'Não foi possível ler a renovação automática.';
    }
    csRenderRenov();
}

// ── Escritas (só por clique, com confirmação) ─────────────────────────────
function csNovoPedidoId() {
    const b = new Uint8Array(16);
    try { (window.crypto || crypto).getRandomValues(b); } catch (e) { for (let i = 0; i < 16; i++) b[i] = Math.floor(Math.random() * 256); }
    return Array.prototype.map.call(b, function (x) { return ('0' + x.toString(16)).slice(-2); }).join('');
}

async function csRenovEscrever(method, path, corpo) {
    csRenov.enviando = true;
    csRenderRenov();
    let r;
    try { r = await csFetch(method, path, corpo); } finally { csRenov.enviando = false; }
    return r;
}

async function csRenovRecarregar() {
    await Promise.all([csRenovCarregarStatus(), csRenovCarregarExecucoes(false)]);
}

// Devolve true se a escrita foi aceita; em 409 mostra o motivo e recarrega.
async function csRenovResultado(r, falhaPadrao) {
    if (r.status === 409) {
        const d = r.data || {};
        csRnMsg(CS_RN_MSG_409[d.error] || d.message || CS_RN_MSG_409.versao, 'warn');
        await csRenovRecarregar();
        return false;
    }
    if ((r.status !== 200 && r.status !== 201) || !r.data || !r.data.ok) {
        csRnMsg((r.data && r.data.error) || falhaPadrao, 'err');
        csRenderRenov();
        return false;
    }
    return true;
}

async function conexaoSiaaRenovAtivar(ativar) {
    const s = csRenov.status;
    if (!s || csRenov.enviando) return;
    const msg = ativar
        ? 'Ativar a renovação automática? Quando o SIAA confirmar que a sessão expirou, dentro da agenda, o sistema fará login no SIAA (enviando credenciais) e substituirá as sessões servidas pela API. Pode haver uma tentativa automática por episódio.'
        : 'Desativar a renovação automática? Nenhuma nova renovação automática será iniciada. Uma execução em andamento não é interrompida.';
    if (!window.confirm(msg)) return;
    const r = await csRenovEscrever('PUT', '/renovacao/config', { ativa: ativar, versao: (s.config || {}).versao });
    if (await csRenovResultado(r, 'Não foi possível alterar a renovação automática.')) {
        csRnMsg(ativar ? 'Renovação automática ativada.' : 'Renovação automática desativada.', 'ok');
        await csRenovRecarregar();
    }
}

async function conexaoSiaaRenovSuspender(suspender) {
    const s = csRenov.status;
    if (!s || csRenov.enviando) return;
    const msg = suspender
        ? 'Suspender novas renovações? Isso não interrompe uma execução em andamento; apenas impede que novas comecem.'
        : 'Retomar as novas renovações?';
    if (!window.confirm(msg)) return;
    const r = await csRenovEscrever('POST', '/renovacao/suspensao', { suspensa: suspender, versao: (s.config || {}).versao });
    if (await csRenovResultado(r, 'Não foi possível alterar a suspensão.')) {
        csRnMsg(suspender ? 'Novas renovações suspensas.' : 'Novas renovações retomadas.', 'ok');
        await csRenovRecarregar();
    }
}

async function conexaoSiaaRenovSolicitar() {
    if (!csRenov.status || csRenov.enviando || csRenovAtiva()) return;
    const msg = 'Renovar agora? O sistema fará login no SIAA, ENVIANDO as credenciais, e as sessões servidas pela API serão SUBSTITUÍDAS pelas novas. ' +
        'Módulos atingidos: ' + csRnAlvoRenovarTexto(csRenov.status) + '. ' +
        'Isso pode rodar fora da agenda e não conta como tentativa automática. Confirma?';
    if (!window.confirm(msg)) return;
    // Um pedido_id por confirmação. Só é reaproveitado (por até 2 min) se a
    // tentativa anterior ficou sem resposta definitiva: o servidor é idempotente.
    const agora = Date.now();
    const reaproveita = csRenov.pedido && (agora - csRenov.pedido.em) < 120000;
    const pedido = reaproveita ? csRenov.pedido : { id: csNovoPedidoId(), em: agora };
    csRenov.pedido = pedido;
    const r = await csRenovEscrever('POST', '/renovacao/solicitar', { pedido_id: pedido.id, confirmacao: 'substituir_sessoes' });
    const ambiguo = r.status === 0 || r.status === 502 || r.status === 504;
    if (!ambiguo) csRenov.pedido = null;
    if (await csRenovResultado(r, 'Não foi possível solicitar a renovação.')) {
        csRnMsg(r.status === 200 ? 'Este pedido já tinha sido registrado; mostrando a execução existente.' : 'Renovação solicitada. O executor assume em instantes.', 'ok');
        await csRenovRecarregar();
    } else if (ambiguo) {
        csRnMsg('Sem resposta definitiva. Se tentar de novo em até 2 minutos, o mesmo pedido é reenviado sem duplicar a renovação.', 'warn');
    }
}

async function conexaoSiaaRenovLiberar() {
    const s = csRenov.status;
    if (!s || csRenov.enviando || !csRnPodeLiberar(s)) return;
    const alvo = csRnAlvo(s, 'liberar');
    const msg = alvo === null
        ? 'Liberar nova tentativa? O bloqueio e as tentativas deste episódio são zerados. Nada é iniciado agora: a próxima renovação automática só roda se todas as regras (agenda, sessão expirada) valerem.'
        : 'Liberar nova tentativa para: ' + csRnNomesMods(alvo) + '? O bloqueio e as tentativas só desse(s) módulo(s) são zerados; módulos resolvidos não mudam. ' +
          'Nada é iniciado agora: a próxima renovação automática só roda se todas as regras (agenda, sessão expirada) valerem.';
    if (!window.confirm(msg)) return;
    const r = await csRenovEscrever('POST', '/renovacao/liberar', { episodio_id: s.episodio.id, versao: (s.config || {}).versao });
    if (await csRenovResultado(r, 'Não foi possível liberar a nova tentativa.')) {
        const lib = csRnValidos(r.data.modulos_liberados);
        csRnMsg('Nova tentativa liberada' + (lib && lib.length ? ': ' + csRnNomesMods(lib) : '') + '.', 'ok');
        await csRenovRecarregar();
    }
}

// ── Histórico de execuções (mesmo comportamento do histórico de eventos) ──
function csRnModCelula(e, m) {
    const sit = csRnSituacao(e, m);
    if (sit === 'nao_solicitado') return csRnBadge(CS_RN_SITUACOES, sit);
    const x = ((e.modulos || {})[m]) || {};
    if (!x.coleta && !x.publicacao) return sit ? csRnBadge(CS_RN_SITUACOES, sit) : '<span class="cs-muted">—</span>';
    return '<div class="cs-two">' + (sit ? '<span>' + csRnBadge(CS_RN_SITUACOES, sit) + '</span>' : '') + '<span>Coleta: ' + csEsc(csRnLabel(CS_RN_COLETA, x.coleta)) + '</span>' +
        '<span>Publicação: ' + csEsc(csRnLabel(CS_RN_PUB, x.publicacao)) + '</span>' +
        '<span>Sessão validada: ' + csEsc(csRnSimNao(x.sessao_validada)) + '</span></div>';
}
function csRnLinha(e) {
    return '<tr><td style="white-space:nowrap">' + csEsc(csData(e.inicio || e.criada_em, true)) + '</td>' +
        '<td>' + csEsc(csRnLabel(CS_RN_TIPOS, e.tipo)) + '</td>' +
        '<td>' + csRnResultadoBadge(e) + '</td>' +
        '<td>' + csEsc(csRnGatilho(e)) + '</td>' +
        '<td style="white-space:nowrap">' + csEsc(csRnDuracao(e)) + '</td>' +
        '<td>' + csEsc(csRnLabel(CS_RN_LOGIN, e.login)) + '</td>' +
        '<td>' + csRnModCelula(e, 'academico') + '</td><td>' + csRnModCelula(e, 'financeiro') + '</td>' +
        '<td>' + (e.erro ? csEsc(csRnLabel(CS_RN_ERROS, e.erro)) : '—') + '</td></tr>';
}
function csRenderExecucoes() {
    const h = csRenov.exec;
    const body = csEl('cs-rn-body');
    if (!body) return;
    body.innerHTML = h.itens.length ? h.itens.map(csRnLinha).join('')
        : '<tr><td colspan="9" class="cs-empty">' + (h.carregando ? 'Carregando…' : 'Nenhuma renovação registrada.') + '</td></tr>';
    csEl('cs-rn-mais').style.display = h.proximo ? '' : 'none';
    csEl('cs-rn-mais').disabled = h.carregando;
    csEl('cs-rn-info').textContent = h.itens.length ? h.itens.length + ' execuções exibidas' + (h.proximo ? '' : ' (fim do histórico)') : '';
}
async function csRenovCarregarExecucoes(anexar, silencioso) {
    const h = csRenov.exec;
    if (h.carregando) return;
    h.carregando = true;
    const q = new URLSearchParams();
    q.set('limite', csEl('cs-rn-limite').value);
    if (anexar && h.proximo) q.set('antes', h.proximo);
    if (anexar) h.paginou = true;
    if (!anexar && !silencioso) { h.itens = []; h.proximo = null; h.paginou = false; }
    if (!silencioso) csRenderExecucoes();
    const r = await csFetch('GET', '/renovacao/execucoes?' + q.toString());
    h.carregando = false;
    if (r.status === 200 && r.data.ok) {
        h.itens = anexar ? h.itens.concat(r.data.itens || []) : (r.data.itens || []);
        h.proximo = r.data.proximo_antes || null;
    }
    csRenderExecucoes();
}
function conexaoSiaaRenovExecucoesRecarregar() { return csRenovCarregarExecucoes(false); }
function conexaoSiaaRenovExecucoesMais() { return csRenovCarregarExecucoes(true); }

// ── Ciclo de vida da página ───────────────────────────────────────────────
function conexaoSiaaRefresh() {
    conexaoSiaaLoadStatus();
    conexaoSiaaHistoricoRecarregar();
    csRenovCarregarStatus();
    csRenovCarregarExecucoes(false);
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
