// Motor de Ativações — página de acompanhamento (só leitura + liga/desliga/parada).

let _mtTimer = null;

const MT_MENSAGENS = {
    lembrete_dia10: 'Lembrete do dia 10',
    aviso_calouro: 'Aviso do calouro (1ª mensalidade)',
    reconhecimento: 'Reconhecimento (em dia)',
    lembrete_dia25: 'Lembrete do dia 25',
    recuperacao: 'Conversa de recuperação',
    firme: 'Mensagem firme',
};
const MT_FRENTES = { dia10: 'Dia 10', dia25: 'Dia 25', recuperacao: 'Recuperação' };
const MT_STATUS = {
    planejado: ['Planejado', 'mt-neutro'], simulado: ['Liberado (simulação)', 'mt-ok'], liberado: ['Liberado', 'mt-ok'],
    pausado: ['Pausado', 'mt-aviso'], concluido: ['Concluído', 'mt-neutro'], adiado: ['Adiado', 'mt-aviso'], recusado: ['Recusado', 'mt-ruim'],
};
const MT_DECISAO = { liberar: ['Libera', 'mt-ok'], segurar: ['Segura', 'mt-aviso'], pausar: ['Pausa', 'mt-ruim'] };
const MT_EXCLUSAO = {
    grupo_de_controle: 'grupo de controle', situacao_diferente_de_em_curso: 'fora de Em Curso', sem_telefone: 'sem telefone',
    erro_131026: 'número com erro 131026', recusou_mensagens: 'recusou mensagens', ja_recebeu_hoje: 'já recebeu hoje',
    limite_de_mensagens_no_ciclo: 'limite de 2 no ciclo', ja_recebeu_desta_frente_no_ciclo: 'já recebeu desta frente',
    ja_recebeu_esta_mensagem_no_ciclo: 'já recebeu esta mensagem', declarou_pagamento: 'declarou pagamento',
};

function _mtEsc(s) {
    return String(s ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function _mtNum(n) { return n == null ? '—' : Number(n).toLocaleString('pt-BR'); }
function _mtHora(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit', timeZone: 'America/Sao_Paulo' });
}
function _mtDiaHora(iso) {
    if (!iso) return '—';
    const d = new Date(iso);
    return d.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', timeZone: 'America/Sao_Paulo' });
}
function _mtDia(isoDia) {
    const [a, m, d] = isoDia.split('-').map(Number);
    return new Date(a, m - 1, d).toLocaleDateString('pt-BR', { weekday: 'short', day: '2-digit', month: '2-digit' });
}
function _mtSet(id, texto, classe) {
    const el = document.getElementById(id);
    if (!el) return;
    el.textContent = texto;
    if (classe !== undefined) {
        el.classList.remove('mt-ok', 'mt-aviso', 'mt-ruim', 'mt-neutro');
        if (classe) el.classList.add(classe);
    }
}
function _mtMostrar(id, sim) {
    const el = document.getElementById(id);
    if (el) el.classList.toggle('hidden', !sim);
}

function loadMotorAtivacoes() {
    mtAtualizar();
    if (_mtTimer) clearInterval(_mtTimer);
    _mtTimer = setInterval(() => {
        const pagina = document.getElementById('page-motor_ativacoes');
        if (!pagina || pagina.classList.contains('hidden')) { clearInterval(_mtTimer); _mtTimer = null; return; }
        if (!document.hidden) mtAtualizar();
    }, 30000);
}

async function mtAtualizar() {
    try {
        const [rEstado, rDiario] = await Promise.all([api('/api/motor/estado'), api('/api/motor/diario?limit=120')]);
        const estado = await rEstado.json();
        const diario = await rDiario.json();
        if (!rEstado.ok || !estado.ok) throw new Error(estado.error || 'Falha ao ler o estado do motor.');
        _mtRender(estado, diario.itens || []);
    } catch (e) {
        _mtFaixa(e.message || 'Falha ao ler o estado do motor.', 'is-parada');
    }
}

function _mtFaixa(texto, classe) {
    const el = document.getElementById('mt-faixa');
    if (!el) return;
    el.className = 'mt-faixa ' + (classe || '');
    el.textContent = texto;
    el.classList.toggle('hidden', !texto);
}

function _mtRender(e, diario) {
    const botoes = ['mt-btn-ligar', 'mt-btn-desligar', 'mt-btn-retomar', 'mt-btn-parar'];
    if (!e.instalado) {
        botoes.forEach(b => _mtMostrar(b, false));
        _mtSet('mt-modo', 'Não instalado', 'mt-neutro');
        _mtSet('mt-modo-sub', 'As tabelas do motor ainda não foram criadas neste banco.');
        _mtFaixa('O motor ainda não foi instalado: faltam as tabelas motor_ (scripts/apply_motor_sql.py) e o serviço do worker.', 'is-sim');
        _mtAgenda(e.agenda || []);
        return;
    }
    const c = e.controle || {};
    const w = e.worker;
    const vivo = !!e.worker_vivo;
    const modo = vivo && w ? w.modo : null;

    _mtMostrar('mt-btn-ligar', !c.ligado);
    _mtMostrar('mt-btn-desligar', !!c.ligado);
    _mtMostrar('mt-btn-parar', !c.parada_geral);
    _mtMostrar('mt-btn-retomar', !!c.parada_geral);

    if (c.parada_geral) {
        _mtSet('mt-modo', 'Parado', 'mt-ruim');
        _mtFaixa('PARADA GERAL acionada por ' + (c.atualizado_por || '—') + (c.parada_motivo ? ': ' + c.parada_motivo : '') + '. Nenhum lote sai até a parada ser liberada.', 'is-parada');
    } else if (!c.ligado) {
        _mtSet('mt-modo', 'Desligado', 'mt-neutro');
        _mtFaixa('', '');
    } else if (modo === 'simulacao') {
        _mtSet('mt-modo', 'Simulação', 'mt-aviso');
        _mtFaixa('Modo simulação: o motor monta os lotes e registra o que faria. Nenhuma mensagem é enviada e nada é gravado no CRM.', 'is-sim');
    } else if (modo === 'desligado') {
        _mtSet('mt-modo', 'Desligado', 'mt-neutro');
        _mtFaixa('Ligado na página, mas o worker está com MOTOR_ENABLED=0. Ele só começa a simular depois que a variável mudar no serviço do worker.', 'is-sim');
    } else {
        _mtSet('mt-modo', 'Ligado', 'mt-aviso');
        _mtFaixa('Ligado na página, mas o worker não está respondendo.', 'is-parada');
    }
    _mtSet('mt-modo-sub', c.atualizado_por ? 'Última mudança: ' + c.atualizado_por + ', ' + _mtDiaHora(c.atualizado_em) : '');

    if (!w) {
        _mtSet('mt-worker', 'Nunca subiu', 'mt-neutro');
        _mtSet('mt-worker-sub', 'Crie o segundo serviço no EasyPanel.');
    } else {
        _mtSet('mt-worker', vivo ? 'Ativo' : 'Sem sinal', vivo ? 'mt-ok' : 'mt-ruim');
        _mtSet('mt-worker-sub', 'Último tick ' + _mtDiaHora(w.ultimo_tick_em) + (w.ultimo_erro ? ' · erro: ' + w.ultimo_erro : ''));
    }

    const ultima = (e.fila || [])[0];
    if (ultima && ultima.ok) {
        const d = MT_DECISAO[ultima.decisao] || [ultima.decisao || '—', 'mt-neutro'];
        _mtSet('mt-fila', _mtNum(ultima.aguardando), d[1]);
        _mtSet('mt-fila-sub', d[0] + ' · ' + _mtNum(ultima.atendentes_online) + ' online · ' + _mtHora(ultima.lida_em) + (ultima.erro ? ' · ensaio' : ''));
    } else {
        _mtSet('mt-fila', '—', 'mt-neutro');
        _mtSet('mt-fila-sub', ultima ? 'Sem leitura: ' + (ultima.erro || ultima.motivo || '') : 'Sem leituras ainda.');
    }

    const lotes = e.lotes || [];
    const liberados = lotes.filter(l => l.status === 'simulado' || l.status === 'liberado');
    const total = liberados.reduce((s, l) => s + (l.tamanho || 0), 0);
    _mtSet('mt-hoje', _mtNum(total));
    _mtSet('mt-hoje-sub', liberados.length + ' de ' + lotes.length + ' lotes');

    _mtPlano(e);
    _mtLotes(lotes);
    _mtFilaTabela(e.fila || []);
    _mtAgenda(e.agenda || []);
    _mtLimites(w && w.config);
    _mtDiario(diario);
}

function _mtPlano(e) {
    const p = e.plano;
    _mtMostrar('mt-plano-corpo', !!p);
    if (!p) {
        const carga = e.carga;
        _mtSet('mt-plano-vazio', carga
            ? 'Sem plano para hoje. Última carga: ' + _mtDiaHora(carga.concluida_em) + ' (' + _mtNum(carga.abertos_dia10) + ' com Dia 10 em aberto).'
            : 'Sem plano para hoje. O motor ainda não fez nenhuma carga da base.');
        _mtSet('mt-plano-meta', '');
        return;
    }
    const r = p.resumo || {};
    _mtSet('mt-plano-vazio', '');
    _mtSet('mt-plano-meta', 'Ciclo ' + p.ciclo + ' · ' + _mtNum(r.total) + ' alunos em ' + _mtNum(r.lotes) + ' lotes · ' + _mtNum(r.adiados) + ' adiados · ' + _mtNum(r.controle) + ' no grupo de controle');
    document.getElementById('mt-plano-acoes').innerHTML = (r.acoes || []).map(a => {
        const excl = Object.entries(a.exclusoes || {}).map(([k, n]) => _mtEsc(MT_EXCLUSAO[k] || k) + ' ' + _mtNum(n)).join(' · ') || '—';
        return '<tr><td>' + _mtEsc(MT_FRENTES[a.frente] || a.frente) + '</td><td>' + _mtEsc(MT_MENSAGENS[a.mensagem] || a.mensagem)
            + '</td><td class="mt-num">' + _mtNum(a.elegiveis) + '</td><td class="mt-num">' + _mtNum(a.fora_do_alvo) + '</td><td>' + excl + '</td></tr>';
    }).join('');
    const rotulos = e.estados || {};
    document.getElementById('mt-plano-estados').innerHTML = Object.entries(r.por_estado || {}).map(([k, n]) =>
        '<span class="mt-sel mt-neutro">' + _mtEsc(rotulos[k] || k) + ' ' + _mtNum(n) + '</span>').join('');
}

function _mtLotes(lotes) {
    _mtMostrar('mt-lotes-wrap', lotes.length > 0);
    _mtMostrar('mt-lotes-vazio', lotes.length === 0);
    document.getElementById('mt-lotes').innerHTML = lotes.map(l => {
        const s = MT_STATUS[l.status] || [l.status, 'mt-neutro'];
        return '<tr><td class="mt-num">' + _mtNum(l.ordem) + '</td><td>' + _mtEsc(MT_MENSAGENS[l.mensagem] || l.mensagem)
            + '</td><td class="mt-num">' + _mtNum(l.tamanho_previsto) + '</td><td class="mt-num">' + (l.tamanho ? _mtNum(l.tamanho) : '—')
            + '</td><td><span class="mt-sel ' + s[1] + '">' + _mtEsc(s[0]) + '</span></td><td>' + _mtHora(l.liberado_em)
            + '</td><td>' + _mtEsc(l.motivo || '') + '</td></tr>';
    }).join('');
}

function _mtFilaTabela(fila) {
    _mtMostrar('mt-fila-wrap', fila.length > 0);
    _mtMostrar('mt-fila-vazio', fila.length === 0);
    document.getElementById('mt-fila-linhas').innerHTML = fila.slice(0, 12).map(f => {
        const d = MT_DECISAO[f.decisao] || [f.decisao || '—', 'mt-neutro'];
        const espera = f.maior_espera_s == null ? '—' : Math.round(f.maior_espera_s / 60) + ' min';
        return '<tr><td>' + _mtHora(f.lida_em) + '</td><td class="mt-num">' + _mtNum(f.aguardando) + '</td><td class="mt-num">' + espera
            + '</td><td class="mt-num">' + _mtNum(f.atendentes_online) + '</td><td><span class="mt-sel ' + d[1] + '" title="' + _mtEsc(f.motivo || '') + '">'
            + _mtEsc(d[0]) + '</span>' + (f.erro ? ' <span class="text-xs text-[var(--text-muted)]">' + _mtEsc(f.erro) + '</span>' : '') + '</td></tr>';
    }).join('');
}

function _mtAgenda(agenda) {
    const el = document.getElementById('mt-agenda');
    if (!el) return;
    el.innerHTML = agenda.length ? agenda.map(a =>
        '<div><b>' + _mtEsc(_mtDia(a.dia)) + '</b> — ' + a.mensagens.map(m => _mtEsc(MT_MENSAGENS[m] || m)).join(', ') + '</div>').join('')
        : '<span class="text-[var(--text-muted)]">Nenhum dia de envio nos próximos dois meses.</span>';
}

function _mtLimites(cfg) {
    const el = document.getElementById('mt-limites');
    if (!el) return;
    if (!cfg) { el.textContent = 'O worker ainda não informou os limites.'; return; }
    const linhas = [
        'Janela: ' + cfg.janela_inicio_h + 'h às ' + cfg.janela_fim_h + 'h, em dia útil',
        'Teto diário: ' + _mtNum(cfg.teto_diario) + ' · lote de até ' + _mtNum(cfg.lote_max),
        'Fila: teto ' + cfg.fila_teto + ' · crítico ' + cfg.fila_critica + ' · mínimo de ' + cfg.atendentes_min + ' atendentes online',
        'SLA de espera: ' + cfg.sla_espera_min + ' min · falha de envio até ' + Math.round(cfg.falha_envio_max * 100) + '%',
        'Grupo de controle: ' + cfg.controle_pct + '% · até ' + cfg.max_msgs_ciclo + ' mensagens por aluno no ciclo',
    ];
    el.innerHTML = linhas.map(l => '<div>' + _mtEsc(l) + '</div>').join('');
}

function _mtDiario(itens) {
    _mtMostrar('mt-diario-vazio', itens.length === 0);
    document.getElementById('mt-diario').innerHTML = itens.map(i =>
        '<div class="mt-linha"><span class="mt-hora">' + _mtDiaHora(i.criado_em) + '</span><span class="mt-hora">' + _mtEsc(i.autor)
        + '</span><span>' + _mtEsc(i.texto) + '</span></div>').join('');
}

async function mtControle(acao) {
    let motivo = '';
    if (acao === 'parar') {
        motivo = window.prompt('Parada geral: nenhum lote sai até alguém liberar. Motivo (opcional):', '');
        if (motivo === null) return;
    } else if (acao === 'ligar') {
        if (!window.confirm('Ligar o motor? Nesta versão ele só simula: nada é enviado.')) return;
    }
    try {
        const res = await api('/api/motor/controle', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ acao, motivo }),
        });
        const data = await res.json();
        if (!res.ok || !data.ok) throw new Error(data.error || 'Não consegui gravar.');
        toast({ ligar: 'Motor ligado.', desligar: 'Motor desligado.', parar: 'Parada geral acionada.', retomar: 'Parada liberada.' }[acao], acao === 'parar' ? 'warning' : 'success');
        mtAtualizar();
    } catch (e) {
        toast(e.message || 'Não consegui gravar.', 'error');
    }
}
