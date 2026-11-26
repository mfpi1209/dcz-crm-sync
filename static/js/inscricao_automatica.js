// ---------------------------------------------------------------------------
// Inscrição - Automática (página #inscricao_automatica)
//
// Porte das telas da interface do serviço de inscrição (V2 public/automacao.js,
// app.js e polos.js) para o painel. Toda chamada passa por
// /api/inscricao-auto/<rota> (routes/inscricao_auto.py), que confere a permissão,
// injeta o token do serviço, o usuário (X-Usuario) e o papel.
//
// Papel: data-papel do #page-inscricao_automatica ("admin" | "operar"). O que é de
// admin fica escondido para operar e o servidor recusa de qualquer forma.
// Atualiza só enquanto a página está aberta e a aba do navegador visível.
// ---------------------------------------------------------------------------
(function () {
    'use strict';

    const BASE = '/api/inscricao-auto/';
    const $ = (id) => document.getElementById(id);
    const raiz = () => $('page-inscricao_automatica');

    // ── escapes e formatação ────────────────────────────────────────────────
    function h(v) {
        return String(v == null ? '' : v)
            .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
    }
    const fmt = {
        quando(iso) {
            if (!iso) return '—';
            const d = new Date(iso);
            if (isNaN(d)) return '—';
            return d.toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' });
        },
        ha(iso) {
            if (!iso) return 'nunca';
            const s = Math.round((Date.now() - Date.parse(iso)) / 1000);
            if (isNaN(s)) return '—';
            if (s < 60) return `há ${s}s`;
            if (s < 3600) return `há ${Math.round(s / 60)} min`;
            if (s < 86400) return `há ${Math.round(s / 3600)} h`;
            return `há ${Math.round(s / 86400)} d`;
        },
        duracao(ms) {
            if (ms == null) return '—';
            const s = Math.round(ms / 1000);
            return s < 90 ? `${s}s` : `${Math.floor(s / 60)} min ${s % 60}s`;
        },
    };
    const hojeSP = () => new Date().toLocaleDateString('sv-SE', { timeZone: 'America/Sao_Paulo' });
    const diasAtrasSP = (n) => new Date(Date.now() - n * 864e5).toLocaleDateString('sv-SE', { timeZone: 'America/Sao_Paulo' });
    const clone = (v) => JSON.parse(JSON.stringify(v));
    const igual = (a, b) => JSON.stringify(a) === JSON.stringify(b);
    const dig = (v) => String(v || '').replace(/\D/g, '');
    const linkSeguro = (u) => (typeof u === 'string' && /^https:\/\//i.test(u) ? u : null);

    // ── rótulos (os mesmos da interface do serviço) ─────────────────────────
    const NOME_MOTOR = { api: 'HTTP', browser: 'Playwright' };
    const STATUS_JOB = {
        na_fila: { rotulo: 'Na fila', tom: 'neutro' },
        executando: { rotulo: 'Executando', tom: 'ativo' },
        sucesso: { rotulo: 'Inscrito', tom: 'ok' },
        recusado: { rotulo: 'Recusado (regra)', tom: 'neutro' },
        revisao: { rotulo: 'Revisão', tom: 'atencao' },
        falhou: { rotulo: 'Falhou', tom: 'erro' },
        manual: { rotulo: 'Manual', tom: 'atencao' },
        cancelado: { rotulo: 'Cancelado', tom: 'neutro' },
        ja_concluido: { rotulo: 'Já concluído', tom: 'neutro' },
    };
    const STATUS_CONF = {
        confirmado: { rotulo: 'Confirmado no SIAA', tom: 'ok' },
        divergente: { rotulo: 'Nº diferente no SIAA', tom: 'atencao' },
        ausente: { rotulo: 'Não está no SIAA', tom: 'erro' },
        pendente: { rotulo: 'A conferir', tom: 'neutro' },
    };
    const ORIGEM = { crm1: 'crm1', api: 'API', painel: 'Painel' };
    const DESFECHO = {
        inscrito: { rotulo: 'Inscrito', tom: 'ok' },
        pendencia: { rotulo: 'Pendência', tom: 'neutro' },
        manual: { rotulo: 'Inscrição manual', tom: 'atencao' },
        revisao: { rotulo: 'Conferir', tom: 'atencao' },
    };
    const ENTREGA = {
        pendente: { rotulo: 'Enviando', tom: 'ativo' },
        enviado: { rotulo: 'No crm1', tom: 'ok' },
        falhou: { rotulo: 'Não entregue', tom: 'erro' },
        pulado: { rotulo: 'Não enviado', tom: 'erro' },
    };
    const FILTROS_FILA = [
        { valor: '', nome: 'Todas' },
        { valor: 'na_fila,executando', nome: 'Em andamento' },
        { valor: 'manual', nome: 'Processo manual' },
        { valor: 'falhou,revisao', nome: 'Falhas e revisão' },
        { valor: 'sucesso', nome: 'Inscritos' },
    ];
    const TIPOS_EVENTO = {
        '': 'Todos',
        automacao_desligada: 'Desligamentos',
        automacao_ligada: 'Religamentos',
        alerta: 'Avisos aos gestores',
        crm1: 'crm1',
        job: 'Inscrições',
        conferencia: 'Conferência SIAA',
        config_alterada: 'Configuração',
        agente: 'Agente',
    };
    const NIVEL = { erro: { tom: 'erro', rotulo: 'Erro' }, aviso: { tom: 'atencao', rotulo: 'Atenção' }, info: { tom: 'neutro', rotulo: 'Info' } };
    const TIPO_ACHADO = {
        api_mudou: 'A loja mudou uma resposta da API',
        tela_mudou: 'Uma tela mudou e o seletor não encontra mais o campo',
        falhas_em_producao: 'Falhas repetidas nas inscrições reais',
        check_quebrou: 'Uma verificação não conseguiu rodar',
    };
    const CHECK = { api: 'API da loja', telas: 'Telas (replay)', producao: 'Inscrições reais (24 h)' };
    const MODOS_SIM = { ok: 'Inscreve com sucesso', tecnico: 'Falha técnica (tela mudou)', negocio: 'Recusa por regra (já inscrito)', revisao: 'Pedido sem SIAA (revisão)' };
    const TIPO_LINK = { prova: 'Link da prova', pagamento: 'Link de pagamento', documentos: 'Envio de documentos' };
    const ROTULOS_ETAPA = {
        '1_sessions': 'Sessão da loja',
        '2c_getprices': 'Preços',
        '2d_campaigns': 'Campanha',
        '3_lead_post': 'Lead criado',
        '4_lead_patch_polo': 'Lead: polo',
        '5_profile_initial': 'Perfil',
        '8b_client': 'Cadastro do cliente (CL)',
        '8b_clientupdate': 'Cadastro do cliente (update)',
        '10_profile_cpf': 'Perfil: CPF',
        '10c_clientupdate': 'Cadastro do cliente (CPF/nascimento)',
        '11_preferences': 'Preferências',
        '12_postal': 'Endereço (CEP)',
    };
    const rotuloEtapa = (n) => ROTULOS_ETAPA[n] || (/^8_setprices/.test(n) ? 'Preço da inscrição (setprices)' : String(n).replace(/^\d+[a-z]?_/, '').replace(/_/g, ' '));

    // ── pequenos construtores de HTML ───────────────────────────────────────
    const pill = (tom, texto) => `<span class="insa-pill t-${h(tom || 'neutro')}">${h(texto)}</span>`;
    const sub = (texto) => (texto ? `<small>${h(texto)}</small>` : '');
    const alerta = (tom, html) => `<div class="insa-alert t-${tom}"${tom === 'erro' ? ' role="alert"' : ''}>${html}</div>`;
    const icone = (nome) => `<span class="material-symbols-outlined">${nome}</span>`;
    function botao(texto, { acao, classe = 'btn-secondary', mini = true, dados = {}, desab = false, titulo = '' } = {}) {
        const attrs = Object.entries(dados).map(([k, v]) => ` data-${k}="${h(v)}"`).join('');
        return `<button type="button" class="${classe} insa-btn${mini ? ' mini' : ''}" data-acao="${h(acao)}"${attrs}${desab ? ' disabled' : ''}${titulo ? ` title="${h(titulo)}"` : ''}>${h(texto)}</button>`;
    }
    function tabela(colunas, linhasHtml, vazio) {
        const corpo = linhasHtml && linhasHtml.length
            ? linhasHtml.join('')
            : `<tr><td colspan="${colunas.length}" class="vazio">${h(vazio)}</td></tr>`;
        return `<table class="insa-table"><thead><tr>${colunas.map((c) => `<th>${h(c)}</th>`).join('')}</tr></thead><tbody>${corpo}</tbody></table>`;
    }
    function setStatus(id, texto, tom = 'info') {
        const el = $(id);
        if (!el) return;
        el.textContent = texto || '';
        el.className = `insa-status t-${tom}`;
    }
    // Não redesenha o que não mudou (preserva seleção e foco); o minuto entra para os "há X min".
    const ultimoDesenho = new Map();
    function mudou(chave, dados) {
        const assinatura = JSON.stringify(dados) + Math.floor(Date.now() / 60000);
        if (ultimoDesenho.get(chave) === assinatura) return false;
        ultimoDesenho.set(chave, assinatura);
        return true;
    }

    // ── chamada ao proxy do painel ──────────────────────────────────────────
    // Devolve { ok, http, ...json } (o `ok` do corpo, quando vier, prevalece — como na interface do serviço).
    async function chamar(rota, { method = 'GET', corpo } = {}) {
        const opts = { method, headers: { Accept: 'application/json' } };
        if (method !== 'GET') {
            opts.headers['Content-Type'] = 'application/json';
            opts.body = JSON.stringify(corpo == null ? {} : corpo);
        }
        let res;
        try {
            res = await api(BASE + rota, opts);
        } catch (e) {
            return { ok: false, http: 0, erro: `Sem resposta do painel: ${e.message}` };
        }
        let j = {};
        try { j = await res.json(); } catch (_) { /* sem corpo */ }
        if (!j || typeof j !== 'object' || Array.isArray(j)) j = { dados: j };
        return { ok: res.ok, http: res.status, ...j };
    }
    const erroDe = (r) => (r.problemas && r.problemas.length ? `Não salvei: ${r.problemas.join('; ')}.` : r.erro || r.error || r.motivo || `Erro HTTP ${r.http}`);

    // ── estado ──────────────────────────────────────────────────────────────
    const st = {
        iniciado: false,
        ativo: false,
        papel: 'operar',
        aba: 'visao',
        saude: null,
        automacao: null,
        config: null,
        tick: 0,
        ocupado: false,
        timer: null,
        carregadas: new Set(),
    };
    const ehAdmin = () => st.papel === 'admin';
    const ABAS = ['visao', 'fila', 'nova', 'cadastros', 'siaa', 'agente', 'config', 'eventos', 'simulacao'];

    function lerLS(k) { try { return localStorage.getItem(k) || ''; } catch (_) { return ''; } }
    function gravarLS(k, v) { try { localStorage.setItem(k, v); } catch (_) { /* navegação privada */ } }

    // ── diálogo de confirmação (motivo opcional) ────────────────────────────
    let dlgOcupado = false;
    /** executar(motivo) devolve texto de erro (diálogo continua aberto) ou nada (fecha). */
    function confirmar({ titulo, texto, acao, perigo = false, alertaTexto = false, campoMotivo = null, executar }) {
        const dlg = $('insa-dlg');
        $('insa-dlg-titulo').textContent = titulo;
        $('insa-dlg-texto').textContent = texto;
        $('insa-dlg-texto').className = `insa-dlg-texto${alertaTexto ? ' alerta' : ''}`;
        $('insa-dlg-campo').style.display = campoMotivo ? 'flex' : 'none';
        $('insa-dlg-rotulo').textContent = campoMotivo || '';
        $('insa-dlg-motivo').value = '';
        $('insa-dlg-status').textContent = '';
        const ok = $('insa-dlg-ok');
        const voltar = $('insa-dlg-voltar');
        ok.textContent = acao;
        ok.className = `${perigo ? 'btn-danger' : 'btn-primary'} insa-btn`;
        ok.onclick = async () => {
            dlgOcupado = true;
            ok.disabled = voltar.disabled = true;
            try {
                const erro = await executar($('insa-dlg-motivo').value.trim());
                if (erro) $('insa-dlg-status').textContent = erro;
                else dlg.close();
            } catch (e) {
                $('insa-dlg-status').textContent = e.message;
            } finally {
                dlgOcupado = false;
                ok.disabled = voltar.disabled = false;
            }
        };
        voltar.onclick = () => { if (!dlgOcupado) dlg.close(); };
        dlg.oncancel = (e) => { if (dlgOcupado) e.preventDefault(); };
        if (typeof dlg.showModal === 'function') dlg.showModal();
        else dlg.setAttribute('open', '');
        (campoMotivo ? $('insa-dlg-motivo') : voltar).focus();
    }

    // =======================================================================
    // Cabeçalho, modo e disjuntor
    // =======================================================================
    function desenharCabecalho() {
        const a = st.automacao;
        const p = $('insa-pill-automacao');
        if (p) {
            if (!a) { p.className = 'insa-pill t-neutro'; p.textContent = 'Automação: —'; }
            else {
                p.className = `insa-pill t-${a.ligada ? 'ok' : 'erro'}`;
                p.textContent = `Automação ${a.ligada ? 'ligada' : 'desligada'}${st.saude ? ` · modo ${st.saude.simulacao ? 'simulação' : st.saude.modo}` : ''}`;
            }
        }
    }

    function desenharModo() {
        const hh = st.saude;
        const box = $('insa-modo');
        if (!box || !hh || !mudou('modo', [hh.modo, hh.simulacao])) return;
        if (hh.modo === 'ensaio') {
            box.innerHTML = alerta('atencao', `<strong>Modo ensaio${hh.simulacao ? ' + simulação' : ''}.</strong> ${h(hh.simulacao
                ? 'Motores falsos: nada vai para a loja nem para o SIAA.'
                : 'O motor vai até antes do pedido e para: nenhuma inscrição é criada (lead e cadastro são gravados na loja). Playwright fora. Para inscrever de verdade, o serviço precisa subir com ORQ_MODO=real.')}`);
        } else if (hh.simulacao) {
            box.innerHTML = alerta('atencao', '<strong>Simulação.</strong> Motores falsos: nada vai para a loja nem para o SIAA.');
        } else {
            box.innerHTML = alerta('info', '<strong>Modo real.</strong> Cada inscrição da fila cria o pedido na loja com vínculo no SIAA.');
        }
    }

    function desenharDisjuntor() {
        const a = st.automacao;
        const box = $('insa-disjuntor');
        if (!box || !a || !mudou('disjuntor', [a, st.papel])) return;
        const ligada = !!a.ligada;
        const limite = (a.politica && a.politica.falhasSeguidas) || 3;
        const queimados = Math.min(a.falhasSeguidas || 0, limite);
        const j = a.janela || {};
        const quem = a.por === 'automatico' ? ', pelo desligamento automático' : a.por ? `, por ${a.por}` : '';
        const fusiveis = Array.from({ length: limite }, (_, i) => `<i class="${i < queimados ? 'queimado' : ''}"></i>`).join('');
        const texto = ligada
            ? `Inscrevendo automaticamente desde ${fmt.quando(a.desde)}`
            : `${a.motivo || 'Desligada'}. Os leads seguem pelo processo manual desde ${fmt.quando(a.desde)}${quem}.`;
        box.innerHTML = `<div class="insa-disjuntor">
            <div class="insa-row"><div class="insa-alavanca${ligada ? ' ligada' : ''}" aria-hidden="true"></div>
                <strong class="text-lg ${ligada ? '' : 'text-[var(--danger)]'}">${ligada ? 'Ligada' : 'Desligada'}</strong></div>
            <div class="min-w-0">
                <p class="text-sm text-[var(--text-primary)]">${h(texto)}</p>
                <div class="mt-2" title="Falhas finais seguidas contra o limite do desligamento automático">
                    <div class="insa-fusiveis" aria-hidden="true">${fusiveis}</div>
                    <small class="insa-muted">${h(`${a.falhasSeguidas || 0} de ${limite} falhas seguidas; ${Math.round((j.taxa || 0) * 100)}% de falha nas últimas ${j.amostras || 0}`)}</small>
                </div>
            </div>
            <div>${ehAdmin()
                ? botao(ligada ? 'Desligar automação' : 'Religar automação', { acao: 'disjuntor', classe: ligada ? 'btn-danger' : 'btn-primary', mini: false })
                : '<small class="insa-muted">Ligar e desligar é de quem administra.</small>'}</div>
        </div>`;
    }

    function alternarAutomacao() {
        const ligada = !!(st.automacao && st.automacao.ligada);
        confirmar({
            titulo: ligada ? 'Desligar a inscrição automática?' : 'Religar a inscrição automática?',
            texto: ligada
                ? 'A fila para, os leads que ainda não começaram voltam para o processo manual e a regra no crm1 é desativada. Inscrições em andamento terminam normalmente.'
                : 'A regra no crm1 é reativada e a fila volta a inscrever automaticamente. Os contadores de falha começam do zero.',
            acao: ligada ? 'Desligar' : 'Religar',
            perigo: ligada,
            campoMotivo: ligada ? 'Motivo (aparece no aviso aos gestores)' : null,
            async executar(motivo) {
                const r = await chamar(ligada ? 'orq/automacao/desligar' : 'orq/automacao/ligar', { method: 'POST', corpo: ligada ? (motivo ? { motivo } : {}) : {} });
                if (!r.ok) return erroDe(r);
                st.automacao = r.automacao;
                desenharDisjuntor();
                desenharCabecalho();
                setStatus('insa-ov-disj-status', ligada ? 'Automação desligada: os leads seguem pelo processo manual.' : 'Automação religada.', 'ok');
                atualizarAba(true);
            },
        });
    }

    // =======================================================================
    // Tabela de inscrições (visão geral e fila)
    // =======================================================================
    function linhaJob(j, { compacta, acoes }) {
        const r = j.resumo || {};
        const res = j.resultado || {};
        const s = j.code === 'ENSAIO_OK' ? { rotulo: 'Ensaio OK (sem pedido)', tom: 'ok' } : STATUS_JOB[j.status] || { rotulo: j.status, tom: 'neutro' };
        const polo = res.polo && res.polo !== r.polo ? `${r.polo || ''} → ${res.polo}` : r.polo || '';
        let situacao = pill(s.tom, s.rotulo);
        if (j.modo === 'ensaio' && j.code !== 'ENSAIO_OK') situacao += sub('ensaio');
        // andamento da inscrição em execução (o motor anuncia as etapas: login, cadastro, pedido, nº SIAA)
        if (j.status === 'executando' && j.progresso) {
            const pct = Math.max(0, Math.min(100, Number(j.progresso.pct) || 0));
            situacao += `<div class="insa-progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${pct}" aria-label="Andamento: ${pct}%"><span style="width:${pct}%"></span></div>`;
            situacao += sub(`${pct}% · ${j.progresso.etapa || ''}`);
        }
        if (j.code && j.status !== 'sucesso' && j.code !== 'ENSAIO_OK') situacao += sub(j.code);
        let motor = h(j.engine ? NOME_MOTOR[j.engine] || j.engine : '—');
        if (j.failoverUsado) motor += sub('após contingência');
        let tr = `<tr>
            <td class="num">${h(fmt.quando(j.criadoEm))}${sub(`${j.id}, via ${ORIGEM[j.origem] || j.origem || '—'}`)}</td>
            <td>${h(r.primeiroNome || '—')}${sub(r.cpf || '')}</td>
            <td>${h(r.curso || '—')}${sub(polo)}</td>
            <td>${situacao}</td>
            <td>${motor}</td>`;
        if (!compacta) {
            const link = linkSeguro(res.linkAluno);
            const linkHtml = link ? `<small><a href="${h(link)}" target="_blank" rel="noopener noreferrer" class="text-[var(--primary)] underline">${h(TIPO_LINK[res.linkTipo] || 'Link do aluno')}</a></small>` : '';
            tr += `<td class="num">${h(res.inscricaoSIAA || res.numeroInscricao || '—')}${linkHtml}</td><td class="num">${h(fmt.duracao(j.ms))}</td>`;
        }
        if (acoes) {
            const b = [];
            if (['manual', 'falhou', 'revisao'].includes(j.status) && j.temPayload) {
                b.push(botao('Inscrever de novo', { acao: 'job-reprocessar', classe: j.status === 'revisao' ? 'btn-danger' : 'btn-secondary', dados: { job: j.id } }));
            }
            if (['na_fila', 'manual'].includes(j.status)) b.push(botao('Cancelar', { acao: 'job-cancelar', dados: { job: j.id } }));
            if (ehAdmin() && j.origem === 'crm1' && j.dealId && !['na_fila', 'executando'].includes(j.status)) {
                b.push(botao('Devolver ao crm1', { acao: 'job-devolver', dados: { job: j.id } }));
            }
            tr += `<td class="acoes">${b.join('')}</td>`;
        }
        return tr + '</tr>';
    }
    function tabelaJobs(jobs, { compacta = false, acoes = false } = {}) {
        const cols = ['Recebida', 'Candidato', 'Curso e polo', 'Situação', 'Motor'];
        if (!compacta) cols.push('Nº SIAA', 'Duração');
        if (acoes) cols.push('');
        return tabela(cols, (jobs || []).map((j) => linhaJob(j, { compacta, acoes })), 'Nenhuma inscrição por aqui ainda. Elas aparecem quando a regra do crm1 dispara.');
    }

    // =======================================================================
    // Aba: Visão geral
    // =======================================================================
    const visao = {
        async atualizar({ abriu, lento }) {
            desenharModo();
            desenharDisjuntor();
            const pedidos = [chamar('orq/fila?limite=8')];
            if (abriu || lento || !st.config) pedidos.push(chamar('orq/config'));
            const [fila, cfg] = await Promise.all(pedidos);
            if (cfg && cfg.ok) st.config = cfg.config;
            const hh = st.saude || {};
            const ordem = (st.config && st.config.motores && st.config.motores.ordem) || ['api', 'browser'];
            const habilitados = (st.config && st.config.motores && st.config.motores.habilitados) || ordem;
            if (mudou('ov-motores', [ordem, habilitados, hh.motores, hh.engines, st.config && st.config.motores && st.config.motores.fallback, st.papel])) {
                $('insa-ov-ordem').textContent = `Ordem atual: ${ordem.map((n) => NOME_MOTOR[n] || n).join(' e depois ')}${st.config && !st.config.motores.fallback ? ', sem contingência' : ''}.`;
                $('insa-ov-motores').innerHTML = ordem.map((n, i) => cardMotor(n, i, habilitados.includes(n), (hh.motores || {})[n])).join('');
            }
            const s = hh.fila || {};
            const ps = s.porStatus || {};
            $('insa-ov-ritmo').textContent = `${s.concorrencia ? `${s.concorrencia} inscrição${s.concorrencia > 1 ? 'ões' : ''} por vez` : ''}${s.mediaMs ? `, média de ${fmt.duracao(s.mediaMs)} por inscrição` : ''}`;
            const kpis = [
                { title: 'Aguardando', value: s.naFila != null ? s.naFila : '—', subtitle: s.etaMs ? `termina em ~${fmt.duracao(s.etaMs)}` : '', variant: 'primary' },
                { title: 'Executando', value: s.rodando != null ? s.rodando : '—' },
                { title: 'Inscritos', value: ps.sucesso || 0 },
                { title: 'Falharam ou em revisão', value: (ps.falhou || 0) + (ps.revisao || 0) },
                { title: 'Processo manual', value: ps.manual || 0 },
            ];
            if (mudou('ov-kpis', kpis)) $('insa-ov-kpis').innerHTML = kpis.map((k) => kpiCard(k)).join('');
            if (!fila.ok) return setStatus('insa-ov-status', erroDe(fila), 'erro');
            if (mudou('ov-jobs', [fila.jobs, st.papel])) $('insa-ov-jobs').innerHTML = tabelaJobs(fila.jobs, { compacta: true });
        },
    };
    function cardMotor(nome, i, habilitado, s) {
        s = s || {};
        const fora = s.available === false;
        // motor habilitado na configuração mas não carregado neste serviço (ex.: ORQ_ENGINES=api)
        const carregados = st.saude && Array.isArray(st.saude.engines) ? st.saude.engines : null;
        const ausente = carregados && !carregados.includes(nome);
        const tag = !habilitado ? pill('neutro', 'Desabilitado') : ausente ? pill('neutro', 'Não carregado no serviço') : fora ? pill('erro', 'Fora de rotação') : pill('ok', 'Disponível');
        const ultimo = s.lastSuccessAt ? fmt.ha(new Date(s.lastSuccessAt).toISOString()) : 'nunca';
        let html = `<article class="glass-card p-4">
            <div class="insa-cab"><div><strong class="text-[var(--text-primary)]">${h(NOME_MOTOR[nome] || nome)}</strong>
                ${sub(!habilitado ? 'Desabilitado na configuração' : ausente ? 'O serviço subiu sem este motor (ORQ_ENGINES)' : i === 0 ? 'Executa primeiro' : 'Contingência')}</div>${tag}</div>
            <dl class="insa-kv">
                <dt>Acerto recente</dt><dd class="num">${h(s.runs ? `${Math.round((s.score || 0) * 100)}% em ${s.runs} execuções` : 'sem execuções ainda')}</dd>
                <dt>Falhas técnicas seguidas</dt><dd class="num">${h(s.consecutiveFails || 0)}</dd>
                <dt>Último sucesso</dt><dd>${h(ultimo)}</dd>
            </dl>`;
        if (fora) {
            html += `<div class="insa-row mt-3"><small class="insa-muted">Saiu após falhas seguidas; volta sozinho depois do intervalo.</small>
                ${ehAdmin() ? botao('Recolocar em rotação', { acao: 'motor-reset', dados: { motor: nome } }) : ''}</div>`;
        }
        return html + '</article>';
    }

    // =======================================================================
    // Aba: Fila (+ devolvidos ao crm1)
    // =======================================================================
    const fila = {
        filtro: '',
        jobs: [],
        retornoAtivo: false,
        desenharFiltros() {
            $('insa-fl-filtros').innerHTML = FILTROS_FILA.map((f) => `<button type="button" class="${f.valor === this.filtro ? dsSegActive() : dsSegInactive()}" data-acao="fl-filtro" data-valor="${h(f.valor)}" aria-pressed="${f.valor === this.filtro}">${h(f.nome)}</button>`).join('');
        },
        async atualizar() {
            if (!$('insa-fl-filtros').children.length) this.desenharFiltros();
            const pedido = this.filtro;
            const [f, r] = await Promise.all([chamar(`orq/fila?limite=300${pedido ? `&status=${encodeURIComponent(pedido)}` : ''}`), chamar('orq/crm1/retorno')]);
            if (pedido !== this.filtro) return; // o filtro mudou enquanto a resposta vinha
            if (!f.ok) setStatus('insa-fl-status', erroDe(f), 'erro');
            else {
                this.jobs = f.jobs || [];
                this.retornoAtivo = !!(r.ok && r.retorno && r.retorno.ativo);
                if (mudou('fl-jobs', [pedido, f.jobs, st.papel, this.retornoAtivo])) $('insa-fl-jobs').innerHTML = tabelaJobs(this.jobs, { acoes: true });
            }
            const ret = $('insa-ret');
            ret.classList.toggle('hidden', !r.ok || !r.retorno);
            if (r.ok && r.retorno && mudou('fl-ret', [r.retorno, st.papel])) desenharRetorno(r.retorno);
        },
    };
    function desenharRetorno(d) {
        const c = d.contagem || {};
        const problemas = (c.falhou || 0) + (c.pulado || 0);
        $('insa-ret-acao').innerHTML = problemas > 0 && ehAdmin() ? botao(`Reenviar ${problemas} não entregue(s)`, { acao: 'ret-reenviar', classe: 'btn-primary' }) : '';
        const avisos = [];
        if (!d.ativo) avisos.push(alerta('atencao', 'O retorno está desligado: os negócios ficam só neste painel. Ligue em Configuração → Retorno ao crm1.'));
        if (d.ativo && !d.crm1Configurado) avisos.push(alerta('erro', 'Retorno ligado, mas falta o endereço ou o token do crm1: nada chega ao crm1.'));
        if (problemas > 0) avisos.push(alerta('erro', h(`${problemas} negócio(s) não chegaram ao crm1. Até reenviar, devolva-os ao humano por fora.`)));
        $('insa-ret-avisos').innerHTML = avisos.join('');
        const linhas = (d.itens || []).map((i) => {
            const des = DESFECHO[i.desfecho] || { rotulo: i.desfecho, tom: 'neutro' };
            const ent = ENTREGA[i.estado] || { rotulo: i.estado, tom: 'neutro' };
            return `<tr>
                <td class="num">${h(fmt.quando(i.ultimaEm || i.criadoEm))}${sub(i.jobId || '')}</td>
                <td class="num insa-mono">${h(i.dealId || '—')}</td>
                <td>${pill(des.tom, des.rotulo)}</td>
                <td>${pill(ent.tom, ent.rotulo)}${i.tentativas > 1 ? sub(`${i.tentativas} tentativas`) : ''}</td>
                <td class="insa-muted">${h(i.motivo || (i.etapaId ? `etapa ${i.etapaId}` : 'sem mudar de etapa'))}</td>
            </tr>`;
        });
        $('insa-ret-tabela').innerHTML = tabela(['Quando', 'Negócio no crm1', 'Resultado', 'Entrega', 'Detalhe'], linhas, 'Nenhum negócio devolvido ainda.');
    }
    function acaoJob(acao, id) {
        const job = fila.jobs.find((j) => j.id === id) || { id, resumo: {} };
        if (acao === 'devolver') {
            return confirmar({
                titulo: `Devolver ${id} ao crm1?`,
                texto: 'O resultado desta inscrição é enviado ao negócio no crm1 (campos, nota, tag e etapa, conforme a configuração do retorno). Se já foi devolvido antes, nada é reenviado.',
                acao: 'Devolver ao crm1',
                async executar() {
                    const r = await chamar(`orq/crm1/retorno/devolver/${id}`, { method: 'POST' });
                    if (!r.ok) return erroDe(r);
                    setStatus('insa-fl-status', r.jaDevolvido ? `${id} já tinha sido devolvido ao crm1 (nada a fazer).` : `${id} enviado para devolução ao crm1.`, 'ok');
                    fila.atualizar();
                },
            });
        }
        const revisao = acao === 'reprocessar' && job.status === 'revisao';
        confirmar({
            titulo: acao === 'cancelar' ? `Cancelar ${id}?` : `Inscrever ${(job.resumo && job.resumo.primeiroNome) || ''} de novo?`,
            texto: acao === 'cancelar'
                ? 'A inscrição sai da fila e não será feita automaticamente.'
                : revisao
                    ? 'Esta inscrição pode já ter gerado pedido na VTEX. Confira no SIAA antes: inscrever de novo pode duplicar a inscrição do candidato.'
                    : 'A inscrição volta para o fim da fila e roda com a configuração atual.',
            alertaTexto: revisao,
            acao: acao === 'cancelar' ? 'Cancelar inscrição' : revisao ? 'Já conferi, inscrever de novo' : 'Inscrever de novo',
            perigo: revisao || acao === 'cancelar',
            async executar() {
                const r = await chamar(`orq/fila/${id}/${acao}${revisao ? '?force=true' : ''}`, { method: 'POST' });
                if (!r.ok) return erroDe(r);
                setStatus('insa-fl-status', acao === 'cancelar' ? `${id} cancelada.` : `${id} voltou para a fila.`, 'ok');
                fila.atualizar();
            },
        });
    }

    // =======================================================================
    // Aba: Nova inscrição (ui-server: status, catálogo, campanha, inscricoes)
    // =======================================================================
    const nova = {
        carregado: false,
        status: null,
        cursos: [],
        polos: [],
        campanha: null,
        modoClicado: 'validar',
        jobAtual: null,
        poll: null,
        async atualizar({ abriu, lento }) {
            if (!this.carregado) {
                this.carregado = true;
                const [s, cs, ps, cp] = await Promise.all([chamar('status'), chamar('catalogo/cursos'), chamar('catalogo/polos'), chamar('campanha')]);
                if (!s.ok) { this.carregado = false; return setStatus('insa-nv-status', erroDe(s), 'erro'); }
                this.status = s;
                this.cursos = cs.cursos || [];
                this.polos = ps.polos || [];
                this.campanha = cp.ok ? cp : null;
                montarFormulario();
                this.historico();
                return;
            }
            if (abriu) {
                const [s, cp] = await Promise.all([chamar('status'), chamar('campanha')]);
                if (s.ok) { this.status = s; aplicarModos(); }
                if (cp.ok) { this.campanha = cp; if (!$('insa-nv-cupom').dataset.editado) aplicarForma(); }
            }
            if (abriu || lento) this.historico();
        },
        async historico() {
            const j = await chamar('inscricoes');
            const lista = j.inscricoes || [];
            const ok = lista.filter((x) => x.resultado && x.resultado.ok).length;
            const erro = lista.filter((x) => x.status === 'concluido' && !(x.resultado && x.resultado.ok)).length;
            $('insa-nv-hist-resumo').textContent = lista.length ? `${lista.length} execução(ões) · ${ok} ok · ${erro} com problema (desde o último reinício do serviço)` : 'desde o último reinício do serviço';
            if (!mudou('nv-hist', lista)) return;
            const linhas = lista.map((x) => {
                const res = x.resultado || {};
                const resultado = x.status === 'executando'
                    ? pill('ativo', 'executando…')
                    : `${pill(res.ok ? 'ok' : 'erro', res.status || '?')}${res.orderId ? sub(res.orderId) : ''}${res.siaa ? sub(`SIAA ${res.siaa}`) : ''}`;
                return `<tr>
                    <td class="num">${h(new Date(x.criadoEm).toLocaleString('pt-BR', { day: '2-digit', month: '2-digit', hour: '2-digit', minute: '2-digit' }))}</td>
                    <td>${h(`${x.modo} · ${x.motor || 'http'}`)}</td>
                    <td>${h(x.nome)}</td>
                    <td>${h(x.curso)}${sub(x.polo)}</td>
                    <td class="insa-mono">${h(x.cupom || '—')}</td>
                    <td>${resultado}</td>
                    <td class="acoes">${botao('Ver', { acao: 'nv-ver', dados: { id: x.id } })}</td>
                </tr>`;
            });
            $('insa-nv-hist').innerHTML = tabela(['Quando', 'Modo · motor', 'Candidato', 'Curso · polo', 'Cupom', 'Resultado', ''], linhas, 'Nenhuma ainda.');
        },
    };

    const formaAtual = () => ((nova.status && nova.status.formas) || []).find((f) => f.id === $('insa-nv-forma').value) || null;
    const ehPos = (f) => f && f.departamento === 'Pós-Graduação';
    const cursosDaForma = (f) => nova.cursos.filter((c) => (ehPos(f) ? c.nivel === 'pos' : c.nivel !== 'pos' && (!f || (c.formas || []).includes(f.formaIngresso))));
    function cursoEscolhido() {
        const nome = $('insa-nv-curso').value.trim().toLowerCase();
        return cursosDaForma(formaAtual()).find((c) => c.nome.toLowerCase() === nome) || null;
    }

    function montarFormulario() {
        const s = nova.status;
        $('insa-nv-polo').innerHTML = `<option value="">Selecione o polo</option><option value="proximo">Polo mais próximo (pelo CEP)</option>`
            + nova.polos.map((p) => `<option value="${h(p.id)}">${h(`${p.nome} — ${p.cidade || ''} (${p.id})`)}</option>`).join('');
        $('insa-nv-forma').innerHTML = (s.formas || []).map((f) => `<option value="${h(f.id)}">${h(f.label)}</option>`).join('');
        aplicarModos();
        aplicarForma();
    }

    function aplicarModos() {
        const s = nova.status || {};
        const m = s.modos || {};
        const loja = s.loja || { producao: true };
        const partes = [];
        if (!loja.producao) partes.push(`Loja ${loja.conta || '?'} (não é a produção)`);
        partes.push(m.real ? 'Inscrição REAL ligada no serviço' : m.ensaio ? 'Ensaio ligado · real desligado' : 'Só validação (seguro): ensaio e real estão desligados no serviço');
        const tom = m.real ? 'erro' : m.ensaio || !loja.producao ? 'atencao' : 'info';
        let texto = `<strong>${h(partes.join(' · '))}.</strong>`;
        if (m.real && !ehAdmin()) texto += ' A inscrição real é só de quem administra; você pode validar e ensaiar.';
        if (s.operador === false) texto += ' Falta cadastrar o atendente (aba Cadastros).';
        $('insa-nv-modos').innerHTML = alerta(tom, texto);
        $('insa-nv-ocupado').textContent = s.ocupado ? 'Há uma inscrição em execução no serviço.' : '';
        const mt = s.motores || {};
        for (const o of $('insa-nv-motor').options) {
            o.disabled = mt[o.value] === false;
            if (o.disabled && !o.textContent.includes('indisponível')) o.textContent += ' — indisponível neste servidor';
        }
        const bReal = $('insa-nv-btn-real');
        bReal.classList.toggle('hidden', !(ehAdmin() && m.real));
        travar(!!nova.jobAtual);
    }

    function travar(sim) {
        const m = (nova.status && nova.status.modos) || {};
        for (const b of raiz().querySelectorAll('#insa-nv-form [data-modo]')) {
            const modo = b.dataset.modo;
            b.disabled = sim || (modo === 'ensaio' && !m.ensaio) || (modo === 'real' && (!m.real || !ehAdmin()));
            if (modo === 'ensaio') b.title = m.ensaio ? '' : 'Ensaio desligado no serviço (UI_PERMITIR_ENSAIO=1)';
        }
    }

    // forma de ingresso: cursos, campos extras, cupom e motor de cada forma
    function aplicarForma() {
        const f = formaAtual();
        const lista = cursosDaForma(f);
        $('insa-nv-cursos').innerHTML = lista.map((c) => `<option value="${h(c.nome)}"></option>`).join('');
        if ($('insa-nv-curso').value && !cursoEscolhido()) $('insa-nv-curso').value = '';
        $('insa-nv-curso').placeholder = `Digite para buscar (${lista.length} cursos)`;

        const campos = ((f && f.camposExtras) || []).filter((c) => c.tipo !== 'fixo');
        const box = $('insa-nv-extras');
        box.innerHTML = campos.map((c) => `<label class="insa-field"><span>${h(c.label + (c.obrigatorio ? '' : ' (opcional)'))}</span>
            <input id="insa-nv-extra-${h(c.key)}" data-extra="${h(c.key)}" inputmode="decimal" autocomplete="off" placeholder="${h(c.tipo === 'ano' ? 'AAAA' : c.tipo === 'nota' ? '0 a 1000' : '')}" class="input-glass px-3 py-2 text-sm">
            <small class="insa-erro" data-erro="extras.${h(c.key)}"></small></label>`).join('');
        box.classList.toggle('hidden', !campos.length);

        const aceita = f ? f.cupom : true;
        const cupom = $('insa-nv-cupom');
        const camp = nova.campanha || {};
        cupom.disabled = !aceita;
        $('insa-nv-cupom-validar').disabled = !aceita;
        if (!aceita) cupom.value = '';
        else if (!cupom.value && !cupom.dataset.editado) cupom.value = camp.cupom || '';
        cupom.placeholder = aceita ? (camp.obrigatorio ? 'obrigatório' : 'opcional') : 'não se aplica a esta forma';

        // o robô Playwright só cobre a graduação
        const mt = (nova.status && nova.status.motores) || {};
        for (const o of $('insa-nv-motor').options) o.disabled = (ehPos(f) && o.value !== 'http') || mt[o.value] === false;
        if ($('insa-nv-motor').selectedOptions[0] && $('insa-nv-motor').selectedOptions[0].disabled) $('insa-nv-motor').value = 'http';

        const e = f && f.ensaio;
        setStatus('insa-nv-forma-info', f ? `${f.label}: ${aceita ? 'com cupom' : 'sem cupom'}${ehPos(f) ? ' · só motor HTTP' : ''}${e ? ` · ${e.ok ? 'ensaiada' : 'não ensaiada'} em ${e.em}` : ''}` : '', 'info');
    }

    const MASCARAS = {
        'insa-nv-cpf': (d) => d.slice(0, 11).replace(/(\d{3})(\d)/, '$1.$2').replace(/(\d{3})\.(\d{3})(\d)/, '$1.$2.$3').replace(/\.(\d{3})(\d{1,2})$/, '.$1-$2'),
        'insa-nv-cep': (d) => d.slice(0, 8).replace(/(\d{5})(\d)/, '$1-$2'),
        'insa-pl-cep': (d) => d.slice(0, 8).replace(/(\d{5})(\d)/, '$1-$2'),
        'insa-nv-nascimento': (d) => d.slice(0, 8).replace(/(\d{2})(\d)/, '$1/$2').replace(/(\d{2})\/(\d{2})(\d)/, '$1/$2/$3'),
        'insa-nv-telefone': (d) => d.slice(0, 11),
    };

    function extrasDoFormulario() {
        const x = {};
        for (const i of raiz().querySelectorAll('[data-extra]')) if (i.value.trim()) x[i.dataset.extra] = i.value.trim().replace(',', '.');
        return x;
    }
    function limparErros() {
        for (const s of raiz().querySelectorAll('#insa-nv-form [data-erro]')) s.textContent = '';
        for (const i of raiz().querySelectorAll('#insa-nv-form [aria-invalid]')) i.removeAttribute('aria-invalid');
    }
    const CAMPO_DO_ERRO = { skuId: 'curso', poleId: 'polo' };
    function mostrarErros(erros) {
        for (const [k, msg] of Object.entries(erros || {})) {
            const s = raiz().querySelector(`#insa-nv-form [data-erro="${CSS.escape(k)}"]`);
            if (s) s.textContent = msg;
            const id = k.startsWith('extras.') ? `insa-nv-extra-${k.slice(7)}` : `insa-nv-${CAMPO_DO_ERRO[k] || k}`;
            const campo = $(id);
            if (campo) campo.setAttribute('aria-invalid', 'true');
        }
    }

    async function validarCupom(cupom, alvo) {
        if (!cupom) { setStatus(alvo, 'Informe um cupom.', 'erro'); return false; }
        setStatus(alvo, 'Validando na loja…', 'info');
        const c = cursoEscolhido();
        const r = await chamar('cupom/validar', { method: 'POST', corpo: { cupom, skuId: c ? c.skuId : undefined, poleId: $('insa-nv-polo').value, forma: $('insa-nv-forma').value } });
        if (r.valido) setStatus(alvo, `Cupom válido — ${r.resumo || ''} (conferido em ${(r.conferidoEm && r.conferidoEm.curso) || '?'} · ${(r.conferidoEm && r.conferidoEm.polo) || '?'})`, 'ok');
        else setStatus(alvo, `Cupom recusado: ${r.erro || 'sem resposta'}`, 'erro');
        return !!r.valido;
    }

    async function enviarInscricao(modo) {
        limparErros();
        const curso = cursoEscolhido();
        const body = {
            modo,
            skuId: curso ? curso.skuId : undefined,
            poleId: $('insa-nv-polo').value,
            forma: $('insa-nv-forma').value,
            nome: $('insa-nv-nome').value,
            email: $('insa-nv-email').value,
            telefone: $('insa-nv-telefone').value,
            cpf: $('insa-nv-cpf').value,
            nascimento: $('insa-nv-nascimento').value,
            cep: $('insa-nv-cep').value,
            cupom: $('insa-nv-cupom').value.trim(),
            motor: $('insa-nv-motor').value,
            extras: extrasDoFormulario(),
        };
        setStatus('insa-nv-status', modo === 'validar' ? 'Validando…' : 'Enviando…', 'info');
        travar(true);
        let iniciou = false;
        try {
            const r = await chamar('inscricoes', { method: 'POST', corpo: body });
            if (r.http === 422) {
                mostrarErros(r.erros);
                return setStatus('insa-nv-status', 'Corrija os campos destacados.', 'erro');
            }
            if (!r.ok) return setStatus('insa-nv-status', erroDe(r), 'erro');
            if (modo === 'validar') {
                mostrarChecks(r.checks || [], curso, body);
                const falhas = (r.checks || []).filter((c) => !c.ok).length;
                return setStatus('insa-nv-status', falhas ? `${falhas} verificação(ões) com problema.` : 'Tudo certo para inscrever.', falhas ? 'erro' : 'ok');
            }
            iniciou = true;
            setStatus('insa-nv-status', `Execução iniciada (${modo}). Acompanhe abaixo — leva cerca de 2 min.`, 'info');
            acompanhar(r.id);
        } finally {
            if (!iniciou) travar(false);
        }
    }

    function abrirExecucao(titulo) {
        $('insa-nv-exec').classList.remove('hidden');
        $('insa-nv-exec-titulo').textContent = titulo;
        $('insa-nv-checks').innerHTML = '';
        $('insa-nv-etapas').innerHTML = '';
        $('insa-nv-eventos').innerHTML = '';
        $('insa-nv-eventos-box').classList.add('hidden');
        $('insa-nv-resultado').innerHTML = '';
    }
    function mostrarChecks(checks, curso, body) {
        abrirExecucao(`Validação · ${body.nome} · ${(curso && curso.nome) || '?'}`);
        $('insa-nv-checks').innerHTML = checks.map((c) => `<li class="${c.ok ? 'ok' : 'err'}"><strong>${c.ok ? '✓' : '✗'} ${h(c.nome)}</strong>${sub(c.detalhe || '')}</li>`).join('');
        $('insa-nv-exec').scrollIntoView({ behavior: 'smooth', block: 'start' });
    }

    function acompanhar(id, { soVer = false } = {}) {
        clearInterval(nova.poll);
        nova.poll = null;
        if (!soVer) nova.jobAtual = id;
        abrirExecucao(soVer ? 'Carregando…' : 'Executando…');
        const tick = async () => {
            const j = await chamar(`inscricoes/${id}`);
            if (!j.ok) {
                if (j.http === 404) { pararAcompanhamento(); travar(false); setStatus('insa-nv-status', 'Execução não encontrada (o serviço pode ter reiniciado).', 'erro'); }
                return;
            }
            $('insa-nv-exec-titulo').textContent = `${String(j.modo || '').toUpperCase()} · motor ${j.motor || 'http'} · ${j.nome || ''} · ${j.forma ? j.forma + ' · ' : ''}${j.curso || ''} · ${j.polo || ''}${j.cupom ? ` · cupom ${j.cupom}` : ''}`;
            const etapas = j.etapas || [];
            $('insa-nv-etapas').innerHTML = etapas.map((s, i) => {
                const ultimo = i === etapas.length - 1 && j.status === 'executando';
                const cls = s.status == null ? (ultimo ? 'atual' : '') : s.status < 400 ? 'ok' : 'err';
                return `<li class="${cls}">${h(`${rotuloEtapa(s.nome)}${s.status ? ` · ${s.status}` : ultimo ? ' …' : ''}`)}</li>`;
            }).join('');
            const ev = j.eventos || [];
            if (ev.length) {
                $('insa-nv-eventos-box').classList.remove('hidden');
                $('insa-nv-eventos').innerHTML = ev.slice(-80).map((e) => `<li><time>${h(new Date(e.em).toLocaleTimeString('pt-BR'))}</time><div>${h(e.texto)}</div></li>`).join('');
            }
            if (j.status === 'concluido') {
                clearInterval(nova.poll);
                nova.poll = null;
                if (!soVer) nova.jobAtual = null;
                mostrarResultado(j.resultado || {});
                if (!soVer) {
                    travar(false);
                    setStatus('insa-nv-status', j.resultado && j.resultado.ok ? 'Concluído.' : 'Terminou com problema — veja o resultado.', j.resultado && j.resultado.ok ? 'ok' : 'erro');
                    nova.historico();
                }
            }
        };
        tick();
        nova.poll = setInterval(() => { if (paginaNaFrente()) tick(); }, 2500);
    }
    function pararAcompanhamento() {
        clearInterval(nova.poll);
        nova.poll = null;
        nova.jobAtual = null;
    }
    function mostrarResultado(r) {
        const link = linkSeguro(r.linkAluno);
        const linhas = [
            ['Resultado', r.ok ? (r.status === 'ENSAIO_OK_ATE_PEDIDO' ? 'Ensaio OK (parou antes do pedido)' : 'Inscrição feita e com nº SIAA') : r.status],
            ['Motor', r.motor ? `${r.motor}${r.fallbackDe ? ` (fallback: HTTP falhou com ${r.fallbackDe.status})` : ''}` : null],
            ['Tentativas', r.tentativas && r.tentativas.length > 1 ? r.tentativas.map((t) => `${t.motor}: ${t.tipo} (${t.status})`).join(' → ') : null],
            ['Pedido', r.orderId],
            ['Nº inscrição (tela)', r.numeroInscricao],
            ['Link da prova', r.linkProva],
            ['Nº no lead (VTEX)', r.siaa],
            ['Polo gravado', r.polo_gravado],
            ['Cupom', r.cupom],
            ['Afiliado', r.afiliado === undefined ? null : r.afiliado ? 'indicado (BeeViral)' : r.afiliadoErro ? `falhou: ${r.afiliadoErro}` : 'não sorteado'],
            ['Trava pós-pedido', r.item ? `item ${r.item} · operador ${r.operador ? 'sim' : 'NÃO'}` : null],
            ['Erro', r.erro],
            ['Tempo', r.s != null ? `${r.s}s` : null],
        ].filter(([, v]) => v != null && v !== '');
        let html = `<dl class="insa-kv mt-3">${linhas.map(([k, v]) => `<dt>${h(k)}</dt><dd>${h(v)}</dd>`).join('')}`;
        if (link) html += `<dt>${h(TIPO_LINK[r.linkTipo] || 'Link para o aluno')}</dt><dd><a href="${h(link)}" target="_blank" rel="noopener noreferrer" class="text-[var(--primary)] underline break-all">${h(link)}</a></dd>`;
        html += '</dl>';
        $('insa-nv-resultado').innerHTML = alerta(r.ok ? 'ok' : 'erro', `<strong>${r.ok ? 'Concluído' : 'Terminou com problema'}</strong>`) + html;
    }

    // =======================================================================
    // Aba: Cadastros (campanha, atendente, polos)
    // =======================================================================
    const cadastros = {
        catalogoPolos: [],
        async atualizar({ abriu }) {
            if (!abriu) return;
            await Promise.all([carregarCampanha(), carregarAtendentes(), carregarPolos()]);
        },
    };
    async function carregarCampanha() {
        const c = await chamar('campanha');
        if (!c.ok) return setStatus('insa-cp-status', erroDe(c), 'erro');
        nova.campanha = c;
        $('insa-cp-cupom').value = c.cupom || '';
        $('insa-cp-obrigatorio').checked = !!c.obrigatorio || !c.cupom;
        const quando = c.atualizadoEm ? new Date(c.atualizadoEm).toLocaleString('pt-BR') : '—';
        $('insa-cp-origem').textContent = `${c.descricao ? c.descricao + ' · ' : ''}fonte: ${c.origem || '—'} · atualizado ${quando}${c.atualizadoPor ? ` por ${c.atualizadoPor}` : ''}`;
        $('insa-cp-cupom').disabled = !ehAdmin();
        $('insa-cp-obrigatorio').disabled = !ehAdmin();
    }
    async function salvarCampanha() {
        const cupom = $('insa-cp-cupom').value.trim();
        if (cupom && !(await validarCupom(cupom, 'insa-cp-status'))) return;
        const atual = nova.campanha || {};
        const r = await chamar('campanha', { method: 'PUT', corpo: { cupom, obrigatorio: $('insa-cp-obrigatorio').checked, descricao: atual.cupom === cupom ? atual.descricao : '' } });
        if (!r.ok) return setStatus('insa-cp-status', erroDe(r), 'erro');
        nova.campanha = r;
        delete $('insa-nv-cupom').dataset.editado;
        await carregarCampanha();
        if (nova.carregado) aplicarForma();
        setStatus('insa-cp-status', cupom ? `Campanha salva: ${cupom}${r.obrigatorio ? ' (obrigatório)' : ''}. Vale para a inscrição avulsa, o lote e o Playwright.` : 'Campanha salva sem cupom.', 'ok');
    }

    async function carregarAtendentes() {
        const r = await chamar('atendente');
        if (!r.ok) return setStatus('insa-at-status', erroDe(r), 'erro');
        mostrarAtendentes(r.atendentes || []);
    }
    function mostrarAtendentes(lista) {
        const ul = $('insa-at-lista');
        if (!lista.length) {
            ul.innerHTML = '<li class="insa-muted">Nenhum atendente cadastrado.</li>';
            return;
        }
        ul.innerHTML = lista.map((a, i) => `<li>
            <span class="mail">${h(a.email)}</span>
            <span class="insa-muted insa-mono">${h(a.senha)}</span>
            ${a.principal ? pill('ativo', 'Principal') : ''}
            ${ehAdmin() ? `<span class="ml-auto">${botao('Remover', { acao: 'at-remover', dados: { indice: i, email: a.email } })}</span>` : ''}
        </li>`).join('');
    }
    async function salvarAtendente() {
        const r = await chamar('atendente', { method: 'PUT', corpo: { email: $('insa-at-email').value, senha: $('insa-at-senha').value } });
        $('insa-at-senha').value = '';
        $('insa-at-senha').type = 'password';
        if (!r.ok) return setStatus('insa-at-status', erroDe(r), 'erro');
        $('insa-at-email').value = '';
        mostrarAtendentes(r.atendentes || []);
        setStatus('insa-at-status', 'Atendente salvo como principal. Use "Testar login" para conferir.', 'ok');
    }
    function removerAtendente(indice, mascarado) {
        confirmar({
            titulo: 'Remover atendente?',
            texto: `Remover o atendente ${mascarado}? As inscrições usam o atendente principal.`,
            acao: 'Remover',
            perigo: true,
            async executar() {
                const r = await chamar('atendente', { method: 'DELETE', corpo: { indice: Number(indice) } });
                if (!r.ok) return erroDe(r);
                mostrarAtendentes(r.atendentes || []);
                setStatus('insa-at-status', 'Atendente removido.', 'ok');
            },
        });
    }
    async function testarAtendente(btn) {
        btn.disabled = true;
        setStatus('insa-at-status', 'Fazendo login com o robô (pode levar até 1 min)…', 'info');
        try {
            const r = await chamar('atendente/testar', { method: 'POST' });
            if (r.ok) setStatus('insa-at-status', `Login OK — sessão de atendente válida até ${r.expiraEm ? new Date(r.expiraEm).toLocaleString('pt-BR') : '?'}.`, 'ok');
            else setStatus('insa-at-status', `Login falhou: ${erroDe(r)}${r.detalhe && r.detalhe.length ? ' — ' + r.detalhe.join(' · ') : ''}`, 'erro');
        } finally {
            btn.disabled = false;
        }
    }

    function linhaPolo(p) {
        const dis = ehAdmin() ? '' : ' disabled';
        const inp = (v, extra = '') => `<input class="input-glass" value="${h(v == null ? '' : v)}" autocomplete="off" spellcheck="false"${extra}${dis}>`;
        return `<tr class="${p.aceitaInscricao === false ? 'sem-cota' : ''}" data-nome-catalogo="${h(p.nomeCatalogo || '')}" data-endereco="${h(p.endereco || '')}">
            <td>${inp(p.poleId, ' inputmode="numeric" aria-label="Código do polo"')}</td>
            <td>${inp(p.nome, ' aria-label="Nome do polo"')}</td>
            <td>${inp((p.apelidos || []).join(', '), ' aria-label="Nomes no CRM"')}</td>
            <td>${inp(p.latitude, ' inputmode="decimal" placeholder="-23.5" aria-label="Latitude"')}</td>
            <td>${inp(p.longitude, ' inputmode="decimal" placeholder="-46.6" aria-label="Longitude"')}</td>
            <td><input type="checkbox" data-aceita${p.aceitaInscricao !== false ? ' checked' : ''} title="Aceita inscrição" aria-label="Aceita inscrição"${dis}></td>
            <td>${inp(p.observacao, ' aria-label="Observação"')}</td>
            <td class="acoes">${ehAdmin() ? botao('×', { acao: 'pl-remover', titulo: 'Remover' }) : ''}</td>
        </tr>`;
    }
    function lerPolos() {
        return [...$('insa-pl-lista').querySelectorAll('tr')].filter((tr) => tr.querySelectorAll('input').length).map((tr) => {
            const [cod, nome, apelidos, lat, lng, aceita, obs] = tr.querySelectorAll('input');
            return {
                poleId: cod.value.trim(), nome: nome.value, apelidos: apelidos.value,
                latitude: lat.value.trim().replace(',', '.'), longitude: lng.value.trim().replace(',', '.'),
                aceitaInscricao: aceita.checked, observacao: obs.value,
                nomeCatalogo: tr.dataset.nomeCatalogo, endereco: tr.dataset.endereco,
            };
        });
    }
    async function carregarPolos() {
        const r = await chamar('polos');
        if (!r.ok) return setStatus('insa-pl-status', erroDe(r), 'erro');
        cadastros.catalogoPolos = r.catalogo || [];
        const polos = r.polos || [];
        $('insa-pl-lista').innerHTML = polos.length ? polos.map(linhaPolo).join('') : '<tr><td colspan="8" class="vazio">Nenhum polo cadastrado.</td></tr>';
        $('insa-pl-novo').innerHTML = '<option value="">Polo do catálogo…</option>' + cadastros.catalogoPolos.map((c) => `<option value="${h(c.id)}">${h(`${c.nome} — ${c.cidade || ''} (${c.id})`)}</option>`).join('');
        $('insa-pl-google').textContent = r.google ? `chave ${r.google} (Google)` : 'sem chave (usa a VTEX)';
        const quando = r.atualizadoEm ? new Date(r.atualizadoEm).toLocaleString('pt-BR') : '—';
        setStatus('insa-pl-status', `${polos.length} polos · ${polos.filter((p) => p.aceitaInscricao === false).length} sem cota · alterado em ${quando} (${r.atualizadoPor || '—'})`, 'info');
    }

    // =======================================================================
    // Aba: Inscrições × SIAA
    // =======================================================================
    const siaa = {
        f: { de: diasAtrasSP(7), ate: hojeSP(), polo: '', status: '', conferencia: '' },
        sessao: {},
        conferindo: false,
        espera: null,
        qs() { return new URLSearchParams(Object.entries(this.f).filter(([, v]) => v)).toString(); },
        montarFiltros() {
            const form = $('insa-rel-filtros');
            if (form.dataset.montado) return;
            form.dataset.montado = '1';
            const opcoes = (mapa) => '<option value="">Todas</option>' + Object.entries(mapa).map(([v, x]) => `<option value="${h(v)}">${h(x.rotulo)}</option>`).join('');
            $('insa-rel-f-status').innerHTML = opcoes(STATUS_JOB);
            $('insa-rel-f-conf').innerHTML = opcoes(STATUS_CONF);
            for (const c of form.querySelectorAll('[data-f]')) c.value = this.f[c.dataset.f] || '';
        },
        async atualizar({ abriu, lento }) {
            this.montarFiltros();
            if (!abriu && !lento) return; // a cada ~15 s
            const pedido = this.qs();
            const d = await chamar(`orq/relatorio/inscricoes?${pedido}`);
            if (pedido !== this.qs()) return;
            if (!d.ok) return setStatus('insa-rel-status', erroDe(d), 'erro');
            const s = this.sessao = d.sessaoSiaa || {};
            $('insa-ss-texto').textContent = !s.definida
                ? 'Para conferir, cole o cURL de uma pesquisa no SVREL28 (Rede, botão direito na requisição svrel28.jsf, Copiar como cURL).'
                : s.valida === false
                    ? `A sessão expirou (${s.motivo || 'o SIAA recusou'}). Cole um cURL novo.`
                    : `Sessão colada por ${s.por || '?'} ${fmt.ha(s.em)}${s.valida ? ', funcionando na última conferência' : ''}.`;
            $('insa-ss-abrir').textContent = s.definida ? 'Trocar sessão' : 'Colar sessão';
            const bConf = $('insa-rel-conferir');
            if (!this.conferindo) bConf.disabled = !s.definida;
            bConf.title = s.definida ? '' : 'Cole a sessão do SIAA primeiro';
            const tc = (d.totais && d.totais.conferencia) || {};
            const ts = (d.totais && d.totais.status) || {};
            const kpis = [
                { title: 'Inscritas pelo robô', value: ts.sucesso || 0, variant: 'primary' },
                { title: 'Confirmadas no SIAA', value: tc.confirmado || 0 },
                { title: 'Fora do SIAA', value: tc.ausente || 0 },
                { title: 'Com número diferente', value: tc.divergente || 0 },
                { title: 'A conferir', value: tc.pendente || 0 },
            ];
            if (mudou('rel-kpis', kpis)) $('insa-rel-kpis').innerHTML = kpis.map((k) => kpiCard(k)).join('');
            if (!mudou('rel-linhas', [pedido, d.linhas])) return;
            const linhas = (d.linhas || []).map((l) => {
                const s1 = STATUS_JOB[l.status] || { rotulo: l.status, tom: 'neutro' };
                const c = l.conferencia ? STATUS_CONF[l.conferencia] : null;
                let noSiaa = c ? pill(c.tom, c.rotulo) : '—';
                if (l.inscricaoSiaa && l.conferencia === 'divergente') noSiaa += sub(`SIAA: ${l.inscricaoSiaa}`);
                if (l.conferidoEm) noSiaa += sub(fmt.ha(l.conferidoEm));
                return `<tr>
                    <td class="num">${h(fmt.quando(l.criadoEm))}</td>
                    <td>${h(l.primeiroNome || '—')}${sub(l.cpf || '')}</td>
                    <td>${h(l.curso || '—')}${sub(l.polo || '')}</td>
                    <td>${pill(s1.tom, s1.rotulo)}</td>
                    <td>${h(l.motor ? NOME_MOTOR[l.motor] || l.motor : '—')}</td>
                    <td class="num">${h(l.inscricaoMotor || '—')}</td>
                    <td>${noSiaa}</td>
                </tr>`;
            });
            $('insa-rel-tabela').innerHTML = tabela(['Recebida', 'Candidato', 'Curso e polo', 'Situação', 'Motor', 'Nº do robô', 'No SIAA'], linhas, 'Nenhuma inscrição neste período.');
        },
    };
    async function conferirSiaa(btn) {
        siaa.conferindo = true;
        btn.disabled = true;
        const rotulo = btn.innerHTML;
        btn.textContent = 'Conferindo no SIAA…';
        setStatus('insa-rel-status', 'Conferindo polo a polo no SIAA…', 'info');
        try {
            const r = await chamar('orq/conferencia/siaa', { method: 'POST' });
            if (r.ok) {
                const c = r.contagem || {};
                setStatus('insa-rel-status', `Conferidas ${r.jobs} inscrições em ${(r.polos || []).length} polos: ${c.confirmado || 0} confirmadas, ${c.divergente || 0} com número diferente, ${c.ausente || 0} fora do SIAA.`, c.ausente || c.divergente ? 'erro' : 'ok');
            } else if (r.http === 504) {
                setStatus('insa-rel-status', `${erroDe(r)} A conferência continua no serviço; o resultado aparece na tabela quando terminar.`, 'info');
            } else setStatus('insa-rel-status', erroDe(r), 'erro');
        } finally {
            siaa.conferindo = false;
            btn.innerHTML = rotulo;
            btn.disabled = !siaa.sessao.definida;
            siaa.atualizar({ abriu: true });
        }
    }
    async function baixarCsv() {
        try {
            const r = await api(`${BASE}orq/relatorio/inscricoes.csv?${siaa.qs()}`, { headers: { Accept: 'text/csv' } });
            if (!r.ok) {
                let j = {};
                try { j = await r.json(); } catch (_) { /* sem corpo */ }
                return setStatus('insa-rel-status', j.erro || j.error || `Erro HTTP ${r.status}`, 'erro');
            }
            const href = URL.createObjectURL(await r.blob());
            const a = document.createElement('a');
            a.href = href;
            a.download = `inscricoes-siaa-${siaa.f.de || 'inicio'}-a-${siaa.f.ate || 'hoje'}.csv`;
            document.body.append(a);
            a.click();
            a.remove();
            setTimeout(() => URL.revokeObjectURL(href), 2000);
        } catch (e) {
            setStatus('insa-rel-status', `Não baixei: ${e.message}`, 'erro');
        }
    }

    // =======================================================================
    // Aba: Agente
    // =======================================================================
    const agente = {
        async atualizar() {
            const r = await chamar('orq/agente');
            if (!r.ok) {
                $('insa-ag-corpo').innerHTML = '';
                return setStatus('insa-ag-status', r.http === 404 ? 'O agente de manutenção não está ativo neste serviço.' : erroDe(r), r.http === 404 ? 'info' : 'erro');
            }
            const a = r.agente || {};
            $('insa-ag-verificar').disabled = $('insa-ag-corrigir').disabled = !!a.rodando;
            $('insa-ag-verificar').textContent = a.rodando ? 'Verificando…' : 'Verificar agora';
            $('insa-ag-explica').textContent = `O agente confere se a loja ou as telas mudaram, sem criar nada na VTEX nem no SIAA. Quando acha uma mudança, prepara a correção num branch separado e só entrega depois que os testes passam. ${a.abrirPr ? 'Ele abre um PR em rascunho; o merge é sempre de uma pessoa.' : 'Abertura de PR está desligada (AGENTE_ABRIR_PR); a correção fica num branch local.'}`;
            if (!mudou('agente', a)) return;
            const u = a.ultima;
            if (!u) { $('insa-ag-corpo').innerHTML = '<p class="insa-muted">Nenhuma verificação feita ainda.</p>'; return; }
            const achados = u.achados || [];
            let html = `<div class="insa-bloco"><div class="insa-cab"><h3 class="insa-h3">${h(`Última verificação, ${fmt.ha(u.em)}`)}</h3>
                ${u.ok ? pill('ok', 'Nada mudou') : pill('atencao', `${achados.length} possível(is) mudança(s)`)}</div>
                <div class="insa-row mb-3">${(u.checks || []).map((c) => pill(c.erroExecucao ? 'erro' : c.ok ? 'ok' : 'atencao', `${CHECK[c.nome] || c.nome}: ${c.erroExecucao ? 'não rodou' : c.ok ? 'ok' : `${c.achados} achado(s)`}`)).join(' ')}</div>`;
            if (achados.length) {
                html += `<div class="insa-table-wrap">${tabela(['O que mudou', 'Onde', 'Detalhe'], achados.map((x) => `<tr><td>${h(TIPO_ACHADO[x.tipo] || x.tipo)}</td><td class="num">${h(x.alvo || '—')}</td><td class="insa-muted">${h(detalheAchado(x))}</td></tr>`), '')}</div>`;
            }
            if (u.avisos && u.avisos.length) html += `<p class="insa-muted mt-2">${h(`Avisos: ${u.avisos.join('; ')}.`)}</p>`;
            if (u.correcao) {
                const c = u.correcao;
                if (c.ok) {
                    const pr = linkSeguro(c.pr);
                    const onde = pr ? `<a href="${h(pr)}" target="_blank" rel="noopener noreferrer" class="underline">no PR</a>` : h(`no branch ${c.branch || '?'}`);
                    html += alerta('ok', `Correção pronta para revisão ${onde}. ${h((c.relato && c.relato.causa) || '')}`);
                } else html += alerta('erro', h(`Correção não concluída: ${c.motivo || 'sem detalhe'}`));
            }
            html += '</div>';
            const hist = a.historico || [];
            html += `<div class="insa-bloco"><h3 class="insa-h3 mb-2">Histórico</h3>${hist.length ? `<ul class="insa-eventos">${hist.map((x) => `<li class="${x.ok ? '' : 'aviso'}"><time>${h(fmt.quando(x.em))}</time><div>${h(x.ok ? 'Nada mudou' : `${(x.achados || []).length} achado(s): ${(x.achados || []).map((y) => y.alvo).join(', ')}`)}${x.correcao ? ` <span class="insa-muted">${h(`; correção ${x.correcao.ok ? 'pronta' : 'não concluída'}`)}</span>` : ''}</div></li>`).join('')}</ul>` : '<p class="insa-muted">Sem histórico.</p>'}</div>`;
            $('insa-ag-corpo').innerHTML = html;
        },
    };
    function detalheAchado(x) {
        const d = x.detalhe;
        if (d && d.ocorrencias24h) return `${d.ocorrencias24h} vezes nas últimas 24 h`;
        if (d && d.faltando) return `campos que sumiram: ${d.faltando.join(', ') || '—'} (HTTP ${d.status})`;
        if (d && d.sugestoes) return `${d.sugestoes.length} sugestão(ões) de seletor`;
        if (typeof d === 'string') return d;
        return d == null ? '' : JSON.stringify(d);
    }
    async function verificarAgente(corrigir) {
        const r = await chamar(`orq/agente/verificar${corrigir ? '?corrigir=true' : ''}`, { method: 'POST' });
        setStatus('insa-ag-status', r.ok
            ? (corrigir ? 'Verificando e, se houver mudança nova, preparando a correção. Pode levar alguns minutos.' : 'Verificando a loja e as telas.')
            : erroDe(r), r.ok ? 'ok' : 'erro');
        agente.atualizar();
    }

    // =======================================================================
    // Aba: Configuração (admin) — PUT com diff por seção; histórico
    // =======================================================================
    const config = {
        original: null,
        edit: null,
        segredos: {},
        alteradas() {
            return this.edit && this.original ? Object.keys(this.edit).filter((k) => !igual(this.edit[k], this.original[k])) : [];
        },
        atualizarBarra() {
            const n = this.alteradas().length;
            $('insa-cf-barra').classList.toggle('hidden', n === 0);
            $('insa-cf-barra-txt').textContent = `Alterações não salvas em ${n} seção(ões).`;
        },
        async atualizar({ abriu }) {
            if (!abriu || !ehAdmin()) return; // não recarrega por cima do que está sendo editado
            carregarHistoricoConfig();
            if (this.edit && this.alteradas().length) return;
            const r = await chamar('orq/config');
            if (!r.ok) return setStatus('insa-cf-status', erroDe(r), 'erro');
            if (this.edit && this.alteradas().length) return;
            this.original = clone(r.config);
            this.edit = clone(r.config);
            this.segredos = r.segredos || {};
            st.config = clone(r.config);
            desenharConfig();
        },
    };

    // caminho "a.b.c" dentro de config.edit
    function lerCaminho(obj, caminho) { return caminho.split('.').reduce((o, k) => (o == null ? o : o[k]), obj); }
    function gravarCaminho(obj, caminho, valor) {
        const ks = caminho.split('.');
        let o = obj;
        for (let i = 0; i < ks.length - 1; i++) { if (o[ks[i]] == null || typeof o[ks[i]] !== 'object') o[ks[i]] = {}; o = o[ks[i]]; }
        o[ks[ks.length - 1]] = valor;
    }
    // tipos de campo: num (número), seg (ms mostrado em segundos), pct (fração mostrada em %), txt, txtcru (sem trim), lista, chk
    function valorParaTela(tipo, v) {
        if (tipo === 'seg') return v === '' || v == null ? '' : v / 1000;
        if (tipo === 'pct') return v === '' || v == null ? '' : Math.round(v * 100);
        if (tipo === 'lista') return (v || []).join(', ');
        return v == null ? '' : v;
    }
    function valorDaTela(tipo, el) {
        if (tipo === 'chk') return el.checked;
        const s = el.value;
        if (tipo === 'num') return s === '' ? '' : Number(s);
        if (tipo === 'seg') return s === '' ? '' : Math.round(Number(s) * 1000);
        if (tipo === 'pct') return s === '' ? '' : Number(s) / 100;
        if (tipo === 'lista') return s.split(',').map((x) => x.trim()).filter(Boolean);
        if (tipo === 'txtcru') return s;
        return s.trim();
    }
    function cfCampo(rotulo, caminho, tipo, { min, max, passo, sufixo, ajuda, placeholder = '', inputType } = {}) {
        const v = valorParaTela(tipo, lerCaminho(config.edit, caminho));
        if (tipo === 'chk') {
            return `<div class="insa-field"><label class="insa-check"><input type="checkbox" data-cf="${h(caminho)}" data-cf-tipo="chk"${v ? ' checked' : ''}> ${h(rotulo)}</label>${ajuda ? `<small>${h(ajuda)}</small>` : ''}</div>`;
        }
        const numerico = ['num', 'seg', 'pct'].includes(tipo);
        const attrs = [
            `type="${numerico ? 'number' : inputType || 'text'}"`,
            numerico ? 'inputmode="decimal"' : 'autocomplete="off" spellcheck="false"',
            min != null ? `min="${min}"` : '', max != null ? `max="${max}"` : '', passo != null ? `step="${passo}"` : (numerico ? 'step="1"' : ''),
            placeholder ? `placeholder="${h(placeholder)}"` : '',
        ].filter(Boolean).join(' ');
        const input = `<input ${attrs} value="${h(v)}" data-cf="${h(caminho)}" data-cf-tipo="${tipo}" class="input-glass px-3 py-2 text-sm">`;
        return `<label class="insa-field"><span>${h(rotulo)}</span>${sufixo ? `<div class="insa-row">${input.replace('class="input-glass', 'class="flex-1 input-glass')}<span class="insa-muted">${h(sufixo)}</span></div>` : input}${ajuda ? `<small>${h(ajuda)}</small>` : ''}</label>`;
    }
    function cfEscolha(rotulo, caminho, opcoes) {
        const v = lerCaminho(config.edit, caminho);
        return `<label class="insa-field"><span>${h(rotulo)}</span><select data-cf="${h(caminho)}" data-cf-tipo="txt" class="input-glass px-3 py-2 text-sm">${opcoes.map(([o, t]) => `<option value="${h(o)}"${o === v ? ' selected' : ''}>${h(t)}</option>`).join('')}</select></label>`;
    }
    const cfBloco = (titulo, descricao, ...filhos) => `<section class="insa-bloco"><h3 class="insa-h3 mb-1">${h(titulo)}</h3>${descricao ? `<p class="insa-desc">${h(descricao)}</p>` : ''}${filhos.join('')}</section>`;
    const cfGrade = (...filhos) => `<div class="insa-grid mb-3">${filhos.join('')}</div>`;
    const CAMPUS = [['18', '18, UNICID EAD'], ['16', '16, Cruzeiro do Sul EAD']];

    function desenharConfig() {
        const e = config.edit;
        const seg = config.segredos;
        e.crm1 = e.crm1 || {};
        e.crm1.retorno = e.crm1.retorno || {};
        e.crm1.retorno.etapas = e.crm1.retorno.etapas || {};
        e.crm1.retorno.campos = e.crm1.retorno.campos || { numeroInscricao: '', situacao: '', polo: '', cursoNaoEncontrado: '' };
        e.crm1.retorno.tags = e.crm1.retorno.tags || { inscrito: '', pendencia: '', manual: '', revisao: '' };
        // o campo do link do aluno vem com a versão nova do serviço; sem ele, a tela não o inventa
        const temLinkAluno = Object.prototype.hasOwnProperty.call(config.original.crm1 && config.original.crm1.retorno && config.original.crm1.retorno.campos || {}, 'linkAluno');

        const ordem = `<ol class="insa-ordem">${e.motores.ordem.map((n, i) => `<li>
            <span class="pos">${i + 1}</span>
            <span class="nome"><strong>${h(NOME_MOTOR[n] || n)}</strong>${sub(n === 'api' ? 'Chamadas diretas, cerca de 2 min' : 'Navegador simulando a tela, cerca de 6 min')}</span>
            <label class="insa-check"><input type="checkbox" data-cf-motor="${h(n)}"${e.motores.habilitados.includes(n) ? ' checked' : ''}> Ativo</label>
            ${botao('Subir', { acao: 'cf-mover', dados: { i, d: -1 }, desab: i === 0, titulo: `Subir ${NOME_MOTOR[n] || n}` })}
            ${botao('Descer', { acao: 'cf-mover', dados: { i, d: 1 }, desab: i === e.motores.ordem.length - 1, titulo: `Descer ${NOME_MOTOR[n] || n}` })}
        </li>`).join('')}</ol>`;

        const etapas = [
            ['inscrito', 'Inscrito', 'Inscrição concluída (nº SIAA no campo, pedido na nota).'],
            ['pendencia', 'Pendência', 'Regra da loja: já inscrito, CPF inválido, polo sem vaga.'],
            ['manual', 'Inscrição manual', 'Falha técnica ou automação desligada: o humano faz.'],
            ['revisao', 'Conferir inscrição', 'Pode ter pedido sem SIAA: conferir antes de refazer.'],
        ].map(([k, rotulo, ajuda]) => cfCampo(`Etapa para "${rotulo}"`, `crm1.retorno.etapas.${k}`, 'txt', { placeholder: 'id da etapa no crm1 (vazio = não move)', ajuda }));
        const camposDef = [
            ['numeroInscricao', 'Campo do nº de inscrição SIAA', 'Ex.: nro_da_inscricao (Nro. da Inscrição). Só no inscrito.'],
            ['situacao', 'Campo da situação', "Ex.: situacao. Recebe 'Inscrito pela automação', 'Pendência: …', 'Inscrição manual: …' ou 'Conferir: …'."],
            ['polo', 'Campo do polo usado', 'Ex.: polo. O polo em que a inscrição foi feita (útil quando veio pelo CEP).'],
            ['cursoNaoEncontrado', 'Campo do curso não encontrado', 'Ex.: curso_nao_encontrado. Quando o curso não existe ou é ambíguo.'],
        ];
        if (temLinkAluno) camposDef.push(['linkAluno', 'Campo do link para o aluno', 'Ex.: link_do_aluno. Link da prova, do pagamento ou do envio de documentos, conforme a forma. Só no inscrito.']);
        const camposRetorno = camposDef.map(([k, rotulo, ajuda]) => cfCampo(rotulo, `crm1.retorno.campos.${k}`, 'txt', { placeholder: 'vazio = não preenche', ajuda }));
        const tagsRetorno = [
            ['inscrito', 'Tag quando inscrito', 'Ex.: Inscrição Feita.'],
            ['pendencia', 'Tag na pendência', 'Opcional.'],
            ['manual', 'Tag na inscrição manual', 'Opcional. Ex.: Erro - Inscrição.'],
            ['revisao', 'Tag no conferir', 'Opcional.'],
        ].map(([k, rotulo, ajuda]) => cfCampo(rotulo, `crm1.retorno.tags.${k}`, 'txtcru', { placeholder: 'vazio = nenhuma', ajuda }));

        const pares = Object.entries((e.siaa && e.siaa.campusPorPolo) || {});
        const linhasCampus = pares.map(([polo, campus]) => {
            const opcoes = [...CAMPUS];
            if (!['16', '18'].includes(String(campus))) opcoes.push([String(campus), String(campus)]);
            return `<tr><td class="num">${h(polo)}</td>
                <td><select data-cf-campus="${h(polo)}" class="input-glass px-2 py-1 text-sm" aria-label="Campus do polo ${h(polo)}">${opcoes.map(([v, t]) => `<option value="${h(v)}"${v === String(campus) ? ' selected' : ''}>${h(t)}</option>`).join('')}</select></td>
                <td class="acoes">${botao('Remover', { acao: 'cf-campus-remover', dados: { polo } })}</td></tr>`;
        });

        const papeis = ['ADMIN', 'MANAGER'].map((r) => `<label class="insa-check"><input type="checkbox" data-cf-papel="${r}"${(e.alertas.roles || []).includes(r) ? ' checked' : ''}> ${r === 'ADMIN' ? 'Administradores' : 'Gestores'} do crm1</label>`).join('<br>');

        $('insa-cf-form').innerHTML = [
            cfBloco('Ordem de execução', 'Quem tenta primeiro e quem assume se ele falhar por erro técnico. Regra de negócio (ex.: CPF já inscrito) não passa para o outro motor.',
                ordem,
                cfGrade(
                    cfCampo('Usar o segundo motor como contingência', 'motores.fallback', 'chk', { ajuda: 'Desmarcado, a inscrição para no primeiro motor.' }),
                    cfEscolha('Como escolher o primeiro', 'motores.estrategia', [['ordem', 'Sempre a ordem acima'], ['auto', 'O que tiver acertado mais nas últimas 20']]),
                )),
            cfBloco('Ritmo da inscrição', 'Pausas entre as etapas. Mais lento parece mais humano e sobrecarrega menos a loja; o lote validado usou 19 s no HTTP.',
                cfGrade(
                    cfCampo('Pausa entre etapas no HTTP', 'timer.passoMsApi', 'seg', { min: 0, max: 120, sufixo: 'segundos', ajuda: '5 pausas por inscrição.' }),
                    cfCampo('Ritmo do Playwright', 'timer.ritmoBrowser', 'num', { min: 0.1, max: 5, passo: 0.1, sufixo: '× as esperas', ajuda: '1 é o normal; 2 deixa tudo duas vezes mais lento.' }),
                    cfCampo('Atraso entre ações no Playwright', 'timer.slowMoBrowserMs', 'num', { min: 0, max: 5000, sufixo: 'ms' }),
                )),
            cfBloco('Fila', 'Quantas inscrições rodam ao mesmo tempo. Todas usam o mesmo login de atendente; comece com 1 e suba aos poucos.',
                cfGrade(
                    cfCampo('Inscrições ao mesmo tempo', 'fila.concorrencia', 'num', { min: 1, max: 10 }),
                    cfCampo('Limite da fila', 'fila.maxFila', 'num', { min: 1, max: 100000, ajuda: "Acima disso o crm1 recebe 'fila cheia' e tenta de novo." }),
                )),
            cfBloco('Desligamento automático', 'Quando a inscrição começa a falhar, a automação desliga sozinha, os leads voltam para o processo manual e os gestores são avisados. Religar é sempre manual.',
                cfGrade(
                    cfCampo('Desligar automaticamente', 'turnOff.ativo', 'chk'),
                    cfCampo('Falhas seguidas para desligar', 'turnOff.falhasSeguidas', 'num', { min: 1, max: 100, ajuda: 'Conta só quando todos os motores falharam.' }),
                    cfCampo('Ou taxa de falha acima de', 'turnOff.taxaMax', 'pct', { min: 1, max: 100, sufixo: '%' }),
                    cfCampo('Nas últimas', 'turnOff.janela', 'num', { min: 1, max: 500, sufixo: 'inscrições' }),
                    cfCampo('Mínimo de inscrições para avaliar a taxa', 'turnOff.minAmostras', 'num', { min: 1, max: 500 }),
                    cfCampo('Pedido sem SIAA conta como falha', 'turnOff.contarRevisao', 'chk', { ajuda: 'Recomendado: é o sinal de que o vínculo quebrou.' }),
                    cfCampo('Desativar a regra no crm1 ao desligar', 'turnOff.desativarRegraCrm', 'chk'),
                    cfCampo('Avisar os gestores ao desligar e religar', 'turnOff.alertarGestores', 'chk'),
                )),
            cfBloco('crm1', 'A regra do crm1 chama este serviço; ao desligar, o serviço desativa essas automações no crm1.',
                cfGrade(
                    cfCampo('Endereço do crm1', 'crm1.baseUrl', 'txt', { inputType: 'url', placeholder: 'https://crm1.suaempresa.com.br' }),
                    cfCampo('Automações da inscrição', 'crm1.automacaoIds', 'lista', { placeholder: 'ids separados por vírgula' }),
                    `<div class="insa-field"><span>Credenciais no servidor</span><div class="insa-row">${pill(seg.crm1Token ? 'ok' : 'atencao', seg.crm1Token ? 'Token do crm1 definido' : 'Falta CRM1_TOKEN')} ${pill(seg.webhookSegredo ? 'ok' : 'atencao', seg.webhookSegredo ? 'Segredo do webhook definido' : 'Falta WEBHOOK_SEGREDO')}</div><small>Ficam no .env do servidor, nunca no painel.</small></div>`,
                )),
            cfBloco('Retorno ao crm1', 'Cada negócio que veio do crm1 volta para lá com o resultado: preenche os campos do negócio, ganha nota e tag e, se você configurar, muda de etapa. Usa rotas que já existem no crm1 (endereço da API pública + CRM1_TOKEN).',
                cfGrade(cfCampo('Devolver os negócios ao crm1', 'crm1.retorno.ativo', 'chk', { ajuda: 'Desligado: o resultado fica só neste painel.' })),
                cfGrade(...etapas,
                    cfCampo('Etapa que dispara a automação', 'crm1.retorno.etapaGatilho', 'txt', { placeholder: 'id da etapa gatilho', ajuda: 'Nenhuma etapa de retorno pode ser esta: o negócio entraria em loop.' }),
                    cfCampo('Tag em todos os negócios devolvidos', 'crm1.retorno.tag', 'txtcru')),
                `<h3 class="insa-h3 mb-2 mt-2">Campos do negócio</h3>`,
                cfGrade(...camposRetorno),
                temLinkAluno ? '' : `<p class="insa-muted mb-3">O campo do link para o aluno aparece aqui quando o serviço de inscrição for atualizado.</p>`,
                `<h3 class="insa-h3 mb-2 mt-2">Tags por resultado</h3>`,
                cfGrade(...tagsRetorno)),
            cfBloco('Aviso aos gestores', 'Mensagem de WhatsApp com template aprovado na Meta, enviada pelo crm1 aos perfis escolhidos.',
                cfGrade(
                    cfCampo('Template', 'alertas.template', 'txt', { ajuda: 'Variáveis: o que aconteceu, motivo, quando, link do painel.' }),
                    cfCampo('Idioma do template', 'alertas.idioma', 'txt'),
                    `<div class="insa-field"><span>Quem recebe</span>${papeis}</div>`,
                    cfCampo('Ou só estes usuários', 'alertas.userIds', 'lista', { placeholder: 'ids do crm1, separados por vírgula' }),
                    `<div class="insa-field"><span>Envio</span><div>${pill(seg.alertaGestores ? 'ok' : 'atencao', seg.alertaGestores ? 'Liberado no servidor' : 'Travado (ALERTA_GESTORES)')}</div><small>Travado, o aviso aparece só em Eventos.</small></div>`,
                )),
            cfBloco('Conferência no SIAA', 'Confere se cada inscrição feita aparece no SIAA (relatório SVREL28), polo a polo.',
                cfGrade(
                    cfCampo('Processo seletivo', 'siaa.processoSeletivo', 'txt', { ajuda: '626 = 1º semestre de 2027.' }),
                    cfCampo('Conferir sozinho a cada', 'siaa.conferirACadaMin', 'num', { min: 0, max: 1440, sufixo: 'min', ajuda: '0 = só pelo botão.' }),
                    cfCampo('Conferir inscrições dos últimos', 'siaa.diasConferencia', 'num', { min: 1, max: 90, sufixo: 'dias' }),
                ),
                `<h3 class="insa-h3 mb-2">Polos conferidos e campus no SIAA</h3>`,
                `<div class="insa-table-wrap mb-2" style="max-width:520px">${tabela(['Polo (id)', 'Campus', ''], linhasCampus, 'Nenhum polo na conferência.')}</div>`,
                `<div class="insa-row"><input id="insa-cf-novo-polo" type="text" inputmode="numeric" placeholder="id do polo" aria-label="Id do polo" autocomplete="off" class="input-glass px-3 py-2 text-sm w-32">
                    <select id="insa-cf-novo-campus" aria-label="Campus" class="input-glass px-3 py-2 text-sm">${CAMPUS.map(([v, t]) => `<option value="${v}">${h(t)}</option>`).join('')}</select>
                    ${botao('Adicionar polo', { acao: 'cf-campus-adicionar' })}</div>`),
            cfBloco('Agente de manutenção', 'Verificação de mudanças na loja e nas telas, sem criar nada na VTEX ou no SIAA. A correção roda num branch separado e passa pelos testes antes de chegar a uma pessoa.',
                cfGrade(
                    cfCampo('Verificar sozinho a cada', 'agente.verificarACadaMin', 'num', { min: 0, max: 1440, sufixo: 'min', ajuda: '0 = só pelo botão.' }),
                    cfCampo('Preparar a correção sozinho quando achar mudança', 'agente.corrigirSozinho', 'chk', { ajuda: 'Usa o Claude Code; tem custo por correção.' }),
                    cfCampo('Gasto máximo por correção', 'agente.orcamentoUsd', 'num', { min: 0.5, max: 100, passo: 0.5, sufixo: 'US$' }),
                )),
        ].join('');
        config.atualizarBarra();
    }

    async function carregarHistoricoConfig() {
        const r = await chamar('orq/config/historico?limite=15');
        if (!r.ok) return;
        const itens = r.historico || [];
        $('insa-cf-hist').innerHTML = itens.length
            ? itens.map((x) => `<li><time>${h(fmt.quando(x.em))}</time><div><strong>${h(x.por || '?')}</strong> ${h(`alterou ${(x.mudancas || []).map((m) => `${m.campo} (${JSON.stringify(m.de)} para ${JSON.stringify(m.para)})`).join('; ')}`)}</div></li>`).join('')
            : '<li class="insa-muted">Nenhuma alteração registrada.</li>';
    }

    async function salvarConfig() {
        const secoes = config.alteradas();
        if (!secoes.length) return;
        const b = $('insa-cf-salvar');
        b.disabled = true;
        b.textContent = 'Salvando…';
        setStatus('insa-cf-status', '');
        try {
            const r = await chamar('orq/config', { method: 'PUT', corpo: Object.fromEntries(secoes.map((k) => [k, config.edit[k]])) });
            if (!r.ok) return setStatus('insa-cf-status', erroDe(r), 'erro');
            config.original = clone(r.config);
            config.edit = clone(r.config);
            st.config = clone(r.config);
            desenharConfig();
            carregarHistoricoConfig();
            setStatus('insa-cf-status', r.mudancas && r.mudancas.length ? `Salvo: ${r.mudancas.map((m) => m.campo).join(', ')}. Vale a partir da próxima inscrição.` : 'Nada mudou.', 'ok');
        } finally {
            b.disabled = false;
            b.textContent = 'Salvar alterações';
        }
    }

    // =======================================================================
    // Aba: Eventos
    // =======================================================================
    const eventos = {
        qs() {
            return new URLSearchParams(Object.entries({ tipo: $('insa-ev-tipo').value, nivel: $('insa-ev-nivel').value, limite: '200' }).filter(([, v]) => v)).toString();
        },
        async atualizar() {
            const sel = $('insa-ev-tipo');
            if (!sel.options.length) sel.innerHTML = Object.entries(TIPOS_EVENTO).map(([v, t]) => `<option value="${h(v)}">${h(t)}</option>`).join('');
            const qs = this.qs();
            const r = await chamar(`orq/eventos?${qs}`);
            if (qs !== this.qs()) return;
            if (!r.ok) return setStatus('insa-ev-status', erroDe(r), 'erro');
            setStatus('insa-ev-status', '');
            const itens = r.eventos || [];
            if (!mudou('eventos', [qs, itens])) return;
            $('insa-ev-lista').innerHTML = itens.length ? itens.map((e) => {
                const n = NIVEL[e.nivel] || NIVEL.info;
                let corpo = e.nivel && e.nivel !== 'info' ? `${pill(n.tom, n.rotulo)} ` : '';
                corpo += h(e.mensagem || '');
                const ue = e.dados && e.dados.ultimosErros;
                if (ue && ue.length) corpo += sub(`Últimos erros: ${ue.map((x) => `${x.code || '?'} (${x.engine || '?'})`).join(', ')}`);
                return `<li class="${e.nivel === 'erro' ? 'erro' : e.nivel === 'aviso' ? 'aviso' : ''}"><time datetime="${h(e.em || '')}">${h(fmt.quando(e.em))}</time><div>${corpo}</div></li>`;
            }).join('') : '<li class="insa-muted">Nada registrado com esse filtro.</li>';
        },
    };

    // =======================================================================
    // Aba: Simulação (só quando o serviço roda com motores falsos)
    // =======================================================================
    const simulacao = {
        async atualizar({ abriu }) {
            if (!abriu) return;
            const r = await chamar('orq/simulacao');
            if (!r.ok) return setStatus('insa-sim-status', erroDe(r), 'erro');
            const c = r.simulacao || {};
            $('insa-sim-motores').innerHTML = ['api', 'browser'].map((m) => `<label class="insa-field"><span>${h(NOME_MOTOR[m])}: próximas execuções</span>
                <select data-sim-motor="${m}" class="input-glass px-3 py-2 text-sm"${ehAdmin() ? '' : ' disabled'}>${Object.entries(MODOS_SIM).map(([v, t]) => `<option value="${v}"${(c[m] && c[m].modo || 'ok') === v ? ' selected' : ''}>${h(t)}</option>`).join('')}</select></label>`).join('');
        },
    };
    function cpfTeste(n) {
        const d = String(900000000 + n).split('').map(Number);
        for (const len of [9, 10]) {
            let s = 0;
            for (let i = 0; i < len; i++) s += d[i] * (len + 1 - i);
            const r = (s * 10) % 11;
            d.push(r === 10 ? 0 : r);
        }
        return d.join('');
    }
    async function mudarSimulacao(motor, modo) {
        const r = await chamar('orq/simulacao', { method: 'PUT', corpo: { [motor]: { modo } } });
        setStatus('insa-sim-status', r.ok ? `${NOME_MOTOR[motor]} nas próximas execuções: ${MODOS_SIM[modo]}.` : erroDe(r), r.ok ? 'ok' : 'erro');
        simulacao.atualizar({ abriu: true });
    }
    async function enviarSimuladas(btn) {
        const n0 = Math.min(20, Math.max(1, Math.round(Number($('insa-sim-qtd').value) || 1)));
        btn.disabled = true;
        let falhas = 0;
        let ultimoErro = '';
        try {
            const base = Date.now() % 100000;
            for (let i = 0; i < n0; i++) {
                const n = base * 10 + i;
                const r = await chamar('orq/inscricao/simulada', { method: 'POST', corpo: { nome: `Teste Simulado ${n}`, cpf: cpfTeste(n % 99999999), email: `sim${n}@teste.local`, curso: 'Pedagogia', polo: 'Barra Funda' } });
                if (!r.ok) { falhas++; ultimoErro = erroDe(r); }
            }
        } finally {
            btn.disabled = false;
        }
        setStatus('insa-sim-status', falhas ? `${n0 - falhas} de ${n0} enviadas. Erro: ${ultimoErro}` : `${n0} inscrição(ões) de teste enviadas para a fila.`, falhas ? 'erro' : 'ok');
    }

    // =======================================================================
    // Abas, ciclo de atualização e visibilidade
    // =======================================================================
    const PAINEIS = { visao, fila, nova, cadastros, siaa, agente, config, eventos, simulacao };

    function abaPermitida(nome) {
        if (!PAINEIS[nome]) return false;
        if (nome === 'config' && !ehAdmin()) return false;
        if (nome === 'simulacao' && !(st.saude && st.saude.simulacao)) return false;
        return true;
    }

    function abrirAba(nome, { semAtualizar = false } = {}) {
        if (!abaPermitida(nome)) nome = 'visao';
        st.aba = nome;
        gravarLS('insa.aba', nome);
        ABAS.forEach((t) => {
            const b = $('insa-tab-' + t);
            const s = $('insa-sec-' + t);
            if (b) {
                b.className = (t === nome ? dsSegActive('flex items-center gap-2') : dsSegInactive('flex items-center gap-2'))
                    + ((t === 'simulacao' && !(st.saude && st.saude.simulacao)) || (t === 'config' && !ehAdmin()) ? ' hidden' : '');
                b.setAttribute('aria-selected', String(t === nome));
            }
            if (s) s.classList.toggle('hidden', t !== nome);
        });
        if (!semAtualizar) atualizarAba(true);
    }

    async function atualizarAba(abriu = false) {
        const aba = st.aba;
        const primeira = !st.carregadas.has(aba);
        st.carregadas.add(aba);
        try {
            await PAINEIS[aba].atualizar({ abriu: abriu || primeira, lento: st.tick % 3 === 0 });
        } catch (e) {
            setStatus('insa-status', `Falha ao atualizar: ${e.message}`, 'erro');
        }
    }

    function mostrarFora(r) {
        const box = $('insa-fora');
        box.classList.remove('hidden');
        $('insa-fora-erro').textContent = ` ${r.erro || r.error || `HTTP ${r.http}`}`;
    }

    async function ciclo() {
        if (st.ocupado) return;
        st.ocupado = true;
        try {
            const [hh, a] = await Promise.all([chamar('orq/health'), chamar('orq/automacao')]);
            if (!hh.ok) {
                mostrarFora(hh);
                st.saude = null;
            } else {
                $('insa-fora').classList.add('hidden');
                st.saude = hh;
                const tabSim = $('insa-tab-simulacao');
                tabSim.classList.toggle('hidden', !hh.simulacao);
                if (!hh.simulacao && st.aba === 'simulacao') abrirAba('visao');
            }
            if (a.ok) st.automacao = a.automacao;
            desenharCabecalho();
            await atualizarAba();
            st.tick++;
        } finally {
            st.ocupado = false;
        }
    }

    const paginaNaFrente = () => st.ativo && document.visibilityState === 'visible';

    function aplicarPapel() {
        const r = raiz();
        st.papel = (r && r.dataset.papel) === 'admin' ? 'admin' : 'operar';
        const p = $('insa-pill-papel');
        p.className = `insa-pill t-${ehAdmin() ? 'ativo' : 'neutro'}`;
        p.textContent = ehAdmin() ? 'Você administra' : 'Você opera';
        p.title = ehAdmin() ? 'Pode inscrever de verdade, ligar/desligar e mudar a configuração.' : 'Pode acompanhar, validar, ensaiar e reprocessar. Ações de administração ficam escondidas.';
        if (!ehAdmin()) r.querySelectorAll('[data-so-admin]').forEach((el) => el.classList.add('hidden'));
    }

    // ── eventos da página (delegação) ───────────────────────────────────────
    function ligarEventos() {
        const r = raiz();
        r.addEventListener('click', (ev) => {
            const tab = ev.target.closest('[data-aba]');
            if (tab && r.contains(tab) && tab.closest('[role="tablist"]')) { abrirAba(tab.dataset.aba); return; }
            const b = ev.target.closest('[data-acao]');
            if (!b || !r.contains(b) || b.disabled) return;
            const acao = b.dataset.acao;
            switch (acao) {
                case 'atualizar': st.carregadas.delete(st.aba); ciclo(); break;
                case 'ir-aba': abrirAba(b.dataset.abaDestino); break;
                case 'disjuntor': alternarAutomacao(); break;
                case 'motor-reset': (async () => {
                    const x = await chamar(`orq/engines/${encodeURIComponent(b.dataset.motor)}/reset`, { method: 'POST' });
                    setStatus('insa-ov-status', x.ok ? `${NOME_MOTOR[b.dataset.motor] || b.dataset.motor} recolocado em rotação.` : erroDe(x), x.ok ? 'ok' : 'erro');
                    ciclo();
                })(); break;
                case 'fl-filtro': fila.filtro = b.dataset.valor || ''; fila.desenharFiltros(); fila.atualizar(); break;
                case 'job-reprocessar': acaoJob('reprocessar', b.dataset.job); break;
                case 'job-cancelar': acaoJob('cancelar', b.dataset.job); break;
                case 'job-devolver': acaoJob('devolver', b.dataset.job); break;
                case 'ret-reenviar': (async () => {
                    const x = await chamar('orq/crm1/retorno/reenviar', { method: 'POST' });
                    setStatus('insa-ret-status', x.ok ? `${x.reenfileirados} negócio(s) voltaram para o envio.` : erroDe(x), x.ok ? 'ok' : 'erro');
                    fila.atualizar();
                })(); break;
                case 'nv-cupom-validar': validarCupom($('insa-nv-cupom').value.trim(), 'insa-nv-cupom-status'); break;
                case 'nv-ver':
                    if (nova.jobAtual && nova.jobAtual !== b.dataset.id) { setStatus('insa-nv-status', 'Aguarde a execução atual terminar para ver outra.', 'info'); break; }
                    acompanhar(b.dataset.id, { soVer: nova.jobAtual !== b.dataset.id });
                    break;
                case 'cp-validar': validarCupom($('insa-cp-cupom').value.trim(), 'insa-cp-status'); break;
                case 'at-ver': {
                    const i = $('insa-at-senha');
                    const ver = i.type === 'password';
                    i.type = ver ? 'text' : 'password';
                    b.setAttribute('aria-pressed', String(ver));
                    b.setAttribute('aria-label', ver ? 'Ocultar senha' : 'Mostrar senha');
                    b.querySelector('.material-symbols-outlined').textContent = ver ? 'visibility_off' : 'visibility';
                    break;
                }
                case 'at-testar': testarAtendente(b); break;
                case 'at-remover': removerAtendente(b.dataset.indice, b.dataset.email); break;
                case 'pl-testar': (async () => {
                    const cep = dig($('insa-pl-cep').value);
                    $('insa-pl-resultado').textContent = 'Calculando…';
                    const x = await chamar('polos/testar', { method: 'POST', corpo: { cep } });
                    if (!x.ok) { $('insa-pl-resultado').textContent = erroDe(x); return; }
                    const outros = (x.ranking || []).filter((y) => y.poleId !== x.poleId).slice(0, 3).map((y) => `${y.nome} ${y.km} km${y.aceitaInscricao ? '' : ' (sem cota)'}`);
                    $('insa-pl-resultado').textContent = `→ ${x.nome} (${x.poleId}), ${x.km} km em linha reta · localização pelo ${x.fonte === 'google' ? 'Google' : 'CEP da VTEX'}${outros.length ? ` · depois: ${outros.join(', ')}` : ''}`;
                })(); break;
                case 'pl-adicionar': {
                    const c = cadastros.catalogoPolos.find((x) => String(x.id) === $('insa-pl-novo').value);
                    const vazio = $('insa-pl-lista').querySelector('td[colspan]');
                    if (vazio) vazio.parentElement.remove();
                    $('insa-pl-lista').insertAdjacentHTML('beforeend', linhaPolo(c ? { poleId: c.id, nome: c.nome, nomeCatalogo: c.nome, apelidos: [], aceitaInscricao: true } : { aceitaInscricao: true }));
                    setStatus('insa-pl-status', 'Preencha a latitude e a longitude do novo polo (ex.: pelo Google Maps) e clique em Salvar polos.', 'info');
                    break;
                }
                case 'pl-remover': b.closest('tr').remove(); setStatus('insa-pl-status', 'Polo tirado da lista. Clique em Salvar polos para gravar.', 'info'); break;
                case 'pl-salvar': (async () => {
                    const x = await chamar('polos', { method: 'PUT', corpo: { polos: lerPolos() } });
                    if (!x.ok) return setStatus('insa-pl-status', erroDe(x), 'erro');
                    await carregarPolos();
                    setStatus('insa-pl-status', `Salvo: ${(x.polos || []).length} polos. Já vale para as próximas inscrições.`, 'ok');
                })(); break;
                case 'rel-csv': baixarCsv(); break;
                case 'rel-conferir': conferirSiaa(b); break;
                case 'ss-abrir': {
                    const f = $('insa-ss-form');
                    f.classList.toggle('hidden');
                    b.setAttribute('aria-expanded', String(!f.classList.contains('hidden')));
                    if (!f.classList.contains('hidden')) $('insa-ss-curl').focus();
                    break;
                }
                case 'ag-verificar': verificarAgente(false); break;
                case 'ag-corrigir': verificarAgente(true); break;
                case 'cf-descartar': config.edit = clone(config.original); desenharConfig(); break;
                case 'cf-salvar': salvarConfig(); break;
                case 'cf-mover': {
                    const i = Number(b.dataset.i);
                    const d = Number(b.dataset.d);
                    const o = [...config.edit.motores.ordem];
                    [o[i], o[i + d]] = [o[i + d], o[i]];
                    config.edit.motores.ordem = o;
                    desenharConfig();
                    break;
                }
                case 'cf-campus-remover': {
                    const resto = { ...config.edit.siaa.campusPorPolo };
                    delete resto[b.dataset.polo];
                    config.edit.siaa.campusPorPolo = resto;
                    desenharConfig();
                    break;
                }
                case 'cf-campus-adicionar': {
                    const polo = $('insa-cf-novo-polo').value.trim();
                    if (!/^\d+$/.test(polo)) { setStatus('insa-cf-status', 'O id do polo tem só números.', 'erro'); break; }
                    config.edit.siaa.campusPorPolo = { ...config.edit.siaa.campusPorPolo, [polo]: $('insa-cf-novo-campus').value };
                    desenharConfig();
                    break;
                }
                case 'sim-enviar': enviarSimuladas(b); break;
                default: break;
            }
        });

        // submits dos formulários
        r.addEventListener('submit', (ev) => {
            const f = ev.target;
            ev.preventDefault();
            if (f.id === 'insa-nv-form') {
                const modo = (ev.submitter && ev.submitter.dataset.modo) || nova.modoClicado || 'validar';
                if (modo === 'real') {
                    const forma = formaAtual();
                    confirmar({
                        titulo: 'Criar a inscrição REAL?',
                        texto: `${$('insa-nv-nome').value || '(sem nome)'} em ${$('insa-nv-curso').value || '(sem curso)'}. Forma: ${(forma && forma.label) || '?'} · Cupom: ${$('insa-nv-cupom').value || '(sem cupom)'} · Motor: ${$('insa-nv-motor').selectedOptions[0].textContent}. Cria o pedido na loja com vínculo no SIAA; não dá para desfazer por aqui.`,
                        alertaTexto: true,
                        acao: 'Inscrever de verdade',
                        perigo: true,
                        async executar() { enviarInscricao('real'); },
                    });
                    return;
                }
                enviarInscricao(modo);
            } else if (f.id === 'insa-cp-form') {
                if (ehAdmin()) salvarCampanha();
            } else if (f.id === 'insa-at-form') {
                salvarAtendente();
            } else if (f.id === 'insa-pl-google-form') {
                (async () => {
                    const chave = $('insa-pl-google-chave').value.trim();
                    const x = await chamar('polos/google', { method: 'PUT', corpo: { chave } });
                    $('insa-pl-google-chave').value = '';
                    if (!x.ok) return setStatus('insa-pl-status', erroDe(x), 'erro');
                    $('insa-pl-google').textContent = x.google ? `chave ${x.google} (Google)` : 'sem chave (usa a VTEX)';
                    setStatus('insa-pl-status', x.google ? 'Chave do Google salva: o CEP passa a ser localizado pelo Google.' : 'Chave removida: volta a usar a VTEX.', 'ok');
                })();
            } else if (f.id === 'insa-ss-form') {
                (async () => {
                    const x = await chamar('orq/siaa/sessao', { method: 'PUT', corpo: { curl: $('insa-ss-curl').value } });
                    if (!x.ok) return setStatus('insa-ss-status', erroDe(x), 'erro');
                    $('insa-ss-curl').value = '';
                    $('insa-ss-salvar').disabled = true;
                    $('insa-ss-form').classList.add('hidden');
                    $('insa-ss-abrir').setAttribute('aria-expanded', 'false');
                    setStatus('insa-ss-status', 'Sessão salva no servidor. Ela não aparece mais aqui.', 'ok');
                    siaa.atualizar({ abriu: true });
                })();
            }
        });

        // digitação: máscaras, cupom, filtros e configuração
        r.addEventListener('input', (ev) => {
            const t = ev.target;
            if (MASCARAS[t.id]) { t.value = MASCARAS[t.id](dig(t.value)); return; }
            if (t.id === 'insa-nv-cupom' || t.id === 'insa-cp-cupom') {
                t.value = t.value.toUpperCase().replace(/[^A-Z0-9_-]/g, '');
                if (t.id === 'insa-nv-cupom') { t.dataset.editado = '1'; setStatus('insa-nv-cupom-status', ''); }
                return;
            }
            if (t.id === 'insa-ss-curl') { $('insa-ss-salvar').disabled = !t.value.trim(); return; }
            if (t.matches('#insa-rel-filtros input[data-f]')) {
                siaa.f[t.dataset.f] = t.value.trim();
                clearTimeout(siaa.espera);
                siaa.espera = setTimeout(() => siaa.atualizar({ abriu: true }), t.type === 'date' ? 0 : 400);
                return;
            }
            if (t.dataset.cf && t.dataset.cfTipo !== 'chk' && t.tagName !== 'SELECT') {
                gravarCaminho(config.edit, t.dataset.cf, valorDaTela(t.dataset.cfTipo, t));
                config.atualizarBarra();
            }
        });
        r.addEventListener('change', (ev) => {
            const t = ev.target;
            if (t.id === 'insa-nv-forma') { aplicarForma(); return; }
            if (t.matches('#insa-rel-filtros select[data-f]')) {
                siaa.f[t.dataset.f] = t.value.trim();
                siaa.atualizar({ abriu: true });
                return;
            }
            if (t.id === 'insa-ev-tipo' || t.id === 'insa-ev-nivel') { eventos.atualizar(); return; }
            if (t.dataset.simMotor) { mudarSimulacao(t.dataset.simMotor, t.value); return; }
            if (t.matches('#insa-pl-lista [data-aceita]')) { t.closest('tr').classList.toggle('sem-cota', !t.checked); return; }
            if (!config.edit) return;
            if (t.dataset.cf && (t.dataset.cfTipo === 'chk' || t.tagName === 'SELECT')) {
                gravarCaminho(config.edit, t.dataset.cf, valorDaTela(t.dataset.cfTipo, t));
            } else if (t.dataset.cfMotor) {
                const n = t.dataset.cfMotor;
                const hab = config.edit.motores.habilitados;
                config.edit.motores.habilitados = t.checked ? [...new Set([...hab, n])] : hab.filter((x) => x !== n);
            } else if (t.dataset.cfPapel) {
                const p = t.dataset.cfPapel;
                const roles = config.edit.alertas.roles || [];
                config.edit.alertas.roles = t.checked ? [...new Set([...roles, p])] : roles.filter((x) => x !== p);
            } else if (t.dataset.cfCampus) {
                config.edit.siaa.campusPorPolo = { ...config.edit.siaa.campusPorPolo, [t.dataset.cfCampus]: t.value };
            } else return;
            config.atualizarBarra();
        });
        // Enter no CEP de teste não envia formulário nenhum
        $('insa-pl-cep').addEventListener('keydown', (ev) => { if (ev.key === 'Enter') { ev.preventDefault(); r.querySelector('[data-acao="pl-testar"]').click(); } });
        $('insa-cf-form').addEventListener('keydown', (ev) => {
            if (ev.key === 'Enter' && ev.target.id === 'insa-cf-novo-polo') { ev.preventDefault(); r.querySelector('[data-acao="cf-campus-adicionar"]').click(); }
        });
        document.addEventListener('visibilitychange', () => { if (paginaNaFrente()) ciclo(); });
    }

    function iniciar() {
        if (st.iniciado) return;
        st.iniciado = true;
        aplicarPapel();
        ligarEventos();
        const lembrada = lerLS('insa.aba');
        st.aba = lembrada && lembrada !== 'simulacao' && PAINEIS[lembrada] ? lembrada : 'visao';
        if (!abaPermitida(st.aba)) st.aba = 'visao';
    }

    // ── ganchos chamados pelo navigate() de utils.js ────────────────────────
    window.loadInscricaoAutomatica = function () {
        if (!raiz()) return;
        iniciar();
        st.ativo = true;
        abrirAba(st.aba, { semAtualizar: true });
        st.carregadas.delete(st.aba);
        ciclo();
        clearInterval(st.timer);
        st.timer = setInterval(() => { if (paginaNaFrente()) ciclo(); }, 5000);
        // acompanhamento da inscrição avulsa que ficou rodando enquanto a página estava fechada
        if (nova.jobAtual && !nova.poll) acompanhar(nova.jobAtual);
    };
    window.inscricaoAutoSair = function () {
        if (!st.ativo) return;
        st.ativo = false;
        clearInterval(st.timer);
        st.timer = null;
        if (nova.poll) { clearInterval(nova.poll); nova.poll = null; }
        const dlg = $('insa-dlg');
        if (dlg && dlg.open && !dlgOcupado) dlg.close();
    };
})();
