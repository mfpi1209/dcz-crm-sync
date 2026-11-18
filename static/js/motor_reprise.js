// Motor de Ativações — reprise de uma rodada real (nada é enviado; só conta).

let _mtrPadrao = null;
let _mtrPronto = false;
let _mtrDados = null;

const MTR_ESTADO = { segurando: ['Segurando', 'mt-aviso'], pausado: ['Pausado', 'mt-ruim'] };

function _mtrEl(id) { return document.getElementById(id); }

async function mtrIniciar() {
    if (_mtrPronto) return;
    try {
        const res = await api('/api/motor/reprise/rodadas');
        const d = await res.json();
        if (!res.ok || !d.ok || !d.configurada || !(d.rodadas || []).length) return;
        _mtrPronto = true;
        _mtrPadrao = d.padrao;
        _mtrEl('mtr-rodada').innerHTML = d.rodadas.map(r => {
            const [a, m, dia] = String(r.dia).split('-');
            return `<option value="${_mtEsc(r.arquivo)}">${dia}/${m}/${a} · ${_mtNum(r.enviados)} envios · ${_mtNum(r.foram_a_atendente)} foram a atendente</option>`;
        }).join('');
        _mtrEl('mtr-ia').addEventListener('input', e => { _mtrEl('mtr-ia-valor').textContent = e.target.value + '%'; });
        _mtMostrar('mtr-local', !!d.local);
        _mtMostrar('mtr-bloco', true);
        mtrPadrao(false);
        mtrRodar(false);
    } catch (e) { /* sem reprise: o bloco fica escondido */ }
}

function mtrPadrao(rodar) {
    const p = _mtrPadrao;
    if (!p) return;
    _mtrEl('mtr-inicio').value = p.inicio;
    [1, 2, 3].forEach(i => { _mtrEl('mtr-lote' + i).value = p.lotes[i - 1]; });
    _mtrEl('mtr-tamanho').value = p.pela_folga ? 'folga' : 'fixo';
    _mtrEl('mtr-intervalo').value = p.intervalo_min;
    _mtrEl('mtr-segura').value = p.fila_sobe_segura;
    _mtrEl('mtr-pausa').value = p.fila_sobe_pausa;
    _mtrEl('mtr-base').value = p.base;
    _mtrEl('mtr-ia').value = Math.round(p.corte_ia * 100);
    _mtrEl('mtr-ia-valor').textContent = Math.round(p.corte_ia * 100) + '%';
    _mtrEl('mtr-atend').value = p.atend_por_hora;
    _mtrEl('mtr-teto').value = p.teto_dia;
    if (rodar !== false) mtrRodar();
}

function _mtrParametros() {
    return {
        inicio: _mtrEl('mtr-inicio').value,
        lotes: [1, 2, 3].map(i => Number(_mtrEl('mtr-lote' + i).value)),
        pela_folga: _mtrEl('mtr-tamanho').value === 'folga',
        intervalo_min: Number(_mtrEl('mtr-intervalo').value),
        fila_sobe_segura: Number(_mtrEl('mtr-segura').value),
        fila_sobe_pausa: Number(_mtrEl('mtr-pausa').value),
        base: _mtrEl('mtr-base').value,
        corte_ia: Number(_mtrEl('mtr-ia').value) / 100,
        atend_por_hora: Number(_mtrEl('mtr-atend').value),
        teto_dia: Number(_mtrEl('mtr-teto').value),
    };
}

async function mtrRodar(rolar) {
    const botao = _mtrEl('mtr-rodar');
    botao.disabled = true;
    _mtrEl('mtr-estado').textContent = 'Refazendo a rodada…';
    try {
        const res = await api('/api/motor/reprise', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ arquivo: _mtrEl('mtr-rodada').value, parametros: _mtrParametros() }),
        });
        const d = await res.json();
        if (!res.ok || !d.ok) throw new Error(d.error || 'Não consegui refazer a rodada.');
        _mtrDados = d;
        _mtrRender(d);
        _mtrAutoFilme(rolar !== false);
        _mtrEl('mtr-estado').textContent = '';
    } catch (e) {
        _mtrEl('mtr-estado').textContent = e.message || 'Não consegui refazer a rodada.';
    } finally {
        botao.disabled = false;
    }
}

function _mtrMin(v) { return v == null ? '—' : _mtNum(v) + ' min'; }

function _mtrRender(d) {
    const m = d.motor, r = d.real, p = d.parametros;
    // A tela mostra o que a conta usou de fato (o início, por exemplo, fica entre 9h e 10h).
    _mtrEl('mtr-inicio').value = p.inicio;
    _mtrEl('mtr-k-lotes').textContent = _mtNum(m.lotes);
    _mtrEl('mtr-k-lotes-sub').textContent = _mtNum(m.enviados) + ' envios' + (m.nao_enviados ? ' · ' + _mtNum(m.nao_enviados) + ' não enviados' : '');
    _mtrEl('mtr-k-fim').textContent = m.ultimo_envio || '—';
    _mtrEl('mtr-k-fim-sub').textContent = 'começou às ' + (m.primeiro_envio || '—') + ' · real: ' + r.primeiro_envio + ' às ' + r.ultimo_envio;
    _mtrEl('mtr-k-pico').textContent = _mtNum(m.pico);
    _mtrEl('mtr-k-pico-sub').textContent = 'às ' + m.hora_do_pico + ' · real: ' + _mtNum(r.pico) + ' às ' + r.hora_do_pico;
    _mtrEl('mtr-k-seg').textContent = _mtNum(m.min_segurando) + ' · ' + _mtNum(m.min_pausado) + ' min';
    _mtrEl('mtr-k-seg-sub').textContent = 'fila antes do 1º lote: ' + _mtNum(m.fila_base);

    const linhas = [
        ['Envios', _mtNum(m.enviados), _mtNum(r.enviados)],
        ['Primeiro e último envio', (m.primeiro_envio || '—') + ' às ' + (m.ultimo_envio || '—'), r.primeiro_envio + ' às ' + r.ultimo_envio],
        ['Foram a atendente', _mtNum(m.foram_a_atendente), _mtNum(r.foram_a_atendente)],
        ['Pico da fila', _mtNum(m.pico) + ' às ' + m.hora_do_pico, _mtNum(r.pico) + ' às ' + r.hora_do_pico],
        ['Minutos com fila acima de 30', _mtNum(m.min_fila_acima_de_30), _mtNum(r.min_fila_acima_de_30)],
        ['Espera pelo atendente (mediana)', _mtrMin(m.espera_p50_min), _mtrMin(r.espera_p50_min)],
        ['Espera pelo atendente (9 em cada 10)', 'até ' + _mtrMin(m.espera_p90_min), 'até ' + _mtrMin(r.espera_p90_min)],
        ['Sem atendimento no fim do dia', _mtNum(m.sem_atendimento_no_fim), _mtNum(r.sem_atendimento_no_fim)],
        ['Lotes', _mtNum(m.lotes), '—'],
        ['Minutos segurando', _mtNum(m.min_segurando), '—'],
        ['Minutos pausado', _mtNum(m.min_pausado), '—'],
        ['Não enviados no dia', _mtNum(m.nao_enviados), '0'],
    ];
    _mtrEl('mtr-compara').innerHTML = linhas.map(l => `<tr><td>${l[0]}</td><td class="mt-num">${_mtEsc(l[1])}</td><td class="mt-num">${_mtEsc(l[2])}</td></tr>`).join('');
    _mtrEl('mtr-horas').innerHTML = d.horas.map(h => `<tr><td>${h.hora}h</td><td class="mt-num">${_mtNum(h.atendentes)}</td><td class="mt-num">${h.motor_enviados ? _mtNum(h.motor_enviados) : '—'}</td><td class="mt-num">${_mtNum(h.motor_pico)}</td><td class="mt-num">${h.real_enviados ? _mtNum(h.real_enviados) : '—'}</td><td class="mt-num">${_mtNum(h.real_pico)}</td></tr>`).join('');
    _mtrEl('mtr-lotes').innerHTML = d.lotes.map(l => `<tr><td>${l.n}</td><td class="mtr-fixo">${l.inicio} às ${l.fim}</td><td class="mt-num">${_mtNum(l.enviados)}</td><td class="mt-num">${_mtNum(l.fila)}</td><td class="mt-num">${_mtNum(l.atendentes)}</td></tr>`).join('')
        || '<tr><td colspan="5">Nenhum lote saiu.</td></tr>';
    _mtrEl('mtr-trechos').innerHTML = d.trechos.map(t => {
        const [rotulo, classe] = MTR_ESTADO[t.estado] || [t.estado, 'mt-neutro'];
        return `<tr><td class="mtr-fixo">${t.de} às ${t.ate}</td><td class="mt-num">${_mtNum(t.minutos)}</td><td class="mtr-fixo"><span class="${classe}">${rotulo}</span></td><td title="${_mtEsc(t.motivo)}">${_mtEsc(t.causa)}</td></tr>`;
    }).join('');
    _mtMostrar('mtr-trechos-vazio', !d.trechos.length);
    _mtrEl('mtr-avisos').innerHTML = d.avisos.map(a => `<p>${_mtEsc(a)}</p>`).join('');
    _mtMostrar('mtr-resultado', true);
    _mtrGrafico(d);
}

function _mtrHm(minuto) { return String(Math.floor(minuto / 60)).padStart(2, '0') + ':' + String(minuto % 60).padStart(2, '0'); }

let _mtrFilme = null;   // animação em curso: { quadro, aoParar }
let _mtrObservador = null;

// Toda rodada começa passando o dia como filme. Se o gráfico não está na tela, o filme espera ele aparecer.
function _mtrAutoFilme(rolar) {
    const alvo = _mtrEl('mtr-grafico');
    if (_mtrObservador) { _mtrObservador.disconnect(); _mtrObservador = null; }
    if (rolar) _mtrEl('mtr-filme').scrollIntoView({ block: 'start' });   // título, relógio e gráfico inteiros na tela
    const caixa = alvo.getBoundingClientRect();
    if (rolar || (caixa.top < window.innerHeight * 0.8 && caixa.bottom > 0)) { mtrFilme(); return; }
    if (typeof IntersectionObserver !== 'function') return;
    _mtrObservador = new IntersectionObserver(entradas => {
        if (!entradas.some(e => e.isIntersecting)) return;
        _mtrObservador.disconnect(); _mtrObservador = null;
        if (!_mtrFilme) mtrFilme();
    }, { threshold: 0.4 });
    _mtrObservador.observe(alvo);
}

function _mtrGrafico(d) {
    mtrPararFilme();
    // Dois painéis com o mesmo eixo de horas: envios acumulados em cima, fila embaixo (cada um com a sua escala).
    const W = 900, E = 46, D = 14, T1 = 22, H1 = 120, VAO = 34, H2 = 190, B = 24;
    const T2 = T1 + H1 + VAO, H = T2 + H2 + B;
    const s = d.serie, n = s.motor.length;
    const acumular = serie => { let t = 0; return serie.map(v => (t += v)); };
    const envM = acumular(s.motor_envios), envR = acumular(s.real_envios);
    const escala = max => { const passo = [5, 10, 20, 25, 50, 100, 200, 500, 1000, 2000, 2500, 5000, 10000].find(v => max / v <= 4) || 20000; return { passo, topo: Math.max(passo, Math.ceil(max / passo) * passo) }; };
    const eE = escala(Math.max(10, envM[n - 1], envR[n - 1])), eF = escala(Math.max(10, ...s.motor, ...s.real));
    const x = i => E + (W - E - D) * i / (n - 1);
    const yE = v => T1 + H1 * (1 - v / eE.topo), yF = v => T2 + H2 * (1 - v / eF.topo);
    const linha = (serie, y) => serie.map((v, i) => (i ? 'L' : 'M') + x(i).toFixed(1) + ' ' + y(v).toFixed(1)).join('');
    const grade = (e, y) => { let g = ''; for (let v = 0; v <= e.topo; v += e.passo) g += `<line x1="${E}" x2="${W - D}" y1="${y(v)}" y2="${y(v)}" stroke="var(--border)" stroke-width="1"/><text x="${E - 6}" y="${y(v) + 4}" text-anchor="end" font-size="11" fill="var(--text-muted)">${v.toLocaleString('pt-BR')}</text>`; return g; };
    let fundo = grade(eE, yE) + grade(eF, yF)
        + `<text x="${E}" y="${T1 - 8}" font-size="12" font-weight="600" fill="var(--text-primary)">Envios acumulados</text>`
        + `<text x="${E}" y="${T2 - 8}" font-size="12" font-weight="600" fill="var(--text-primary)">Conversas esperando atendente</text>`;
    for (let h = 8; h <= 22; h += 2) fundo += `<text x="${x(h * 60 - s.de_min)}" y="${H - 6}" text-anchor="middle" font-size="11" fill="var(--text-muted)">${h}h</text>`;

    const minutoDe = hm => { const [a, b] = hm.split(':').map(Number); return a * 60 + b - s.de_min; };
    let dados = '';
    d.lotes.forEach(l => {
        const i = minutoDe(l.inicio);
        if (i >= 0 && i < n) dados += `<line x1="${x(i)}" x2="${x(i)}" y1="${T2 + H2}" y2="${T2 + H2 - 9}" stroke="var(--text-muted)" stroke-width="2"/>`;
    });
    const pico = (serie, cor, acima) => {
        const v = Math.max(...serie), i = serie.indexOf(v), ancora = x(i) > W - 90 ? 'end' : 'start';
        return `<circle cx="${x(i)}" cy="${yF(v)}" r="4" fill="${cor}" stroke="var(--bg-card)" stroke-width="2"/>`
             + `<text x="${x(i) + (ancora === 'end' ? -8 : 8)}" y="${yF(v) + (acima ? -6 : 14)}" text-anchor="${ancora}" font-size="12" font-weight="600" fill="var(--text-primary)">${v} às ${_mtrHm(i + s.de_min)}</text>`;
    };
    const tracos = (a, b, y) => `<path d="${linha(a, y)}" fill="none" stroke="var(--mtr-real)" stroke-width="2" stroke-linejoin="round"/><path d="${linha(b, y)}" fill="none" stroke="var(--mtr-motor)" stroke-width="2" stroke-linejoin="round"/>`;
    dados += tracos(envR, envM, yE) + tracos(s.real, s.motor, yF)
        + pico(s.real, 'var(--mtr-real)', true) + pico(s.motor, 'var(--mtr-motor)', Math.max(...s.motor) < Math.max(...s.real) * 0.85 || Math.max(...s.motor) > Math.max(...s.real));
    _mtrEl('mtr-grafico').innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Envios acumulados e conversas esperando atendente ao longo do dia: motor e dia real">`
        + `<defs><clipPath id="mtr-corte"><rect id="mtr-corte-area" x="0" y="0" width="${W}" height="${H}"/></clipPath></defs>${fundo}`
        + `<g clip-path="url(#mtr-corte)">${dados}</g>`
        + `<line id="mtr-cursor" x1="0" x2="0" y1="${T1}" y2="${T2 + H2}" stroke="var(--text-muted)" stroke-width="1" visibility="hidden"/>`
        + `<rect id="mtr-alvo" x="${E}" y="${T1}" width="${W - E - D}" height="${T2 + H2 - T1}" fill="transparent"/></svg>`;

    // Estado do motor em cada minuto, para o relógio do filme.
    const estados = new Array(n).fill('');
    d.trechos.forEach(t => { for (let i = Math.max(0, minutoDe(t.de)); i < Math.min(n, minutoDe(t.ate)); i++) estados[i] = (t.estado === 'pausado' ? 'pausado: ' : 'segurando: ') + t.causa; });
    const primeiro = s.motor_envios.findIndex(v => v > 0), total = envM[n - 1];
    const estadoEm = i => s.motor_envios[i] > 0 ? 'enviando' : estados[i] || (primeiro < 0 || i < primeiro ? 'aguardando o início' : (envM[i] >= total ? 'envio do dia concluído' : 'entre lotes'));
    const relogio = i => `${_mtrHm(i + s.de_min)} · motor enviou ${envM[i].toLocaleString('pt-BR')} de ${total.toLocaleString('pt-BR')} · fila ${s.motor[i]} · ${estadoEm(i)}`;

    const alvo = _mtrEl('mtr-alvo'), cursor = _mtrEl('mtr-cursor'), dica = _mtrEl('mtr-dica'), area = _mtrEl('mtr-corte-area');
    const mover = i => { cursor.setAttribute('x1', x(i)); cursor.setAttribute('x2', x(i)); cursor.setAttribute('visibility', 'visible'); };
    alvo.addEventListener('mousemove', ev => {
        if (_mtrFilme) return;
        const caixa = alvo.getBoundingClientRect();
        const i = Math.max(0, Math.min(n - 1, Math.round((ev.clientX - caixa.left) / caixa.width * (n - 1))));
        mover(i);
        dica.innerHTML = `<strong>${_mtrHm(i + s.de_min)}</strong><br><span class="mtr-chave" style="background: var(--mtr-motor)"></span>Motor: ${envM[i].toLocaleString('pt-BR')} envios · fila ${s.motor[i]}`
            + `<br><span class="mtr-chave" style="background: var(--mtr-real)"></span>Dia real: ${envR[i].toLocaleString('pt-BR')} envios · fila ${s.real[i]}`;
        dica.classList.remove('hidden');
        const pai = dica.parentElement.getBoundingClientRect(), esquerda = ev.clientX - pai.left;
        dica.style.left = (esquerda > pai.width - 250 ? esquerda - dica.offsetWidth - 12 : esquerda + 12) + 'px';
        dica.style.top = Math.max(0, ev.clientY - pai.top - 30) + 'px';
    });
    alvo.addEventListener('mouseleave', () => { if (_mtrFilme) return; cursor.setAttribute('visibility', 'hidden'); dica.classList.add('hidden'); });
    _mtrEl('mtr-relogio').textContent = '';
    _mtrGrafico.filme = { n, x, W, area, mover, relogio, cursor };
}

function mtrPararFilme() {
    if (!_mtrFilme) return;
    cancelAnimationFrame(_mtrFilme.quadro);
    _mtrFilme.aoParar();
    _mtrFilme = null;
}

// Passa o dia como um filme: o gráfico vai sendo revelado das 8h às 22h, com o relógio e o estado do motor.
function mtrFilme() {
    if (_mtrFilme) { mtrPararFilme(); return; }
    const f = _mtrGrafico.filme, botao = _mtrEl('mtr-filme');
    if (!f) return;
    const duracao = Number(_mtrEl('mtr-filme-dur').value || 30) * 1000, inicio = performance.now();
    _mtrEl('mtr-dica').classList.add('hidden');
    botao.textContent = '■ Mostrar tudo';
    const quadro = agora => {
        const i = Math.min(f.n - 1, Math.floor((agora - inicio) / duracao * (f.n - 1)));
        f.area.setAttribute('width', f.x(i));
        f.mover(i);
        _mtrEl('mtr-relogio').textContent = f.relogio(i);
        if (i >= f.n - 1) { mtrPararFilme(); return; }
        _mtrFilme.quadro = requestAnimationFrame(quadro);
    };
    _mtrFilme = {
        quadro: 0,
        aoParar: () => { f.area.setAttribute('width', f.W); f.cursor.setAttribute('visibility', 'hidden'); botao.textContent = '▶ Assistir de novo'; },
    };
    _mtrFilme.quadro = requestAnimationFrame(quadro);
}
