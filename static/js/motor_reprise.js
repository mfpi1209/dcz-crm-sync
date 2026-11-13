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
        mtrRodar();
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

async function mtrRodar() {
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

function _mtrGrafico(d) {
    const W = 900, H = 250, E = 34, D = 14, T = 16, B = 24;
    const s = d.serie, n = s.motor.length, maxY = Math.max(10, ...s.motor, ...s.real);
    const passo = [5, 10, 20, 25, 50, 100, 200, 500].find(v => maxY / v <= 5) || 1000;
    const topo = Math.ceil(maxY / passo) * passo;
    const x = i => E + (W - E - D) * i / (n - 1);
    const y = v => T + (H - T - B) * (1 - v / topo);
    const linha = serie => serie.map((v, i) => (i ? 'L' : 'M') + x(i).toFixed(1) + ' ' + y(v).toFixed(1)).join('');
    let g = '';
    for (let v = 0; v <= topo; v += passo) {
        g += `<line x1="${E}" x2="${W - D}" y1="${y(v)}" y2="${y(v)}" stroke="var(--border)" stroke-width="1"/>`
           + `<text x="${E - 6}" y="${y(v) + 4}" text-anchor="end" font-size="11" fill="var(--text-muted)">${v}</text>`;
    }
    for (let h = 8; h <= 22; h += 2) {
        g += `<text x="${x((h * 60) - s.de_min)}" y="${H - 6}" text-anchor="middle" font-size="11" fill="var(--text-muted)">${h}h</text>`;
    }
    const minutoDe = hm => { const [a, b] = hm.split(':').map(Number); return a * 60 + b - s.de_min; };
    d.lotes.forEach(l => {
        const i = minutoDe(l.inicio);
        if (i >= 0 && i < n) g += `<line x1="${x(i)}" x2="${x(i)}" y1="${H - B}" y2="${H - B - 9}" stroke="var(--text-muted)" stroke-width="2"/>`;
    });
    const pico = (serie, cor, acima) => {
        const v = Math.max(...serie), i = serie.indexOf(v);
        const ancora = x(i) > W - 90 ? 'end' : 'start';
        return `<circle cx="${x(i)}" cy="${y(v)}" r="4" fill="${cor}" stroke="var(--bg-card)" stroke-width="2"/>`
             + `<text x="${x(i) + (ancora === 'end' ? -8 : 8)}" y="${y(v) + (acima ? -6 : 14)}" text-anchor="${ancora}" font-size="12" font-weight="600" fill="var(--text-primary)">${v} às ${_mtrHm(i + s.de_min)}</text>`;
    };
    g += `<path d="${linha(s.real)}" fill="none" stroke="var(--mtr-real)" stroke-width="2" stroke-linejoin="round"/>`
       + `<path d="${linha(s.motor)}" fill="none" stroke="var(--mtr-motor)" stroke-width="2" stroke-linejoin="round"/>`
       + pico(s.real, 'var(--mtr-real)', true) + pico(s.motor, 'var(--mtr-motor)', Math.max(...s.motor) < Math.max(...s.real) * 0.85 || Math.max(...s.motor) > Math.max(...s.real))
       + `<line id="mtr-cursor" x1="0" x2="0" y1="${T}" y2="${H - B}" stroke="var(--text-muted)" stroke-width="1" visibility="hidden"/>`
       + `<rect id="mtr-alvo" x="${E}" y="${T}" width="${W - E - D}" height="${H - T - B}" fill="transparent"/>`;
    _mtrEl('mtr-grafico').innerHTML = `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Conversas esperando atendente por minuto: motor e dia real">${g}</svg>`;

    const alvo = _mtrEl('mtr-alvo'), cursor = _mtrEl('mtr-cursor'), dica = _mtrEl('mtr-dica');
    alvo.addEventListener('mousemove', ev => {
        const caixa = alvo.getBoundingClientRect();
        const i = Math.max(0, Math.min(n - 1, Math.round((ev.clientX - caixa.left) / caixa.width * (n - 1))));
        cursor.setAttribute('x1', x(i)); cursor.setAttribute('x2', x(i)); cursor.setAttribute('visibility', 'visible');
        dica.innerHTML = `<strong>${_mtrHm(i + s.de_min)}</strong><br><span class="mtr-chave" style="background: var(--mtr-motor)"></span>Motor: ${s.motor[i]}<br><span class="mtr-chave" style="background: var(--mtr-real)"></span>Dia real: ${s.real[i]}`;
        dica.classList.remove('hidden');
        const pai = dica.parentElement.getBoundingClientRect();
        const esquerda = ev.clientX - pai.left;
        dica.style.left = (esquerda > pai.width - 170 ? esquerda - dica.offsetWidth - 12 : esquerda + 12) + 'px';
        dica.style.top = Math.max(0, ev.clientY - pai.top - 30) + 'px';
    });
    alvo.addEventListener('mouseleave', () => { cursor.setAttribute('visibility', 'hidden'); dica.classList.add('hidden'); });
}
