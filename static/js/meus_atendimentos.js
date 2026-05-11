// ============================================================================
// Meus Atendimentos — painel individual do consultor (mesma fonte do Feedback)
// Hierarquia: admin/Supervisor Acadêmico veem tudo; demais só veem o seu.
// ============================================================================

(function () {
    let _ma = {
        me: null,              // { is_admin, kommo_user_id, consultor_nome, categoria }
        mePromise: null,
        chart: null,
        rangeData: null,       // payload do período atual
        prevData: null,        // payload do mês anterior (para delta)
        consultor: null,       // consultor selecionado (admin/supervisor)
        loaded: false,
        fetchToken: 0,         // incrementado a cada chamada → descarta respostas atrasadas
        consultoresAll: [],    // cache da lista completa de consultores (vinda sem filtro)
    };

    const FB_CANON = {
        'danubia':'Danubia','danúbia':'Danubia',
        'debora mani moreira':'Débora Mani Moreira',
        'débora mani moreira':'Débora Mani Moreira',
        'emanuel felipe':'Emanuel Felipe','emnauel felipe':'Emanuel Felipe',
        'maitê carine da silva':'Maitê Carine da Silva',
        'maite carine da silva':'Maitê Carine da Silva',
        // unificação solicitada (mesma pessoa, nomes variantes vindos do n8n)
        'felipe':'Felipe Guimarães','felipe guimaraes':'Felipe Guimarães',
        'felipe guimarães':'Felipe Guimarães',
        'marilia':'Marilia Souza','marilia souza':'Marilia Souza',
    };
    function _norm(s) { return (s||'').normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim().toLowerCase(); }
    function _canon(n) { if (!n) return n; const k = _norm(n); return FB_CANON[k] || n; }
    function _equalsName(a, b) { return _norm(_canon(a)) === _norm(_canon(b)); }

    function _fmtNum(v) {
        if (v == null || isNaN(v)) return '—';
        return new Intl.NumberFormat('pt-BR').format(Number(v));
    }
    function _fmtDec(v, d=1) {
        if (v == null || isNaN(v)) return '—';
        return Number(v).toFixed(d);
    }
    function _fmtMin(v) {
        if (v == null || isNaN(v) || v < 0) return '—';
        const m = Math.floor(v);
        const s = Math.round((v - m) * 60);
        return m + 'm' + (s ? ' ' + s + 's' : '');
    }
    function _isoDate(d) {
        return d.getFullYear() + '-' + String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0');
    }

    function _maAlert(msg) {
        const box = document.getElementById('ma-alert');
        const txt = document.getElementById('ma-alert-text');
        if (!box || !txt) return;
        if (!msg) { box.classList.add('hidden'); return; }
        txt.textContent = msg;
        box.classList.remove('hidden');
    }
    function _maLoading(on) {
        const el = document.getElementById('ma-loading');
        if (el) el.classList.toggle('hidden', !on);
    }

    function _maInitDates() {
        const today = new Date();
        const start = new Date(); start.setDate(start.getDate() - 6);
        document.getElementById('ma-start').value = _isoDate(start);
        document.getElementById('ma-end').value   = _isoDate(today);
    }

    window.maPreset = function (days) {
        const today = new Date();
        const start = new Date(); start.setDate(start.getDate() - (days - 1));
        document.getElementById('ma-start').value = _isoDate(start);
        document.getElementById('ma-end').value   = _isoDate(today);
        document.querySelectorAll('.ma-tab[data-preset]').forEach(b => {
            b.classList.toggle('is-active', String(b.dataset.preset) === String(days));
        });
        maFetch();
    };

    function _loadMe() {
        if (_ma.mePromise) return _ma.mePromise;
        _ma.mePromise = fetch('/api/meus-atendimentos/me', { cache: 'no-store' })
            .then(r => r.ok ? r.json() : null)
            .then(j => {
                if (j && j.ok) {
                    _ma.me = {
                        is_admin: !!j.is_admin,
                        kommo_user_id: j.kommo_user_id || null,
                        consultor_nome: (j.consultor_nome || '').trim() || null,
                        categoria: j.categoria || null,
                    };
                } else {
                    _ma.me = { is_admin:false, kommo_user_id:null, consultor_nome:null, categoria:null };
                }
                return _ma.me;
            })
            .catch(() => {
                _ma.me = { is_admin:false, kommo_user_id:null, consultor_nome:null, categoria:null };
                return _ma.me;
            });
        return _ma.mePromise;
    }

    function _applyUiPermissions() {
        const helloEl = document.getElementById('ma-hello');
        if (helloEl) {
            const cat = _ma.me?.categoria;
            const nome = _ma.me?.consultor_nome || _ma.me?.categoria || 'Consultor';
            if (_ma.me?.is_admin) {
                helloEl.textContent = 'Visão geral — você pode filtrar pelo consultor desejado.';
            } else {
                helloEl.textContent = 'Bem-vindo, ' + nome + '. Aqui está o seu desempenho recente.';
            }
        }
        const bar = document.getElementById('ma-cons-bar');
        if (bar) bar.classList.toggle('hidden', !_ma.me?.is_admin);
        const sel = document.getElementById('ma-cons-select');
        if (sel && !sel.dataset.listening) {
            sel.dataset.listening = '1';
            let _debounceId = null;
            sel.addEventListener('change', () => {
                _ma.consultor = (sel.value || '').trim() || null;
                if (_debounceId) clearTimeout(_debounceId);
                _debounceId = setTimeout(() => { _debounceId = null; maFetch(); }, 180);
            });
        }
    }

    function _populateConsultorSelect(consultores) {
        if (!_ma.me?.is_admin) return;
        const sel = document.getElementById('ma-cons-select');
        if (!sel) return;

        // Atualiza cache se vier uma lista maior do que a atual (resposta sem filtro)
        const incoming = (consultores || []).map(c => c?.consultor).filter(Boolean);
        if (incoming.length > (_ma.consultoresAll || []).length) {
            _ma.consultoresAll = incoming.slice();
        }

        const cur = (sel.value || '').trim() || _ma.consultor || '';
        const names = new Set();
        (_ma.consultoresAll || []).forEach(name => names.add(_canon(name)));
        incoming.forEach(name => names.add(_canon(name)));
        if (cur) names.add(cur);

        const sorted = Array.from(names).sort((a, b) => a.localeCompare(b, 'pt-BR'));
        const opts = ['<option value="">Todos os consultores</option>']
            .concat(sorted.map(n => '<option value="' + n + '">' + n + '</option>'));
        sel.innerHTML = opts.join('');
        sel.value = cur;
    }

    async function _fetchRange(start, end, consultor) {
        const qs = new URLSearchParams();
        qs.set('start', start);
        qs.set('end', end);
        qs.set('topN', '5');
        if (consultor) qs.set('consultor', consultor);
        const r = await fetch('/api/meus-atendimentos?' + qs.toString(), { cache: 'no-store' });
        if (!r.ok) {
            let msg = 'HTTP ' + r.status;
            try {
                const j = await r.json();
                if (j?.hint) msg = j.hint;
                else if (j?.error) msg = j.error;
            } catch (_) { /* ignore */ }
            const err = new Error(msg);
            err.status = r.status;
            throw err;
        }
        return await r.json();
    }

    function _pickDetalhe(data) {
        if (!data) return null;
        if (data.consultor_detalhe) return data.consultor_detalhe;
        if (data.detalhe) return data.detalhe;
        if (Array.isArray(data.consultores) && data.consultores.length) {
            // se houver apenas um consultor (caso ACL/forçado) → usar ele
            const target = _ma.me?.is_admin ? _ma.consultor : _ma.me?.consultor_nome;
            if (target) {
                const c = data.consultores.find(x => _equalsName(x.consultor, target));
                if (c) return c;
            }
            if (data.consultores.length === 1) return data.consultores[0];
        }
        return null;
    }

    function _serieDia(payload, det) {
        if (!payload && !det) return [];
        if (det && det.serie_dia) return det.serie_dia;
        if (det && det.metricas && det.metricas.serie_dia) return det.metricas.serie_dia;
        if (payload && payload.serie_dia) return payload.serie_dia;
        if (payload && payload.serie_dia_global) return payload.serie_dia_global;
        if (payload && payload.global && payload.global.serie_dia_global) return payload.global.serie_dia_global;
        return [];
    }

    function _metricasFromPayload(payload, det) {
        // det pode estar no formato bruto (consultor) ou wrap {metricas:{...}}
        const m = det?.metricas || det || {};
        const g = payload?.global || {};
        return {
            total_atendimentos:
                m.total_atendimentos ?? g.total_atendimentos ?? 0,
            notas_informadas:
                m.notas_informadas ?? g.notas_informadas ?? 0,
            nota_media:
                m.nota_media ?? g.nota_media ?? null,
            tempo_medio_resposta_min:
                m.tempo_medio_resposta_min ?? g.tempo_medio_resposta_min ?? null,
            tempo_medio_atendimento_min:
                m.tempo_medio_atendimento_min ?? g.tempo_medio_atendimento_min ?? null,
            feedback_geral:
                m.feedback_geral || det?.feedback_geral || null,
            feedback_positivo:
                m.feedback_positivo || det?.feedback_positivo || null,
            feedback_negativo:
                m.feedback_negativo || det?.feedback_negativo || null,
        };
    }

    function _calcProdutividade(consultores, met) {
        if (!met) return null;
        if (!consultores || !consultores.length) {
            const ta = met.total_atendimentos || 0;
            const sv = Math.min(10, Math.sqrt(ta / 50) * 10);
            const t  = met.tempo_medio_resposta_min;
            let st = null;
            if (t != null) {
                if (t === 0) st = 10;
                else st = Math.min(10, (20 / t) * 7);
            }
            return Math.min(10, st != null ? sv * 0.6 + st * 0.4 : sv);
        }
        const ta = met.total_atendimentos || 0;
        const allTAs = consultores
            .map(c => c.metricas?.total_atendimentos ?? c.total_atendimentos ?? 0)
            .concat([ta]);
        const maxTA = Math.max(...allTAs, 1);
        const tempos = consultores
            .map(c => c.metricas?.tempo_medio_resposta_min ?? c.tempo_medio_resposta_min)
            .filter(v => v != null);
        if (met.tempo_medio_resposta_min != null) tempos.push(met.tempo_medio_resposta_min);
        tempos.sort((a,b) => a-b);
        const medT = tempos.length ? tempos[Math.floor((tempos.length - 1)/2)] : null;
        const sv = Math.min(10, Math.sqrt(ta / maxTA) * 10);
        let st = null;
        if (met.tempo_medio_resposta_min != null && met.tempo_medio_resposta_min > 0 && medT) {
            st = Math.min(10, (medT / met.tempo_medio_resposta_min) * 7);
        } else if (met.tempo_medio_resposta_min === 0) {
            st = 10;
        }
        return Math.min(10, st != null ? sv * 0.6 + st * 0.4 : sv);
    }

    function _renderStars(value) {
        const wrap = document.getElementById('ma-kpi-stars');
        if (!wrap) return;
        const rounded = value == null ? 0 : Math.round(Number(value) * 2) / 2;
        const stars = [];
        // escala 0..5 a partir de 0..10
        const v5 = rounded != null ? rounded / 2 : 0;
        for (let i = 1; i <= 5; i++) {
            const on = i <= Math.round(v5);
            stars.push('<svg class="w-3.5 h-3.5 ' + (on ? 'ma-star-on' : 'ma-star') + '" fill="currentColor" viewBox="0 0 24 24"><path d="M11.049 2.927c.3-.921 1.603-.921 1.902 0l1.286 3.957a1 1 0 00.95.69h4.162c.969 0 1.371 1.24.588 1.81l-3.37 2.449a1 1 0 00-.363 1.118l1.287 3.957c.3.922-.755 1.688-1.54 1.118l-3.37-2.448a1 1 0 00-1.175 0l-3.37 2.448c-.784.57-1.838-.196-1.539-1.118l1.287-3.957a1 1 0 00-.363-1.118L2.05 9.384c-.783-.57-.38-1.81.588-1.81h4.16a1 1 0 00.951-.69l1.287-3.957z"/></svg>');
        }
        wrap.innerHTML = stars.join('');
    }

    function _renderKPIs(payload) {
        const det = _pickDetalhe(payload);
        const met = _metricasFromPayload(payload, det);

        // Tempo de resposta
        document.getElementById('ma-kpi-resp').textContent = _fmtMin(met.tempo_medio_resposta_min);
        const respDelta = document.getElementById('ma-kpi-resp-delta');
        if (met.tempo_medio_resposta_min != null && met.tempo_medio_resposta_min <= 20) {
            respDelta.textContent = 'Dentro da meta'; respDelta.classList.remove('hidden','ma-pill-neu','ma-pill-neg'); respDelta.classList.add('ma-pill-pos');
        } else if (met.tempo_medio_resposta_min != null) {
            respDelta.textContent = 'Acima da meta'; respDelta.classList.remove('hidden','ma-pill-pos','ma-pill-neu'); respDelta.classList.add('ma-pill-neg');
        } else {
            respDelta.classList.add('hidden');
        }

        // Nota
        document.getElementById('ma-kpi-nota').textContent = _fmtDec(met.nota_media, 2);
        document.getElementById('ma-kpi-nota-sub').textContent = _fmtNum(met.notas_informadas) + ' avaliações';
        _renderStars(met.nota_media);

        // Produtividade
        const prod = _calcProdutividade(payload?.consultores, met);
        const prodEl = document.getElementById('ma-kpi-prod');
        const prodSub = document.getElementById('ma-kpi-prod-sub');
        const prodPill = document.getElementById('ma-kpi-prod-pill');
        const prodBar = document.getElementById('ma-kpi-prod-bar');
        if (prod != null && !isNaN(prod)) {
            const pct = Math.max(0, Math.min(100, (prod / 10) * 100));
            prodEl.textContent = pct.toFixed(0) + '%';
            prodSub.textContent = 'Nota: ' + _fmtDec(prod, 1) + ' / 10';
            prodBar.style.width = pct + '%';
            const label = prod >= 7 ? 'ALTA' : prod >= 5 ? 'MÉDIA' : 'BAIXA';
            const cls   = prod >= 7 ? 'ma-pill-pos' : prod >= 5 ? 'ma-pill-neu' : 'ma-pill-neg';
            prodPill.textContent = label;
            prodPill.className = 'ma-pill ' + cls;
            prodPill.classList.remove('hidden');
        } else {
            prodEl.textContent = '—';
            prodSub.textContent = '—';
            prodBar.style.width = '0%';
            prodPill.classList.add('hidden');
        }

        // Atendimento do dia: pega o último ponto da série COM dado (> 0).
        // Se o último ponto for 0 (dia atual incompleto), regride até achar um dia com valor.
        const serie = _serieDia(payload, det);
        const serieVals = (Array.isArray(serie) ? serie : []).map(p => ({
            label: p?.data || p?.dia || p?.date || '',
            v: Number(p?.atendimentos ?? p?.total ?? p?.qtd ?? 0) || 0,
        }));
        let dayIdx = -1;
        for (let i = serieVals.length - 1; i >= 0; i--) {
            if (serieVals[i].v > 0) { dayIdx = i; break; }
        }
        const dayValue = dayIdx >= 0 ? serieVals[dayIdx].v : null;
        const dayPrev  = dayIdx > 0 ? serieVals[dayIdx - 1].v : null;
        const periodAvg = serieVals.length ? Math.round((met.total_atendimentos || 0) / serieVals.length) : null;

        document.getElementById('ma-kpi-day').textContent = dayValue != null ? _fmtNum(dayValue) : '—';
        const daySub = document.getElementById('ma-kpi-day-sub');
        if (dayValue != null && dayIdx >= 0 && serieVals[dayIdx].label) {
            const d = new Date(serieVals[dayIdx].label);
            const lbl = isNaN(d) ? String(serieVals[dayIdx].label) : d.toLocaleDateString('pt-BR', { day: '2-digit', month: 'short' });
            daySub.textContent = 'Último dia: ' + lbl + (periodAvg != null ? ' • média ' + _fmtNum(periodAvg) + '/dia' : '');
        } else {
            daySub.textContent = periodAvg != null ? 'Média do período: ' + _fmtNum(periodAvg) + '/dia' : 'Sem dados';
        }
        const dayDelta = document.getElementById('ma-kpi-day-delta');
        if (dayValue != null && dayPrev != null && dayPrev > 0) {
            const pct = ((dayValue - dayPrev) / dayPrev) * 100;
            const sign = pct >= 0 ? '+' : '';
            dayDelta.textContent = sign + pct.toFixed(1) + '%';
            dayDelta.classList.remove('hidden','ma-pill-neg','ma-pill-neu','ma-pill-pos');
            dayDelta.classList.add(pct >= 0 ? 'ma-pill-pos' : 'ma-pill-neg');
        } else {
            dayDelta.classList.add('hidden');
        }
    }

    function _renderChart(payload) {
        const det = _pickDetalhe(payload);
        const serie = _serieDia(payload, det);
        const canvas = document.getElementById('ma-chart');
        if (!canvas) return;
        if (_ma.chart) { _ma.chart.destroy(); _ma.chart = null; }
        if (!Array.isArray(serie) || !serie.length) {
            const ctx = canvas.getContext('2d');
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            return;
        }
        const labels = serie.map(p => {
            const raw = p.data || p.dia || p.date || '';
            if (!raw) return '';
            const d = new Date(raw);
            return isNaN(d) ? String(raw).slice(5) : d.toLocaleDateString('pt-BR', { day:'2-digit', month:'short' });
        });
        const atend = serie.map(p => Number(p.atendimentos ?? p.total ?? p.qtd ?? 0));
        const nota  = serie.map(p => p.nota_media != null ? Number(p.nota_media) : null);

        const dark = document.documentElement.classList.contains('dark');
        const txt  = dark ? '#94a3b8' : '#475569';
        const grid = dark ? 'rgba(51,65,85,.4)' : '#e2e8f0';

        const ctx = canvas.getContext('2d');
        const gradAt = ctx.createLinearGradient(0, 0, 0, 280);
        gradAt.addColorStop(0, 'rgba(0,45,94,.25)');
        gradAt.addColorStop(1, 'rgba(0,45,94,0)');
        const gradNt = ctx.createLinearGradient(0, 0, 0, 280);
        gradNt.addColorStop(0, 'rgba(16,185,129,.25)');
        gradNt.addColorStop(1, 'rgba(16,185,129,0)');

        _ma.chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Atendimentos',
                        data: atend,
                        yAxisID: 'y',
                        borderColor: '#002D5E',
                        backgroundColor: gradAt,
                        fill: true,
                        tension: .4,
                        borderWidth: 3,
                        pointRadius: 3,
                        pointHoverRadius: 5,
                    },
                    {
                        label: 'Nota média',
                        data: nota,
                        yAxisID: 'y1',
                        borderColor: '#10b981',
                        backgroundColor: gradNt,
                        borderDash: [5, 5],
                        fill: false,
                        tension: .4,
                        borderWidth: 2.5,
                        pointRadius: 3,
                        pointHoverRadius: 5,
                        spanGaps: true,
                    },
                ],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        backgroundColor: dark ? 'rgba(15,23,42,.95)' : '#fff',
                        titleColor: dark ? '#f1f5f9' : '#0f172a',
                        bodyColor:  dark ? '#cbd5e1' : '#475569',
                        borderColor: grid, borderWidth: 1,
                        padding: 10,
                    },
                },
                scales: {
                    x: { ticks: { color: txt, font: { size: 10, weight: 700 } }, grid: { display:false } },
                    y: { position: 'left', ticks: { color: txt, font: { size: 11 } }, grid: { color: grid }, beginAtZero: true, title:{ display:false } },
                    y1:{ position: 'right', min:0, max:10, ticks:{ color: txt, font:{ size:11 } }, grid:{ drawOnChartArea:false }, title:{ display:false } },
                },
            },
        });
    }

    function _renderMonthTotal(rangeData, prevData) {
        const detR = _pickDetalhe(rangeData);
        const metR = _metricasFromPayload(rangeData, detR);
        document.getElementById('ma-month-total').textContent = _fmtNum(metR.total_atendimentos);

        const detP = _pickDetalhe(prevData);
        const metP = _metricasFromPayload(prevData, detP);
        const cur = metR.total_atendimentos || 0;
        const prev = metP.total_atendimentos || 0;
        const el = document.getElementById('ma-month-delta');
        if (prev > 0) {
            const pct = ((cur - prev) / prev) * 100;
            const sign = pct >= 0 ? '+' : '';
            el.textContent = sign + pct.toFixed(1) + '% vs período anterior';
            el.style.color = pct >= 0 ? '#34d399' : '#fca5a5';
        } else if (cur > 0) {
            el.textContent = 'Sem dado anterior';
            el.style.color = '#cbd5e1';
        } else {
            el.textContent = '— vs período anterior';
            el.style.color = '#cbd5e1';
        }
    }

    function _renderFeedback(payload) {
        const det = _pickDetalhe(payload);
        const m = _metricasFromPayload(payload, det);
        const wrap = document.getElementById('ma-feedback');
        if (!wrap) return;
        const hasAny = !!(m.feedback_geral || m.feedback_positivo || m.feedback_negativo);
        wrap.classList.toggle('hidden', !hasAny);
        if (!hasAny) return;
        document.getElementById('ma-feed-geral').textContent = m.feedback_geral || '—';
        document.getElementById('ma-feed-pos').textContent   = m.feedback_positivo || '—';
        document.getElementById('ma-feed-neg').textContent   = m.feedback_negativo || '—';
    }

    window.maFetch = async function () {
        const start = (document.getElementById('ma-start').value || '').trim();
        const end   = (document.getElementById('ma-end').value || '').trim();
        if (!start || !end) { _maAlert('Selecione as datas.'); return; }
        _maAlert(null);
        _maLoading(true);

        const myToken = ++_ma.fetchToken;
        try {
            const sd = new Date(start), ed = new Date(end);
            const ms = ed - sd;
            const prevEnd   = new Date(sd.getTime() - 86400000);
            const prevStart = new Date(prevEnd.getTime() - ms);

            let consultor = null;
            if (_ma.me?.is_admin) {
                const sel = document.getElementById('ma-cons-select');
                const fromSel = sel ? (sel.value || '').trim() : '';
                consultor = fromSel || _ma.consultor || null;
                _ma.consultor = consultor;
            }

            const info = document.getElementById('ma-cons-info');
            if (info) {
                if (_ma.me?.is_admin && !consultor) info.textContent = 'Carregando visão global…';
                else if (_ma.me?.is_admin && consultor) info.textContent = 'Carregando ' + consultor + '…';
                else info.textContent = '';
            }

            const [cur, prev] = await Promise.all([
                _fetchRange(start, end, consultor),
                _fetchRange(_isoDate(prevStart), _isoDate(prevEnd), consultor).catch(() => null),
            ]);

            // Se outro fetch mais novo já disparou, descarta este resultado.
            if (myToken !== _ma.fetchToken) return;

            _ma.rangeData = cur;
            _ma.prevData  = prev;
            if (cur && cur.consultores) _populateConsultorSelect(cur.consultores);

            _renderKPIs(cur);
            _renderChart(cur);
            _renderMonthTotal(cur, prev);
            _renderFeedback(cur);

            if (info) {
                if (_ma.me?.is_admin && !consultor) info.textContent = 'Visão global do período.';
                else if (_ma.me?.is_admin && consultor) info.textContent = 'Filtrando por ' + consultor + '.';
                else info.textContent = '';
            }
        } catch (e) {
            if (myToken !== _ma.fetchToken) return;
            _maAlert('Erro ao carregar dados: ' + (e?.message || e));
        } finally {
            if (myToken === _ma.fetchToken) _maLoading(false);
        }
    };

    window.loadMeusAtendimentos = function () {
        if (_ma.loaded) return;
        _ma.loaded = true;
        _maInitDates();
        _loadMe().then(() => {
            _applyUiPermissions();
            maFetch();
        });
    };
})();
