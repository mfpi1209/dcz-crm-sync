import type { ChartConfiguration } from "chart.js";
import { useMemo } from "react";
import { Barra, COR_TIER, Secao, Selo, TEXTO_FRACO } from "./comum";
import { data, dinheiro, potencialMaximo, ROTULO_TIER } from "./formato";
import { Grafico } from "./Grafico";
import { MensagemServidor } from "./Mensagem";
import type { Historico, Insights } from "./tipos";

/* Cada bloco segue o _mpRender* de mesmo nome em static/js/minha_performance.js. */

/** _mpRenderHero: saldo, potencial, nível, matrículas, dias e o medidor (aqui em barra). */
export function Saldo({ d }: { d: Insights }) {
  const prem = d.premiacao || {};
  const total = prem.total || 0;
  const tier = String(d.tier || "base");
  const metas = d.metas || {};
  const totalMat = d.total_matriculas || 0;
  const potencial = potencialMaximo(d);
  const sup = metas.supermeta || metas.meta || metas.intermediaria || 1;
  const pct = Math.min(Math.round((totalMat / sup) * 100), 100);
  const cor = COR_TIER[tier] || COR_TIER.base;
  return (
    <section aria-label="Saldo" className="cartao p-6 grid gap-6 md:grid-cols-[1fr_260px] items-center">
      <div className="flex flex-col gap-2 min-w-0">
        <p className="text-xs" style={TEXTO_FRACO}>
          {d.campanha?.nome}
        </p>
        <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
          Seu saldo acumulado
        </p>
        <p className="text-4xl font-headline font-extrabold tabular-nums">{dinheiro(total)}</p>
        {potencial > total && (
          <p className="text-sm" style={TEXTO_FRACO}>
            pode chegar a <span className="font-semibold text-emerald-700 dark:text-emerald-400">{dinheiro(potencial)}</span>
          </p>
        )}
        <div className="flex flex-wrap items-center gap-2 mt-1">
          <Selo cor={cor.selo}>{ROTULO_TIER[tier] || tier || "Base"}</Selo>
          <span className="text-sm">{`${totalMat} matrículas`}</span>
          <span className="text-sm" style={TEXTO_FRACO}>
            {`${d.dias_restantes || 0} dias restantes`}
          </span>
        </div>
        {d.mensagem && (
          <p className="text-sm leading-relaxed mt-1">
            <MensagemServidor html={d.mensagem} />
          </p>
        )}
      </div>
      <div className="flex flex-col gap-2" role="group" aria-label="Progresso até a supermeta">
        <div className="flex items-baseline justify-between">
          <span className="text-xs" style={TEXTO_FRACO}>{`${totalMat} / ${sup}`}</span>
          <span className="text-3xl font-headline font-extrabold tabular-nums">{`${pct}%`}</span>
        </div>
        <Barra pct={pct} cor={cor.barra} altura="h-3" />
      </div>
    </section>
  );
}

const NIVEIS: Record<string, { rotulo: string; selo: string; icone: string }> = {
  intermediaria: { rotulo: "Intermediária", selo: "Bronze", icone: "workspace_premium" },
  meta: { rotulo: "Meta", selo: "Prata", icone: "military_tech" },
  supermeta: { rotulo: "Supermeta", selo: "Ouro", icone: "emoji_events" },
};

/** _mpRenderIncentiveTiers. */
export function NiveisDeMeta({ d }: { d: Insights }) {
  const progresso = (d.tier_progress || []).filter((p) => p.tier !== "base");
  if (!progresso.length) return null;
  const totalMat = d.total_matriculas || 0;
  return (
    <Secao titulo="Níveis de meta" icone="workspace_premium" extra={<span className="text-xs font-normal" style={TEXTO_FRACO}>recompensa por nível atingido</span>}>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
        {progresso.map((p) => {
          const n = NIVEIS[p.tier];
          if (!n) return null;
          const cor = COR_TIER[p.tier] || COR_TIER.base;
          const alvo = p.target || 0;
          const pct = Math.min(p.pct || 0, 100);
          const ganhoTotal = (p.valor_por_mat || 0) * alvo;
          const ganhoAtual = p.atingido ? ganhoTotal : (p.valor_por_mat || 0) * Math.min(totalMat, alvo);
          const falta = Math.max(0, alvo - totalMat);
          return (
            <div key={p.tier} className="cartao p-4 flex flex-col gap-3">
              <div className="flex items-start gap-3">
                <span className={`material-symbols-outlined text-[22px] ${cor.texto}`} aria-hidden>
                  {n.icone}
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center justify-between gap-2 flex-wrap">
                    <div className="flex items-center gap-2">
                      <h4 className="text-sm font-bold">{n.rotulo}</h4>
                      <Selo cor={cor.selo}>{n.selo}</Selo>
                    </div>
                    {p.atingido ? (
                      <span className="text-xs font-semibold text-emerald-700 dark:text-emerald-400">Atingido</span>
                    ) : falta > 0 ? (
                      <span className="text-xs" style={TEXTO_FRACO}>
                        faltam <strong style={{ color: "var(--text-primary)" }}>{falta}</strong> mat.
                      </span>
                    ) : null}
                  </div>
                  <p className="text-xs mt-0.5" style={TEXTO_FRACO}>
                    {`${alvo} matrículas · ${dinheiro(p.valor_por_mat || 0)}/mat`}
                  </p>
                </div>
              </div>
              <div className="flex items-end justify-between gap-2">
                <span className="text-xs" style={TEXTO_FRACO}>Recompensa</span>
                <span className="flex items-baseline gap-1">
                  <span className="text-base font-extrabold tabular-nums">{dinheiro(ganhoAtual)}</span>
                  <span className="text-xs" style={TEXTO_FRACO}>{`/ ${dinheiro(ganhoTotal)}`}</span>
                </span>
              </div>
              <Barra pct={pct} cor={cor.barra} />
              <div className="flex items-center justify-between text-xs tabular-nums" style={TEXTO_FRACO}>
                <span>{`${Math.min(totalMat, alvo)}/${alvo}`}</span>
                <span className={`font-bold ${cor.texto}`}>{`${pct}%`}</span>
              </div>
            </div>
          );
        })}
      </div>
    </Secao>
  );
}

/** _mpRenderPixDia (o anel virou número + barra; os textos são os mesmos). */
export function PixDoDia({ d }: { d: Insights }) {
  const pixFila = !!(d.pix_suporte_equipe && d.pix_fonte_fila);
  const notaTime = pixFila ? (
    <p className="text-xs text-violet-700 dark:text-violet-400 mb-1">
      PIX do time: <strong>fila de aceite</strong> agora (negócios em Aceite no Bwipo)
    </p>
  ) : d.pix_suporte_equipe ? (
    <p className="text-xs text-violet-700 dark:text-violet-400 mb-1">PIX do time Suporte</p>
  ) : null;
  const hoje = d.hoje || {};
  const meta = hoje.meta || 0;
  const realizadas = pixFila ? (hoje.aceites_fila ?? hoje.realizadas ?? 0) : hoje.realizadas || 0;
  const fila = hoje.aceites_fila || 0;
  const fixo = hoje.bonus_fixo || 0;
  const extra = hoje.bonus_extra || 0;
  const pct = meta > 0 ? Math.min(Math.round((realizadas / meta) * 100), 100) : 0;
  const batida = realizadas >= meta && meta > 0;
  const ontem = hoje.ontem_realizadas || 0;
  let ontemTxt = "";
  let ontemCor = "";
  if (realizadas > ontem && ontem >= 0) {
    ontemTxt = `📈 +${realizadas - ontem} a mais que ontem — continue assim!`;
    ontemCor = "text-emerald-700 dark:text-emerald-400";
  } else if (realizadas === ontem && realizadas > 0) {
    ontemTxt = "⚡ Mesmo ritmo de ontem — hora de ultrapassar!";
    ontemCor = "text-amber-700 dark:text-amber-400";
  } else if (realizadas < ontem && ontem > 0) {
    ontemTxt = `🔥 Ontem você fez ${ontem} — bora superar!`;
    ontemCor = "text-orange-700 dark:text-orange-400";
  }
  const sufixoFila = fila > 0 ? `${fila} aceite${fila > 1 ? "s" : ""} na fila total` : "";

  let status: string;
  let statusCor = "";
  let detalhe: string;
  let valor = "";
  if (meta <= 0) {
    status = "Sem meta diária hoje";
    detalhe = realizadas > 0 ? `Você já fez ${realizadas} hoje mesmo sem meta!` : "Nenhuma meta PIX configurada para hoje.";
    if (fila > 0) detalhe += ` (${sufixoFila})`;
  } else if (batida) {
    const excedente = Math.max(0, realizadas - meta);
    status = "🎉 PIX Garantido!";
    statusCor = "text-emerald-700 dark:text-emerald-400";
    detalhe = pixFila
      ? `Na fila agora: ${realizadas} aceite${realizadas !== 1 ? "s" : ""}`
      : d.pix_suporte_equipe
        ? `Hoje: ${realizadas} (matrículas do dia)`
        : `Hoje: ${realizadas} (mat + aceites do dia)`;
    detalhe += excedente > 0 ? ` · +${excedente} extra × ${dinheiro(extra)} cada` : " — meta batida!";
    valor = dinheiro(fixo + extra * excedente);
  } else {
    status = `Faltam ${meta - realizadas} para o PIX!`;
    statusCor = "text-cyan-700 dark:text-cyan-400";
    const rotulo = pixFila ? "na fila de aceite" : d.painel_suporte ? "matrículas (data mat.)" : "mat + aceites do dia";
    detalhe = `Hoje: ${realizadas}/${meta} (${rotulo})`;
    valor = `Prêmio: ${dinheiro(fixo)}`;
  }

  return (
    <Secao titulo="PIX do dia" icone="paid">
      <div className="cartao p-6 flex flex-col sm:flex-row items-center gap-6">
        <div className="w-40 flex flex-col items-center gap-2" role="group" aria-label="Realizadas hoje">
          <span className="text-4xl font-headline font-extrabold tabular-nums">{realizadas}</span>
          <span className="text-xs" style={TEXTO_FRACO}>
            {meta > 0 ? `de ${meta}` : "sem meta"}
          </span>
          <div className="w-full">
            <Barra pct={pct} cor={batida ? "bg-emerald-500" : "bg-cyan-500"} />
          </div>
        </div>
        <div className="flex-1 flex flex-col gap-1 min-w-0">
          <p className={`text-lg font-extrabold ${statusCor}`}>{status}</p>
          <div className="text-sm" style={{ color: "var(--text-secondary)" }}>
            {notaTime}
            <p>{detalhe}</p>
            {ontemTxt && <p className={`text-xs font-semibold mt-1 ${ontemCor}`}>{ontemTxt}</p>}
          </div>
          {fila > 0 && (
            <span className="inline-flex items-center gap-1.5 self-start px-3 py-1 rounded-lg text-xs bg-purple-500/10 text-purple-700 dark:text-purple-300 border border-purple-500/25">
              <span className="material-symbols-outlined text-sm" aria-hidden>
                pending
              </span>
              <span>{sufixoFila}</span>
            </span>
          )}
          {valor && <span className="text-2xl font-extrabold tabular-nums text-emerald-700 dark:text-emerald-400">{valor}</span>}
        </div>
      </div>
      <p className="text-xs" style={TEXTO_FRACO}>
        Obs.: nem todos os aceites se convertem em matrícula — considere uma margem de segurança.
      </p>
    </Secao>
  );
}

const MEDALHA: Record<number, { icone: string; texto: string; cor: string }> = {
  1: { icone: "emoji_events", texto: "🏆 Você lidera o ranking!", cor: "text-amber-600 dark:text-amber-400" },
  2: { icone: "workspace_premium", texto: "🥈 Vice-líder!", cor: "text-slate-500 dark:text-slate-300" },
  3: { icone: "workspace_premium", texto: "🥉 Top 3! Pódio!", cor: "text-orange-600 dark:text-orange-400" },
};

/** _mpRenderRanking. */
export function RankingCard({ d }: { d: Insights }) {
  const rk = d.ranking;
  if (!rk || !rk.total_agentes) return null;
  const pos = rk.posicao;
  const diff = rk.diferenca_lider;
  const mat = rk.minhas_mat || 0;
  const ace = rk.meus_aceites || 0;
  const meu = rk.meu_total || mat + ace;
  const media = rk.media_time || 0;
  const m = MEDALHA[pos];
  let motivacao = null;
  if (pos === 1) motivacao = <p className="text-xs mt-2 text-amber-700 dark:text-amber-300">Ninguém te alcançou! Continue dominando! 🔥</p>;
  else if (diff > 0 && diff <= 3)
    motivacao = (
      <p className="text-xs mt-2 font-semibold text-cyan-700 dark:text-cyan-400">
        🔥 Quase lá! Só <strong>{diff}</strong> para o topo!
      </p>
    );
  else if (diff > 0 && diff <= 10)
    motivacao = (
      <p className="text-xs mt-2" style={TEXTO_FRACO}>
        Faltam <strong>{diff}</strong> para o 1°. Cada matrícula conta! 💪
      </p>
    );
  else if (diff > 0)
    motivacao = (
      <p className="text-xs mt-2" style={TEXTO_FRACO}>
        {diff} atrás do líder — foco e consistência! 🚀
      </p>
    );
  let mediaTxt = "";
  let mediaCor = "";
  if (media > 0) {
    const dm = meu - media;
    const abs = Math.abs(dm).toFixed(1);
    if (dm > 1) {
      mediaTxt = `📈 Você está ${abs} acima da média! Continue assim!`;
      mediaCor = "text-emerald-700 dark:text-emerald-400";
    } else if (dm >= -1) {
      mediaTxt = "⚡ Você está na média do time — dá pra mais!";
      mediaCor = "text-amber-700 dark:text-amber-400";
    } else {
      mediaTxt = `🔥 Você está ${abs} abaixo da média — bora reverter esse jogo!`;
      mediaCor = "text-orange-700 dark:text-orange-400";
    }
  }
  return (
    <Secao titulo="Ranking" icone="leaderboard" extra={<span className="text-xs font-normal" style={TEXTO_FRACO}>mat + aceites</span>} className="cartao p-5">
      <div className="flex items-center gap-5">
        <div className="w-16 h-16 rounded-2xl flex items-center justify-center border" style={{ borderColor: "var(--border)", background: "var(--bg-elevated)" }}>
          {m ? (
            <span className={`material-symbols-outlined text-4xl ${m.cor}`} aria-hidden>
              {m.icone}
            </span>
          ) : (
            <span className="text-2xl font-extrabold" aria-hidden>
              {`${pos}°`}
            </span>
          )}
        </div>
        <div className="flex-1">
          <p className="text-3xl font-headline font-extrabold">{`${pos}°`}</p>
          <p className="text-sm" style={TEXTO_FRACO}>{`de ${rk.total_agentes}`}</p>
          <p className="text-xs mt-1" style={TEXTO_FRACO}>
            {`${mat} mat${ace > 0 ? ` + ${ace} aceite${ace > 1 ? "s" : ""}` : ""}`}
          </p>
          {m && <p className={`text-xs font-bold mt-1 ${m.cor}`}>{m.texto}</p>}
          {motivacao}
        </div>
      </div>
      {mediaTxt && (
        <div className="p-3 rounded-lg" style={{ background: "var(--bg-elevated)" }}>
          <p className="text-xs" style={TEXTO_FRACO}>
            Média do time: <strong style={{ color: "var(--text-primary)" }}>{media}</strong>
          </p>
          <p className={`text-xs font-semibold mt-0.5 ${mediaCor}`}>{mediaTxt}</p>
        </div>
      )}
    </Secao>
  );
}

/** _mpRenderConquistas. */
export function Conquistas({ d }: { d: Insights }) {
  const lista = d.conquistas || [];
  return (
    <Secao titulo="Conquistas" icone="workspace_premium" className="cartao p-5">
      {lista.length ? (
        <div className="flex flex-wrap gap-3">
          {lista.map((a) => (
            <div key={a.id} title={a.desc || a.nome} className="w-[92px] flex flex-col items-center gap-2 p-3 rounded-xl border" style={{ borderColor: "var(--border)", background: "var(--bg-elevated)" }}>
              <span className="material-symbols-outlined text-3xl" style={{ color: "var(--primary)" }} aria-hidden>
                {a.icone}
              </span>
              <span className="text-[11px] text-center leading-tight font-semibold">{a.nome}</span>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-center py-6">
          <p className="text-sm font-semibold">Suas conquistas aparecem aqui! 🚀</p>
          <p className="text-xs mt-1" style={TEXTO_FRACO}>
            Faça matrículas, bata metas e suba no ranking.
          </p>
        </div>
      )}
    </Secao>
  );
}

/** _mpRenderDesbloqueie. */
export function Desbloqueie({ d }: { d: Insights }) {
  const desb = d.premiacao?.desbloqueie || [];
  if (!desb.length || desb.every((t) => t.atingido)) return null;
  const abertos = desb.filter((t) => !t.atingido);
  return (
    <Secao titulo="Desbloqueie mais" icone="lock_open">
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {abertos.map((t, i) => {
          const cor = COR_TIER[t.tier] || COR_TIER.meta;
          return (
            <div key={t.tier} className="cartao p-4 relative" style={i === 0 ? { borderColor: "var(--primary)" } : undefined}>
              {i === 0 && (
                <span className="absolute top-2 right-2">
                  <Selo cor="bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border-emerald-500/30">Próximo</Selo>
                </span>
              )}
              <p className={`text-xs font-semibold mb-1 ${cor.texto}`}>{ROTULO_TIER[t.tier] || t.tier}</p>
              <p className="text-2xl font-extrabold tabular-nums mb-1">{`+${dinheiro(t.ganho_adicional)}`}</p>
              <p className="text-xs" style={{ color: "var(--text-secondary)" }}>
                Faltam <strong>{t.falta}</strong> matrículas
              </p>
              <p className="text-xs mt-1" style={TEXTO_FRACO}>
                {`${dinheiro(t.valor_por_mat)}/mat · Total: ${dinheiro(t.ganho_total)}`}
              </p>
            </div>
          );
        })}
      </div>
    </Secao>
  );
}

function configMini(serie: number[], cor: string): ChartConfiguration {
  return {
    type: "line",
    data: { labels: serie.map((_, i) => String(i)), datasets: [{ data: serie, borderColor: cor, backgroundColor: `${cor}33`, fill: true, tension: 0.4, pointRadius: 0, borderWidth: 2 }] },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: { x: { display: false }, y: { display: false } },
    },
  };
}

function Indicador({ rotulo, valor, sub, subCor, mini }: { rotulo: string; valor: string; sub: string; subCor?: string; mini?: ChartConfiguration }) {
  return (
    <div className="cartao p-4 flex flex-col gap-1" role="group" aria-label={rotulo}>
      <p className="rotulo">{rotulo}</p>
      <p className="text-2xl font-headline font-extrabold tabular-nums">{valor}</p>
      <p className={`text-xs ${subCor ?? ""}`} style={subCor ? undefined : TEXTO_FRACO}>
        {sub}
      </p>
      {mini && <Grafico config={mini} altura={35} rotulo={`Últimos dias: ${rotulo}`} />}
    </div>
  );
}

/** _mpRenderMomentum + sparklines dos últimos 7 dias do heatmap. */
export function Momentum({ d }: { d: Insights }) {
  const pace = d.pace_atual || 0;
  const paceMeta = d.pace_meta || 0;
  const minis = useMemo(() => {
    const dias = (d.heatmap || []).filter((h) => h.status !== "future" && h.realizadas != null).slice(-7);
    if (dias.length < 2) return null;
    const feitas = dias.map((h) => h.realizadas || 0);
    let soma = 0;
    const acumulado = feitas.map((v) => (soma += v));
    const fin = dias.map((h) => (d.premiacao?.daily_breakdown || []).find((b) => b.data === h.data)?.total ?? 0);
    return {
      pace: configMini(feitas, "#10b981"),
      meta: configMini(dias.map((h) => h.meta || 0), "#f59e0b"),
      proj: configMini(acumulado, "#3b82f6"),
      fin: configMini(fin, "#10b981"),
    };
  }, [d]);
  const projTier = d.projecao_tier && d.projecao_tier !== "base";
  return (
    <Secao titulo="Momentum" icone="speed">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
        <Indicador rotulo="Ritmo atual" valor={pace.toFixed(1)} sub="mat/dia" mini={minis?.pace} />
        <Indicador rotulo="Necessário" valor={paceMeta > 900 ? "--" : paceMeta.toFixed(1)} sub="mat/dia p/ meta" mini={minis?.meta} />
        <Indicador
          rotulo="Projeção"
          valor={`${d.projecao || 0} mat`}
          sub={projTier ? ROTULO_TIER[d.projecao_tier!] || d.projecao_tier! : "Abaixo da meta"}
          subCor={projTier ? "font-semibold text-emerald-700 dark:text-emerald-400" : "text-red-600 dark:text-red-400"}
          mini={minis?.proj}
        />
        <Indicador rotulo="No seu ritmo" valor={dinheiro(d.projecao_financeira || 0)} sub="projeção financeira" mini={minis?.fin} />
      </div>
    </Secao>
  );
}

const NIVEL_SEQ: Record<string, { rotulo: string; icone: string; cor: string }> = {
  aquecendo: { rotulo: "Aquecendo!", icone: "local_fire_department", cor: "text-amber-600 dark:text-amber-400" },
  em_chamas: { rotulo: "Em Chamas!", icone: "whatshot", cor: "text-orange-600 dark:text-orange-400" },
  imparavel: { rotulo: "IMPARÁVEL!", icone: "bolt", cor: "text-purple-600 dark:text-purple-400" },
};

export const COR_DIA: Record<string, string> = {
  hit: "bg-emerald-500",
  partial: "bg-amber-400",
  miss: "bg-red-500",
  rest: "bg-slate-300 dark:bg-slate-700",
  future: "bg-slate-200 dark:bg-slate-800",
};

/** _mpRenderStreak + heatmap. */
export function Sequencia({ d }: { d: Insights }) {
  const seq = d.sequencia || 0;
  const nivel = d.streak_nivel ? NIVEL_SEQ[d.streak_nivel] : null;
  let texto = seq > 0 ? `${seq} dia${seq > 1 ? "s" : ""} consecutivo${seq > 1 ? "s" : ""} batendo a meta diária` : "Inicie sua sequência hoje! (mat + aceites contam)";
  if (nivel) texto += ` — ${nivel.rotulo}`;
  const breakdown = new Map((d.premiacao?.daily_breakdown || []).map((b) => [b.data, b]));
  const heatmap = d.heatmap || [];
  return (
    <Secao titulo="Sequência" icone="whatshot">
      <div className="cartao p-5 flex flex-col gap-4">
        <div className="flex items-center gap-4">
          <span className={`text-4xl font-headline font-extrabold ${nivel ? nivel.cor : ""}`} style={nivel ? undefined : TEXTO_FRACO}>
            {seq}
          </span>
          <div>
            <p className="text-sm font-semibold">{texto}</p>
            <p className="text-xs" style={TEXTO_FRACO}>
              dias consecutivos batendo a meta diária
            </p>
            {nivel && (
              <p className={`flex items-center gap-1 mt-1 text-xs font-bold ${nivel.cor}`}>
                <span className="material-symbols-outlined text-sm" aria-hidden>
                  {nivel.icone}
                </span>
                {nivel.rotulo}
              </p>
            )}
          </div>
        </div>
        {heatmap.length > 0 && (
          <div className="flex flex-wrap gap-1">
            {heatmap.map((h) => {
              const bd = breakdown.get(h.data);
              let det = `${h.realizadas || 0}`;
              if (h.meta) det += `/${h.meta}`;
              if ((h.aceites || 0) > 0) det += ` (${h.mat || 0}m+${h.aceites}a)`;
              const dica = h.status === "future" ? "Futuro" : `${data(h.data)}: ${det}${bd ? ` · ${dinheiro(bd.total)}` : ""}`;
              return <span key={h.data} title={dica} aria-label={dica} className={`w-4 h-4 rounded-sm ${COR_DIA[h.status] || COR_DIA.future}`} />;
            })}
          </div>
        )}
        <div className="flex flex-wrap gap-4 text-xs" style={TEXTO_FRACO}>
          {[
            ["hit", "Bateu"],
            ["partial", "Parcial"],
            ["miss", "Não bateu"],
            ["future", "Futuro"],
          ].map(([k, r]) => (
            <span key={k} className="flex items-center gap-1">
              <span className={`w-3 h-3 rounded-sm ${COR_DIA[k]}`} aria-hidden />
              {r}
            </span>
          ))}
        </div>
      </div>
    </Secao>
  );
}

/** _mpRenderTierProgress. */
export function ProgressoPorFaixa({ d }: { d: Insights }) {
  const lista = (d.tier_progress || []).filter((p) => p.tier !== "base" || p.valor_por_mat > 0);
  if (!lista.length) return null;
  const totalMat = d.total_matriculas || 0;
  return (
    <Secao titulo="Progresso por faixa" icone="bar_chart">
      <div className="cartao p-5 flex flex-col gap-3">
        {lista.map((p) => {
          const cor = COR_TIER[p.tier] || COR_TIER.base;
          const pct = Math.min(p.pct || 0, 100);
          const falta = Math.max(0, (p.target || 0) - totalMat);
          const ganho = p.valor_por_mat > 0 ? dinheiro(p.ganho) : "";
          return (
            <div key={p.tier} className="flex items-center gap-3">
              <div className="w-28 flex-shrink-0 text-xs">
                <span className={`font-semibold ${cor.texto}`}>{ROTULO_TIER[p.tier] || p.tier}</span>
                {p.target > 0 && <span style={TEXTO_FRACO}>{` (${p.target})`}</span>}
              </div>
              <div className="flex-1 flex items-center gap-2">
                <div className="flex-1">
                  <Barra pct={pct} cor={cor.barra} altura="h-3" />
                </div>
                <span className="text-[11px] tabular-nums w-14" style={TEXTO_FRACO}>{`${totalMat}/${p.target || "∞"}`}</span>
              </div>
              <div className="w-36 text-right flex-shrink-0 text-xs">
                {p.atingido ? (
                  <span className="font-semibold text-emerald-700 dark:text-emerald-400">{`✅ ${ganho}`}</span>
                ) : falta > 0 ? (
                  <span style={TEXTO_FRACO}>{`falta ${falta}${ganho ? ` · +${ganho}` : ""}`}</span>
                ) : null}
              </div>
            </div>
          );
        })}
      </div>
    </Secao>
  );
}

/** _mpRenderFinanceiro. */
export function ResumoFinanceiro({ d }: { d: Insights }) {
  const prem = d.premiacao || {};
  const itens = [
    { rotulo: "Bônus tier", valor: prem.tier_bonus || 0, cor: "bg-amber-500" },
    { rotulo: "PIX diários", valor: prem.daily_bonus || 0, cor: "bg-cyan-500" },
    { rotulo: "Recebimentos", valor: prem.receb_bonus || 0, cor: "bg-violet-500" },
  ];
  const max = Math.max(...itens.map((i) => i.valor), 1);
  const uni = d.unificado;
  return (
    <Secao titulo="Resumo financeiro" icone="payments">
      <div className="cartao p-5 flex flex-col gap-3">
        {uni && (
          <div className="p-3 rounded-xl border border-pink-500/25 bg-pink-500/5">
            <p className="text-xs font-bold text-pink-700 dark:text-pink-400">Campanhas unificadas</p>
            <p className="text-xs" style={TEXTO_FRACO}>
              {(uni.campanhas || []).join(" + ")}
            </p>
            <p className="text-xs text-emerald-700 dark:text-emerald-400 mt-1">{`+${dinheiro(uni.ganho_extra)} a mais vs. individual!`}</p>
          </div>
        )}
        {itens.map((i) => (
          <div key={i.rotulo} className="flex items-center gap-3" role="group" aria-label={i.rotulo}>
            <span className="text-xs w-24 flex-shrink-0" style={TEXTO_FRACO}>
              {i.rotulo}
            </span>
            <div className="flex-1">
              <Barra pct={Math.round((i.valor / max) * 100)} cor={i.cor} altura="h-3" />
            </div>
            <span className="text-xs font-bold w-28 text-right tabular-nums">{dinheiro(i.valor)}</span>
          </div>
        ))}
        <div className="flex items-center justify-between pt-3" style={{ borderTop: "1px solid var(--border)" }} role="group" aria-label="Total">
          <span className="text-sm font-bold text-emerald-700 dark:text-emerald-400">Total</span>
          <span className="text-2xl font-extrabold tabular-nums text-emerald-700 dark:text-emerald-400">{dinheiro(prem.total || 0)}</span>
        </div>
      </div>
    </Secao>
  );
}

/** _mpRenderTimeline: barras de matrículas, linha de meta e linha de bônus (R$). */
export function Timeline({ d }: { d: Insights }) {
  const bd = d.premiacao?.daily_breakdown || [];
  const config = useMemo<ChartConfiguration | null>(() => {
    if (!bd.length) return null;
    return {
      type: "bar",
      data: {
        labels: bd.map((b) => data(b.data)),
        datasets: [
          { type: "bar", label: "Matrículas", data: bd.map((b) => b.realizadas), backgroundColor: "#10b981", borderRadius: 4, yAxisID: "y" },
          { type: "line", label: "Meta", data: bd.map((b) => b.meta), borderColor: "#f59e0b", borderDash: [6, 6], borderWidth: 2, pointRadius: 0, tension: 0.4, yAxisID: "y" },
          { type: "line", label: "Bônus R$", data: bd.map((b) => b.total), borderColor: "#06b6d4", backgroundColor: "#06b6d433", fill: true, borderWidth: 2, pointRadius: 0, tension: 0.4, yAxisID: "y1" },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { boxWidth: 10, font: { size: 11 } } },
          tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${c.datasetIndex === 2 ? dinheiro(Number(c.raw)) : c.raw}` } },
        },
        scales: {
          x: { ticks: { font: { size: 9 }, maxRotation: 45 }, grid: { display: false } },
          y: { beginAtZero: true, ticks: { font: { size: 9 } } },
          y1: { position: "right", beginAtZero: true, grid: { display: false }, ticks: { font: { size: 9 }, callback: (v) => `R$${Math.round(Number(v))}` } },
        },
      },
    } as ChartConfiguration;
  }, [bd]);
  if (!config) return null;
  return (
    <Secao titulo="Timeline diária" icone="timeline">
      <div className="cartao p-4">
        <Grafico config={config} altura={280} rotulo="Matrículas, meta e bônus por dia" />
      </div>
    </Secao>
  );
}

/** _mpRenderHistorico: campanhas já encerradas (sem a ativa). */
export function CampanhasAnteriores({ hist }: { hist: Historico["historico"] }) {
  if (!hist.length) return null;
  const anteriores = hist.filter((h) => !h.ativa);
  return (
    <Secao titulo="Campanhas anteriores" icone="history">
      {anteriores.length ? (
        <div className="flex gap-3 overflow-x-auto pb-2">
          {anteriores.map((h) => (
            <div key={h.campanha_id} className="cartao p-4 min-w-[210px] flex-shrink-0">
              <p className="text-xs font-semibold mb-1">{h.nome}</p>
              <p className="text-xs" style={TEXTO_FRACO}>{`${data(h.dt_inicio)} — ${data(h.dt_fim)}`}</p>
              <p className="flex items-baseline gap-2 mt-2">
                <span className="text-lg font-bold">{h.total_matriculas}</span>
                <span className="text-xs" style={TEXTO_FRACO}>
                  matrículas
                </span>
              </p>
              <p className={`text-xs ${h.tier ? "font-semibold text-emerald-700 dark:text-emerald-400" : ""}`} style={h.tier ? undefined : TEXTO_FRACO}>
                {h.tier ? ROTULO_TIER[h.tier] || h.tier : "Sem tier"}
              </p>
              <p className="text-sm font-bold text-emerald-700 dark:text-emerald-400 mt-1">{dinheiro(h.total_premiacao)}</p>
            </div>
          ))}
        </div>
      ) : (
        <p className="text-xs" style={TEXTO_FRACO}>
          Nenhuma campanha anterior
        </p>
      )}
    </Secao>
  );
}
