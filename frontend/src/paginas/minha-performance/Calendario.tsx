import { Secao, TEXTO_FRACO } from "./comum";
import { dinheiro, hojeLocal } from "./formato";
import type { DiaHeatmap, Insights } from "./tipos";

const MESES = ["Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho", "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"];
const DIAS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];
const DIA_SEMANA = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"];

const ESTILO: Record<string, { celula: string; rotulo: string; icone: string }> = {
  hit: { celula: "bg-emerald-100 border-emerald-500/50 text-emerald-900 dark:bg-emerald-900/50 dark:text-emerald-200", rotulo: "Meta batida! 🎉", icone: "✅" },
  partial: { celula: "bg-amber-100 border-amber-500/50 text-amber-900 dark:bg-amber-900/40 dark:text-amber-200", rotulo: "Quase lá! 💪", icone: "⚡" },
  miss: { celula: "bg-red-100 border-red-500/50 text-red-900 dark:bg-red-950/60 dark:text-red-200", rotulo: "Não bateu 😤", icone: "❌" },
  rest: { celula: "border-transparent", rotulo: "Dia de descanso", icone: "" },
  future: { celula: "border-transparent", rotulo: "Futuro", icone: "" },
};

function dica(dateStr: string, status: string, h: DiaHeatmap | undefined, bonus: number, pct: number, suporte: boolean, hoje: boolean) {
  const dt = new Date(`${dateStr}T00:00:00`);
  const linhas = [`${DIA_SEMANA[dt.getDay()]}, ${String(dt.getDate()).padStart(2, "0")}/${String(dt.getMonth() + 1).padStart(2, "0")}${hoje ? " (hoje)" : ""}`, (ESTILO[status] || ESTILO.future).rotulo];
  const meta = h?.meta ?? 0;
  if (meta > 0) linhas.push(`Progresso: ${pct}%`);
  if (status !== "future") {
    const partes: string[] = [];
    if ((h?.mat ?? 0) > 0) partes.push(`${suporte ? "Matrículas (data mat.)" : "Matrículas"}: ${h!.mat}`);
    if ((h?.aceites ?? 0) > 0) partes.push(`Aceites: ${h!.aceites}`);
    if (meta > 0) partes.push(`Meta: ${meta}`);
    if (bonus > 0) partes.push(`Bônus do dia: ${dinheiro(bonus)}`);
    linhas.push(...(partes.length ? partes : ["Sem atividade"]));
  }
  return linhas.join(" · ");
}

/** _mpRenderCalendar: meses da campanha, cada dia com realizadas/meta e o bônus na dica. */
export function Calendario({ d }: { d: Insights }) {
  const heatmap = d.heatmap || [];
  const ini = d.campanha?.dt_inicio;
  const fim = d.campanha?.dt_fim;
  let corpo;
  if (!heatmap.length) {
    corpo = (
      <p className="text-xs" style={TEXTO_FRACO}>
        Sem dados
      </p>
    );
  } else if (!ini || !fim) {
    corpo = null;
  } else {
    const suporte = !!(d.painel_suporte || d.pix_suporte_equipe);
    const porDia = new Map<string, DiaHeatmap>();
    for (const h of heatmap) {
      const k = String(h.data || "").split("T")[0];
      if (k) porDia.set(k, { ...h, data: k });
    }
    const bonusDia = new Map((d.premiacao?.daily_breakdown || []).map((b) => [b.data, b.total]));
    const hoje = hojeLocal();
    const i0 = new Date(`${ini}T00:00:00`);
    const f0 = new Date(`${fim}T00:00:00`);
    const meses: { ano: number; mes: number }[] = [];
    const cur = new Date(i0.getFullYear(), i0.getMonth(), 1);
    const ultimo = new Date(f0.getFullYear(), f0.getMonth() + 1, 0);
    while (cur <= ultimo) {
      meses.push({ ano: cur.getFullYear(), mes: cur.getMonth() });
      cur.setMonth(cur.getMonth() + 1);
    }
    corpo = (
      <>
        {meses.map(({ ano, mes }) => {
          const ultimoDia = new Date(ano, mes + 1, 0).getDate();
          let inicio = new Date(ano, mes, 1).getDay() - 1;
          if (inicio < 0) inicio = 6;
          const celulas = [];
          for (let i = 0; i < inicio; i++) celulas.push(<div key={`v${i}`} />);
          for (let dia = 1; dia <= ultimoDia; dia++) {
            const ds = `${ano}-${String(mes + 1).padStart(2, "0")}-${String(dia).padStart(2, "0")}`;
            if (ds < ini || ds > fim) {
              celulas.push(
                <div key={ds} className="h-14 flex items-center justify-center text-[10px] opacity-30">
                  {dia}
                </div>,
              );
              continue;
            }
            const h = porDia.get(ds);
            const futuro = ds > hoje;
            const status = futuro ? "future" : h?.status || "future";
            const est = ESTILO[status] || ESTILO.future;
            const realizadas = futuro ? null : (h?.realizadas ?? null);
            const meta = h?.meta ?? 0;
            const bonus = bonusDia.get(ds) ?? 0;
            const pct = meta > 0 && realizadas != null ? Math.min(100, Math.round((realizadas / meta) * 100)) : 0;
            const ehHoje = ds === hoje;
            let razao = "";
            let sub = "";
            if (status !== "future" && status !== "rest" && realizadas != null) {
              if (suporte) {
                if (meta > 0) razao = `${realizadas}/${meta}`;
                if ((h?.mat ?? 0) > 0) sub = `${h!.mat} em curso`;
              } else if (meta > 0) razao = `${realizadas}/${meta}`;
              else if (realizadas > 0) razao = String(realizadas);
            }
            const texto = dica(ds, status, h, bonus, pct, suporte, ehHoje);
            celulas.push(
              <div
                key={ds}
                title={texto}
                aria-label={texto}
                className={`h-14 rounded-lg border flex flex-col items-center justify-center gap-0.5 overflow-hidden ${est.celula} ${ehHoje ? "ring-2 ring-cyan-500" : ""}`}
                style={status === "future" || status === "rest" ? { background: "var(--bg-elevated)", color: "var(--text-muted)" } : undefined}
              >
                <span className="text-sm font-extrabold leading-none">{dia}</span>
                {razao && <span className="text-[10px] font-bold opacity-80">{razao}</span>}
                {sub && <span className="text-[8px] opacity-70">{sub}</span>}
                {est.icone && status !== "future" && status !== "rest" && realizadas != null && <span className="text-[8px] leading-none">{est.icone}</span>}
              </div>,
            );
          }
          return (
            <div key={`${ano}-${mes}`} className="flex flex-col gap-2">
              <p className="text-sm font-bold">{`${MESES[mes]} ${ano}`}</p>
              <div className="grid grid-cols-7 gap-1.5">
                {DIAS.map((n) => (
                  <div key={n} className="text-center text-[10px] font-semibold pb-1" style={TEXTO_FRACO}>
                    {n}
                  </div>
                ))}
                {celulas}
              </div>
            </div>
          );
        })}
        <div className="flex flex-wrap items-center justify-center gap-4 pt-3 text-[11px]" style={{ borderTop: "1px solid var(--border)", color: "var(--text-muted)" }}>
          <span>Bateu ✅</span>
          <span>Parcial ⚡</span>
          <span>Não bateu ❌</span>
          <span>Futuro</span>
          <span className="px-1 rounded ring-2 ring-cyan-500">Hoje</span>
        </div>
      </>
    );
  }
  return (
    <Secao titulo="Calendário de resultados" icone="calendar_month">
      <div className="cartao p-5 flex flex-col gap-5">{corpo}</div>
    </Secao>
  );
}
