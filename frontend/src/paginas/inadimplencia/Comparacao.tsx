import type { ReactNode } from "react";
import { textoErro } from "./comum";
import { corVariacao, fmt, fmtData, fmtPct, plural, sinal } from "./formato";
import type { Comparacao, ComparacaoPeriodo, Lado } from "./tipos";

function Aviso({ children }: { children: ReactNode }) {
  return (
    <p className="text-sm text-center py-4" style={{ color: "var(--text-muted)" }}>
      {children}
    </p>
  );
}

function Lateral({ titulo, taxa, children }: { titulo: string; taxa: number; children: ReactNode }) {
  return (
    <div className="rounded-xl p-4" style={{ background: "var(--bg-elevated)", border: "1px solid var(--border-subtle)" }}>
      <p className="rotulo">{titulo}</p>
      <p className="text-2xl font-extrabold tabular-nums mt-2 text-rose-600 dark:text-rose-400">{fmtPct(taxa)}</p>
      <div className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>
        {children}
      </div>
    </div>
  );
}

function Variacao({ valor, cor, legenda }: { valor: string; cor: string; legenda: ReactNode }) {
  return (
    <div className="flex items-center justify-center">
      <div className="text-center py-4">
        <p className="rotulo mb-2">Variação</p>
        <p className={`text-3xl font-extrabold tabular-nums ${cor}`}>{valor}</p>
        {legenda}
      </div>
    </div>
  );
}

function SeloFallback({ lado }: { lado: Lado }) {
  if (!lado.is_fallback) return null;
  const dias = lado.dias_diferenca || 0;
  return (
    <span className="inline-flex items-center gap-1 text-[11px] font-medium rounded-full px-2 py-0.5 mt-1 border border-amber-500/30 bg-amber-500/10 text-amber-700 dark:text-amber-400">
      <span className="material-symbols-outlined text-[14px]" aria-hidden>warning</span>
      <span>
        {`Sem upload em ${fmtData(lado.requested_date)} — exibindo dados de ${fmtData(lado.fallback_date)} (${dias} dia${plural(dias)} antes)`}
      </span>
    </span>
  );
}

/** Formato do /comparar (data A × data B), igual ao _renderComparison. */
export function ComparacaoDatas({ dados }: { dados: Comparacao | undefined }) {
  if (!dados || (!dados.a && !dados.b)) return <Aviso>Nenhum dado disponível.</Aviso>;
  const { a, b, delta_taxa_pct: dt, delta_inadimplentes: di } = dados;
  if (a.snapshot_id == null && b.snapshot_id == null)
    return <Aviso>Nenhum snapshot encontrado para as datas selecionadas.</Aviso>;
  const lado = (l: Lado, rotulo: string) => (
    <Lateral titulo={`${rotulo} — ${fmtData(l.snapshot_date || l.fallback_date)}`} taxa={l.taxa_pct}>
      <SeloFallback lado={l} />
      <p className="mt-1">{`${fmt(l.inadimplentes)} inadimplentes`}</p>
      <p>{`${fmt(l.em_curso)} em curso`}</p>
    </Lateral>
  );
  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-4">
      {lado(a, "De")}
      <Variacao
        valor={`${sinal(dt)}${fmtPct(dt)}`}
        cor={corVariacao(dt)}
        legenda={<p className={`text-xs mt-1 ${corVariacao(di)}`}>{`${sinal(di)}${fmt(di)} alunos`}</p>}
      />
      {lado(b, "Até")}
    </div>
  );
}

/** Formato do /comparar-periodo (primeiro × último upload da competência). */
export function ComparacaoPeriodoView({ dados }: { dados: ComparacaoPeriodo | undefined }) {
  if (!dados) return <Aviso>Nenhum dado disponível.</Aviso>;
  if (dados.insuficiente) {
    const n = dados.snapshots_count || 0;
    return (
      <Aviso>
        É necessário ter pelo menos dois uploads no período para comparar a evolução.
        <span className="block text-xs mt-1">
          {`(${n} snapshot${plural(n)} encontrado${plural(n)} para a competência selecionada)`}
        </span>
      </Aviso>
    );
  }
  const v = dados.variacao_pp;
  return (
    <>
      <div className="mt-3 mb-1 flex flex-wrap items-baseline gap-2">
        <span className="rotulo">{dados.competencia_label}</span>
        <span className="text-xs" style={{ color: "var(--text-muted)" }}>
          {`${dados.snapshots_count} snapshot${plural(dados.snapshots_count)} no período`}
        </span>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-2">
        <Lateral titulo={`Primeiro upload — ${fmtData(dados.primeiro.uploaded_at)}`} taxa={dados.primeiro.taxa_pct}>
          <p>{`${fmt(dados.primeiro.inadimplentes)} inadimplentes`}</p>
        </Lateral>
        <Variacao
          valor={`${sinal(v)}${Number(v).toFixed(2).replace(".", ",")} pp`}
          cor={corVariacao(v)}
          legenda={<p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>pontos percentuais</p>}
        />
        <Lateral titulo={`Último upload — ${fmtData(dados.ultimo.uploaded_at)}`} taxa={dados.ultimo.taxa_pct}>
          <p>{`${fmt(dados.ultimo.inadimplentes)} inadimplentes`}</p>
        </Lateral>
      </div>
    </>
  );
}

export function ErroComparacao({ erro }: { erro: unknown }) {
  return <Aviso>{textoErro(erro)}</Aviso>;
}
