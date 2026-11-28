import { Cartao } from "./comum";
import { fmt, fmtData, fmtPct } from "./formato";
import type { Snapshot } from "./tipos";

/** "Histórico de snapshots", com o Apagar de cada linha (única escrita da tela). */
export function Historico({
  snapshots,
  apagando,
  onApagar,
}: {
  snapshots: Snapshot[];
  apagando: number | null;
  onApagar: (s: Snapshot) => void;
}) {
  const muted = { color: "var(--text-muted)" };
  return (
    <Cartao titulo="Histórico de snapshots" icone="table_rows" semPadding>
      <div className="overflow-x-auto">
        <table className="tabela-cartao">
          <thead>
            <tr>
              <th className="text-left">Data</th>
              <th className="text-left">Arquivo</th>
              <th className="text-left">Competência</th>
              <th className="text-right">Inadimplentes</th>
              <th className="text-right">Em curso</th>
              <th className="text-right">Taxa %</th>
              <th><span className="sr-only">Ações</span></th>
            </tr>
          </thead>
          <tbody>
            {snapshots.map((s) => {
              const id = s.snapshot_id || s.id;
              return (
                <tr key={id}>
                  <td className="whitespace-nowrap text-xs" style={muted}>{fmtData(s.uploaded_at)}</td>
                  <td className="text-xs max-w-[220px] truncate" title={s.filename || "—"}>{s.filename || "—"}</td>
                  <td className="whitespace-nowrap text-xs" style={muted}>
                    {s.competencia_label ? s.competencia_label : <span className="italic">—</span>}
                  </td>
                  <td className="text-right tabular-nums font-semibold">{fmt(s.inadimplentes)}</td>
                  <td className="text-right tabular-nums text-xs" style={muted}>{fmt(s.em_curso)}</td>
                  <td className="text-right tabular-nums font-semibold text-rose-600 dark:text-rose-400">{fmtPct(s.taxa_pct)}</td>
                  <td className="text-right">
                    <button
                      type="button"
                      className="text-xs px-2 py-1 rounded text-red-600 dark:text-red-400 hover:bg-red-500/10 disabled:opacity-50"
                      disabled={apagando === id}
                      onClick={() => onApagar(s)}
                    >
                      Apagar
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </Cartao>
  );
}
