import type { Acoes } from "./acoes";
import { dataBr, FONTE } from "./formato";
import type { Linha } from "./tipos";

const LIMITE = 200;

/** Cor da situação (EM CURSO verde; cancelado/trancado vermelho; o resto violeta). */
export function Situacao({ situacao }: { situacao: string }) {
  const sit = (situacao || "").toUpperCase();
  const cls =
    sit === "EM CURSO"
      ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-300"
      : sit.includes("CANCEL") || sit.includes("TRANC")
        ? "bg-rose-500/15 text-rose-600 dark:text-rose-400"
        : "bg-violet-500/15 text-violet-700 dark:text-violet-300";
  return <span className={`inline-block px-1.5 py-0.5 rounded-full ${cls}`}>{sit || "—"}</span>;
}

export function BotaoRgm({ rgm, acoes, className = "" }: { rgm: string; acoes: Acoes; className?: string }) {
  return (
    <button type="button" className={`font-mono font-semibold underline decoration-dotted ${className}`} onClick={() => acoes.consultarRgm(rgm)}>
      {rgm}
    </button>
  );
}

/** Lista de alunos da evasão e do fora do padrão (_bwcLista): até 200 linhas. */
export function ListaAlunos({ linhas, contarVenda, acoes }: { linhas: Linha[]; contarVenda?: boolean; acoes: Acoes }) {
  if (!linhas.length) {
    return (
      <p className="px-4 py-3" style={{ color: "var(--text-muted)" }}>
        Ninguém neste recorte.
      </p>
    );
  }
  const admin = !!contarVenda && acoes.admin;
  return (
    <>
      <div className="overflow-x-auto">
        <table className="tabela-cartao text-xs">
          <thead>
            <tr>
              <th className="text-left">RGM</th>
              <th className="text-left">Nome</th>
              <th className="text-left">Situação</th>
              <th className="text-left">Data</th>
              <th className="text-left">Polo</th>
              <th className="text-left">Nível</th>
              <th className="text-left">Fonte</th>
              {admin && <th />}
            </tr>
          </thead>
          <tbody>
            {linhas.slice(0, LIMITE).map((l, i) => (
              <tr key={`${l.rgm}-${l.data}-${l.agente}-${i}`}>
                <td className="whitespace-nowrap">
                  <BotaoRgm rgm={l.rgm} acoes={acoes} />
                </td>
                <td>
                  {l.nome}
                  {l.fora && <span className="text-orange-600 dark:text-orange-400"> fora do padrão</span>}
                </td>
                <td className="whitespace-nowrap">
                  <Situacao situacao={l.situacao} />
                </td>
                <td className="whitespace-nowrap font-mono">{dataBr(l.data)}</td>
                <td className="whitespace-nowrap">{l.polo || "—"}</td>
                <td className="whitespace-nowrap" style={{ color: "var(--text-muted)" }}>{l.nivel || "—"}</td>
                <td className="whitespace-nowrap" style={{ color: "var(--text-muted)" }}>
                  {`${l.agente} · ${FONTE[l.fonte] || "—"}`}
                </td>
                {admin && (
                  <td className="whitespace-nowrap">
                    <button
                      type="button"
                      className="px-2 py-0.5 rounded text-[11px] font-semibold bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border border-emerald-500/40"
                      onClick={() => acoes.contarVenda(l.rgm, true)}
                    >
                      Contar venda
                    </button>
                  </td>
                )}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {linhas.length > LIMITE && (
        <p className="px-4 py-2" style={{ color: "var(--text-muted)" }}>
          + {linhas.length - LIMITE} linhas
        </p>
      )}
    </>
  );
}
