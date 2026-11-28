import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import type { Acoes } from "./acoes";
import { baixarCsvAgente, dataBr, enviar, type LinhaAgente } from "./formato";
import { BotaoRgm } from "./ListaAlunos";
import { Modal } from "./Modal";
import type { Contatos, Linha } from "./tipos";

const TIPOS: Record<string, [string, string]> = {
  "NOVA MATRICULA": ["NOVA", "bg-emerald-500/20 text-emerald-700 dark:text-emerald-300"],
  RETORNO: ["RETORNO", "bg-amber-500/20 text-amber-700 dark:text-amber-300"],
  RECOMPRA: ["RECOMPRA", "bg-blue-500/20 text-blue-700 dark:text-blue-300"],
};

function Tipo({ tipo }: { tipo: string }) {
  const t = String(tipo || "").toUpperCase();
  if (!t) return null;
  const [rotulo, cls] = TIPOS[t] || [tipo, "bg-slate-500/20 text-slate-600 dark:text-slate-400"];
  return <span className={`inline-block px-1.5 py-0.5 rounded text-[11px] font-semibold ${cls}`}>{rotulo}</span>;
}

/** Coluna Contagem (_bwcContagemCell): só RGM fora do padrão tem ação. */
function Contagem({ l, acoes }: { l: Linha; acoes: Acoes }) {
  if (!l.fora) return <span style={{ color: "var(--text-muted)" }}>—</span>;
  if (!l.conta) {
    return acoes.admin ? (
      <button
        type="button"
        className="px-2 py-0.5 rounded text-[11px] font-semibold bg-orange-500/15 text-orange-700 dark:text-orange-300 border border-orange-500/40"
        onClick={() => acoes.contarVenda(l.rgm, true)}
      >
        Contar venda
      </button>
    ) : (
      <span className="text-[11px] text-orange-600 dark:text-orange-400">Não conta</span>
    );
  }
  return (
    <>
      <span className="inline-flex items-center gap-1 px-2 py-0.5 rounded text-[11px] font-semibold bg-emerald-500/15 text-emerald-700 dark:text-emerald-300 border border-emerald-500/30">
        ✓ Contando
      </span>
      {acoes.admin && (
        <button type="button" className="ml-1 text-[11px] underline" style={{ color: "var(--text-muted)" }} onClick={() => acoes.contarVenda(l.rgm, false)}>
          Desfazer
        </button>
      )}
    </>
  );
}

/** Matrículas de um agente no recorte (bwcAbrirAgente), com CPF e telefone do último relatório. */
export function DetalheAgente({
  nome,
  userId,
  linhas,
  ini,
  fim,
  chave,
  acoes,
  onFechar,
}: {
  nome: string;
  userId: number | null;
  linhas: Linha[];
  ini: string;
  fim: string;
  /** Muda quando o painel recarrega: busca os contatos de novo. */
  chave: string;
  acoes: Acoes;
  onFechar: () => void;
}) {
  const [busca, setBusca] = useState("");
  const rgms = linhas.map((l) => l.rgm);
  const contatosQ = useQuery({
    queryKey: ["bwipo", "contatos", nome, chave, rgms.join(",")],
    queryFn: ({ signal }) => enviar<Contatos>("/api/bwipo/painel/contatos", "POST", { rgms }, signal),
    gcTime: 0,
    retry: false,
  });
  const mapa = contatosQ.data?.contatos ?? {};
  const completas: LinhaAgente[] = linhas.map((l) => ({
    ...l,
    cpf: mapa[l.rgm]?.cpf || "",
    telefone: mapa[l.rgm]?.telefone || "",
  }));

  const vendas = linhas.filter((l) => l.conta).length;
  const fora = linhas.filter((l) => l.fora).length;
  const titulo = vendas < linhas.length ? `${vendas} venda(s) · ${linhas.length} matrícula(s)` : `${linhas.length} matrícula(s)`;

  const q = busca.trim().toLowerCase();
  const dig = q.replace(/\D/g, "");
  const filtradas = completas.filter((l) => {
    if (!q) return true;
    const blob = [l.rgm, l.nome, l.cpf, l.telefone, l.polo, l.curso, l.tipo].join(" ").toLowerCase();
    return blob.includes(q) || (dig.length >= 2 && String(l.rgm).includes(dig));
  });

  return (
    <Modal
      titulo={`${nome} · ${titulo} — período ${dataBr(ini)} a ${dataBr(fim)}`}
      onFechar={onFechar}
      acoes={
        userId ? (
          <a href="/#minha_performance" className="controle" title="Ver painel motivacional completo deste agente">
            Ver Performance
          </a>
        ) : null
      }
    >
      <div className="p-5 flex flex-col gap-3">
        <div className="flex items-center justify-between gap-3 flex-wrap">
          <div className="flex items-center gap-2 flex-wrap">
            <span style={{ color: "var(--text-secondary)" }}>
              Total:{" "}
              {vendas !== linhas.length ? (
                <>
                  <span className="text-emerald-700 dark:text-emerald-400 font-bold">{vendas} vendas</span>{" "}
                  <span style={{ color: "var(--text-muted)" }}>· {linhas.length} matrículas listadas</span>
                </>
              ) : (
                <span className="font-semibold">{linhas.length}</span>
              )}
            </span>
            <span style={{ color: "var(--text-muted)" }}>·</span>
            {fora ? (
              <span className="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold bg-orange-500/15 text-orange-700 dark:text-orange-300 border border-orange-500/30">
                {`⚠ ${fora} RGM${fora > 1 ? "s" : ""} fora do padrão`}
              </span>
            ) : (
              <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>
                Todos os RGMs dentro do padrão do ciclo
              </span>
            )}
          </div>
          <button type="button" className="controle" onClick={() => baixarCsvAgente(nome, completas)}>
            <span className="material-symbols-outlined text-[18px]" aria-hidden>download</span>
            Baixar CSV
          </button>
        </div>
        <input
          type="search"
          className="controle font-normal font-mono w-full"
          placeholder="Buscar RGM ou nome"
          aria-label="Buscar RGM ou nome"
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
          autoFocus
        />
        <div className="overflow-auto max-h-[52vh]">
          <table className="tabela-cartao text-xs">
            <thead>
              <tr>
                <th className="text-left">RGM</th>
                <th className="text-left">Nome</th>
                <th className="text-left">CPF</th>
                <th className="text-left">Telefone</th>
                <th className="text-left">Tipo</th>
                <th className="text-left">Polo</th>
                <th className="text-left">Nível</th>
                <th className="text-left">Data matrícula</th>
                <th className="text-left">Curso</th>
                <th className="text-left">Contagem</th>
              </tr>
            </thead>
            <tbody>
              {filtradas.length ? (
                filtradas.map((l, i) => (
                  <tr key={`${l.rgm}-${l.data}-${i}`} className={l.fora ? "bg-orange-500/5" : ""}>
                    <td className="whitespace-nowrap">
                      {l.fora ? (
                        <span title="RGM fora do padrão do ciclo" className="text-orange-700 dark:text-orange-300">
                          <BotaoRgm rgm={l.rgm} acoes={acoes} /> ⚠
                        </span>
                      ) : (
                        <BotaoRgm rgm={l.rgm} acoes={acoes} />
                      )}
                    </td>
                    <td>{l.nome}</td>
                    <td className="font-mono whitespace-nowrap">{l.cpf}</td>
                    <td className="font-mono whitespace-nowrap">{l.telefone}</td>
                    <td className="whitespace-nowrap">
                      <Tipo tipo={l.tipo} />
                    </td>
                    <td className="whitespace-nowrap">{l.polo}</td>
                    <td className="whitespace-nowrap">{l.nivel}</td>
                    <td className="font-mono whitespace-nowrap">{dataBr(l.data)}</td>
                    <td className="max-w-[200px] truncate" title={l.curso}>{l.curso}</td>
                    <td className="whitespace-nowrap">
                      <Contagem l={l} acoes={acoes} />
                    </td>
                  </tr>
                ))
              ) : (
                <tr>
                  <td colSpan={10} style={{ color: "var(--text-muted)" }}>
                    Nenhum RGM nesse filtro.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </Modal>
  );
}
