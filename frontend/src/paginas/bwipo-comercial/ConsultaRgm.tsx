import type { Acoes } from "./acoes";
import { dataBr, FONTE } from "./formato";
import { Modal } from "./Modal";
import type { ConsultaRgm as Consulta, Linha } from "./tipos";

const BOTAO = "px-2 py-0.5 rounded text-[11px] font-semibold border";

/** Consultar RGM (bwcConsultarRgm): dono pela regra, decisão manual e todos os candidatos. */
export function ConsultaRgm({
  dados,
  linha,
  acoes,
  onFechar,
}: {
  dados: Consulta;
  linha?: Linha;
  acoes: Acoes;
  onFechar: () => void;
}) {
  const q = dados.rgm;
  const recorte = linha
    ? `${linha.nome} · ${linha.situacao} · ${linha.data}${linha.fora ? " · fora do padrão" : ""}`
    : "Fora das matrículas deste filtro";
  const cands = dados.candidatos || [];
  return (
    <Modal titulo={`RGM ${q}`} sub={recorte} onFechar={onFechar}>
      <div className="px-4 py-3 flex flex-col gap-1.5" style={{ borderBottom: "1px solid var(--border)" }}>
        <p>
          Venda com <b>{dados.agente || "—"}</b>{" "}
          <span style={{ color: "var(--text-muted)" }}>{`pela regra: ${FONTE[dados.fonte || ""] || dados.fonte || ""}`}</span>
        </p>
        {dados.resolucao && (
          <p>
            Decisão manual: <b>{dados.resolucao.user_name || dados.resolucao.user_id}</b> ({dados.resolucao.por || "manual"}){" "}
            <button type="button" className="underline" style={{ color: "var(--text-muted)" }} onClick={() => acoes.desfazerVenda(q)}>
              desfazer
            </button>
          </p>
        )}
        {acoes.admin && (
          <div>
            <button
              type="button"
              className={`${BOTAO} border-emerald-500/40 text-emerald-700 dark:text-emerald-300`}
              onClick={() => acoes.contarVenda(q, !dados.contar_venda)}
            >
              {dados.contar_venda ? "Deixar de contar (fora do padrão)" : "Contar venda mesmo fora do padrão"}
            </button>
          </div>
        )}
      </div>
      {!cands.length ? (
        <p className="px-4 py-3" style={{ color: "var(--text-muted)" }}>
          Nenhum lead no Kommo nem negócio no Bwipo com esse RGM.
        </p>
      ) : (
        cands.map((c, i) => {
          const escolhido = !!(dados.resolucao && c.user_id && dados.resolucao.user_id === c.user_id);
          const ref = c.origem === "kommo" ? `Kommo #${c.id}` : `Bwipo #${c.number || "—"}`;
          return (
            <div
              key={`${c.origem}-${c.id}-${i}`}
              className="px-4 py-2 flex justify-between items-center gap-3"
              style={{ borderBottom: "1px solid var(--border-subtle)" }}
            >
              <span>
                <b>{c.agente}</b>{" "}
                <span style={{ color: "var(--text-muted)" }}>
                  {`· ${ref} · ${c.etapa}${c.data ? " · " + dataBr(c.data) : ""}`}
                </span>
              </span>
              <span className="flex gap-2">
                {c.user_id ? (
                  <button
                    type="button"
                    className={`${BOTAO} border-amber-500/40 text-amber-700 dark:text-amber-300 ${escolhido ? "bg-amber-500/25" : ""}`}
                    onClick={() => acoes.fixarVenda(q, c.user_id as number, c.agente)}
                  >
                    {escolhido ? "Venda fixada aqui" : "Fixar venda aqui"}
                  </button>
                ) : (
                  <span className="text-[11px]" style={{ color: "var(--text-muted)" }} title="Responsável do Bwipo sem depara com o Kommo">
                    sem depara
                  </span>
                )}
                {c.origem !== "kommo" && (
                  <button
                    type="button"
                    className={`${BOTAO} border-emerald-500/40 text-emerald-700 dark:text-emerald-300`}
                    onClick={() => acoes.syncCandidato(String(c.id), q)}
                  >
                    Atualizar
                  </button>
                )}
              </span>
            </div>
          );
        })
      )}
    </Modal>
  );
}
