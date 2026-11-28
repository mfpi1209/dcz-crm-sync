import type { Acoes } from "./acoes";
import { dataBr } from "./formato";
import type { Conflito, Linha } from "./tipos";

/** Um RGM em conflito (_bwcCardConflito): quem disputa e para quem creditar. */
export function CartaoConflito({ c, linha, acoes }: { c: Conflito; linha?: Linha; acoes: Acoes }) {
  const resolvido = !!c.resolucao;
  const uidAtual = c.resolucao?.user_id || null;
  const vistos = new Set<string | number>();
  const tags = (c.candidatos || []).filter((cand) => {
    const chave = cand.user_id || cand.agente;
    if (vistos.has(chave)) return false;
    vistos.add(chave);
    return true;
  });
  const opcoes = (c.candidatos || []).filter((cand) => cand.user_id);

  function escolher(valor: string) {
    const uid = parseInt(valor, 10);
    const cand = opcoes.find((o) => o.user_id === uid);
    if (!uid || !cand) return;
    if (!window.confirm(`Fixar a venda do RGM ${c.rgm} em ${cand.agente}?`)) return; // o select volta sozinho
    acoes.fixarVenda(c.rgm, uid, cand.agente, true);
  }

  return (
    <div
      className={`rounded-xl border p-4 ${resolvido ? "border-emerald-600/40 bg-emerald-500/5" : "border-amber-600/40 bg-amber-500/5"}`}
    >
      <div className="flex items-center gap-2 mb-2 flex-wrap">
        <span className={`material-symbols-outlined text-[18px] ${resolvido ? "text-emerald-500" : "text-amber-500"}`} aria-hidden>
          {resolvido ? "check_circle" : "warning"}
        </span>
        <p className="font-medium text-sm">{linha?.nome || "Aluno sem nome"}</p>
        <span className="text-xs" style={{ color: "var(--text-muted)" }}>RGM {c.rgm}</span>
        <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>
          {dataBr(linha?.data || c.candidatos[0]?.data)}
        </span>
        {resolvido && (
          <button type="button" className="text-[11px] underline" style={{ color: "var(--text-muted)" }} onClick={() => acoes.desfazerVenda(c.rgm)}>
            Desfazer
          </button>
        )}
      </div>
      <div className="flex flex-wrap gap-1 mb-3">
        {tags.map((cand) => {
          const ativo = uidAtual && cand.user_id === uidAtual;
          return (
            <span
              key={String(cand.user_id || cand.agente)}
              className="px-2 py-0.5 rounded-full text-[11px] font-medium"
              style={
                ativo
                  ? { background: "var(--primary-light)", color: "var(--primary)", border: "1px solid var(--primary)" }
                  : { background: "var(--surface-variant)", color: "var(--text-secondary)" }
              }
            >
              {cand.agente}
            </span>
          );
        })}
      </div>
      {opcoes.length ? (
        <div className="flex items-center gap-3">
          <label className="text-xs shrink-0" style={{ color: "var(--text-muted)" }} htmlFor={`conflito-${c.rgm}`}>
            Creditar para:
          </label>
          <select
            id={`conflito-${c.rgm}`}
            aria-label={`Creditar venda do RGM ${c.rgm}`}
            className="controle font-normal flex-1 min-w-0"
            value={uidAtual ? String(uidAtual) : ""}
            onChange={(e) => escolher(e.target.value)}
          >
            {!uidAtual && <option value="">Escolha o consultor</option>}
            {opcoes.map((cand, i) => (
              <option key={`${cand.user_id}-${i}`} value={String(cand.user_id)}>
                {`${cand.agente} — #${cand.number || "—"} · ${cand.ganho ? "Venda ganha" : cand.etapa || "Aberto"}`}
              </option>
            ))}
          </select>
          {resolvido && <span className="text-emerald-600 dark:text-emerald-400 text-xs shrink-0">Salvo</span>}
        </div>
      ) : (
        <p className="text-xs" style={{ color: "var(--text-muted)" }}>
          Responsável sem depara. Não dá para creditar daqui.
        </p>
      )}
    </div>
  );
}
