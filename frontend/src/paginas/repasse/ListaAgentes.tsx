import { Avatar } from "./comum";
import { dinheiro } from "./formato";
import type { Agente, IdAgente } from "./tipos";

/**
 * Cartões dos consultores (no lugar do carrossel antigo): mesma ordem do
 * servidor (maior recebido primeiro), selo "1" no primeiro, alunos, barra
 * relativa ao maior repasse, recebido (só admin) e repasse.
 */
export function ListaAgentes({
  agentes,
  busca,
  taxa,
  admin,
  selecionado,
  onSelecionar,
}: {
  agentes: Agente[];
  busca: string;
  taxa: number;
  admin: boolean;
  selecionado: IdAgente | null;
  onSelecionar: (a: Agente, indice: number) => void;
}) {
  const maxRepasse = Math.max(1, ...agentes.map((a) => (a.total_valor || 0) * taxa));
  const q = busca.trim().toLowerCase();
  // Busca no cliente pelo nome, como o repFilter antigo; a cor segue a posição original.
  const visiveis = agentes.map((a, i) => ({ a, i })).filter(({ a }) => !q || (a.nome || "").toLowerCase().includes(q));

  if (!visiveis.length) {
    return (
      <p className="text-sm py-4" style={{ color: "var(--text-muted)" }}>
        Nenhum consultor encontrado.
      </p>
    );
  }

  return (
    <ul className="grid gap-3 grid-cols-[repeat(auto-fill,minmax(200px,1fr))]" aria-label="Consultores">
      {visiveis.map(({ a, i }) => {
        const repasse = (a.total_valor || 0) * taxa;
        const ativo = selecionado !== null && String(a.id) === String(selecionado);
        return (
          <li key={String(a.id)}>
            <button
              type="button"
              aria-pressed={ativo}
              onClick={() => onSelecionar(a, i)}
              className="cartao-agente w-full text-left p-4 flex flex-col gap-3"
            >
              <span className="flex items-center gap-3 min-w-0">
                <span className="relative">
                  <Avatar nome={a.nome} indice={i} />
                  {i === 0 && (
                    <span className="absolute -top-1 -right-1 w-5 h-5 rounded-full bg-amber-400 text-amber-900 text-[10px] font-bold flex items-center justify-center">
                      1
                    </span>
                  )}
                </span>
                <span className="min-w-0">
                  <span className="block text-sm font-bold truncate" title={a.nome || "—"} style={{ color: "var(--text-primary)" }}>
                    {a.nome || "—"}
                  </span>
                  <span className="block text-xs" style={{ color: "var(--text-muted)" }}>
                    {(a.qtd_alunos || 0).toLocaleString("pt-BR")} alunos
                  </span>
                </span>
              </span>
              <span className="block h-1.5 rounded-full overflow-hidden" aria-hidden style={{ background: "var(--bg-elevated)" }}>
                <span
                  className="block h-full rounded-full barra-animada bg-amber-500"
                  style={{ width: `${Math.round((repasse / maxRepasse) * 100)}%` }}
                />
              </span>
              <span className="flex flex-col gap-1 text-xs">
                {admin && (
                  <span className="flex justify-between gap-2">
                    <span style={{ color: "var(--text-muted)" }}>Recebido</span>
                    <span className="tabular-nums text-emerald-700 dark:text-emerald-400">{dinheiro(a.total_valor)}</span>
                  </span>
                )}
                <span className="flex justify-between gap-2">
                  <span className="text-amber-700 dark:text-amber-400">Repasse</span>
                  <span className="tabular-nums font-bold text-amber-800 dark:text-amber-300">{dinheiro(repasse)}</span>
                </span>
              </span>
            </button>
          </li>
        );
      })}
    </ul>
  );
}
