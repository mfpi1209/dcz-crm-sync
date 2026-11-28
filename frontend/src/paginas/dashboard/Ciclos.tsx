import { useState, type ReactNode } from "react";
import { item } from "../../menu";
import { NOMES_TELAS_NOVAS } from "../../telas";
import { POS, juntarPolos, numero, variacao } from "./formato";
import type { Agregado, Ciclos, Comparacao } from "./tipos";

/** Comparação de ciclos (loadCicloMaster + renderCicloMaster). */

const COR_SINAL = { sobe: "var(--positivo)", desce: "var(--negativo)", neutro: "var(--text-muted)" } as const;
const VAZIO: Agregado = { grand_total: 0, totals: {} };

function Variacao({ atual, anterior }: { atual: number; anterior: number }) {
  const v = variacao(atual, anterior);
  return (
    <span className="text-[11px] font-bold tabular-nums" style={{ color: COR_SINAL[v.sinal] }}>
      {v.texto}
    </span>
  );
}

function CartaoPeriodo({ c, anterior, remat }: { c: Comparacao | undefined; anterior: Agregado; remat: string }) {
  const atual = c?.current ?? VAZIO;
  const total = atual.grand_total || 0;
  const t = atual.totals || {};
  const linhas: [string, number][] = [
    ["Novos", t.novos || 0],
    [remat, t.rematricula || 0],
    ["Regresso", t.regresso || 0],
    ["Recompra", t.recompra || 0],
  ];
  return (
    <div className="cartao-agente p-4">
      <div className="flex items-center justify-between mb-1">
        <span className="text-xs font-bold">{c?.label}</span>
        <Variacao atual={total} anterior={anterior.grand_total || 0} />
      </div>
      <p className="text-[10px] mb-1" style={{ color: "var(--text-muted)" }}>{c?.period}</p>
      <p className="text-xl font-extrabold font-headline tabular-nums mb-1">{numero(total)}</p>
      <div className="grid grid-cols-2 gap-x-2 gap-y-0.5 text-[11px]">
        {linhas.map(([r, v]) => (
          <div key={r} className="flex justify-between">
            <span style={{ color: "var(--text-muted)" }}>{r}</span>
            <span className="font-medium tabular-nums">{numero(v)}</span>
          </div>
        ))}
      </div>
      <p className="mt-2 pt-2 text-[10px]" style={{ borderTop: "1px solid var(--border-subtle)", color: "var(--text-muted)" }}>
        vs anterior: <span className="font-medium">{numero(anterior.grand_total)}</span>
      </p>
    </div>
  );
}

function Lista({ titulo, linhas, total }: { titulo: string; linhas: [string, number][]; total: number }) {
  return (
    <div>
      <p className="rotulo mb-2">{titulo}</p>
      <div className="space-y-1 text-xs">
        {linhas.map(([k, v]) => (
          <div key={k} className="flex items-center gap-2">
            <span className="flex-1 truncate" style={{ color: "var(--text-secondary)" }}>{k}</span>
            <span className="font-mono tabular-nums">{numero(v)}</span>
            <span className="w-9 text-right text-[10px]" style={{ color: "var(--text-muted)" }}>{total ? Math.round((v / total) * 100) : 0}%</span>
          </div>
        ))}
      </div>
    </div>
  );
}

export function ComparacaoCiclos({
  dados,
  erro,
  carregando,
  nivel,
}: {
  dados: Ciclos | undefined;
  erro: string | null;
  carregando: boolean;
  nivel: string;
}) {
  const [abertos, setAbertos] = useState<Set<string>>(new Set());
  const rematCurto = nivel === POS ? "Veteranos" : "Rematr.";
  const cmp = dados?.comparisons;
  const lista = dados?.ciclos ?? [];
  const max = Math.max(...lista.map((c) => c.grand_total), 1);

  const alternar = (nome: string) =>
    setAbertos((a) => {
      const n = new Set(a);
      if (n.has(nome)) n.delete(nome);
      else n.add(nome);
      return n;
    });

  // Níveis com negócios no snapshot e sem ciclo cadastrado (aviso do rodapé).
  const nivCfg = new Set((dados?.config ?? []).map((c) => c.nivel));
  const faltando = Object.entries(dados?.distinct_nivels ?? {}).filter(([n]) => !nivCfg.has(n));

  let corpo: ReactNode;
  if (erro) {
    corpo = <p role="alert" className="py-10 text-center text-sm" style={{ color: "var(--danger)" }}>Erro: {erro}</p>;
  } else if (carregando || !dados) {
    corpo = <p className="py-10 text-center text-sm" style={{ color: "var(--text-muted)" }}>Carregando dados comparativos...</p>;
  } else if (!lista.length && !cmp) {
    corpo = (
      <p className="py-4 text-center text-sm" style={{ color: "var(--text-muted)" }}>
        Nenhum ciclo configurado.{" "}
        <a className="underline" style={{ color: "var(--primary)" }} href={item("config", NOMES_TELAS_NOVAS).href}>Configure ciclos</a>.
      </p>
    );
  } else {
    corpo = (
      <>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-5">
          <CartaoPeriodo c={cmp?.ytd} anterior={cmp?.ytd_prev?.current ?? VAZIO} remat={rematCurto} />
          <CartaoPeriodo c={cmp?.ytd_prev} anterior={VAZIO} remat={rematCurto} />
          <CartaoPeriodo c={cmp?.m6} anterior={cmp?.m6_prev?.current ?? VAZIO} remat={rematCurto} />
          <CartaoPeriodo c={cmp?.m6_prev} anterior={VAZIO} remat={rematCurto} />
        </div>
        <div className="flex flex-col gap-3">
          {lista.map((c, i) => {
            const anterior = lista[i + 1];
            const t = c.totals || {};
            const ePos = (c.nivel || "").includes("Pós");
            const rCurto = ePos ? "Veteranos" : "Rematr.";
            const rLongo = ePos ? "Veteranos" : "Rematrículas";
            const aberto = abertos.has(c.nome);
            const idPainel = `ciclo-${i}`;
            return (
              <div key={c.nome} className="cartao-agente overflow-hidden">
                <button
                  type="button"
                  className="w-full px-4 py-3 flex items-center justify-between gap-3 text-left"
                  aria-expanded={aberto}
                  aria-controls={idPainel}
                  onClick={() => alternar(c.nome)}
                >
                  <span className="flex flex-wrap items-center gap-x-4 gap-y-1 min-w-0">
                    <span className="flex items-center gap-2">
                      <span className="text-sm font-bold">{c.nome}</span>
                      <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>{c.nivel}</span>
                    </span>
                    <span className="flex flex-wrap gap-3 text-[11px]" style={{ color: "var(--text-muted)" }}>
                      <span>Novos <b className="font-medium" style={{ color: "var(--text-secondary)" }}>{numero(t.novos)}</b></span>
                      <span>{rCurto} <b className="font-medium" style={{ color: "var(--text-secondary)" }}>{numero(t.rematricula)}</b></span>
                      <span>Regresso <b className="font-medium" style={{ color: "var(--text-secondary)" }}>{numero(t.regresso)}</b></span>
                      <span>Recompra <b className="font-medium" style={{ color: "var(--text-secondary)" }}>{numero(t.recompra)}</b></span>
                    </span>
                  </span>
                  <span className="flex items-center gap-3 flex-shrink-0">
                    <span className="text-lg font-bold font-headline tabular-nums">{numero(c.grand_total)}</span>
                    {anterior && <Variacao atual={c.grand_total} anterior={anterior.grand_total} />}
                    <span className="material-symbols-outlined text-[18px]" aria-hidden style={{ color: "var(--text-muted)" }}>
                      {aberto ? "expand_less" : "expand_more"}
                    </span>
                  </span>
                </button>
                <div className="h-0.5" aria-hidden style={{ background: "var(--surface-variant)" }}>
                  <div className="h-full barra-animada" style={{ width: `${Math.round((c.grand_total / max) * 100)}%`, background: "var(--primary)" }} />
                </div>
                {aberto && (
                  <div id={idPainel} className="px-4 py-4 grid grid-cols-2 lg:grid-cols-3 gap-4" style={{ background: "var(--bg-card)" }}>
                    <div>
                      <p className="rotulo mb-2">Por tipo</p>
                      <div className="space-y-1 text-xs">
                        {(
                          [
                            ["Novos (calouros)", t.novos],
                            [rLongo, t.rematricula],
                            ["Regresso", t.regresso],
                            ["Recompra", t.recompra],
                          ] as [string, number | undefined][]
                        ).map(([r, v]) => (
                          <div key={r} className="flex justify-between">
                            <span style={{ color: "var(--text-secondary)" }}>{r}</span>
                            <span className="font-mono tabular-nums">{numero(v)}</span>
                          </div>
                        ))}
                        <div className="flex justify-between pt-1 mt-1 font-bold" style={{ borderTop: "1px solid var(--border)" }}>
                          <span>Total</span>
                          <span className="font-mono tabular-nums">{numero(c.grand_total)}</span>
                        </div>
                      </div>
                    </div>
                    <Lista titulo="Por situação" linhas={Object.entries(c.by_situacao || {}).slice(0, 6)} total={c.grand_total} />
                    <Lista titulo="Top polos" linhas={juntarPolos(c.by_polo).slice(0, 8)} total={c.grand_total} />
                  </div>
                )}
              </div>
            );
          })}
        </div>
        {faltando.length > 0 && (
          <div className="mt-3 flex flex-col gap-1 text-[11px]" style={{ color: "var(--aviso)" }}>
            {faltando.map(([n, qtd]) => (
              <p key={n} className="flex items-center gap-1">
                <span className="material-symbols-outlined text-[14px]" aria-hidden>warning</span>
                <span>{`Existem ${numero(qtd)} negócios com nível "${n}" mas nenhum ciclo configurado para esse nível.`}</span>
              </p>
            ))}
          </div>
        )}
      </>
    );
  }

  return (
    <section className="cartao overflow-hidden" aria-label="Comparação de ciclos">
      <h2 className="text-base flex items-center gap-2 px-5 pt-5 pb-3">
        <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>compare_arrows</span>
        Comparação de ciclos
      </h2>
      <div className="px-5 pb-5">{corpo}</div>
    </section>
  );
}
