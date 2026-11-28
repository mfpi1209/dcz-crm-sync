import { useState, type ReactNode } from "react";
import { item } from "../../menu";
import { NOMES_TELAS_NOVAS } from "../../telas";
import { POS, decimal1, explicarFiltros, juntarPolos, normSit, numero, ordenarSituacoes } from "./formato";
import type { Alunos, FiltrosDash, SaudeFinanceira, Tipo, Transferencias } from "./tipos";

/** Blocos da home que leem /api/dashboard/students (e a saúde financeira). */

const textoMudo = { color: "var(--text-muted)" } as const;

// ── Matrículas por tipo de aluno ──

export function CartoesTipo({
  dados,
  filtros,
  alternarTipo,
}: {
  dados: Alunos;
  filtros: FiltrosDash;
  alternarTipo: (t: Tipo) => void;
}) {
  const [info, setInfo] = useState<"novos" | "remat" | null>(null);
  const t = dados.totals || {};
  const novosAgg = (t.novos || 0) + (t.regresso || 0) + (t.recompra || 0);
  const remat = t.rematricula || 0;
  // Total do cartão segue o tipo escolhido (igual a loadStudentMetrics).
  const ativo = filtros.tipo;
  const gt =
    ativo === "novos_agg"
      ? novosAgg
      : ativo === "rematricula"
        ? remat
        : ativo === "novos"
          ? t.novos || 0
          : ativo === "regresso"
            ? t.regresso || 0
            : ativo === "recompra"
              ? t.recompra || 0
              : dados.grand_total ?? Object.values(t).reduce((a, v) => a + (v || 0), 0);
  const pctNovos = gt ? Math.round((novosAgg / gt) * 100) : 0;
  const pctRemat = gt ? Math.round((remat / gt) * 100) : 0;
  const rotuloRemat = filtros.nivel === POS ? "Veteranos" : "Rematrículas";

  const sub: { tipo: Tipo; rotulo: string; valor: number }[] = [
    { tipo: "novos", rotulo: "Calouros", valor: t.novos || 0 },
    { tipo: "regresso", rotulo: "Regresso", valor: t.regresso || 0 },
    { tipo: "recompra", rotulo: "Recompra", valor: t.recompra || 0 },
  ];

  return (
    <div className="flex flex-col gap-4">
      <div className="flex justify-end">
        <div className="cartao-agente px-3 py-1.5 flex items-center gap-2" role="group" aria-label="Total">
          <span className="material-symbols-outlined text-[18px]" aria-hidden style={{ color: "var(--primary)" }}>groups</span>
          <span className="rotulo">Total</span>
          <span className="text-lg font-bold font-headline tabular-nums">{numero(gt)}</span>
        </div>
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <div className="cartao-agente p-5 flex flex-col gap-3" data-marcado={ativo === "novos_agg"}>
          <div className="flex items-start gap-2">
            <button type="button" className="flex-1 text-left" aria-pressed={ativo === "novos_agg"} onClick={() => alternarTipo("novos_agg")}>
              <span className="block text-sm font-medium" style={{ color: "var(--text-secondary)" }}>Novos</span>
              <span className="block text-[11px]" style={textoMudo}>Calouros + Regresso + Recompra</span>
              <span className="flex items-end justify-between gap-2 mt-1">
                <span className="text-3xl font-extrabold font-headline tabular-nums">{numero(novosAgg)}</span>
                <span className="text-xs font-bold px-2 py-0.5 rounded-full" style={{ background: "var(--primary-light)", color: "var(--primary)" }}>{pctNovos}%</span>
              </span>
            </button>
            <BotaoRegra nome="Novos" aberto={info === "novos"} onClick={() => setInfo(info === "novos" ? null : "novos")} />
          </div>
          {info === "novos" && <Regra texto={explicarFiltros(dados.card_filters?.novos || "", filtros)} />}
          <div className="grid grid-cols-3 gap-2">
            {sub.map((s) => (
              <button
                key={s.tipo}
                type="button"
                aria-pressed={ativo === s.tipo}
                onClick={() => alternarTipo(s.tipo)}
                className="rounded-xl px-3 py-2 text-left"
                style={{
                  background: ativo === s.tipo ? "var(--primary-light)" : "var(--bg-card)",
                  border: `1px solid ${ativo === s.tipo ? "var(--primary)" : "var(--border-subtle)"}`,
                }}
              >
                <span className="block text-[11px] font-semibold" style={textoMudo}>{s.rotulo}</span>
                <span className="block text-lg font-bold font-headline tabular-nums">{numero(s.valor)}</span>
              </button>
            ))}
          </div>
        </div>
        <div className="cartao-agente p-5 flex flex-col gap-3" data-marcado={ativo === "rematricula"}>
          <div className="flex items-start gap-2">
            <button type="button" className="flex-1 text-left" aria-pressed={ativo === "rematricula"} onClick={() => alternarTipo("rematricula")}>
              <span className="block text-sm font-medium" style={{ color: "var(--text-secondary)" }}>{rotuloRemat}</span>
              <span className="block text-[11px]" style={textoMudo}>Renovações de matrícula</span>
              <span className="flex items-end justify-between gap-2 mt-1">
                <span className="text-3xl font-extrabold font-headline tabular-nums">{numero(remat)}</span>
                <span className="text-xs font-bold px-2 py-0.5 rounded-full" style={{ background: "var(--fundo-positivo)", color: "var(--positivo)" }}>{pctRemat}%</span>
              </span>
            </button>
            <BotaoRegra nome={rotuloRemat} aberto={info === "remat"} onClick={() => setInfo(info === "remat" ? null : "remat")} />
          </div>
          {info === "remat" && <Regra texto={explicarFiltros(dados.card_filters?.rematricula || "", filtros)} />}
        </div>
      </div>
    </div>
  );
}

function BotaoRegra({ nome, aberto, onClick }: { nome: string; aberto: boolean; onClick: () => void }) {
  return (
    <button
      type="button"
      onClick={onClick}
      aria-expanded={aberto}
      aria-label={`Regra do cartão ${nome}`}
      title="Ver regra do card"
      className="w-6 h-6 rounded-full text-[11px] font-black flex items-center justify-center flex-shrink-0"
      style={{ border: "1px solid var(--border)", color: "var(--text-muted)" }}
    >
      !
    </button>
  );
}

function Regra({ texto }: { texto: string }) {
  return (
    <p className="text-[11px] leading-5 rounded-lg px-3 py-2" style={{ background: "var(--bg-card)", border: "1px solid var(--border-subtle)", color: "var(--text-secondary)" }}>
      {texto}
    </p>
  );
}

// ── Matrículas por situação ──

const ICONE_SITUACAO: Record<string, string> = { "em curso": "school", cancelado: "cancel", trancado: "pause_circle", transferido: "swap_horiz" };
const COR_SITUACAO: Record<string, string> = {
  "em curso": "var(--positivo)",
  cancelado: "var(--negativo)",
  trancado: "var(--aviso)",
  transferido: "var(--destaque)",
};

export function Situacoes({
  dados,
  transferencias,
  ativa,
  alternar,
  abrirTransferencias,
}: {
  dados: Record<string, number> | undefined;
  transferencias: number;
  ativa: string;
  alternar: (s: string) => void;
  abrirTransferencias: () => void;
}) {
  if (!dados || !Object.keys(dados).length) {
    return <p className="text-sm" style={textoMudo}>—</p>;
  }
  const total = Object.values(dados).reduce((a, b) => a + b, 0);
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-3">
      {ordenarSituacoes(dados).map((k) => {
        const v = dados[k];
        const pct = total ? Math.round((v / total) * 100) : 0;
        const chave = k.toLowerCase();
        const c = COR_SITUACAO[chave] ?? "var(--text-muted)";
        const marcada = normSit(ativa) === normSit(k);
        return (
          <div key={k} className="cartao-agente p-4 flex flex-col gap-2" data-marcado={marcada}>
            <button type="button" className="text-left" data-situacao={k} aria-pressed={marcada} onClick={() => alternar(k)}>
              <span className="flex items-center justify-between mb-2">
                <span className="material-symbols-outlined" aria-hidden style={{ color: c }}>{ICONE_SITUACAO[chave] ?? "help"}</span>
                <span className="text-xs font-bold tabular-nums" style={{ color: c }}>{pct}%</span>
              </span>
              <span className="block text-sm font-medium" style={{ color: "var(--text-secondary)" }}>{k}</span>
              <span className="block text-2xl font-extrabold font-headline tabular-nums">{numero(v)}</span>
              <span className="block h-1.5 rounded-full mt-2" aria-hidden style={{ background: "var(--surface-variant)" }}>
                <span className="block h-full rounded-full barra-animada" style={{ width: `${Math.min(pct, 100)}%`, background: c }} />
              </span>
            </button>
            {normSit(k) === "em curso" && (
              <button
                type="button"
                onClick={abrirTransferencias}
                title="Ver lista dos alunos que vieram de outro polo"
                className="w-full text-left text-[11px] leading-snug rounded-lg px-2 py-1.5"
                style={{ background: "var(--bg-card)", border: "1px solid var(--border-subtle)", color: "var(--destaque)" }}
              >
                <b>{numero(transferencias)}</b> {transferencias === 1 ? "transferência" : "transferências"} de outro polo
                <span style={{ opacity: 0.7 }}> · ver quem são</span>
              </button>
            )}
          </div>
        );
      })}
    </div>
  );
}

export function PainelTransferencias({
  dados,
  erro,
  carregando,
  fechar,
}: {
  dados: Transferencias | undefined;
  erro: string | null;
  carregando: boolean;
  fechar: () => void;
}) {
  const alunos = dados?.alunos ?? [];
  let corpo: ReactNode;
  if (erro) corpo = <p role="alert" className="py-3" style={{ color: "var(--danger)" }}>{erro}</p>;
  else if (carregando || !dados) corpo = <p className="py-3" style={textoMudo}>Carregando…</p>;
  else if (!alunos.length) corpo = <p className="py-3" style={textoMudo}>Nenhuma transferência inbound neste recorte.</p>;
  else
    corpo = (
      <>
        <p className="text-[11px] mb-2 font-medium" style={{ color: "var(--destaque)" }}>{numero(alunos.length)} aluno(s)</p>
        <table className="tabela-cartao">
          <thead>
            <tr>
              <th className="text-left">RGM</th>
              <th className="text-left">Nome</th>
              <th className="text-left">Polo</th>
              <th className="text-left">Tipo</th>
              <th className="text-left">Ciclo</th>
            </tr>
          </thead>
          <tbody>
            {alunos.map((a, i) => (
              <tr key={`${a.rgm}-${i}`}>
                <td className="font-mono">{a.rgm}</td>
                <td>{a.nome}</td>
                <td>{a.polo}</td>
                <td>{a.tipo}</td>
                <td>{a.ciclo}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </>
    );
  return (
    <section className="mt-4 rounded-2xl p-4" aria-label="Transferências recebidas de outro polo" style={{ background: "var(--bg-elevated)", border: "1px solid var(--border)" }}>
      <div className="flex items-center justify-between gap-2 mb-3">
        <div>
          <h3 className="text-sm">Transferências recebidas de outro polo</h3>
          <p className="text-[11px]" style={textoMudo}>Constam como Em Curso — vieram com situação Transferido no relatório.</p>
        </div>
        <button type="button" className="controle" onClick={fechar}>Fechar</button>
      </div>
      <div className="max-h-72 overflow-y-auto text-xs">{corpo}</div>
    </section>
  );
}

// ── Saúde financeira ──

export function SaudeFinanceiraCartoes({ dados, erro, atualizando }: { dados: SaudeFinanceira | undefined; erro: string | null; atualizando: boolean }) {
  if (erro && !dados) {
    return (
      <section className="cartao p-5 text-sm" aria-label="Saúde financeira">
        <p role="alert" style={{ color: "var(--danger)" }}>Não foi possível carregar a saúde financeira: {erro}</p>
      </section>
    );
  }
  if (!dados || !dados.ok || !dados.has_data) return null;
  const pct = dados.pct_inadimplencia || 0;
  const pctAdim = dados.total_alunos ? ((dados.adimplentes || 0) / dados.total_alunos) * 100 : 0;
  const filtros: string[] = [];
  if (dados.filtered_tipo) {
    const r: Record<string, string> = {
      novos: "Calouros",
      rematricula: "Rematrículas",
      regresso: "Regresso",
      recompra: "Recompra",
      novos_agg: "Novos (Calouros+Regresso+Recompra)",
    };
    filtros.push(r[dados.filtered_tipo] || dados.filtered_tipo);
  }
  if (dados.filtered_situacao) filtros.push(dados.filtered_situacao);
  if (dados.filtered_nivel) filtros.push(dados.filtered_nivel);
  const href = item("inadimplencia", NOMES_TELAS_NOVAS).href;

  const cartoes: { rotulo: string; icone: string; valor: string; cor?: string; selo?: string; barra?: number }[] = [
    { rotulo: "Total alunos", icone: "group", valor: numero(dados.total_alunos) },
    { rotulo: "Adimplentes", icone: "check_circle", valor: numero(dados.adimplentes), cor: "var(--positivo)", selo: `${decimal1(pctAdim)}%` },
    { rotulo: "Inadimplentes", icone: "warning", valor: numero(dados.inadimplentes), cor: "var(--aviso)", selo: `${decimal1(pct)}%` },
    { rotulo: "% Inadimplência", icone: "percent", valor: `${decimal1(pct)}%`, barra: Math.min(pct, 100) },
  ];

  return (
    <section className="cartao overflow-hidden" aria-label="Saúde financeira" style={{ opacity: atualizando ? 0.6 : 1 }}>
      <div className="px-5 pt-5 pb-3 flex items-center gap-2 flex-wrap">
        <h2 className="text-base flex items-center gap-2">
          <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>account_balance_wallet</span>
          Saúde financeira
        </h2>
        {dados.snapshot && (
          <span className="ml-auto text-[11px] font-mono" style={textoMudo}>
            <span>{dados.snapshot.uploaded_at}</span>
            {filtros.length > 0 && <span>{`  ·  Filtro: ${filtros.join(" + ")}`}</span>}
          </span>
        )}
      </div>
      <div className="px-5 pb-5 grid grid-cols-2 lg:grid-cols-4 gap-3">
        {cartoes.map((c) => (
          <a key={c.rotulo} href={href} className="cartao-agente p-4 block">
            <span className="flex items-center justify-between mb-2">
              <span className="material-symbols-outlined" aria-hidden style={{ color: c.cor ?? "var(--primary)" }}>{c.icone}</span>
              {c.selo && <span className="text-xs font-bold tabular-nums" style={{ color: c.cor }}>{c.selo}</span>}
            </span>
            <span className="block text-sm font-medium" style={{ color: "var(--text-secondary)" }}>{c.rotulo}</span>
            <span className="block text-2xl font-extrabold font-headline tabular-nums" style={{ color: c.cor }}>{c.valor}</span>
            {c.barra !== undefined && (
              <span className="block h-1.5 rounded-full mt-2" aria-hidden style={{ background: "var(--surface-variant)" }}>
                <span className="block h-full rounded-full barra-animada" style={{ width: `${c.barra}%`, background: "var(--negativo)" }} />
              </span>
            )}
          </a>
        ))}
      </div>
    </section>
  );
}

// ── Polo, nível e detalhes ──

export function TabelaPolos({ porPolo, ativo, alternar }: { porPolo: Record<string, number> | undefined; ativo: string; alternar: (p: string) => void }) {
  const ranking = juntarPolos(porPolo);
  const max = Math.max(...ranking.map(([, v]) => v), 1);
  return (
    <section className="cartao overflow-hidden self-start w-full">
      <h2 className="text-base flex items-center gap-2 px-5 pt-5 pb-3">
        <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>location_on</span>
        Matrículas por polo
      </h2>
      <div className="overflow-auto max-h-[400px]">
        <table className="tabela-cartao" aria-label="Matrículas por polo">
          <thead>
            <tr>
              <th className="text-center w-10">#</th>
              <th className="text-left">Polo</th>
              <th className="text-right">Total</th>
              <th className="text-left w-40"><span className="sr-only">Proporção</span></th>
            </tr>
          </thead>
          <tbody>
            {ranking.length ? (
              ranking.map(([nome, total], i) => {
                const marcado = ativo === nome;
                return (
                  <tr key={nome} style={marcado ? { background: "var(--primary-light)" } : undefined}>
                    <td className="text-center text-xs" style={textoMudo}>{i + 1}</td>
                    <td>
                      <button type="button" className="text-left text-xs font-medium hover:underline" aria-pressed={marcado} onClick={() => alternar(nome)}>
                        {nome}
                      </button>
                    </td>
                    <td className="text-right font-mono font-semibold text-xs tabular-nums">{numero(total)}</td>
                    <td>
                      <div className="h-2 rounded-full overflow-hidden" aria-hidden style={{ background: "var(--surface-variant)" }}>
                        <div className="h-full rounded-full barra-animada" style={{ width: `${Math.round((total / max) * 100)}%`, background: "var(--primary)" }} />
                      </div>
                    </td>
                  </tr>
                );
              })
            ) : (
              <tr>
                <td colSpan={4} className="py-6 text-center" style={textoMudo}>Sem dados</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

const NIVEIS_FILTRO = ["Graduação", POS];

export function PorNivel({ porNivel, nivel, alternar }: { porNivel: Record<string, number> | undefined; nivel: string; alternar: (n: string) => void }) {
  const linhas = Object.entries(porNivel || {}).sort((a, b) => b[1] - a[1]);
  const max = Math.max(...linhas.map(([, v]) => v), 1);
  const total = linhas.reduce((s, [, v]) => s + v, 0);
  return (
    <section className="cartao overflow-hidden self-start w-full" aria-label="Por nível">
      <h2 className="text-base flex items-center gap-2 px-5 pt-5 pb-3">
        <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>school</span>
        Por nível
      </h2>
      {linhas.length ? (
        <ul>
          {linhas.map(([k, v]) => {
            const pct = total ? Math.round((v / total) * 100) : 0;
            const conteudo = (
              <>
                <span className="flex items-center justify-between gap-3 mb-2">
                  <span className="text-xs font-medium" style={{ color: "var(--text-secondary)" }}>{k}</span>
                  <span className="text-xs font-mono font-semibold tabular-nums">{`${numero(v)} (${pct}%)`}</span>
                </span>
                <span className="block h-2 rounded-full overflow-hidden" aria-hidden style={{ background: "var(--surface-variant)" }}>
                  <span className="block h-full rounded-full barra-animada" style={{ width: `${Math.round((v / max) * 100)}%`, background: "var(--primary)" }} />
                </span>
              </>
            );
            const marcado = NIVEIS_FILTRO.includes(k) && nivel === k;
            return (
              <li key={k} style={{ borderTop: "1px solid var(--border-subtle)", background: marcado ? "var(--primary-light)" : undefined }}>
                {NIVEIS_FILTRO.includes(k) ? (
                  <button type="button" className="block w-full text-left px-5 py-3" aria-pressed={marcado} onClick={() => alternar(k)}>
                    {conteudo}
                  </button>
                ) : (
                  <div className="px-5 py-3">{conteudo}</div>
                )}
              </li>
            );
          })}
        </ul>
      ) : (
        <p className="px-5 py-6 text-center text-xs" style={textoMudo}>Sem dados</p>
      )}
    </section>
  );
}

function Quebra({ titulo, dados }: { titulo: string; dados: Record<string, number> | undefined }) {
  const linhas = Object.entries(dados || {});
  const total = linhas.reduce((a, [, b]) => a + b, 0);
  return (
    <section className="rounded-2xl p-4" aria-label={titulo} style={{ background: "var(--bg-elevated)", border: "1px solid var(--border-subtle)" }}>
      <h3 className="rotulo mb-3">{titulo}</h3>
      {linhas.length ? (
        <div className="space-y-1.5 text-[13px]">
          {linhas.map(([k, v]) => {
            const pct = total ? Math.round((v / total) * 100) : 0;
            return (
              <div key={k} className="flex items-center justify-between gap-2">
                <span className="flex items-center gap-2 min-w-0 flex-1">
                  <span className="truncate" style={{ color: "var(--text-secondary)" }}>{k}</span>
                  <span className="flex-1 h-1.5 rounded-full min-w-[40px]" aria-hidden style={{ background: "var(--surface-variant)" }}>
                    <span className="block h-full rounded-full" style={{ width: `${pct}%`, background: "var(--primary)" }} />
                  </span>
                </span>
                <span className="text-xs font-mono whitespace-nowrap tabular-nums" style={{ color: "var(--text-secondary)" }}>{`${numero(v)} (${pct}%)`}</span>
              </div>
            );
          })}
        </div>
      ) : (
        <p style={textoMudo}>—</p>
      )}
    </section>
  );
}

export function MaisDetalhes({ porTurma, porCiclo }: { porTurma: Record<string, number> | undefined; porCiclo: Record<string, number> | undefined }) {
  const [aberto, setAberto] = useState(false);
  return (
    <div className="cartao overflow-hidden">
      <button
        type="button"
        className="w-full px-5 py-4 text-xs font-bold flex items-center justify-center gap-2"
        style={textoMudo}
        aria-expanded={aberto}
        onClick={() => setAberto((a) => !a)}
      >
        Mais detalhes (turma, ciclo)
        <span className="material-symbols-outlined text-[18px]" aria-hidden>{aberto ? "expand_less" : "expand_more"}</span>
      </button>
      {aberto && (
        <div className="p-5 grid grid-cols-1 md:grid-cols-2 gap-4" style={{ borderTop: "1px solid var(--border-subtle)" }}>
          <Quebra titulo="Por turma" dados={porTurma} />
          <Quebra titulo="Por ciclo" dados={porCiclo} />
        </div>
      )}
    </div>
  );
}
