import { useQuery } from "@tanstack/react-query";
import { useEffect, useRef, useState, type ReactNode } from "react";
import { api, ErroApi } from "../../api";
import { contagemK, decimal1, erroKommo, numero, origemFunil } from "./formato";
import type { DadosFunil, Etapa, Funil as RespFunil, LeadsHoje, Ontem, ResumoOntem } from "./tipos";

/**
 * Funil de vendas (bloco do topo da home), como _dashRefreshFunnel + _renderFunnelCards.
 *
 * - Abre buscando o funil e os leads de hoje; quando o funil termina, o resumo de ontem
 *   (yesterday-summary, ou funnel-yesterday se a primeira falhar).
 * - Funil "aquecendo" (primeira carga no servidor): pergunta de novo sem forçar,
 *   4 s e depois a cada 5 s, até 30 vezes. Nesse caso o resumo de ontem vem do próprio funil.
 * - Atualizar força (`?force=1`) funil, leads de hoje e ontem. Nada é atualizado sozinho
 *   (a antiga também não tinha atualização periódica).
 */

/** Tempos em ms; os testes encurtam. */
export const ESPERA_FUNIL = { primeira: 4000, demais: 5000, lento: 8000, limite: 120000, tentativas: 30 };

/** Cores das etapas: variáveis --funil-<etapa> de estilo.css (iguais nos dois temas). */
const ORDEM_VISUAL = [
  "sem_resposta",
  "em_atendimento",
  "aguardando_resposta",
  "aguardando_inscricao",
  "inscricao",
  "processo_seletivo",
  "em_processo",
  "aprovado_reprovado",
  "boleto_enviado",
  "pagamento_confirmado",
  "aceite",
];

const ROTULOS_VISUAL: Record<string, string> = {
  em_atendimento: "Em atendimento",
  sem_resposta: "Sem resposta",
  aguardando_resposta: "Aguardando resposta",
  aguardando_inscricao: "Aguardando inscrição",
  boleto_enviado: "Boleto",
  inscricao: "Inscrição",
  processo_seletivo: "Seletivo",
  em_processo: "Em processo",
  aprovado_reprovado: "Aprovados",
  aceite: "Aceite",
  pagamento_confirmado: "Pagamento",
};

const cor = (chave: string) => (ORDEM_VISUAL.includes(chave) ? `var(--funil-${chave})` : "var(--text-muted)");

function esperar(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((ok, falha) => {
    const t = setTimeout(ok, ms);
    signal.addEventListener("abort", () => {
      clearTimeout(t);
      falha(signal.reason ?? new DOMException("Abortado", "AbortError"));
    });
  });
}

/** funnel-live com o limite de 2 min da antiga; cancela junto com a consulta. */
async function pedirFunil(forcar: boolean, signal: AbortSignal): Promise<RespFunil> {
  const ctrl = new AbortController();
  let estourou = false;
  const timer = setTimeout(() => {
    estourou = true;
    ctrl.abort();
  }, ESPERA_FUNIL.limite);
  const cancelar = () => ctrl.abort();
  signal.addEventListener("abort", cancelar);
  try {
    return await api<RespFunil>("/api/kommo/funnel-live" + (forcar ? "?force=1" : ""), { signal: ctrl.signal });
  } catch (e) {
    if (estourou) throw new Error("Tempo esgotado ao buscar o funil. Clique em Tentar novamente.");
    if (signal.aborted) throw e;
    if (e instanceof ErroApi) throw new Error(erroKommo(e.message) || "Não foi possível carregar o funil.");
    throw new Error("Erro de rede ao carregar o funil.");
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", cancelar);
  }
}

interface Resultado {
  dados: DadosFunil;
  aqueceu: boolean;
}

export function FunilVendas() {
  const forcarFunil = useRef(false);
  const forcarLeads = useRef(false);
  const forcarOntem = useRef(false);
  const [aquecendo, setAquecendo] = useState<string | null>(null);

  const funilQ = useQuery({
    queryKey: ["dashboard", "funil"],
    retry: false,
    queryFn: async ({ signal }): Promise<Resultado> => {
      const forcar = forcarFunil.current;
      forcarFunil.current = false;
      setAquecendo(null);
      const d = await pedirFunil(forcar, signal);
      if (d.warming) {
        setAquecendo(d.error || "Funil carregando (primeira carga)…");
        for (let t = 0; t < ESPERA_FUNIL.tentativas; t++) {
          await esperar(t === 0 ? ESPERA_FUNIL.primeira : ESPERA_FUNIL.demais, signal);
          try {
            const r = await pedirFunil(false, signal);
            if (r.ok && r.data) {
              setAquecendo(null);
              return { dados: r.data, aqueceu: true };
            }
          } catch (e) {
            if (signal.aborted) throw e;
          }
        }
        setAquecendo(null);
        throw new Error("Funil ainda carregando. Clique em Tentar novamente.");
      }
      if (!d.ok || !d.data) {
        throw new Error(erroKommo(d.detail) || d.error || "Não foi possível carregar o funil.");
      }
      return { dados: d.data, aqueceu: false };
    },
  });

  const leadsQ = useQuery({
    queryKey: ["dashboard", "leads-hoje"],
    retry: false,
    queryFn: ({ signal }) => {
      const forcar = forcarLeads.current;
      forcarLeads.current = false;
      return api<LeadsHoje>("/api/kommo/new-leads-today" + (forcar ? "?force=1" : ""), { signal });
    },
  });

  // Cada vez que o funil termina (bem ou mal) vale uma rodada do resumo de ontem,
  // menos quando ele aqueceu (a antiga saía antes e usava o resumo do próprio funil).
  const rodada = Math.max(funilQ.dataUpdatedAt, funilQ.errorUpdatedAt);
  const aqueceu = funilQ.isSuccess && !funilQ.isFetching && funilQ.data.aqueceu;
  const ontemQ = useQuery({
    queryKey: ["dashboard", "ontem", rodada],
    retry: false,
    enabled: rodada > 0 && !funilQ.isFetching && !aqueceu,
    queryFn: async ({ signal }): Promise<ResumoOntem> => {
      const q = forcarOntem.current ? "?force=1" : "";
      forcarOntem.current = false;
      for (const caminho of ["/api/kommo/yesterday-summary", "/api/dashboard/funnel-yesterday"]) {
        try {
          const y = await api<Ontem>(caminho + q, { signal });
          if (y?.ok && y.data) return y.data;
        } catch (e) {
          if (signal.aborted) throw e;
        }
      }
      return { vendas: 0, leads: 0, leads_prev: 0, leads_delta_pct: 0 };
    },
  });

  // Aviso de demora depois de 8 s buscando (sem contar o aquecimento, que tem aviso próprio).
  const [lento, setLento] = useState(false);
  const buscando = funilQ.isFetching;
  useEffect(() => {
    if (!buscando) {
      setLento(false);
      return;
    }
    const t = setTimeout(() => setLento(true), ESPERA_FUNIL.lento);
    return () => clearTimeout(t);
  }, [buscando]);

  function atualizar() {
    forcarFunil.current = true;
    forcarLeads.current = true;
    forcarOntem.current = true;
    void funilQ.refetch();
    void leadsQ.refetch();
  }

  const dados = funilQ.data?.dados;
  const porChave: Record<string, Etapa> = {};
  (dados?.stages ?? []).forEach((s) => (porChave[s.key] = s));
  const aceite = porChave.aceite;
  const leads = leadsQ.data?.ok && leadsQ.data.count != null ? leadsQ.data.count : undefined;
  const novosHoje = dados && dados.new_today !== undefined ? dados.new_today : leads;
  const conversao = dados ? (novosHoje && novosHoje > 0 ? ((aceite?.count || 0) / novosHoje) * 100 : 0) : null;
  const ontem: ResumoOntem | undefined = ontemQ.data ?? dados?.yesterday_summary;
  const mostrarCartoes = !!dados && !funilQ.isError && !aquecendo;

  return (
    <section className="cartao p-5 flex flex-col gap-5" aria-labelledby="titulo-funil">
      <div className="flex items-center justify-between gap-3 flex-wrap">
        <h2 id="titulo-funil" className="text-base flex items-center gap-2">
          <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>filter_alt</span>
          Funil de vendas
        </h2>
        <div className="flex items-center gap-3">
          {dados && <span className="text-[11px] font-mono" style={{ color: "var(--text-muted)" }}>{origemFunil(dados)}</span>}
          <button type="button" className="controle" aria-label="Atualizar funil" onClick={atualizar} disabled={funilQ.isFetching}>
            <span className="material-symbols-outlined text-[18px]" aria-hidden>refresh</span>
            Atualizar
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 md:grid-cols-3 xl:grid-cols-5 gap-3">
        <Kpi rotulo="Novos leads hoje" valor={novosHoje !== undefined ? numero(novosHoje) : "—"} legenda="leads" destaque />
        <Kpi
          rotulo="Fechado — ganho ontem"
          valor={ontem ? numero(ontem.vendas) : "—"}
          legenda="vendas"
          extra={
            <div className="text-right leading-tight">
              <span className="block text-[10px]" style={{ color: "var(--text-muted)" }}>Leads ontem</span>
              <span className="block text-sm font-bold tabular-nums">{ontem ? numero(ontem.leads) : "—"}</span>
              {ontem && ((ontem.leads_prev || 0) > 0 || (ontem.leads || 0) > 0) && (
                <Tendencia
                  valor={ontem.leads_delta_pct || 0}
                  texto={`${(ontem.leads_delta_pct || 0) >= 0 ? "+" : ""}${Number(ontem.leads_delta_pct || 0).toLocaleString("pt-BR", { maximumFractionDigits: 1 })}%`}
                />
              )}
            </div>
          }
        />
        <Kpi rotulo="Total no funil" valor={dados ? numero(dados.total) : "—"} legenda="ativos" />
        <Kpi
          rotulo="Aceites"
          valor={dados ? numero(aceite?.count) : "—"}
          legenda="na meta"
          extra={
            aceite && aceite.delta !== undefined && aceite.delta !== 0 ? (
              <Tendencia valor={aceite.delta} texto={`${aceite.delta > 0 ? "+" : ""}${aceite.delta}`} />
            ) : null
          }
        />
        <Kpi rotulo="Conversão" valor={conversao === null ? "—" : `${decimal1(conversao)}%`} legenda="funil global" />
      </div>

      {funilQ.isError ? (
        <div className="py-6 text-center text-sm flex flex-col items-center gap-3" style={{ color: "var(--text-muted)" }}>
          <span role="alert">{(funilQ.error as Error).message || "Erro ao carregar funil."}</span>
          <button type="button" className="controle" onClick={atualizar} disabled={funilQ.isFetching}>Tentar novamente</button>
        </div>
      ) : aquecendo ? (
        <div className="py-6 text-center text-sm flex flex-col items-center gap-1" style={{ color: "var(--text-muted)" }} role="status">
          <span>{aquecendo}</span>
          <span className="text-xs">Contagem no Bwipo pode levar cerca de 20s</span>
        </div>
      ) : !dados ? (
        <p className="py-6 text-center text-sm" style={{ color: "var(--text-muted)" }} role="status">
          {lento ? "Buscando dados do Bwipo… (pode levar cerca de 20s)" : "Buscando dados do Bwipo..."}
        </p>
      ) : null}

      {mostrarCartoes && <EtapasDestaque etapas={dados.stages ?? []} />}
      {mostrarCartoes && <Visual dados={dados} />}
    </section>
  );
}

function Kpi({ rotulo, valor, legenda, extra, destaque }: { rotulo: string; valor: string; legenda: string; extra?: ReactNode; destaque?: boolean }) {
  return (
    <div
      className="rounded-2xl p-4 flex flex-col gap-1"
      role="group"
      aria-label={rotulo}
      style={{
        background: destaque ? "var(--primary)" : "var(--bg-elevated)",
        color: destaque ? "var(--bg-card)" : "var(--text-primary)",
        border: "1px solid var(--border-subtle)",
      }}
    >
      <span className="rotulo" style={destaque ? { color: "inherit", opacity: 0.85 } : undefined}>{rotulo}</span>
      <div className="flex items-end justify-between gap-2">
        <div className="flex flex-col">
          <span className="text-2xl font-extrabold tabular-nums font-headline">{valor}</span>
          <span className="text-[11px]" style={{ opacity: 0.75 }}>{legenda}</span>
        </div>
        {extra}
      </div>
    </div>
  );
}

function Tendencia({ valor, texto }: { valor: number; texto: string }) {
  const sobe = valor >= 0;
  return (
    <span
      className="inline-flex items-center gap-0.5 text-[11px] font-bold"
      style={{ color: sobe ? "var(--positivo)" : "var(--negativo)" }}
    >
      <span className="material-symbols-outlined text-[14px]" aria-hidden>{sobe ? "trending_up" : "trending_down"}</span>
      {texto}
    </span>
  );
}

function EtapasDestaque({ etapas }: { etapas: Etapa[] }) {
  const destaque = etapas.filter((s) => s.highlight);
  if (!destaque.length) {
    return <p className="py-6 text-center text-sm" style={{ color: "var(--text-muted)" }}>Nenhum dado de funil</p>;
  }
  return (
    <ul className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6 gap-3" aria-label="Etapas em destaque">
      {destaque.map((s) => (
        <li key={s.key} className="cartao-agente overflow-hidden">
          <div className="h-1" style={{ background: cor(s.key) }} aria-hidden />
          <div className="p-4">
            <div className="flex items-start justify-between gap-2 mb-2">
              <p className="text-xs font-bold" style={{ color: cor(s.key) }}>{s.label}</p>
              <span className="text-[10px] font-mono" style={{ color: "var(--text-muted)" }}>{s.pct || 0}%</span>
            </div>
            <p className="text-2xl font-extrabold tabular-nums font-headline mb-1">{numero(s.count)}</p>
            <div className="flex items-center gap-2 text-[11px]">
              <span style={{ color: "var(--text-muted)" }}>D0:</span>
              {s.delta !== undefined && s.delta !== 0 ? (
                <span className="font-bold" style={{ color: s.delta > 0 ? "var(--positivo)" : "var(--negativo)" }}>
                  {s.delta > 0 ? "+" : ""}
                  {s.delta}
                </span>
              ) : (
                <span style={{ color: "var(--text-muted)" }}>—</span>
              )}
              {s.delta_pct !== undefined && s.delta_pct !== 0 && (
                <span style={{ color: s.delta_pct > 0 ? "var(--positivo)" : "var(--negativo)" }}>
                  {s.delta_pct > 0 ? "+" : ""}
                  {s.delta_pct}%
                </span>
              )}
            </div>
          </div>
        </li>
      ))}
    </ul>
  );
}

function Visual({ dados }: { dados: DadosFunil }) {
  const porChave: Record<string, Etapa> = {};
  (dados.stages ?? []).forEach((s) => (porChave[s.key] = s));
  const ordem = ORDEM_VISUAL.map((k) => porChave[k]).filter(Boolean);
  if (!ordem.length) return null;
  const perdidos = porChave.perdido;
  // Altura proporcional ao maior estágio, com mínimo de 14% e teto de 100% (como a antiga).
  const max = Math.max(...ordem.map((s) => s.count || 0), 1);
  const altura = (n: number) => Math.min(100, Math.max(14, Math.round(((n || 0) / max) * 100)));
  const novos = dados.new_today || 0;
  const conversao = novos > 0 ? ((porChave.aceite?.count || 0) / novos) * 100 : 0;

  return (
    <div className="rounded-2xl p-4" style={{ background: "var(--bg-elevated)", border: "1px solid var(--border-subtle)" }}>
      <div className="flex items-center justify-between mb-3">
        <p className="rotulo">Visualização do funil</p>
        <p className="text-xs flex items-center gap-2">
          <span style={{ color: "var(--text-muted)" }}>Conversão global</span>
          <span className="font-extrabold tabular-nums" style={{ color: "var(--positivo)" }}>{decimal1(conversao)}%</span>
        </p>
      </div>
      <div className="overflow-x-auto">
        <div className="flex items-stretch gap-1 h-48 min-w-max">
          {ordem.map((s, i) => (
            <div key={s.key} className="flex items-stretch">
              <div className="w-16 flex flex-col">
                <p className="text-[10px] font-bold text-center leading-tight min-h-[28px]" style={{ color: s.key === "aceite" ? "var(--positivo)" : "var(--text-muted)" }}>
                  {ROTULOS_VISUAL[s.key] || s.label}
                </p>
                <div className="flex-1 flex items-end">
                  <div
                    className="w-full rounded-t-xl flex flex-col items-center justify-center text-white"
                    style={{ height: `${altura(s.count)}%`, background: cor(s.key) }}
                  >
                    <span className="font-extrabold text-base leading-none">{contagemK(s.count)}</span>
                    {s.pct != null && <span className="text-[10px] mt-1 opacity-80">{s.pct}%</span>}
                  </div>
                </div>
              </div>
              {i < ordem.length - 1 && (
                <span className="material-symbols-outlined self-end pb-2 text-[18px]" aria-hidden style={{ color: "var(--text-muted)" }}>chevron_right</span>
              )}
            </div>
          ))}
          {perdidos && (
            <div className="w-20 flex flex-col ml-2 pl-2" style={{ borderLeft: "1px solid var(--border)" }}>
              <p className="text-[10px] font-bold text-center min-h-[28px]" style={{ color: "var(--negativo)" }}>Perdidos</p>
              <div className="flex-1 flex items-end">
                <div
                  className="w-full rounded-t-xl flex flex-col items-center justify-center"
                  style={{ height: `${altura(perdidos.count)}%`, background: "var(--surface-variant)", border: "1px solid var(--border)" }}
                >
                  <span className="font-extrabold text-sm leading-none">{contagemK(perdidos.count)}</span>
                  {perdidos.pct != null && <span className="text-[10px] mt-1" style={{ color: "var(--text-muted)" }}>{perdidos.pct}%</span>}
                </div>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
