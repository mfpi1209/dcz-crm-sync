import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useCallback, useEffect, useState } from "react";
import { useSearchParams } from "react-router-dom";
import { api } from "../api";
import { Toast } from "../componentes/Toast";
import type { Aviso } from "../fixadas";
import { useMe } from "../me";
import { Calendario } from "./minha-performance/Calendario";
import { TEXTO_FRACO } from "./minha-performance/comum";
import { apiOk, data, ehLayoutSuporte, ehVisaoSuporte, hojeLocal, mesclarVendasComercial, sincronizarHeatmap, textoErro } from "./minha-performance/formato";
import { Matriculas } from "./minha-performance/Matriculas";
import { PainelSuporte } from "./minha-performance/PainelSuporte";
import {
  CampanhasAnteriores,
  Conquistas,
  Desbloqueie,
  Momentum,
  NiveisDeMeta,
  PixDoDia,
  ProgressoPorFaixa,
  RankingCard,
  ResumoFinanceiro,
  Saldo,
  Sequencia,
  Timeline,
} from "./minha-performance/Secoes";
import type { Agentes, Campanhas, Historico, Insights, Selecao } from "./minha-performance/tipos";

/**
 * Minha Performance (página `minha_performance`). Mesmas APIs, parâmetros e números da
 * tela antiga (templates/partials/_minha_performance.html + static/js/minha_performance.js);
 * as regras de crédito e de premiação ficam em routes/minha_performance.py.
 *
 * Quem vê o quê (igual à antiga):
 * - consultor: o próprio painel (kommo_uid do /api/me); sem vínculo, aviso;
 * - login do Suporte Comercial sem Premiação: o painel da equipe de suporte;
 * - admin ou quem tem Premiação: escolhe consultor ou a equipe (começa na equipe).
 *   Só esses chamam /api/minha-performance/agentes.
 *
 * A rota de insights (~1 s) só roda ao abrir, ao trocar consultor/campanha e no Atualizar.
 */

function lerUid(v: string | null): number | null {
  if (!v || v === "suporte_equipe") return null;
  const n = Number(v);
  return Number.isNaN(n) ? null : n;
}

export function MinhaPerformance() {
  const me = useMe();
  const cliente = useQueryClient();
  const [params] = useSearchParams();
  const admin = me.data?.role === "admin";
  const podeEscolher = admin || !!me.data?.pages?.includes("premiacao_admin");
  const kommoUid = me.data?.kommo_user_id ?? null;
  const loginSuporte =
    !!me.data?.is_suporte_comercial || !!me.data?.nav.is_suporte_comercial || String(me.data?.categoria || "").toLowerCase() === "suporte comercial";

  const agentesQ = useQuery({
    queryKey: ["mp", "agentes"],
    queryFn: ({ signal }) => api<Agentes>("/api/minha-performance/agentes", { signal }),
    enabled: !!me.data && podeEscolher,
    staleTime: Infinity,
  });

  // Seleção: undefined até decidir (quem escolhe começa na equipe depois de carregar os agentes).
  const [escolha, setEscolha] = useState<Selecao | undefined>(undefined);
  const uidUrl = lerUid(params.get("uid"));
  let selecao: Selecao | undefined = escolha;
  if (selecao === undefined && me.data) {
    if (loginSuporte && !podeEscolher) selecao = "suporte_equipe";
    else if (uidUrl !== null) selecao = uidUrl;
    else if (!podeEscolher) selecao = null;
    else if (agentesQ.isSuccess) selecao = "suporte_equipe";
    else if (agentesQ.isError) selecao = null;
  }
  const suporteEquipe = selecao === "suporte_equipe";
  const uidEfetivo = suporteEquipe ? null : podeEscolher && selecao ? (selecao as number) : kommoUid;
  const semVinculo = !!me.data && selecao !== undefined && !uidEfetivo && !podeEscolher && !suporteEquipe;

  const campanhasQ = useQuery({
    queryKey: ["mp", "campanhas"],
    queryFn: ({ signal }) => apiOk<Campanhas>("/api/minha-performance/campanhas", { signal }),
    enabled: !!me.data && selecao !== undefined && !semVinculo,
    staleTime: Infinity,
  });
  const [campanhaEscolhida, setCampanhaEscolhida] = useState<number | null>(null);
  const listaCampanhas = campanhasQ.data?.campanhas ?? [];
  const campanhaId = campanhaEscolhida ?? (listaCampanhas.length ? campanhasQ.data?.default_id || listaCampanhas[0].id : null);

  const qsp = new URLSearchParams();
  if (suporteEquipe) qsp.set("suporte_equipe", "1");
  else if (uidEfetivo) qsp.set("kommo_uid", String(uidEfetivo));
  if (campanhaId) qsp.set("campanha_id", String(campanhaId));
  const qs = qsp.toString() ? `?${qsp}` : "";

  const insightsQ = useQuery({
    queryKey: ["mp", "insights", qs],
    queryFn: async ({ signal }) => {
      let d = await api<Insights>(`/api/minha-performance/insights${qs}`, { signal });
      if (d?.campanha && ehVisaoSuporte(d, suporteEquipe)) {
        d = await mesclarVendasComercial(d, signal);
        d = sincronizarHeatmap(d, hojeLocal());
      }
      return d;
    },
    enabled: !!me.data && selecao !== undefined && !semVinculo && !campanhasQ.isPending,
    staleTime: 5 * 60 * 1000,
  });
  const ins = insightsQ.data;
  const temCampanha = !!ins?.campanha;

  const historicoQ = useQuery({
    queryKey: ["mp", "historico", qs],
    queryFn: ({ signal }) => api<Historico>(`/api/minha-performance/historico${qs}`, { signal }),
    enabled: temCampanha,
    staleTime: 5 * 60 * 1000,
  });

  // Trocar consultor ou campanha volta à aba Performance (como o loadMinhaPerformance).
  const [aba, setAba] = useState<{ nome: "performance" | "matriculas"; chave: string }>({ nome: "performance", chave: qs });
  const abaAtual = aba.chave === qs ? aba.nome : "performance";
  // A aba Matrículas só monta (e busca) depois de aberta para este consultor/campanha.
  const [matChave, setMatChave] = useState<string | null>(null);
  const matAberta = matChave === qs;
  function irPara(nome: "performance" | "matriculas") {
    setAba({ nome, chave: qs });
    if (nome === "matriculas") setMatChave(qs);
  }

  const [aviso, setAviso] = useState<Aviso | null>(null);
  useEffect(() => {
    if (!aviso) return;
    const t = setTimeout(() => setAviso(null), 4000);
    return () => clearTimeout(t);
  }, [aviso]);
  const avisar = useCallback((texto: string, erro = false) => setAviso({ tipo: erro ? "erro" : "sucesso", texto }), []);

  function atualizar() {
    cliente.invalidateQueries({ queryKey: ["mp", "insights", qs] });
    cliente.invalidateQueries({ queryKey: ["mp", "historico", qs] });
  }

  // Seletor de consultor (admin / Premiação): equipe, suporte individual e demais.
  const agentes = agentesQ.data?.agentes ?? [];
  const suporteSet = new Set((agentesQ.data?.suporte_uids ?? []).map(Number));
  const agentesSuporte = agentes.filter((a) => suporteSet.has(Number(a.kommo_uid)));
  const outros = agentes.filter((a) => !suporteSet.has(Number(a.kommo_uid)));
  const vendoOutro = !!uidEfetivo && uidEfetivo !== kommoUid;
  const nomeVendo = agentes.find((a) => a.kommo_uid === uidEfetivo)?.name;

  const campanhaAtual = listaCampanhas.find((c) => c.id === campanhaId);
  const layoutSuporte = ins ? ehLayoutSuporte(ins, suporteEquipe) : false;
  const visaoSuporte = ins ? ehVisaoSuporte(ins, suporteEquipe) : false;

  let conteudo;
  if (me.isPending || selecao === undefined || (insightsQ.isPending && !semVinculo)) {
    conteudo = me.isError ? (
      <p role="alert" style={{ color: "var(--danger)" }}>
        {textoErro(me.error)}
      </p>
    ) : (
      <p className="py-16 text-center text-sm" style={TEXTO_FRACO}>
        Carregando seu painel...
      </p>
    );
  } else if (semVinculo) {
    conteudo = (
      <div className="cartao p-10 text-center flex flex-col items-center gap-3">
        <span className="material-symbols-outlined text-4xl" style={TEXTO_FRACO} aria-hidden>
          link_off
        </span>
        <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
          Sua conta ainda não está vinculada ao Kommo.
          <br />
          Peça ao administrador para vincular.
        </p>
      </div>
    );
  } else if (insightsQ.isError) {
    conteudo = (
      <p role="alert" className="cartao p-6 text-sm" style={{ color: "var(--danger)" }}>
        {`Erro ao carregar dados: ${textoErro(insightsQ.error)}`}
      </p>
    );
  } else if (!temCampanha) {
    conteudo = (
      <div className="cartao p-10 text-center flex flex-col items-center gap-3">
        <span className="material-symbols-outlined text-4xl" style={TEXTO_FRACO} aria-hidden>
          event_busy
        </span>
        <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
          Nenhuma campanha ativa no momento.
        </p>
      </div>
    );
  } else {
    const d = ins!;
    conteudo = (
      <>
        {d.campanha?.is_active === false && !visaoSuporte && (
          <div className="cartao p-4 flex items-start gap-3" style={{ borderColor: "#f59e0b80" }}>
            <span className="material-symbols-outlined text-amber-600 dark:text-amber-400" aria-hidden>
              info
            </span>
            <div className="text-sm">
              <p className="font-semibold">Não há campanha vigente no momento</p>
              <p className="text-xs" style={{ color: "var(--text-secondary)" }}>
                Exibindo dados da última campanha cadastrada ({d.campanha?.nome || "—"}). Para iniciar uma nova, vá em <strong>Premiação Admin</strong>.
              </p>
            </div>
          </div>
        )}
        {layoutSuporte && <PainelSuporte d={d} campanhaId={campanhaId} />}
        <div className={abaAtual === "performance" ? "flex flex-col gap-6" : "hidden"}>
          {!layoutSuporte && <Saldo d={d} />}
          {!layoutSuporte && <NiveisDeMeta d={d} />}
          <PixDoDia d={d} />
          {!layoutSuporte && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              <RankingCard d={d} />
              <Conquistas d={d} />
            </div>
          )}
          {!layoutSuporte && <Desbloqueie d={d} />}
          {!layoutSuporte && <Momentum d={d} />}
          {!layoutSuporte && <Sequencia d={d} />}
          <Calendario d={d} />
          {!layoutSuporte && <ProgressoPorFaixa d={d} />}
          {!layoutSuporte && <ResumoFinanceiro d={d} />}
          {!layoutSuporte && <Timeline d={d} />}
          {!layoutSuporte && <CampanhasAnteriores hist={historicoQ.data?.historico ?? []} />}
        </div>
        {matAberta && (
          <div className={abaAtual === "matriculas" ? "" : "hidden"}>
            <Matriculas uid={uidMatriculas(podeEscolher, selecao, kommoUid)} admin={admin} avisar={avisar} />
          </div>
        )}
      </>
    );
  }

  return (
    <div className="flex flex-col gap-6 max-w-7xl">
      <section className="flex flex-wrap gap-2 items-center" aria-label="Filtros">
        {podeEscolher && (
          <>
            <select
              className="controle font-normal max-w-[260px]"
              aria-label="Consultor"
              value={selecao === undefined || selecao === null ? "" : String(selecao)}
              onChange={(e) => {
                const v = e.target.value;
                setEscolha(v === "suporte_equipe" ? "suporte_equipe" : v ? Number(v) : null);
              }}
            >
              {!agentesQ.isSuccess && <option value="">{agentesQ.isError ? "Erro ao carregar agentes" : "Selecione um agente..."}</option>}
              {agentesQ.isSuccess && (
                <>
                  <option value="suporte_equipe">★ Equipe Suporte Comercial</option>
                  {agentesSuporte.length > 0 && <option disabled>— Suporte (individual) —</option>}
                  {agentesSuporte.map((a) => (
                    <option key={a.kommo_uid} value={a.kommo_uid}>
                      {a.name}
                    </option>
                  ))}
                  {outros.length > 0 && <option disabled>— Demais agentes —</option>}
                  {outros.map((a) => (
                    <option key={a.kommo_uid} value={a.kommo_uid}>
                      {a.name}
                    </option>
                  ))}
                </>
              )}
            </select>
            {(suporteEquipe || vendoOutro) && (
              <button type="button" className="controle" onClick={() => setEscolha("suporte_equipe")}>
                Voltar ao meu painel
              </button>
            )}
            {suporteEquipe ? (
              <span className="text-xs font-semibold" style={{ color: "var(--primary)" }}>
                Painel: Equipe Suporte Comercial
              </span>
            ) : vendoOutro ? (
              <span className="text-xs font-semibold" style={{ color: "var(--primary)" }}>
                {nomeVendo ? `Visualizando: ${nomeVendo}` : `Visualizando: ID ${uidEfetivo}`}
              </span>
            ) : null}
          </>
        )}
        {temCampanha && (
          <>
            <select
              className="controle font-normal max-w-[340px]"
              aria-label="Campanha"
              value={campanhaId ? String(campanhaId) : ""}
              disabled={!listaCampanhas.length}
              onChange={(e) => setCampanhaEscolhida(e.target.value ? Number(e.target.value) : null)}
            >
              {campanhasQ.isError && <option value="">Erro ao carregar campanhas</option>}
              {campanhasQ.isSuccess && !listaCampanhas.length && <option value="">Nenhuma campanha cadastrada</option>}
              {listaCampanhas.map((c) => (
                <option key={c.id} value={c.id}>
                  {`${c.is_active ? "🟢 " : ""}${c.nome} · ${data(c.dt_inicio)} – ${data(c.dt_fim)}`}
                </option>
              ))}
            </select>
            {campanhaAtual && (
              <span className="text-xs" style={TEXTO_FRACO}>
                {campanhaAtual.is_active ? "Ativa" : "Histórica"}
              </span>
            )}
          </>
        )}
        <div className="flex gap-2 md:ml-auto">
          {temCampanha && (
            <div className="segmentado">
              <button type="button" aria-pressed={abaAtual === "performance"} onClick={() => irPara("performance")}>
                Performance
              </button>
              <button type="button" aria-pressed={abaAtual === "matriculas"} onClick={() => irPara("matriculas")}>
                Minhas matrículas
              </button>
            </div>
          )}
          {temCampanha && abaAtual === "performance" && (
            <button type="button" className="controle" disabled={insightsQ.isFetching} onClick={atualizar} title="Busca de novo os números desta campanha">
              <span className="material-symbols-outlined text-[18px]" aria-hidden>
                refresh
              </span>
              {insightsQ.isFetching ? "Atualizando…" : "Atualizar"}
            </button>
          )}
        </div>
      </section>
      {conteudo}
      <Toast aviso={aviso} onFechar={() => setAviso(null)} />
    </div>
  );
}

/** _mpEffectiveMatUid: o consultor escolhido (não a equipe) ou o próprio; só número positivo. */
function uidMatriculas(podeEscolher: boolean, selecao: Selecao | undefined, kommoUid: number | null): number | null {
  const bruto = podeEscolher && selecao && selecao !== "suporte_equipe" ? selecao : kommoUid;
  const n = Number(bruto);
  return Number.isFinite(n) && n > 0 ? n : null;
}
