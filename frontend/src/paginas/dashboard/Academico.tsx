import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { useEffect, useState } from "react";
import { api } from "../../api";
import { CartoesTipo, MaisDetalhes, PainelTransferencias, PorNivel, SaudeFinanceiraCartoes, Situacoes, TabelaPolos } from "./Blocos";
import { ComparacaoCiclos } from "./Ciclos";
import { EvolucaoMatriculas } from "./Evolucao";
import {
  FILTROS_VAZIOS,
  detalheDoMes,
  normSit,
  numero,
  textoFiltrando,
  urlAlunos,
  urlCiclos,
  urlEvolucao,
  urlSaude,
  urlTransferencias,
  type Detalhe,
} from "./formato";
import { FunilVendas } from "./Funil";
import type { Alunos, CicloDistinto, Ciclos, Evolucao, FiltrosDash, Fonte, SaudeFinanceira, Tipo, Transferencias } from "./tipos";

/**
 * Dashboard Acadêmico (a home de quem não é comercial nem supervisor): o mesmo
 * conteúdo e os mesmos números de templates/partials/_dashboard.html + static/js/dashboard.js.
 *
 * Quando cada busca dispara (igual à antiga, salvo o período):
 * - abre: fonte de dados, funil e leads de hoje; ciclos-distinct escolhe o ciclo
 *   (o salvo em `dash_ciclo_v2`, se ainda existe, ou o mais recente) e só então os alunos;
 * - ciclo, nível e situação aplicam na hora; as datas só no "Aplicar";
 * - um bloco por vez, como o _stuRefreshFiltered: alunos → evolução → saúde financeira → ciclos.
 *   Assim a rota pesada de alunos nunca disputa o servidor com as outras da mesma tela;
 * - cartões (tipo, situação, polo, nível) filtram alunos, evolução e saúde; os ciclos
 *   só dependem do nível e não buscam de novo quando ele não muda.
 * Nenhuma consulta tenta de novo sozinha (a antiga também não).
 */

export const CHAVE_CICLO = "dash_ciclo_v2";

function lerSalvo(): string | null {
  try {
    return localStorage.getItem(CHAVE_CICLO);
  } catch {
    return null;
  }
}

function salvarCiclo(ciclo: string) {
  try {
    if (ciclo) localStorage.setItem(CHAVE_CICLO, ciclo);
    else localStorage.removeItem(CHAVE_CICLO);
  } catch {
    /* navegador sem armazenamento: só não lembra o ciclo */
  }
}

const msg = (e: unknown) => (e instanceof Error && e.message ? e.message : "Falha ao carregar");

const SITUACOES = ["Em Curso", "Cancelado", "Trancado", "Transferido"];

/** Valor do select de situação que bate com a situação ativa (sem acento/maiúscula). */
function valorSituacao(ativa: string): string {
  if (!ativa) return "";
  return SITUACOES.find((s) => normSit(s) === normSit(ativa)) ?? ativa;
}

export function DashboardAcademico() {
  const distintosQ = useQuery({
    queryKey: ["dashboard", "ciclos-distinct"],
    retry: false,
    staleTime: Infinity,
    queryFn: ({ signal }) => api<CicloDistinto[]>("/api/dashboard/ciclos-distinct", { signal }),
  });
  const distintos = Array.isArray(distintosQ.data) ? distintosQ.data : [];
  const maisRecente = distintos[0]?.nome ?? "";

  // Filtros aplicados. Até a pessoa mexer, valem os de abertura (ciclo salvo ou mais recente).
  const [escolhidos, setEscolhidos] = useState<FiltrosDash | null>(null);
  const abertura: FiltrosDash | null = distintosQ.isPending
    ? null
    : (() => {
        const salvo = lerSalvo();
        const nomes = distintos.map((c) => c.nome);
        return { ...FILTROS_VAZIOS, ciclo: salvo && nomes.includes(salvo) ? salvo : maisRecente };
      })();
  const filtros = escolhidos ?? abertura;
  const chave = filtros ? JSON.stringify(filtros) : "";

  // O ciclo aplicado fica salvo para a próxima vez (como o applyDashboardFilters).
  const cicloAplicado = filtros?.ciclo;
  useEffect(() => {
    if (cicloAplicado !== undefined) salvarCiclo(cicloAplicado);
  }, [cicloAplicado]);

  // Período: rascunho até o "Aplicar".
  const [datas, setDatas] = useState({ de: "", ate: "" });
  const [extras, setExtras] = useState(false);

  // Mês aberto no gráfico e painel de transferências: valem só para os filtros em que foram abertos.
  const [detalheSel, setDetalhe] = useState<{ chave: string; d: Detalhe } | null>(null);
  const detalhe = detalheSel && detalheSel.chave === chave ? detalheSel.d : null;
  const [transfAberto, setTransfAberto] = useState<string | null>(null);
  const mostrarTransf = transfAberto === chave && !!filtros;

  function aplicar(novo: Partial<FiltrosDash>) {
    if (!filtros) return;
    setEscolhidos({ ...filtros, ...novo });
  }

  const alternar = (campo: "tipo" | "polo", valor: string) => aplicar({ [campo]: filtros?.[campo] === valor ? "" : valor });
  const alternarSituacao = (s: string) => aplicar({ situacao: normSit(filtros?.situacao) === normSit(s) ? "" : s });
  const alternarNivel = (n: string) => aplicar({ nivel: filtros?.nivel === n ? "" : n });

  /** Limpar (clearStudentFilter): ciclo mais recente, sem período, nível, situação, tipo e polo. */
  function limpar() {
    setDatas({ de: "", ate: "" });
    setEscolhidos({ ...FILTROS_VAZIOS, ciclo: maisRecente });
  }

  const fonteQ = useQuery({
    queryKey: ["dashboard", "fonte"],
    retry: false,
    queryFn: ({ signal }) => api<Fonte>("/api/dashboard", { signal }),
  });

  const alunosQ = useQuery({
    queryKey: ["dashboard", "alunos", filtros ? urlAlunos(filtros) : ""],
    enabled: !!filtros,
    retry: false,
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) => api<Alunos>(urlAlunos(filtros!), { signal }),
  });
  const evolQ = useQuery({
    queryKey: ["dashboard", "evolucao", filtros ? urlEvolucao(filtros, detalhe) : ""],
    enabled: !!filtros && !alunosQ.isFetching,
    retry: false,
    placeholderData: keepPreviousData,
    queryFn: ({ signal }) => api<Evolucao>(urlEvolucao(filtros!, detalhe), { signal }),
  });
  const saudeQ = useQuery({
    queryKey: ["dashboard", "saude", filtros ? urlSaude(filtros) : ""],
    enabled: !!filtros && !alunosQ.isFetching && !evolQ.isFetching,
    retry: false,
    placeholderData: keepPreviousData,
    // Sem período na rota: trocar só as datas (ou abrir um mês no gráfico) não busca de novo.
    staleTime: Infinity,
    queryFn: ({ signal }) => api<SaudeFinanceira>(urlSaude(filtros!), { signal }),
  });
  const nivel = filtros?.nivel ?? "";
  const ciclosQ = useQuery({
    queryKey: ["dashboard", "ciclos", nivel],
    enabled: !!filtros && !alunosQ.isFetching && !evolQ.isFetching && !saudeQ.isFetching,
    retry: false,
    // Só o nível muda os ciclos: cartões e outros filtros não buscam de novo.
    staleTime: Infinity,
    queryFn: ({ signal }) => api<Ciclos>(urlCiclos(nivel), { signal }),
  });
  const transfQ = useQuery({
    queryKey: ["dashboard", "transferencias", filtros ? urlTransferencias(filtros) : ""],
    enabled: mostrarTransf,
    retry: false,
    queryFn: async ({ signal }) => {
      const d = await api<Transferencias>(urlTransferencias(filtros!), { signal });
      if (!d.ok) throw new Error(d.error || "Erro");
      return d;
    },
  });

  const alunos = alunosQ.data;
  const filtrando = filtros ? textoFiltrando(filtros) : "";
  const temCartoes = !!(filtros?.tipo || filtros?.situacao || filtros?.polo);

  return (
    <div className="flex flex-col gap-6 max-w-7xl">
      <FunilVendas />

      <FonteDados fonte={fonteQ.data} erro={fonteQ.isError ? msg(fonteQ.error) : null} />

      {/* Matrículas por tipo de aluno, com os filtros da página. */}
      <section className="cartao overflow-hidden" aria-label="Matrículas por tipo de aluno">
        <div className="px-5 pt-5 pb-3 flex flex-wrap items-center justify-between gap-3">
          <h2 className="text-base flex items-center gap-2">
            <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>groups</span>
            Matrículas por tipo de aluno
          </h2>
          <div className="flex flex-wrap items-center gap-2" aria-label="Filtros da página" role="group">
            <select
              className="controle font-normal"
              aria-label="Ciclo"
              value={filtros?.ciclo ?? ""}
              disabled={!filtros}
              onChange={(e) => aplicar({ ciclo: e.target.value })}
            >
              <option value="">Todos os ciclos</option>
              {distintos.map((c) => (
                <option key={c.nome} value={c.nome}>
                  {c.nome}
                  {c.total != null ? ` (${numero(c.total)})` : ""}
                </option>
              ))}
            </select>
            <select className="controle font-normal" aria-label="Nível" value={nivel} disabled={!filtros} onChange={(e) => aplicar({ nivel: e.target.value })}>
              <option value="">Todos os níveis</option>
              <option value="Graduação">Graduação</option>
              <option value="Pós-Graduação">Pós-Graduação</option>
            </select>
            <button type="button" className="controle" onClick={limpar} disabled={!filtros}>Limpar</button>
            <button type="button" className="controle" aria-expanded={extras} onClick={() => setExtras((x) => !x)}>
              <span className="material-symbols-outlined text-[18px]" aria-hidden>tune</span>
              Filtros
            </button>
            {(alunosQ.isFetching || evolQ.isFetching || saudeQ.isFetching) && filtros && (
              <span className="text-xs" role="status" style={{ color: "var(--text-muted)" }}>Atualizando…</span>
            )}
          </div>
        </div>

        <div className="px-5 pb-5 flex flex-col gap-4">
          {extras && (
            <div className="flex flex-wrap items-end gap-2 rounded-2xl p-3" style={{ background: "var(--bg-elevated)", border: "1px solid var(--border-subtle)" }}>
              <select
                className="controle font-normal"
                aria-label="Situação"
                value={valorSituacao(filtros?.situacao ?? "")}
                disabled={!filtros}
                onChange={(e) => aplicar({ situacao: e.target.value })}
              >
                <option value="">Todas as situações</option>
                {SITUACOES.map((s) => (
                  <option key={s} value={s}>{s}</option>
                ))}
              </select>
              <div className="intervalo">
                <span className="material-symbols-outlined text-[18px] mr-1" aria-hidden>calendar_month</span>
                <input type="date" aria-label="De" value={datas.de} onChange={(e) => setDatas((d) => ({ ...d, de: e.target.value }))} />
                <span className="px-1" aria-hidden>–</span>
                <input type="date" aria-label="Até" value={datas.ate} onChange={(e) => setDatas((d) => ({ ...d, ate: e.target.value }))} />
              </div>
              <button type="button" className="controle controle-primario" disabled={!filtros} onClick={() => aplicar({ de: datas.de, ate: datas.ate })}>
                Aplicar
              </button>
            </div>
          )}

          {filtrando && (
            <div className="flex items-center gap-2 px-3 py-2 rounded-xl text-xs" style={{ background: "var(--primary-light)", color: "var(--primary)" }}>
              <span className="material-symbols-outlined text-[16px]" aria-hidden>filter_alt</span>
              <span className="font-medium">{filtrando}</span>
              {temCartoes && (
                <button
                  type="button"
                  className="ml-auto botao-texto !py-0.5 text-[11px]"
                  aria-label="Limpar filtros dos cartões"
                  onClick={() => aplicar({ tipo: "", situacao: "", polo: "" })}
                >
                  Limpar
                </button>
              )}
            </div>
          )}

          {alunosQ.isError ? (
            <p role="alert" className="py-4 text-center text-sm" style={{ color: "var(--danger)" }}>Erro ao carregar: {msg(alunosQ.error)}</p>
          ) : !alunos || !filtros ? (
            <p className="py-8 text-center text-sm" style={{ color: "var(--text-muted)" }}>Carregando…</p>
          ) : (
            <div style={{ opacity: alunosQ.isPlaceholderData ? 0.6 : 1 }}>
              <CartoesTipo dados={alunos} filtros={filtros} alternarTipo={(t: Tipo) => alternar("tipo", t)} />
            </div>
          )}
        </div>
      </section>

      <section className="cartao overflow-hidden" aria-label="Matrículas por situação">
        <div className="px-5 pt-5 pb-3 flex flex-wrap items-center justify-between gap-2">
          <h2 className="text-base flex items-center gap-2">
            <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>check_circle</span>
            Matrículas por situação
          </h2>
          <div className="flex flex-wrap items-center gap-2">
            {filtros?.situacao && (
              <span className="text-[11px] px-2 py-0.5 rounded-full font-medium" style={{ background: "var(--primary-light)", color: "var(--primary)" }}>
                Filtro: {filtros.situacao}
              </span>
            )}
            <select
              className="controle font-normal"
              aria-label="Filtrar por situação"
              title="Filtrar todo o painel por situação"
              value={valorSituacao(filtros?.situacao ?? "")}
              disabled={!filtros}
              onChange={(e) => aplicar({ situacao: e.target.value })}
            >
              <option value="">Todas as situações</option>
              {SITUACOES.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
            {filtros?.situacao && (
              <button type="button" className="controle" aria-label="Limpar situação" onClick={() => aplicar({ situacao: "" })}>
                Limpar
              </button>
            )}
            <span className="text-[11px] hidden sm:inline" style={{ color: "var(--text-muted)" }}>ou clique no cartão</span>
          </div>
        </div>
        <div className="px-5 pb-5">
          {alunos && filtros ? (
            <>
              <Situacoes
                dados={alunos.by_situacao}
                transferencias={Number(alunos.inbound_transfers) || 0}
                ativa={filtros.situacao}
                alternar={alternarSituacao}
                abrirTransferencias={() => setTransfAberto(chave)}
              />
              {mostrarTransf && (
                <PainelTransferencias
                  dados={transfQ.data}
                  erro={transfQ.isError ? msg(transfQ.error) : null}
                  carregando={transfQ.isFetching}
                  fechar={() => setTransfAberto(null)}
                />
              )}
            </>
          ) : (
            <p className="text-sm" style={{ color: "var(--text-muted)" }}>—</p>
          )}
        </div>
      </section>

      <SaudeFinanceiraCartoes dados={saudeQ.data} erro={saudeQ.isError ? msg(saudeQ.error) : null} atualizando={saudeQ.isPlaceholderData} />

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6 items-start">
        <TabelaPolos porPolo={alunos?.by_polo} ativo={filtros?.polo ?? ""} alternar={(p) => alternar("polo", p)} />
        <PorNivel porNivel={alunos?.by_nivel} nivel={nivel} alternar={alternarNivel} />
      </div>

      <MaisDetalhes porTurma={alunos?.by_turma} porCiclo={alunos?.by_ciclo} />

      {filtros && (
        <EvolucaoMatriculas
          dados={evolQ.data}
          erro={evolQ.isError ? msg(evolQ.error) : null}
          atualizando={evolQ.isFetching}
          filtros={filtros}
          detalhe={detalhe}
          abrirMes={(mes) => setDetalhe({ chave, d: detalheDoMes(mes) })}
          voltar={() => setDetalhe(null)}
        />
      )}

      <ComparacaoCiclos dados={ciclosQ.data} erro={ciclosQ.isError ? msg(ciclosQ.error) : null} carregando={ciclosQ.isPending} nivel={nivel} />
    </div>
  );
}

function FonteDados({ fonte, erro }: { fonte: Fonte | undefined; erro: string | null }) {
  let texto: string;
  let cor = "var(--text-primary)";
  if (erro) {
    texto = `Não foi possível carregar: ${erro}`;
    cor = "var(--danger)";
  } else if (!fonte) texto = "Carregando...";
  else if (fonte.snapshot) {
    const s = fonte.snapshot;
    texto = `${s.filename} — ${numero(s.row_count)} registros (${s.uploaded_at})`;
  } else {
    texto = "Nenhum snapshot de matriculados carregado";
    cor = "var(--aviso)";
  }
  const status = fonte?.sync_running ? "Sync..." : fonte?.update_running ? "Update..." : "Conectado";
  const corStatus = fonte?.sync_running ? "var(--primary)" : fonte?.update_running ? "var(--aviso)" : "var(--positivo)";
  return (
    <section className="cartao p-4 flex items-center justify-between gap-3 flex-wrap" aria-label="Fonte de dados">
      <div className="flex items-center gap-3 min-w-0">
        <span className="material-symbols-outlined text-[20px]" aria-hidden style={{ color: "var(--primary)" }}>database</span>
        <div className="min-w-0">
          <p className="rotulo">Fonte de dados</p>
          <p className="text-xs font-medium mt-0.5" style={{ color: cor }}>{texto}</p>
        </div>
      </div>
      {fonte && (
        <span className="flex items-center gap-2 text-xs" style={{ color: "var(--text-muted)" }}>
          <span className={`w-2.5 h-2.5 rounded-full ${status === "Conectado" ? "" : "animate-pulse"}`} aria-hidden style={{ background: corStatus }} />
          {status}
        </span>
      )}
    </section>
  );
}
