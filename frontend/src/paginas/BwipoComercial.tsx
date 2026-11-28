import { useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useRef, useState, type ReactNode } from "react";
import { api } from "../api";
import { Toast } from "../componentes/Toast";
import { hojeIso } from "../datas";
import type { Aviso } from "../fixadas";
import { useMe } from "../me";
import type { Acoes } from "./bwipo-comercial/acoes";
import { AtualizarLead } from "./bwipo-comercial/AtualizarLead";
import { CartaoConflito } from "./bwipo-comercial/CartaoConflito";
import { ConsultaRgm } from "./bwipo-comercial/ConsultaRgm";
import { DetalheAgente } from "./bwipo-comercial/DetalheAgente";
import { ModalDiagnostico, ModalDuplicatas, ModalSemData, rgmsRecorte } from "./bwipo-comercial/Ferramentas";
import {
  apiOk,
  digitos,
  diaMes,
  dinheiro,
  enviar,
  linhasRanking,
  NIVEIS,
  nivelRanking,
  num,
  periodoMaisRecente,
  qsPainel,
  textoErro,
} from "./bwipo-comercial/formato";
import { GraficoAgentes, GraficoDias } from "./bwipo-comercial/Graficos";
import { ListaAlunos } from "./bwipo-comercial/ListaAlunos";
import type {
  Ciclo,
  Conflitos,
  ConsultaRgm as Consulta,
  Extras,
  Filtros,
  Kpis,
  MiniSync,
  Painel,
  SnapshotInfo,
  SyncAgentes,
  Turma,
} from "./bwipo-comercial/tipos";

/**
 * Dashboard Comercial Bwipo (página `bwipo_comercial`). Mesmas APIs e mesmos números
 * da tela antiga (templates/partials/_bwipo_comercial.html + static/js/bwipo_comercial.js).
 * Venda, dono da venda, evasão e conflitos são calculados em routes/bwipo_sync.py.
 *
 * - `GET /api/bwipo/painel` (pesada, cache de 5 min no servidor) só vai na abertura,
 *   no Atualizar e ao trocar ciclo/turma, como na antiga; polo, nível, agente e datas
 *   mexem num rascunho até o Atualizar.
 * - Depois de cada resposta do painel: YTD (`/extras`) e conflitos (`POST /conflitos`).
 * - Ações que gravam (fixar/desfazer venda, contar venda) pedem a mesma confirmação.
 */

interface Periodo {
  ini: string;
  fim: string;
}

/** Período inicial (bwcPreparar): meta ou campanha mais recente; senão, do dia 1 até hoje. */
async function periodoInicial(signal?: AbortSignal): Promise<Periodo> {
  type ComPeriodos = { metas?: { dt_inicio?: string; dt_fim?: string }[]; campanhas?: { dt_inicio?: string; dt_fim?: string }[] };
  const [metas, camps] = await Promise.all([
    api<ComPeriodos>("/api/comercial-rgm/metas?categoria=matriculas", { signal }).catch((): ComPeriodos => ({})),
    api<ComPeriodos>("/api/premiacao/campanhas-periodos", { signal }).catch((): ComPeriodos => ({})),
  ]);
  const p = periodoMaisRecente([...(metas.metas || []), ...(camps.campanhas || [])]);
  if (p) return { ini: p.dt_inicio, fim: p.dt_fim };
  const hoje = new Date();
  return { ini: hojeIso(new Date(hoje.getFullYear(), hoje.getMonth(), 1)), fim: hojeIso(hoje) };
}

type ModalAberto =
  | { tipo: "rgm"; dados: Consulta }
  | { tipo: "agente"; nome: string; dia: string | null }
  | { tipo: "duplicatas" }
  | { tipo: "semdata" }
  | { tipo: "diagnostico" };

const SEM_FILTRO = { polo: "", nivel: "", agente: "" };

export function BwipoComercial() {
  const me = useMe();
  const admin = me.data?.role === "admin";

  // ---- Aviso flutuante (o toast da antiga) ----
  const [aviso, setAviso] = useState<Aviso | null>(null);
  const tempo = useRef<ReturnType<typeof setTimeout>>();
  const avisar = useCallback((tipo: Aviso["tipo"], texto: string, ms = 4000) => {
    clearTimeout(tempo.current);
    setAviso({ tipo, texto });
    tempo.current = setTimeout(() => setAviso(null), ms);
  }, []);
  useEffect(() => () => clearTimeout(tempo.current), []);

  // ---- Filtros ----
  const periodoQ = useQuery({ queryKey: ["bwipo", "periodo-inicial"], queryFn: ({ signal }) => periodoInicial(signal), staleTime: Infinity, retry: false });
  const padrao: Filtros | null = periodoQ.data ? { ...SEM_FILTRO, ...periodoQ.data } : null;
  const [rascunhoEscolhido, setRascunho] = useState<Filtros | null>(null);
  const [aplicadoEscolhido, setAplicado] = useState<Filtros | null>(null);
  const rascunho = rascunhoEscolhido ?? padrao;
  const aplicado = aplicadoEscolhido ?? padrao;
  const qs = aplicado ? qsPainel(aplicado) : "";

  const [ciclo, setCiclo] = useState("");
  const [turma, setTurma] = useState("");
  const ciclosQ = useQuery({
    queryKey: ["bwipo", "ciclos"],
    queryFn: async ({ signal }) => (await api<{ ok?: boolean; ciclos?: Ciclo[] }>("/api/comercial-rgm/ciclos", { signal })).ciclos ?? [],
    staleTime: Infinity,
    retry: false,
  });
  const turmasQ = useQuery({
    queryKey: ["bwipo", "turmas", ciclo],
    queryFn: async ({ signal }) =>
      (await api<{ ok?: boolean; turmas?: Turma[] }>(`/api/comercial-rgm/turmas${ciclo ? `?ciclo_id=${ciclo}` : ""}`, { signal })).turmas ?? [],
    staleTime: Infinity,
    retry: false,
    placeholderData: (anterior) => anterior,
  });
  const turmas = turmasQ.data ?? [];
  const turmaValor = turmas.some((t) => String(t.id) === turma) ? turma : "";
  const snapshotQ = useQuery({
    queryKey: ["bwipo", "snapshot"],
    queryFn: ({ signal }) => api<SnapshotInfo>("/api/comercial-rgm/snapshot-info", { signal }),
    staleTime: Infinity,
    retry: false,
  });

  // ---- Painel (rota principal) ----
  const painelQ = useQuery({
    queryKey: ["bwipo", "painel", qs],
    queryFn: ({ signal }) => apiOk<Painel>(`/api/bwipo/painel?${qs}`, signal),
    enabled: !!aplicado,
    retry: false,
  });
  const painel = painelQ.data;
  /** Muda a cada resposta do painel: extras, conflitos e o filtro de dia seguem essa versão. */
  const versao = painelQ.dataUpdatedAt;
  const k: Partial<Kpis> = painel?.kpis ?? {};
  const linhas = painel?.linhas ?? [];
  const opcoes = painel?.opcoes ?? { polos: [], agentes: [] };

  const extrasQ = useQuery({
    queryKey: ["bwipo", "extras", qs, versao],
    queryFn: ({ signal }) => apiOk<Extras>(`/api/bwipo/painel/extras?${qs}`, signal),
    enabled: !!painel && versao > 0,
    staleTime: Infinity,
    retry: false,
  });
  const conflitosQ = useQuery({
    queryKey: ["bwipo", "conflitos", qs, versao],
    queryFn: ({ signal }) =>
      enviar<Conflitos>(
        "/api/bwipo/painel/conflitos",
        "POST",
        { rgms: rgmsRecorte(linhas), dt_ini: aplicado?.ini || "", dt_fim: aplicado?.fim || "" },
        signal,
      ),
    enabled: !!painel && versao > 0,
    staleTime: Infinity,
    retry: false,
  });

  /** Valor do select só se ainda existe nas opções (_bwcFillSelect). */
  // Só depois de uma resposta do painel (enquanto carrega ou após erro, o rascunho fica como está).
  const valido = (valor: string, lista: string[]) => (!painel || (valor && lista.includes(valor)) ? valor : "");
  const rascunhoVisivel: Filtros | null = rascunho
    ? { ...rascunho, polo: valido(rascunho.polo, opcoes.polos), agente: valido(rascunho.agente, opcoes.agentes) }
    : null;

  /** Atualizar (bwcAtualizar): aplica o que está nos filtros e busca o painel. */
  function atualizar(base?: Filtros) {
    const novo = base ?? rascunhoVisivel;
    if (!novo) return;
    setRascunho(novo);
    if (aplicado && qsPainel(novo) === qsPainel(aplicado)) {
      setAplicado(novo);
      void painelQ.refetch();
    } else {
      setAplicado(novo);
    }
  }

  function mudar(campo: keyof Filtros, valor: string) {
    if (!rascunhoVisivel) return;
    setRascunho({ ...rascunhoVisivel, [campo]: valor });
  }

  /** Ciclo comercial só preenche De/Até (trocando se vierem invertidas) e já busca. */
  function mudarCiclo(valor: string) {
    setCiclo(valor);
    if (!rascunhoVisivel) return;
    const c = (ciclosQ.data ?? []).find((x) => x.id === parseInt(valor, 10));
    let novo = rascunhoVisivel;
    if (c) {
      const [ini, fim] = c.dt_inicio && c.dt_fim && c.dt_inicio > c.dt_fim ? [c.dt_fim, c.dt_inicio] : [c.dt_inicio, c.dt_fim];
      novo = { ...novo, ini, fim };
    }
    atualizar(novo);
  }

  /** Turma comercial preenche De/Até e o nível, e já busca. */
  function mudarTurma(valor: string) {
    setTurma(valor);
    if (!rascunhoVisivel) return;
    const t = turmas.find((x) => x.id === parseInt(valor, 10));
    let novo = rascunhoVisivel;
    if (t) {
      novo = { ...novo, ini: String(t.dt_inicio).slice(0, 10), fim: String(t.dt_fim).slice(0, 10) };
      if (t.nivel) novo.nivel = NIVEIS.includes(t.nivel) ? t.nivel : "";
    }
    atualizar(novo);
  }

  // ---- Dia escolhido no gráfico (vale só para a resposta atual do painel) ----
  const [diaSel, setDiaSel] = useState<{ dia: string; versao: number } | null>(null);
  const dia = diaSel && diaSel.versao === versao ? diaSel.dia : null;
  const ranking = linhasRanking(painel, dia);
  const linhasDia = dia ? linhas.filter((l) => l.data === dia) : linhas;
  const evasaoDia = linhasDia.filter((l) => l.situacao && l.situacao !== "EM CURSO");
  const foraDia = linhasDia.filter((l) => l.fora);

  // ---- Painéis que abrem e fecham ----
  const [abertos, setAbertos] = useState({ evasao: false, fora: false, conflitos: false, lead: false });
  const alternar = (p: keyof typeof abertos) => setAbertos((a) => ({ ...a, [p]: !a[p] }));

  // ---- Modais e ações ----
  const [modal, setModal] = useState<ModalAberto | null>(null);
  const modalRef = useRef(modal);
  modalRef.current = modal;
  const [rgmBusca, setRgmBusca] = useState("");
  const [sincronizando, setSincronizando] = useState(false);

  async function consultarRgm(arg?: string) {
    const q = digitos(arg ?? rgmBusca);
    if (q.length !== 8) {
      avisar("erro", "Informe um RGM com 8 dígitos.");
      return;
    }
    try {
      const d = await apiOk<Consulta>(`/api/bwipo/painel/rgm?rgm=${q}`);
      setModal({ tipo: "rgm", dados: d });
    } catch (e) {
      avisar("erro", textoErro(e));
    }
  }

  /** Depois de gravar (_bwcDepoisDeDecidir): aviso, consulta o RGM de novo se havia janela aberta e atualiza. */
  function depoisDeDecidir(rgm: string | undefined, msg: string) {
    avisar("sucesso", msg);
    if (modalRef.current && rgm) void consultarRgm(rgm);
    atualizar();
  }

  const acoes: Acoes = {
    admin,
    consultarRgm: (rgm) => void consultarRgm(rgm),
    fixarVenda(rgm, userId, nome, jaConfirmou) {
      if (!jaConfirmou && !window.confirm(`Fixar a venda do RGM ${rgm} em ${nome}?`)) return;
      enviar("/api/bwipo/painel/conflitos/resolver", "POST", { rgm, user_id: userId, user_name: nome })
        .then(() => depoisDeDecidir(rgm, `Venda do RGM ${rgm} fixada em ${nome}.`))
        .catch((e) => avisar("erro", textoErro(e)));
    },
    desfazerVenda(rgm) {
      if (!window.confirm(`Tirar a decisão manual do RGM ${rgm}? A venda volta para a regra automática.`)) return;
      enviar("/api/bwipo/painel/conflitos/resolver", "DELETE", { rgm })
        .then(() => depoisDeDecidir(rgm, `RGM ${rgm} voltou para a regra automática.`))
        .catch((e) => avisar("erro", textoErro(e)));
    },
    contarVenda(rgm, contar) {
      if (!admin) return;
      const txt = contar ? "passar a contar como venda" : "deixar de contar como venda";
      if (!window.confirm(`RGM ${rgm}: ${txt}?`)) return;
      enviar("/api/bwipo/painel/contar-venda", contar ? "POST" : "DELETE", { rgm })
        .then(() => depoisDeDecidir(rgm, contar ? `RGM ${rgm} passa a contar.` : `RGM ${rgm} deixou de contar.`))
        .catch((e) => avisar("erro", textoErro(e)));
    },
    syncCandidato(dealId, rgm) {
      enviar<MiniSync>("/api/bwipo/painel/mini-sync", "POST", { deal_id: dealId })
        .then((d) => depoisDeDecidir(rgm || d.rgm, d.msg || "Sincronizado."))
        .catch((e) => avisar("erro", textoErro(e), 7000));
    },
  };

  function syncAgentes() {
    if (!admin) return;
    setSincronizando(true);
    enviar<SyncAgentes>("/api/bwipo/painel/sync-agentes", "POST", {})
      .then((d) => {
        avisar("sucesso", `Kommo: ${d.kommo_users ?? "—"} usuários · Bwipo ligados: ${d.depara} · sem depara: ${d.bwipo_sem_depara}`, 7000);
        atualizar();
      })
      .catch((e) => avisar("erro", textoErro(e)))
      .finally(() => setSincronizando(false));
  }

  const fecharModal = useCallback(() => setModal(null), []);

  // ---- Números derivados ----
  const emCursoFora = linhas.filter((l) => l.fora && l.situacao === "EM CURSO").length;
  const chips = (
    [
      ["Cancelado", /cancel/i, "bg-rose-500/15 text-rose-700 dark:text-rose-300"],
      ["Trancado", /tranc/i, "bg-amber-500/15 text-amber-700 dark:text-amber-300"],
      ["Transferido", /transfer/i, "bg-violet-500/15 text-violet-700 dark:text-violet-300"],
    ] as const
  )
    .map(([rotulo, re, cls]) => {
      const n = linhas.filter((l) => l.situacao && l.situacao !== "EM CURSO" && re.test(l.situacao)).length;
      return { rotulo, n, cls };
    })
    .filter((c) => c.n);
  const porRgm = Object.fromEntries(linhas.map((l) => [l.rgm, l]));
  const maxPolo = (painel?.polos ?? []).reduce((m, p) => Math.max(m, p.total), 1);
  const conflitos = conflitosQ.data;
  const snap = snapshotQ.data;
  let infoSnapshot = "";
  if (snap?.ok && snap.total) {
    infoSnapshot = `${Number(snap.total).toLocaleString("pt-BR")} registros CSV`;
    if (snap.min_date && snap.max_date) infoSnapshot += ` | ${snap.min_date} a ${snap.max_date}`;
    if (snap.mm_inscritos || snap.mm_matriculados) {
      infoSnapshot += ` | M&M: ${num(snap.mm_inscritos)} insc. / ${num(snap.mm_matriculados)} matr.`;
    }
  }
  const temDados = !!painel;
  const traco = (v: string) => (temDados ? v : "—");

  return (
    <div className="flex flex-col gap-6 max-w-7xl">
      {/* Filtros numa linha (controles de 36px) e Atualizar à direita. */}
      <section className="flex flex-wrap gap-2 items-center" aria-label="Filtros">
        <select className="controle font-normal" aria-label="Ciclo" value={ciclo} onChange={(e) => mudarCiclo(e.target.value)}>
          <option value="">Todos os ciclos</option>
          {(ciclosQ.data ?? []).map((c) => (
            <option key={c.id} value={String(c.id)}>
              {`${c.descricao || c.nome}${c.ativo ? " (ativo)" : ""}`}
            </option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Turma" value={turmaValor} onChange={(e) => mudarTurma(e.target.value)}>
          <option value="">Todas as turmas</option>
          {Object.entries(
            turmas.reduce<Record<string, Turma[]>>((g, t) => {
              (g[t.nivel || "Outros"] ||= []).push(t);
              return g;
            }, {}),
          ).map(([nivel, lista]) => (
            <optgroup key={nivel} label={nivel}>
              {lista.map((t) => (
                <option key={t.id} value={String(t.id)}>{t.nome}</option>
              ))}
            </optgroup>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Polo" value={rascunhoVisivel?.polo ?? ""} onChange={(e) => mudar("polo", e.target.value)}>
          <option value="">Todos os polos</option>
          {opcoes.polos.map((p) => (
            <option key={p} value={p}>{p}</option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Nível" value={rascunhoVisivel?.nivel ?? ""} onChange={(e) => mudar("nivel", e.target.value)}>
          <option value="">Todos os níveis</option>
          {NIVEIS.map((n) => (
            <option key={n} value={n}>{n}</option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Agente" value={rascunhoVisivel?.agente ?? ""} onChange={(e) => mudar("agente", e.target.value)}>
          <option value="">Todos os agentes</option>
          {opcoes.agentes.map((a) => (
            <option key={a} value={a}>{a}</option>
          ))}
        </select>
        <div className="intervalo">
          <input type="date" aria-label="De" value={rascunhoVisivel?.ini ?? ""} onChange={(e) => mudar("ini", e.target.value)} />
          <span className="px-1">até</span>
          <input type="date" aria-label="Até" value={rascunhoVisivel?.fim ?? ""} onChange={(e) => mudar("fim", e.target.value)} />
        </div>
        <div className="flex gap-2 md:ml-auto">
          <button
            type="button"
            className="controle controle-primario"
            disabled={!rascunhoVisivel || painelQ.isFetching}
            onClick={() => atualizar()}
          >
            <span className="material-symbols-outlined text-[18px]" aria-hidden>refresh</span>
            {painelQ.isFetching ? "Atualizando…" : "Atualizar"}
          </button>
        </div>
      </section>

      {/* Ferramentas da tela; as que dependem dos painéis do comercial antigo abrem lá. */}
      <section className="flex flex-wrap gap-2 items-center -mt-3" aria-label="Ferramentas">
        <p className="text-xs flex-1 min-w-[12rem]" style={{ color: "var(--text-muted)" }}>
          {infoSnapshot || "Performance comercial e ranking de agentes"}
        </p>
        <button type="button" className="controle" onClick={() => alternar("lead")} aria-expanded={abertos.lead}>
          <span className="material-symbols-outlined text-[18px]" aria-hidden>sync</span>
          Atualizar lead
        </button>
        <button type="button" className="controle" disabled={!temDados} onClick={() => setModal({ tipo: "duplicatas" })}>
          Duplicatas
        </button>
        <button type="button" className="controle" disabled={!temDados} onClick={() => setModal({ tipo: "semdata" })}>
          Sem data
        </button>
        <button type="button" className="controle" onClick={() => setModal({ tipo: "diagnostico" })}>
          Diagnóstico
        </button>
        {admin && (
          <button type="button" className="controle" disabled={sincronizando} onClick={syncAgentes}>
            {sincronizando ? "Sincronizando…" : "Sync agentes"}
          </button>
        )}
        <a className="controle" href="/#bwipo_comercial" title="Metas, consultores, ciclos, turmas, upload, Sync Kommo, histórico, congelar e solicitações">
          <span className="material-symbols-outlined text-[18px]" aria-hidden>open_in_new</span>
          Mais na tela atual
        </a>
      </section>

      {abertos.lead && <AtualizarLead onFechar={() => alternar("lead")} onAviso={avisar} onFixou={() => atualizar()} />}

      {painelQ.isError && (
        <div role="alert" className="cartao p-4 text-sm" style={{ color: "var(--danger)" }}>
          {textoErro(painelQ.error)}
        </div>
      )}

      {/* Destaque: matrículas, em curso, fora do padrão e YTD. */}
      <div className="grid gap-4 md:grid-cols-[2fr_1fr]">
        <div className="cartao p-5 flex flex-wrap items-center gap-5" role="group" aria-label="Matrículas no período">
          <span className="w-12 h-12 rounded-xl flex items-center justify-center" style={{ background: "var(--primary-light)", color: "var(--primary)" }} aria-hidden>
            <span className="material-symbols-outlined">trending_up</span>
          </span>
          <div>
            <p className="rotulo">Matrículas no período</p>
            <p className="text-4xl font-extrabold font-headline tabular-nums">{traco(num(k.matriculas))}</p>
            <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
              <span className="font-bold text-emerald-700 dark:text-emerald-400">{traco(num(k.em_curso))}</span> em curso
            </p>
          </div>
          {!!k.fora_padrao && (
            <button
              type="button"
              className="ml-auto text-left px-3 py-2 rounded-xl border border-orange-500/40 bg-orange-500/10 text-xs"
              onClick={() => alternar("fora")}
            >
              <b>{num(k.fora_padrao)}</b> RGM fora do padrão
              <span className="block" style={{ color: "var(--text-muted)" }}>
                {emCursoFora ? `${emCursoFora.toLocaleString("pt-BR")} alunos em curso — ver lista` : "ver lista"}
              </span>
            </button>
          )}
        </div>
        <div className="cartao p-5" role="group" aria-label="YTD">
          <p className="rotulo">YTD</p>
          <p className="text-2xl font-extrabold font-headline tabular-nums mt-1">
            {extrasQ.isFetching ? "…" : extrasQ.data ? num(extrasQ.data.ytd) : "—"}
          </p>
          <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>de 1º de janeiro até a data Até</p>
        </div>
      </div>

      {/* Evasão */}
      <PainelRecolhivel
        borda="border-l-rose-500/60"
        aberto={abertos.evasao}
        onAlternar={() => alternar("evasao")}
        regiao="Lista de evasão"
        cabeca={
          <>
            <span className="block rotulo text-rose-600 dark:text-rose-400">Evasão no período</span>
            <span className="text-xl font-extrabold font-headline tabular-nums">{traco(num(k.evasao))}</span>
            <span className="ml-2 text-[11px]">
              {chips.map((c) => (
                <span key={c.rotulo} className={`inline-block px-2 py-0.5 mr-1 rounded-full ${c.cls}`}>
                  {`${c.rotulo}: ${c.n.toLocaleString("pt-BR")}`}
                </span>
              ))}
            </span>
            <span className="block text-[11px]" style={{ color: "var(--text-muted)" }}>Matrícula do período que saiu de em curso. Clique para ver quem.</span>
          </>
        }
      >
        <ListaAlunos linhas={evasaoDia} acoes={acoes} />
      </PainelRecolhivel>

      {/* Fora do padrão RGM */}
      <PainelRecolhivel
        borda="border-l-orange-500/60"
        aberto={abertos.fora}
        onAlternar={() => alternar("fora")}
        regiao="Lista fora do padrão"
        cabeca={
          <>
            <span className="block rotulo text-orange-600 dark:text-orange-400">Fora do padrão RGM</span>
            <span className="block text-[11px]" style={{ color: "var(--text-muted)" }}>
              Prefixo abaixo do dominante do período ({k.prefixo || "—"}). Não entram na matrícula que conta.
            </span>
            <span className="text-xl font-extrabold font-headline tabular-nums">{traco(num(k.fora_padrao))}</span>
            <span className="flex gap-2 flex-wrap mt-2">
              {(painel?.prefixos ?? []).map((p) => (
                <span key={p.pfx} className="text-[11px] px-2 py-0.5 rounded-full bg-orange-500/15 text-orange-700 dark:text-orange-300">
                  {`${p.pfx} · ${p.n}`}
                </span>
              ))}
            </span>
          </>
        }
      >
        <ListaAlunos linhas={foraDia} contarVenda acoes={acoes} />
      </PainelRecolhivel>

      {/* Vendas em conflito */}
      <PainelRecolhivel
        borda="border-l-amber-500/60"
        aberto={abertos.conflitos}
        onAlternar={() => alternar("conflitos")}
        regiao="Lista de conflitos"
        cabeca={
          <>
            <span className="block rotulo text-amber-600 dark:text-amber-400">Vendas em conflito</span>
            <span className="block text-[11px]" style={{ color: "var(--text-muted)" }}>
              RGM com dois ou mais negócios no Bwipo dentro do período filtrado. Escolha quem fica com a venda.
            </span>
            <span className="text-xl font-extrabold font-headline tabular-nums">
              {conflitosQ.isFetching ? "…" : conflitos ? num(conflitos.total) : "—"}
            </span>{" "}
            {!conflitosQ.isFetching && conflitos && conflitos.total > 0 && (
              <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>{`· ${num(conflitos.total_nao_resolvidos)} sem decisão`}</span>
            )}
          </>
        }
      >
        {conflitosQ.isError ? (
          <p className="px-4 py-3" style={{ color: "var(--danger)" }}>{textoErro(conflitosQ.error)}</p>
        ) : conflitos && conflitos.conflitos.length ? (
          <div className="p-3 flex flex-col gap-3">
            {conflitos.conflitos.map((c) => (
              <CartaoConflito key={c.rgm} c={c} linha={porRgm[c.rgm]} acoes={acoes} />
            ))}
          </div>
        ) : conflitos ? (
          <p className="px-4 py-3" style={{ color: "var(--text-muted)" }}>Nenhum conflito neste recorte.</p>
        ) : (
          <p className="px-4 py-3" style={{ color: "var(--text-muted)" }}>Carregando…</p>
        )}
      </PainelRecolhivel>

      {/* Indicadores */}
      <div className="grid gap-4 grid-cols-2 lg:grid-cols-4">
        <Cartao rotulo="Média diária" valor={traco((k.media || 0).toLocaleString("pt-BR"))} contexto={`${(temDados && k.dias) || "—"} dias no período`} />
        <Cartao rotulo="Ticket médio (30%)" valor={k.ticket ? dinheiro(k.ticket) : "—"} />
        <Comparativo rotulo="vs 6 meses" valor={k.vendas_6m} delta={k.delta_6m} pct={k.pct_6m} periodo={k.compare_6m_period} carregado={temDados} />
        <Comparativo rotulo="vs 1 ano" valor={k.vendas_1a} delta={k.delta_1a} pct={k.pct_1a} periodo={k.compare_1a_period} carregado={temDados} />
      </div>

      <section className="cartao p-5 flex flex-col gap-3" aria-label="Matrículas por dia">
        <div className="flex flex-wrap items-baseline gap-3">
          <h3 className="text-sm">Matrículas por dia</h3>
          <span className="text-[11px]" style={{ color: "var(--text-muted)" }}>Clique num dia para filtrar o ranking</span>
        </div>
        <GraficoDias evolucao={painel?.evolucao ?? []} dia={dia} onDia={(d) => setDiaSel({ dia: d, versao })} />
      </section>

      {dia && (
        <div className="text-xs px-3 py-2 rounded-lg" style={{ border: "1px solid var(--primary)", background: "var(--primary-light)" }}>
          {`Dia ${diaMes(dia)} filtrando o ranking. `}
          <button type="button" className="underline" onClick={() => setDiaSel(null)}>
            limpar
          </button>
        </div>
      )}

      {/* Ranking */}
      <section className="cartao overflow-hidden" aria-label="Ranking de agentes comerciais">
        <div className="px-5 py-3 flex flex-wrap items-center justify-between gap-3" style={{ borderBottom: "1px solid var(--border-subtle)" }}>
          <h3 className="text-sm">Ranking de agentes comerciais</h3>
          <div className="flex items-center gap-2">
            <input
              type="text"
              inputMode="numeric"
              maxLength={12}
              className="controle font-normal w-36"
              placeholder="Consultar RGM"
              aria-label="Consultar RGM"
              value={rgmBusca}
              onChange={(e) => setRgmBusca(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") void consultarRgm();
              }}
            />
            <button type="button" className="controle" onClick={() => void consultarRgm()}>
              Buscar
            </button>
            <span className="text-xs" style={{ color: "var(--text-muted)" }}>{`${ranking.length} agentes`}</span>
          </div>
        </div>
        <div className="overflow-x-auto max-h-[500px] overflow-y-auto">
          <table className="tabela-cartao">
            <thead>
              <tr>
                <th className="text-center w-10">#</th>
                <th className="text-left">Agente</th>
                <th className="text-right">Matr.</th>
                <th className="text-right">Interm.</th>
                <th className="text-right">Meta</th>
                <th className="text-right">Super</th>
                <th className="text-right">Nível</th>
                <th className="text-right" title="Carteira atual, não filtrada pela data">Aberto</th>
                <th className="text-right">Perdidos</th>
                <th className="text-right">Evasão</th>
                <th className="text-right">Conv.</th>
              </tr>
            </thead>
            <tbody>
              {!temDados ? (
                <tr>
                  <td colSpan={11} className="py-8 text-center" style={{ color: "var(--text-muted)" }}>
                    {painelQ.isError ? "—" : "Carregando…"}
                  </td>
                </tr>
              ) : ranking.length === 0 ? (
                <tr>
                  <td colSpan={11} className="py-8 text-center" style={{ color: "var(--text-muted)" }}>
                    Nenhuma matrícula neste período.
                  </td>
                </tr>
              ) : (
                ranking.map((r, i) => (
                  <tr key={r.agente} className="cursor-pointer" onClick={() => setModal({ tipo: "agente", nome: r.agente, dia })}>
                    <td className="text-center" style={{ color: "var(--text-muted)" }}>{i + 1}</td>
                    <td className="font-medium">
                      <button
                        type="button"
                        className="text-left hover:underline"
                        onClick={(e) => {
                          e.stopPropagation();
                          setModal({ tipo: "agente", nome: r.agente, dia });
                        }}
                      >
                        {r.agente}
                      </button>
                    </td>
                    <td className="text-right font-bold tabular-nums">{num(r.matriculas)}</td>
                    <td className="text-right tabular-nums">{r.intermediaria ? num(r.intermediaria) : "—"}</td>
                    <td className="text-right tabular-nums">{r.meta ? num(r.meta) : "—"}</td>
                    <td className="text-right tabular-nums">{r.supermeta ? num(r.supermeta) : "—"}</td>
                    <td className="text-right" style={{ color: "var(--text-muted)" }}>{nivelRanking(r)}</td>
                    <td className="text-right tabular-nums">{num(r.aberto)}</td>
                    <td className="text-right tabular-nums" style={{ color: "var(--text-muted)" }}>{num(r.perdido)}</td>
                    <td className="text-right tabular-nums">{num(r.evasao)}</td>
                    <td className="text-right tabular-nums">{`${(r.conv || 0).toLocaleString("pt-BR")}%`}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>

      <div className="grid gap-6 lg:grid-cols-2">
        <section className="cartao p-5 flex flex-col gap-3" aria-label="Matrículas por agente">
          <h3 className="text-sm">Matrículas por agente</h3>
          <GraficoAgentes ranking={ranking} />
        </section>
        <section className="cartao overflow-hidden" aria-label="Matrículas por polo">
          <h3 className="text-sm px-5 py-3" style={{ borderBottom: "1px solid var(--border-subtle)" }}>Matrículas por polo</h3>
          <div className="overflow-x-auto max-h-[400px] overflow-y-auto">
            <table className="tabela-cartao">
              <thead>
                <tr>
                  <th className="text-center w-10">#</th>
                  <th className="text-left">Polo</th>
                  <th className="text-right">Total</th>
                  <th className="text-left w-40">Proporção</th>
                </tr>
              </thead>
              <tbody>
                {(painel?.polos ?? []).length ? (
                  (painel?.polos ?? []).map((p, i) => (
                    <tr key={p.polo}>
                      <td className="text-center" style={{ color: "var(--text-muted)" }}>{i + 1}</td>
                      <td>{p.polo}</td>
                      <td className="text-right font-bold tabular-nums">{num(p.total)}</td>
                      <td>
                        <span
                          data-barra
                          className="block h-1.5 rounded barra-animada bg-cyan-500"
                          style={{ width: `${Math.max(6, Math.round((100 * p.total) / maxPolo))}%` }}
                        />
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={4} className="py-6 text-center" style={{ color: "var(--text-muted)" }}>
                      {temDados ? "Sem polo." : "—"}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </section>
      </div>

      {modal?.tipo === "rgm" && <ConsultaRgm dados={modal.dados} linha={porRgm[modal.dados.rgm]} acoes={acoes} onFechar={fecharModal} />}
      {modal?.tipo === "agente" && (
        <DetalheAgente
          nome={modal.nome}
          userId={(painel?.ranking ?? []).find((r) => r.agente === modal.nome)?.user_id ?? null}
          linhas={linhas.filter((l) => l.agente === modal.nome && (!modal.dia || l.data === modal.dia))}
          ini={aplicado?.ini || ""}
          fim={aplicado?.fim || ""}
          chave={String(versao)}
          acoes={acoes}
          onFechar={fecharModal}
        />
      )}
      {modal?.tipo === "duplicatas" && <ModalDuplicatas linhas={linhas} acoes={acoes} onFechar={fecharModal} />}
      {modal?.tipo === "semdata" && <ModalSemData linhas={linhas} acoes={acoes} onFechar={fecharModal} />}
      {modal?.tipo === "diagnostico" && <ModalDiagnostico linhas={linhas} onFechar={fecharModal} />}

      <Toast aviso={aviso} onFechar={() => setAviso(null)} />
    </div>
  );
}

function PainelRecolhivel({
  borda,
  aberto,
  onAlternar,
  regiao,
  cabeca,
  children,
}: {
  borda: string;
  aberto: boolean;
  onAlternar: () => void;
  regiao: string;
  cabeca: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className={`cartao overflow-hidden border-l-[3px] ${borda}`}>
      <button type="button" className="w-full p-4 text-left flex items-start gap-3" aria-expanded={aberto} onClick={onAlternar}>
        <span className="block flex-1 min-w-0">{cabeca}</span>
        <span className="material-symbols-outlined" aria-hidden style={{ color: "var(--text-muted)" }}>
          {aberto ? "expand_less" : "expand_more"}
        </span>
      </button>
      {aberto && (
        <section aria-label={regiao} className="max-h-96 overflow-y-auto text-xs" style={{ borderTop: "1px solid var(--border-subtle)" }}>
          {children}
        </section>
      )}
    </div>
  );
}

function Cartao({ rotulo, valor, contexto }: { rotulo: string; valor: string; contexto?: string }) {
  return (
    <div className="cartao p-5" role="group" aria-label={rotulo}>
      <p className="rotulo mb-1">{rotulo}</p>
      <p className="text-2xl font-headline font-extrabold tabular-nums">{valor}</p>
      {contexto && (
        <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>{contexto}</p>
      )}
    </div>
  );
}

/** vs 6 meses / vs 1 ano (_bwcCompare + _bwcBadge). */
function Comparativo({
  rotulo,
  valor,
  delta,
  pct,
  periodo,
  carregado,
}: {
  rotulo: string;
  valor?: number | null;
  delta?: number | null;
  pct?: number | null;
  periodo?: string | null;
  carregado: boolean;
}) {
  const v = valor || 0;
  const sub = !carregado
    ? "mesmo período"
    : v > 0 && delta != null
      ? `${delta >= 0 ? "+" : ""}${delta.toLocaleString("pt-BR")} matrículas`
      : "sem histórico no período";
  return (
    <div className="cartao p-5" role="group" aria-label={rotulo}>
      <div className="flex items-center justify-between gap-2 mb-1">
        <p className="rotulo">{rotulo}</p>
        {carregado && pct != null && (
          <span
            className={`font-bold px-1.5 py-0.5 rounded text-[11px] ${
              pct >= 0 ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400" : "bg-red-500/15 text-red-600 dark:text-red-400"
            }`}
          >
            {`${pct >= 0 ? "↑ " : "↓ "}${Math.abs(pct).toLocaleString("pt-BR")}%`}
          </span>
        )}
      </div>
      <p className="text-2xl font-headline font-extrabold tabular-nums">{v > 0 ? num(v) : "—"}</p>
      <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }} title={(carregado && periodo) || "Período de referência"}>
        {sub}
      </p>
    </div>
  );
}
