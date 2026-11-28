import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../api";
import { useMe } from "../me";
import { DetalheAgente } from "./repasse/DetalheAgente";
import { Indicador, textoErro } from "./repasse/comum";
import { apiOk, dinheiro, qs, rotuloTaxa } from "./repasse/formato";
import { ListaAgentes } from "./repasse/ListaAgentes";
import { MatriculadosRepasse } from "./repasse/MatriculadosRepasse";
import { PlanilhaRecebimentos } from "./repasse/PlanilhaRecebimentos";
import type { Agentes, Filtros, FiltrosTela, IdAgente, Taxa } from "./repasse/tipos";

/**
 * Repasse (página `repasse`). Mesmas APIs e mesmos números da tela antiga
 * (templates/partials/_repasse.html + static/js/repasse.js); as regras de
 * valor e de dono da venda ficam em routes/repasse.py.
 *
 * - Admin: todos os consultores, KPIs, cartões com busca, detalhe, planilha,
 *   matriculados, edição da taxa e CSVs.
 * - Consultor: o servidor só devolve os dados dele; a tela abre direto no
 *   detalhe, sem planilha nem matriculados, e a taxa é só leitura.
 *
 * O repasse é calculado aqui (valor × taxa), como na antiga, para a tela
 * refazer as contas na hora em que a taxa muda.
 */

const TAXA_PADRAO = 20;
export const ERRO_TAXA = "Taxa inválida. Use um valor entre 0 e 100.";

function lerTaxa(texto: string): number | null {
  if (texto.trim() === "") return null;
  const n = Number(texto.replace(",", "."));
  return Number.isFinite(n) && n >= 0 && n <= 100 ? n : null;
}

/** Seleção ligada aos filtros: trocar filtro fecha o detalhe, como o repLoad antigo. */
interface Selecao {
  chave: string;
  id: IdAgente | null;
}

export function Repasse() {
  const me = useMe();
  const admin = me.data?.role === "admin";
  const cliente = useQueryClient();

  const filtrosQ = useQuery({ queryKey: ["repasse", "filtros"], queryFn: () => apiOk<Filtros>("/api/repasse/filtros") });
  const taxaQ = useQuery({ queryKey: ["repasse", "taxa"], queryFn: () => api<Taxa>("/api/repasse/taxa") });

  // Filtros de cima. Até o usuário mexer, vale o padrão (tipo "Mensalidade" quando existe).
  const tipoPadrao = filtrosQ.data?.tipos.includes("Mensalidade") ? "Mensalidade" : "";
  const [escolha, setEscolha] = useState<FiltrosTela | null>(null);
  const filtros: FiltrosTela = escolha ?? { ciclo: "", tipo: tipoPadrao, turma: "" };
  const chave = `${filtros.ciclo}|${filtros.tipo}|${filtros.turma}`;

  const porCiclo = filtrosQ.data?.turmas_por_ciclo ?? {};
  const turmas = filtros.ciclo
    ? porCiclo[filtros.ciclo] ?? []
    : [...new Set(Object.values(porCiclo).flat())].sort();

  function mudarFiltro(campo: keyof FiltrosTela, valor: string) {
    setEscolha({
      ...filtros,
      [campo]: valor,
      // Turmas dependem do ciclo: trocar o ciclo limpa a turma (repAtualizarTurmas).
      ...(campo === "ciclo" ? { turma: "" } : {}),
    });
  }

  // Taxa: texto do campo, a última taxa válida e o aviso.
  const [textoTaxa, setTextoTaxa] = useState<string | null>(null);
  const [taxaLocal, setTaxaLocal] = useState<number | null>(null);
  const [avisoTaxa, setAvisoTaxa] = useState<{ tipo: "erro" | "ok"; texto: string } | null>(null);
  const taxaServidor = typeof taxaQ.data?.taxa === "number" ? taxaQ.data.taxa : TAXA_PADRAO;
  const pct = taxaLocal ?? taxaServidor;
  const taxa = pct / 100;

  const salvar = useMutation({
    mutationFn: (valor: number) =>
      api<{ ok?: boolean; taxa?: number; error?: string }>("/api/repasse/taxa", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ taxa: valor }),
      }),
    onSuccess: (d, valor) => {
      if (d && d.ok === false) {
        setAvisoTaxa({ tipo: "erro", texto: d.error || "Não foi possível salvar a taxa." });
        return;
      }
      cliente.setQueryData<Taxa>(["repasse", "taxa"], { taxa: d?.taxa ?? valor });
      setAvisoTaxa({ tipo: "ok", texto: "Taxa salva." });
    },
    onError: (e) => setAvisoTaxa({ tipo: "erro", texto: `Não foi possível salvar a taxa: ${textoErro(e)}` }),
  });

  function mudarTaxa(texto: string) {
    setTextoTaxa(texto);
    setAvisoTaxa(null);
    const n = lerTaxa(texto);
    if (n !== null) setTaxaLocal(n); // valores refeitos na hora (repAtualizarTaxa)
  }

  function salvarTaxa() {
    if (!admin) return;
    const n = lerTaxa(textoTaxa ?? String(pct));
    if (n === null) {
      setAvisoTaxa({ tipo: "erro", texto: ERRO_TAXA });
      return;
    }
    salvar.mutate(n);
  }

  const agentesQ = useQuery({
    queryKey: ["repasse", "agentes", filtros.ciclo, filtros.tipo, filtros.turma],
    queryFn: () =>
      apiOk<Agentes>(
        `/api/repasse/agentes?${qs([
          ["ciclo", filtros.ciclo],
          ["tipo", filtros.tipo],
          ["turma", filtros.turma],
        ])}`,
      ),
    // Espera os filtros para já buscar com o tipo padrão.
    enabled: !filtrosQ.isPending,
  });

  const [busca, setBusca] = useState("");
  const [selecao, setSelecao] = useState<Selecao | null>(null);
  const agentes = agentesQ.data?.agentes ?? [];
  const totais = agentesQ.data?.totais;
  // Consultor com um agente só abre direto no detalhe (igual à antiga), até fechar.
  const idSelecionado =
    selecao && selecao.chave === chave
      ? selecao.id
      : !admin && agentes.length === 1
        ? agentes[0].id
        : null;
  const indiceSel = idSelecionado === null ? -1 : agentes.findIndex((a) => String(a.id) === String(idSelecionado));
  const agenteSel = indiceSel >= 0 ? agentes[indiceSel] : null;

  function limpar() {
    setEscolha(null);
    setBusca("");
    setSelecao(null);
  }

  const nomeConsultor = me.data?.display_name || me.data?.username || "";

  return (
    <div className="flex flex-col gap-6 max-w-7xl">
      {/* Filtros numa linha, controles de 36px; ações à direita. */}
      <section className="flex flex-wrap gap-2 items-center" aria-label="Filtros">
        <select className="controle font-normal" aria-label="Ciclo" value={filtros.ciclo} onChange={(e) => mudarFiltro("ciclo", e.target.value)}>
          <option value="">Todos os ciclos</option>
          {(filtrosQ.data?.ciclos ?? []).map((c) => (
            <option key={c} value={c}>{c}</option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Turma" value={filtros.turma} onChange={(e) => mudarFiltro("turma", e.target.value)}>
          <option value="">Todas as turmas</option>
          {turmas.map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
        <select className="controle font-normal" aria-label="Tipo de pagamento" value={filtros.tipo} onChange={(e) => mudarFiltro("tipo", e.target.value)}>
          <option value="">Todos os tipos</option>
          {(filtrosQ.data?.tipos ?? []).map((t) => (
            <option key={t} value={t}>{t}</option>
          ))}
        </select>
        <div className="intervalo" title={admin ? "Taxa de repasse" : "Taxa de repasse (só o administrador altera)"}>
          <span className="pr-1">Taxa</span>
          <input
            type="number"
            min={0}
            max={100}
            step={0.1}
            className="w-14 text-right tabular-nums"
            aria-label="Taxa de repasse (%)"
            readOnly={!admin}
            value={textoTaxa ?? String(pct)}
            onChange={(e) => mudarTaxa(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && admin) salvarTaxa();
            }}
          />
          <span className="pl-0.5">%</span>
        </div>
        {admin && (
          <button type="button" className="controle" onClick={salvarTaxa} disabled={salvar.isPending}>
            {salvar.isPending ? "Salvando…" : "Salvar taxa"}
          </button>
        )}
        {avisoTaxa?.tipo === "erro" && (
          <span role="alert" className="text-xs" style={{ color: "var(--danger)" }}>{avisoTaxa.texto}</span>
        )}
        {avisoTaxa?.tipo === "ok" && (
          <span role="status" className="text-xs text-emerald-700 dark:text-emerald-400">{avisoTaxa.texto}</span>
        )}
        <div className="flex gap-2 md:ml-auto">
          <button type="button" className="controle" onClick={limpar}>
            Limpar
          </button>
          <button
            type="button"
            className="controle controle-primario"
            disabled={agentesQ.isFetching}
            onClick={() => cliente.invalidateQueries({ queryKey: ["repasse"] })}
          >
            <span className="material-symbols-outlined text-[18px]" aria-hidden>refresh</span>
            {agentesQ.isFetching ? "Atualizando…" : "Atualizar"}
          </button>
        </div>
      </section>

      {filtrosQ.isError && (
        <div role="alert" className="cartao p-4 text-sm">
          Não foi possível carregar os filtros: {textoErro(filtrosQ.error)}
        </div>
      )}

      {agentesQ.isError ? (
        <div role="alert" className="cartao p-5 text-sm">
          <p className="font-semibold mb-1">Não foi possível carregar</p>
          <p style={{ color: "var(--danger)" }}>{textoErro(agentesQ.error)}</p>
        </div>
      ) : (
        <>
          <div className={`grid gap-4 grid-cols-2 ${admin ? "md:grid-cols-4" : "md:grid-cols-2"}`}>
            {admin && (
              <Indicador
                rotulo="Recebido"
                valor={totais ? dinheiro(totais.valor) : "—"}
                cor="text-emerald-600 dark:text-emerald-400"
              />
            )}
            <Indicador
              rotulo="Total repasse"
              valor={totais ? dinheiro((totais.valor || 0) * taxa) : "—"}
              cor="text-amber-600 dark:text-amber-400"
              contexto={`taxa de ${rotuloTaxa(pct)}%`}
            />
            <Indicador rotulo="Com pagamento" valor={totais ? (totais.alunos || 0).toLocaleString("pt-BR") : "—"} contexto="alunos" />
            {admin && (
              <Indicador rotulo="Consultores" valor={totais ? (totais.agentes || 0).toLocaleString("pt-BR") : "—"} />
            )}
          </div>

          <section className="cartao p-5 flex flex-col gap-4" aria-label="Consultores com recebimento">
            <div className="flex flex-wrap items-center gap-3">
              <h2 className="text-sm flex items-center gap-2 flex-1">
                <span className="material-symbols-outlined text-violet-500" aria-hidden>groups</span>
                Consultores
              </h2>
              {admin ? (
                <input
                  type="search"
                  className="controle font-normal w-full sm:w-56"
                  placeholder="Buscar consultor…"
                  aria-label="Buscar consultor"
                  value={busca}
                  onChange={(e) => setBusca(e.target.value)}
                />
              ) : (
                <span className="text-xs px-3 py-1 rounded-full" style={{ background: "var(--primary-light)", color: "var(--primary)" }}>
                  <span>Seus dados:</span> <strong>{nomeConsultor}</strong>
                </span>
              )}
            </div>
            {agentesQ.isPending ? (
              <p className="text-sm py-4" style={{ color: "var(--text-muted)" }}>Carregando…</p>
            ) : agentes.length === 0 ? (
              <p className="text-sm py-4" style={{ color: "var(--text-muted)" }}>Nenhum consultor encontrado neste filtro.</p>
            ) : (
              <ListaAgentes
                agentes={agentes}
                busca={admin ? busca : ""}
                taxa={taxa}
                admin={admin}
                selecionado={idSelecionado}
                onSelecionar={(a) => setSelecao({ chave, id: a.id })}
              />
            )}
          </section>

          {agenteSel && (
            <DetalheAgente
              key={`${String(agenteSel.id)}|${chave}`}
              agente={agenteSel}
              indice={indiceSel}
              filtros={filtros}
              taxa={taxa}
              admin={admin}
              onFechar={() => setSelecao({ chave, id: null })}
            />
          )}
        </>
      )}

      {admin && !filtrosQ.isPending && (
        <>
          <PlanilhaRecebimentos filtros={filtros} />
          <MatriculadosRepasse />
        </>
      )}
    </div>
  );
}
