import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";
import { api, ErroApi } from "../../api";
import { Secao, Selo, TEXTO_FRACO } from "./comum";
import { apiOk, classificarSituacao, data, mesCorrente, textoErro, ultimoPeriodo } from "./formato";
import { ModalAjuste, TIPO_AJUSTE } from "./ModalAjuste";
import { ModalMinhaMatricula } from "./ModalMinhaMatricula";
import type { Ajustes, MatriculaOficial, MatriculasOficiais, MinhaMatricula, MinhasMatriculas, Periodo } from "./tipos";

type Filtro = "all" | "ativo" | "evadido" | "outros";

interface Aplicado {
  ini: string;
  fim: string;
  /** Muda a cada Filtrar/Limpar/Aplicar meta: busca de novo mesmo com as mesmas datas. */
  versao: number;
}

const STATUS_AJUSTE: Record<string, { rotulo: string; cor: string }> = {
  pendente: { rotulo: "Pendente", cor: "bg-amber-500/15 text-amber-700 dark:text-amber-400 border-amber-500/25" },
  em_analise: { rotulo: "Em análise", cor: "bg-blue-500/15 text-blue-700 dark:text-blue-400 border-blue-500/25" },
  aprovado: { rotulo: "Aprovado", cor: "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/25" },
  rejeitado: { rotulo: "Rejeitado", cor: "bg-red-500/15 text-red-700 dark:text-red-400 border-red-500/25" },
};

function Linha({ colunas, texto, erro = false }: { colunas: number; texto: string; erro?: boolean }) {
  return (
    <tr>
      <td colSpan={colunas} className="py-6 text-center text-xs" style={{ color: erro ? "var(--danger)" : "var(--text-muted)" }}>
        {texto}
      </td>
    </tr>
  );
}

function SeloSituacao({ sit }: { sit: string }) {
  if (!sit) return <span style={TEXTO_FRACO}>—</span>;
  const cat = classificarSituacao(sit);
  if (sit.includes("TRANSFER"))
    return (
      <span title="Saiu do relatório atual — transferência para outro polo. Não conta venda.">
        <Selo cor="bg-purple-500/15 text-purple-700 dark:text-purple-300 border-purple-500/25">{sit}</Selo>
      </span>
    );
  const cor =
    cat === "evadido"
      ? "bg-red-500/15 text-red-700 dark:text-red-400 border-red-500/25"
      : cat === "ativo"
        ? "bg-emerald-500/15 text-emerald-700 dark:text-emerald-400 border-emerald-500/25"
        : "bg-amber-500/15 text-amber-700 dark:text-amber-400 border-amber-500/25";
  return <Selo cor={cor}>{sit}</Selo>;
}

/**
 * Aba "Minhas matrículas": matrículas oficiais (com o período De/Até), minha lista
 * de vendas (CRUD) e solicitações de ajuste. Mesmas rotas e parâmetros da antiga.
 */
export function Matriculas({ uid, admin, avisar }: { uid: number | null; admin: boolean; avisar: (texto: string, erro?: boolean) => void }) {
  const cliente = useQueryClient();
  const periodosQ = useQuery({
    queryKey: ["mp", "periodos"],
    queryFn: ({ signal }) => apiOk<{ ok?: boolean; campanhas: Periodo[] }>("/api/premiacao/campanhas-periodos", { signal }),
    staleTime: Infinity,
  });

  // Período: o rascunho é o que os campos mostram; Filtrar/Limpar/Aplicar meta aplicam.
  const [aplicado, setAplicado] = useState<Aplicado | null>(null);
  const [rascunho, setRascunho] = useState<{ ini: string; fim: string }>({ ini: "", fim: "" });

  // Primeira abertura (_mpInitMatriculasFiltroPadrao): período de meta mais recente, ou o mês corrente.
  useEffect(() => {
    if (aplicado || periodosQ.isPending) return;
    const p = ultimoPeriodo(periodosQ.data?.campanhas ?? []) || mesCorrente();
    setRascunho({ ini: p.dt_inicio, fim: p.dt_fim });
    setAplicado({ ini: p.dt_inicio, fim: p.dt_fim, versao: 0 });
  }, [aplicado, periodosQ.isPending, periodosQ.data]);

  function aplicar(ini: string, fim: string) {
    setRascunho({ ini, fim });
    setAplicado((a) => ({ ini, fim, versao: (a?.versao ?? 0) + 1 }));
  }

  const oficiaisQ = useQuery({
    queryKey: ["mp", "oficiais", uid, aplicado?.ini, aplicado?.fim, aplicado?.versao],
    queryFn: ({ signal }) => {
      let qs = `kommo_uid=${uid}`;
      if (aplicado?.ini) qs += `&dt_ini=${aplicado.ini}`;
      if (aplicado?.fim) qs += `&dt_fim=${aplicado.fim}`;
      return apiOk<MatriculasOficiais>(`/api/minha-performance/matriculas?${qs}`, { signal });
    },
    enabled: !!uid && !!aplicado,
    staleTime: Infinity,
  });

  const minhasQ = useQuery({
    queryKey: ["mp", "minhas", uid, aplicado?.ini, aplicado?.fim, aplicado?.versao],
    queryFn: ({ signal }) => {
      const p = new URLSearchParams();
      if (uid) p.set("kommo_uid", String(uid));
      if (aplicado?.ini) p.set("dt_ini", aplicado.ini);
      if (aplicado?.fim) p.set("dt_fim", aplicado.fim);
      const qs = p.toString() ? `?${p}` : "";
      return apiOk<MinhasMatriculas>(`/api/minha-performance/minhas-matriculas${qs}`, { signal });
    },
    enabled: !!aplicado,
    staleTime: Infinity,
  });

  const ajustesQ = useQuery({
    queryKey: ["mp", "ajustes", uid],
    queryFn: ({ signal }) => apiOk<Ajustes>(`/api/minha-performance/ajustes${uid ? `?kommo_uid=${uid}` : ""}`, { signal }),
    enabled: !!aplicado,
    staleTime: Infinity,
  });

  const [busca, setBusca] = useState("");
  const [filtro, setFiltro] = useState<Filtro>("all");
  const [metaAberta, setMetaAberta] = useState(false);
  const metaRef = useRef<HTMLDivElement>(null);
  const [modalMat, setModalMat] = useState<{ editando: MinhaMatricula | null } | null>(null);
  const [modalAjuste, setModalAjuste] = useState(false);

  // Fecha o menu de metas ao clicar fora.
  useEffect(() => {
    if (!metaAberta) return;
    const fora = (e: MouseEvent) => {
      if (metaRef.current && !metaRef.current.contains(e.target as Node)) setMetaAberta(false);
    };
    document.addEventListener("click", fora);
    return () => document.removeEventListener("click", fora);
  }, [metaAberta]);

  const oficiais = oficiaisQ.data?.matriculas ?? [];
  const contagem = { all: oficiais.length, ativo: 0, ativoContando: 0, evadido: 0, outros: 0, foraPadrao: 0 };
  for (const m of oficiais) {
    const cat = classificarSituacao(m.situacao);
    contagem[cat]++;
    if (cat === "ativo") {
      if (m.conta_para_meta !== false) contagem.ativoContando++;
      else if (m.outlier) contagem.foraPadrao++;
    }
  }
  const totalContando = oficiaisQ.data?.total_contando ?? oficiaisQ.data?.total_contavel ?? contagem.ativoContando;
  const q = busca.toLowerCase();
  const visiveis = oficiais.filter((m) => {
    const texto = `${(m.nome || "").toLowerCase()} ${(m.rgm || "").toLowerCase()} ${(m.curso || "").toLowerCase()}`;
    return (!q || texto.includes(q)) && (filtro === "all" || classificarSituacao(m.situacao) === filtro);
  });

  async function contarVenda(m: MatriculaOficial, contar: boolean) {
    try {
      const d = await api<{ ok?: boolean; error?: string }>("/api/comercial-rgm/outlier/contar-venda", {
        method: contar ? "POST" : "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ rgm: m.rgm }),
      });
      if (!d.ok) {
        avisar(d.error || "Erro ao atualizar contagem", true);
        return;
      }
      await cliente.invalidateQueries({ queryKey: ["mp", "oficiais"] });
    } catch (e) {
      avisar(e instanceof ErroApi ? textoErro(e) : "Erro de rede ao atualizar contagem", true);
    }
  }

  async function excluir(m: MinhaMatricula) {
    if (!window.confirm("Excluir esta matrícula da sua lista?")) return;
    try {
      await api(`/api/minha-performance/minhas-matriculas/${m.id}`, { method: "DELETE" });
    } catch (e) {
      avisar(`Erro ao excluir: ${textoErro(e)}`, true);
    }
    cliente.invalidateQueries({ queryKey: ["mp", "minhas"] });
  }

  const pilulas: [Filtro, string, string, number][] = [
    ["all", "list", "Todos", contagem.all],
    ["ativo", "trending_up", "Em curso", totalContando],
    ["evadido", "person_off", "Evadidos", contagem.evadido],
    ["outros", "help", "Outros", contagem.outros],
  ];
  const minhas = minhasQ.data?.matriculas ?? [];
  const ajustes = ajustesQ.data?.ajustes ?? [];

  return (
    <div className="flex flex-col gap-6">
      <Secao
        titulo="Matrículas oficiais"
        icone="verified"
        extra={
          oficiaisQ.data ? (
            <span className="text-xs font-normal">
              <span className="font-semibold text-emerald-700 dark:text-emerald-400">{`${totalContando} em curso`}</span>
              {contagem.foraPadrao > 0 && (
                <>
                  {" · "}
                  <span className="text-amber-700 dark:text-amber-400">{`${contagem.foraPadrao} fora do padrão`}</span>
                </>
              )}
            </span>
          ) : null
        }
      >
        <div className="flex flex-wrap items-center gap-2">
          <div className="intervalo">
            <label className="flex items-center gap-1">
              <span>De</span>
              <input type="date" aria-label="De" value={rascunho.ini} onChange={(e) => setRascunho((r) => ({ ...r, ini: e.target.value }))} />
            </label>
            <label className="flex items-center gap-1 pl-2">
              <span>Até</span>
              <input type="date" aria-label="Até" value={rascunho.fim} onChange={(e) => setRascunho((r) => ({ ...r, fim: e.target.value }))} />
            </label>
          </div>
          <div className="relative" ref={metaRef}>
            <button type="button" className="controle" aria-expanded={metaAberta} onClick={() => setMetaAberta((v) => !v)}>
              <span className="material-symbols-outlined text-[18px]" aria-hidden>
                flag
              </span>
              Aplicar meta
              <span className="material-symbols-outlined text-[18px]" aria-hidden>
                {metaAberta ? "expand_less" : "expand_more"}
              </span>
            </button>
            {metaAberta && (
              <div className="absolute left-0 top-full mt-1 z-40 min-w-[240px] max-h-72 overflow-y-auto cartao p-1 text-xs" style={{ background: "var(--bg-card)" }}>
                {periodosQ.isPending ? (
                  <p className="p-2">Carregando...</p>
                ) : periodosQ.isError ? (
                  <p className="p-2" style={{ color: "var(--danger)" }}>{`Erro: ${textoErro(periodosQ.error)}`}</p>
                ) : !periodosQ.data?.campanhas.length ? (
                  <p className="p-2" style={TEXTO_FRACO}>
                    Nenhuma meta cadastrada
                  </p>
                ) : (
                  periodosQ.data.campanhas.map((c) => (
                    <button
                      key={`${c.dt_inicio}|${c.dt_fim}|${c.nome}`}
                      type="button"
                      className="w-full text-left px-3 py-2 rounded-lg hover:bg-[var(--surface-variant)]"
                      onClick={() => {
                        setMetaAberta(false);
                        aplicar(c.dt_inicio, c.dt_fim);
                      }}
                    >
                      <div className="font-semibold">{c.nome || `${data(c.dt_inicio)} → ${data(c.dt_fim)}`}</div>
                      <div style={TEXTO_FRACO}>{`${data(c.dt_inicio)} → ${data(c.dt_fim)}`}</div>
                    </button>
                  ))
                )}
              </div>
            )}
          </div>
          <input
            type="text"
            className="controle font-normal w-64"
            placeholder="Buscar por nome, RGM, curso..."
            aria-label="Buscar matrícula"
            value={busca}
            onChange={(e) => setBusca(e.target.value)}
          />
          <div className="flex gap-2 md:ml-auto">
            <button type="button" className="controle" onClick={() => aplicar("", "")}>
              Limpar
            </button>
            <button type="button" className="controle controle-primario" onClick={() => aplicar(rascunho.ini, rascunho.fim)}>
              <span className="material-symbols-outlined text-[18px]" aria-hidden>
                filter_alt
              </span>
              Filtrar
            </button>
          </div>
        </div>
        <div className="segmentado self-start" role="group" aria-label="Situação">
          {pilulas.map(([k, icone, rotulo, n]) => (
            <button key={k} type="button" aria-pressed={filtro === k} onClick={() => setFiltro(k)} className="flex items-center gap-1">
              <span className="material-symbols-outlined text-[16px]" aria-hidden>
                {icone}
              </span>
              {rotulo} <span className="tabular-nums">{n}</span>
            </button>
          ))}
        </div>
        <div className="cartao overflow-x-auto">
          <table className="tabela-cartao">
            <thead>
              <tr>
                <th className="text-left">Nome</th>
                <th className="text-left">RGM</th>
                <th className="text-left">Curso</th>
                <th className="text-left">Polo</th>
                <th className="text-left">Data mat.</th>
                <th className="text-left">Situação</th>
                <th className="text-left">Contagem</th>
              </tr>
            </thead>
            <tbody>
              {!uid ? (
                <Linha colunas={7} texto="Sem vínculo com o Kommo para listar as matrículas oficiais." />
              ) : oficiaisQ.isPending ? (
                <Linha colunas={7} texto="Carregando..." />
              ) : oficiaisQ.isError ? (
                <Linha colunas={7} texto={textoErro(oficiaisQ.error)} erro />
              ) : !oficiais.length ? (
                <Linha colunas={7} texto="Nenhuma matrícula encontrada" />
              ) : !visiveis.length ? (
                <Linha colunas={7} texto="Nenhuma matrícula corresponde aos filtros." />
              ) : (
                visiveis.map((m, i) => {
                  const sit = (m.situacao || "").toUpperCase();
                  return (
                    <tr key={`${m.rgm}-${i}`} className={m.outlier ? "bg-orange-500/5" : ""}>
                      <td>{m.nome || "—"}</td>
                      <td className="font-mono">
                        {m.outlier ? (
                          <span className="text-orange-700 dark:text-orange-300" title="RGM fora do padrão do ciclo">{`${m.rgm || "—"} ⚠`}</span>
                        ) : (
                          m.rgm || "—"
                        )}
                      </td>
                      <td>{m.curso || m.nivel || "—"}</td>
                      <td>{m.polo || "—"}</td>
                      <td>{data(m.data_matricula)}</td>
                      <td>
                        <SeloSituacao sit={sit} />
                      </td>
                      <td>
                        {m.outlier &&
                          (!m.conta_para_meta ? (
                            admin ? (
                              <button type="button" className="text-xs font-semibold text-orange-700 dark:text-orange-300 underline" onClick={() => contarVenda(m, true)}>
                                Contar venda
                              </button>
                            ) : (
                              <span className="text-xs text-orange-700 dark:text-orange-400">Não conta</span>
                            )
                          ) : (
                            <span className="text-xs">
                              <span className="font-semibold text-emerald-700 dark:text-emerald-400">✓ Contando</span>
                              {admin && (
                                <button type="button" className="ml-2 underline" style={TEXTO_FRACO} onClick={() => contarVenda(m, false)}>
                                  Desfazer
                                </button>
                              )}
                            </span>
                          ))}
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>
      </Secao>

      <Secao
        titulo="Minha lista de vendas"
        icone="edit_note"
        extra={
          minhasQ.data ? (
            <span className="text-xs font-normal" style={TEXTO_FRACO}>
              {minhas.length ? `${minhas.length} no período` : "nenhuma no período"}
            </span>
          ) : null
        }
      >
        <div className="flex flex-wrap items-center gap-2">
          <p className="text-xs" style={TEXTO_FRACO}>
            Usa o mesmo período <strong>De / Até</strong> das matrículas oficiais acima.
          </p>
          <button type="button" className="controle controle-primario md:ml-auto" onClick={() => setModalMat({ editando: null })}>
            <span className="material-symbols-outlined text-[18px]" aria-hidden>
              add
            </span>
            Adicionar
          </button>
        </div>
        <div className="cartao overflow-x-auto">
          <table className="tabela-cartao">
            <thead>
              <tr>
                <th className="text-left">Nome</th>
                <th className="text-left">RGM</th>
                <th className="text-left">Curso</th>
                <th className="text-left">Polo</th>
                <th className="text-left">Data mat.</th>
                <th className="text-left">Negócio</th>
                <th className="text-left">Ações</th>
              </tr>
            </thead>
            <tbody>
              {minhasQ.isPending ? (
                <Linha colunas={7} texto="Carregando..." />
              ) : minhasQ.isError ? (
                <Linha colunas={7} texto={textoErro(minhasQ.error)} erro />
              ) : !minhas.length ? (
                <Linha colunas={7} texto='Nenhuma matrícula cadastrada. Clique em "Adicionar" acima.' />
              ) : (
                minhas.map((m) => (
                  <tr key={m.id}>
                    <td>{m.nome || "—"}</td>
                    <td className="font-mono">{m.rgm || "—"}</td>
                    <td>{m.curso || "—"}</td>
                    <td>{m.polo || "—"}</td>
                    <td>{data(m.data_matricula)}</td>
                    <td className="font-mono">{m.kommo_lead_id || "—"}</td>
                    <td>
                      <div className="flex items-center gap-1">
                        <button type="button" aria-label="Editar" title="Editar" className="text-blue-700 dark:text-blue-400" onClick={() => setModalMat({ editando: m })}>
                          <span className="material-symbols-outlined text-[18px]" aria-hidden>
                            edit
                          </span>
                        </button>
                        <button type="button" aria-label="Excluir" title="Excluir" style={{ color: "var(--danger)" }} onClick={() => excluir(m)}>
                          <span className="material-symbols-outlined text-[18px]" aria-hidden>
                            delete
                          </span>
                        </button>
                      </div>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </Secao>

      <Secao titulo="Solicitações de ajuste" icone="support_agent">
        <div className="flex">
          <button type="button" className="controle md:ml-auto" onClick={() => setModalAjuste(true)}>
            <span className="material-symbols-outlined text-[18px]" aria-hidden>
              add_circle
            </span>
            Nova solicitação
          </button>
        </div>
        <div className="flex flex-col gap-2">
          {ajustesQ.isPending ? (
            <p className="text-xs py-4 text-center" style={TEXTO_FRACO}>
              Carregando...
            </p>
          ) : ajustesQ.isError ? (
            <p className="text-xs py-4 text-center" style={{ color: "var(--danger)" }}>
              {textoErro(ajustesQ.error)}
            </p>
          ) : !ajustes.length ? (
            <p className="text-xs py-4 text-center" style={TEXTO_FRACO}>
              Nenhuma solicitação enviada.
            </p>
          ) : (
            ajustes.map((a) => {
              const st = STATUS_AJUSTE[a.status] || STATUS_AJUSTE.pendente;
              return (
                <div key={a.id} className="cartao p-3 flex flex-col gap-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <Selo cor={st.cor}>{STATUS_AJUSTE[a.status]?.rotulo || a.status}</Selo>
                    <span className="text-xs" style={TEXTO_FRACO}>
                      {TIPO_AJUSTE[a.tipo] || a.tipo}
                    </span>
                    <span className="text-xs ml-auto" style={TEXTO_FRACO}>
                      {data(String(a.created_at).substring(0, 10))}
                    </span>
                  </div>
                  <p className="text-xs">
                    <strong>{a.nome_aluno || "—"}</strong>
                    {` — RGM: ${a.rgm || "—"} — Lead: ${a.kommo_lead_id || "—"}`}
                  </p>
                  {a.descricao && (
                    <p className="text-xs" style={TEXTO_FRACO}>
                      {a.descricao}
                    </p>
                  )}
                  {a.resposta_admin && (
                    <p className="text-xs mt-1 px-3 py-2 rounded-lg" style={{ background: "var(--bg-elevated)" }}>
                      <span className="font-semibold">Resposta:</span> {a.resposta_admin}
                    </p>
                  )}
                </div>
              );
            })
          )}
        </div>
      </Secao>

      {modalMat && (
        <ModalMinhaMatricula
          editando={modalMat.editando}
          minhas={minhas}
          oficiais={oficiais}
          oficiaisCarregando={oficiaisQ.isFetching}
          ciclosPainel={oficiaisQ.data?.ciclos_painel ?? {}}
          onFechar={() => setModalMat(null)}
          onSalvo={(editou) => {
            setModalMat(null);
            avisar(editou ? "Matrícula atualizada!" : "Matrícula adicionada à sua lista!");
            cliente.invalidateQueries({ queryKey: ["mp", "minhas"] });
          }}
        />
      )}
      {modalAjuste && (
        <ModalAjuste
          onFechar={() => setModalAjuste(false)}
          onEnviado={() => {
            setModalAjuste(false);
            avisar("Solicitação enviada! Acompanhe em Solicitações de Ajuste.");
            cliente.invalidateQueries({ queryKey: ["mp", "ajustes"] });
          }}
        />
      )}
    </div>
  );
}
