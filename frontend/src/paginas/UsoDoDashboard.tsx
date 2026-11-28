import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { api, ErroApi } from "../api";
import { hojeIso, numero, somarDias } from "../datas";
import { TITULOS } from "../titulos";

/**
 * Uso do Dashboard (página `page_views`, só admin). Primeira tela migrada
 * (Fase 4, etapa 3). Mesma API e mesmos números da tela antiga
 * (templates/partials/_page_views.html + static/js/page_views.js).
 */

interface Usuario {
  username: string | null;
  role: string;
}

export interface UsoStats {
  ok: boolean;
  start_date: string;
  end_date: string;
  totais: { total_hits?: number; total_users?: number; total_pages?: number; dias_ativos?: number };
  por_pagina: { page: string; hits: number; users: number }[];
  nao_acessadas: { page: string; users_with_access: Usuario[] }[];
}

const PRESETS = [
  { dias: 0, rotulo: "Hoje" },
  { dias: 6, rotulo: "7 dias" },
  { dias: 14, rotulo: "15 dias" },
  { dias: 29, rotulo: "30 dias" },
];

interface Periodo {
  inicio: string;
  fim: string;
}

function periodoDoPreset(dias: number): Periodo {
  const fim = hojeIso();
  return { inicio: somarDias(fim, -dias), fim };
}

function nome(p: string) {
  return TITULOS[p] ?? p;
}

function Pagina({ page }: { page: string }) {
  return (
    <>
      <div className="font-medium" style={{ color: "var(--text-primary)" }}>{nome(page)}</div>
      <div className="text-[10px]" style={{ color: "var(--text-muted)" }}>{page}</div>
    </>
  );
}

function Selo({ u }: { u: Usuario }) {
  const admin = u.role === "admin";
  const cls = admin
    ? "bg-violet-500/15 text-violet-700 dark:text-violet-300 border-violet-500/40"
    : "bg-slate-500/10 text-slate-700 dark:text-slate-300 border-slate-500/30";
  return (
    <span className={`inline-flex items-center text-[10px] font-medium px-2 py-0.5 rounded-full border ${cls}`}>
      {u.username || "—"}
      {admin ? " · admin" : ""}
    </span>
  );
}

function Indicador({
  rotulo,
  valor,
  cor,
  contexto,
}: {
  rotulo: string;
  valor: string;
  cor?: string;
  contexto?: string;
}) {
  return (
    <div className="cartao p-5" role="group" aria-label={rotulo}>
      <p className="rotulo mb-1">{rotulo}</p>
      <p className={`text-3xl font-headline font-extrabold ${cor ?? ""}`}>{valor}</p>
      {contexto && (
        <p className="text-xs mt-1" style={{ color: "var(--text-muted)" }}>{contexto}</p>
      )}
    </div>
  );
}

export function UsoDoDashboard() {
  const [preset, setPreset] = useState<number | null>(29);
  const [rascunho, setRascunho] = useState<Periodo>(() => periodoDoPreset(29));
  const [periodo, setPeriodo] = useState<Periodo>(rascunho);

  const q = useQuery({
    queryKey: ["page-views", periodo.inicio, periodo.fim],
    queryFn: () =>
      api<UsoStats>(
        `/api/page-views/stats?start_date=${encodeURIComponent(periodo.inicio)}&end_date=${encodeURIComponent(periodo.fim)}`,
      ),
  });

  function escolherPreset(dias: number) {
    const p = periodoDoPreset(dias);
    setPreset(dias);
    setRascunho(p);
    setPeriodo(p);
  }

  function aplicar() {
    setPreset(null);
    setPeriodo({ ...rascunho });
  }

  const d = q.data;
  const carregando = q.isPending;
  const totalTelas = d ? d.por_pagina.length + d.nao_acessadas.length : 0;
  const maxHits = d ? Math.max(1, ...d.por_pagina.map((r) => r.hits)) : 1;

  return (
    <div className="flex flex-col gap-6 max-w-6xl">
      <section className="flex flex-wrap gap-3 items-end" aria-label="Período">
        <div className="flex flex-wrap items-center gap-2">
          {PRESETS.map((p) => (
            <button
              key={p.dias}
              type="button"
              aria-pressed={preset === p.dias}
              onClick={() => escolherPreset(p.dias)}
              className="px-3.5 py-1.5 rounded-full text-xs font-semibold border"
              style={
                preset === p.dias
                  ? { background: "var(--primary)", borderColor: "var(--primary)", color: "var(--bg-card)" }
                  : { background: "transparent", borderColor: "var(--border)", color: "var(--text-secondary)" }
              }
            >
              {p.rotulo}
            </button>
          ))}
        </div>
        <label className="flex flex-col gap-1.5">
          <span className="rotulo">De</span>
          <input
            type="date"
            value={rascunho.inicio}
            onChange={(e) => setRascunho((r) => ({ ...r, inicio: e.target.value }))}
            className="px-3 py-2 rounded-lg text-sm border w-[150px]"
            style={{ background: "var(--bg-elevated)", borderColor: "var(--border)", color: "var(--text-primary)" }}
          />
        </label>
        <label className="flex flex-col gap-1.5">
          <span className="rotulo">Até</span>
          <input
            type="date"
            value={rascunho.fim}
            onChange={(e) => setRascunho((r) => ({ ...r, fim: e.target.value }))}
            className="px-3 py-2 rounded-lg text-sm border w-[150px]"
            style={{ background: "var(--bg-elevated)", borderColor: "var(--border)", color: "var(--text-primary)" }}
          />
        </label>
        <button
          type="button"
          onClick={aplicar}
          className="px-4 py-2 rounded-lg text-sm font-semibold border"
          style={{ background: "transparent", borderColor: "var(--border)", color: "var(--text-primary)" }}
        >
          Aplicar
        </button>
        <button
          type="button"
          onClick={() => q.refetch()}
          disabled={q.isFetching}
          className="md:ml-auto px-4 py-2 rounded-lg text-sm font-semibold flex items-center gap-2 disabled:opacity-60"
          style={{ background: "var(--primary)", color: "var(--bg-card)" }}
        >
          <span className="material-symbols-outlined" aria-hidden>refresh</span>
          {q.isFetching ? "Atualizando…" : "Atualizar"}
        </button>
      </section>

      {q.isError && (
        <div role="alert" className="cartao p-5 text-sm">
          {q.error instanceof ErroApi && q.error.status === 403
            ? "Acesso negado. Apenas administrador."
            : `Não foi possível carregar: ${(q.error as Error).message}`}
        </div>
      )}

      {!q.isError && (
        <>
          <div className="grid grid-cols-2 md:grid-cols-3 gap-4">
            <Indicador rotulo="Acessos" valor={carregando ? "—" : numero(d?.totais.total_hits)} />
            <Indicador
              rotulo="Telas usadas"
              valor={carregando ? "—" : numero(d?.por_pagina.length)}
              contexto={d ? `de ${totalTelas} telas` : undefined}
              cor="text-emerald-600 dark:text-emerald-400"
            />
            <Indicador
              rotulo="Telas sem uso"
              valor={carregando ? "—" : numero(d?.nao_acessadas.length)}
              contexto={d ? "ninguém abriu no período" : undefined}
              cor="text-amber-600 dark:text-amber-400"
            />
          </div>

          <section className="cartao p-5">
            <h2 className="text-sm mb-3 flex items-center gap-2">
              <span className="material-symbols-outlined text-emerald-500" aria-hidden>check_circle</span>
              Páginas acessadas
            </h2>
            <div className="overflow-auto max-h-[480px] rounded-lg border" style={{ borderColor: "var(--border)" }}>
              <table className="w-full text-xs">
                <thead className="sticky top-0" style={{ background: "var(--bg-card)" }}>
                  <tr className="border-b" style={{ borderColor: "var(--border)", color: "var(--text-muted)" }}>
                    <th className="text-left px-3 py-2 font-semibold">Página</th>
                    <th className="px-3 py-2 w-[34%]" aria-hidden />
                    <th className="text-right px-3 py-2 font-semibold">Acessos</th>
                    <th className="text-right px-3 py-2 font-semibold">Usuários únicos</th>
                  </tr>
                </thead>
                <tbody>
                  {carregando ? (
                    <tr><td colSpan={4} className="px-3 py-6 text-center" style={{ color: "var(--text-muted)" }}>Carregando…</td></tr>
                  ) : d && d.por_pagina.length ? (
                    d.por_pagina.map((r) => (
                      <tr key={r.page} className="border-b" style={{ borderColor: "var(--border)" }}>
                        <td className="px-3 py-2"><Pagina page={r.page} /></td>
                        <td className="px-3 py-2">
                          <div className="h-1.5 rounded-full" style={{ background: "var(--bg-elevated)" }}>
                            <div
                              data-testid={`barra-${r.page}`}
                              className="h-1.5 rounded-full"
                              style={{ width: `${(r.hits / maxHits) * 100}%`, background: "var(--primary)" }}
                            />
                          </div>
                        </td>
                        <td className="px-3 py-2 text-right tabular-nums font-semibold text-emerald-600 dark:text-emerald-400">{numero(r.hits)}</td>
                        <td className="px-3 py-2 text-right tabular-nums" style={{ color: "var(--text-secondary)" }}>{numero(r.users)}</td>
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={4} className="px-3 py-6 text-center" style={{ color: "var(--text-muted)" }}>Nenhuma página acessada no período.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="cartao p-5">
            <h2 className="text-sm mb-1 flex items-center gap-2">
              <span className="material-symbols-outlined text-amber-500" aria-hidden>warning</span>
              Páginas sem nenhum acesso no período
            </h2>
            <p className="text-[11px] mb-3" style={{ color: "var(--text-muted)" }}>
              A coluna "Quem tem acesso" lista os usuários que poderiam estar usando a página mas não usaram.
            </p>
            <div className="overflow-auto max-h-[640px] rounded-lg border" style={{ borderColor: "var(--border)" }}>
              <table className="w-full text-xs">
                <thead className="sticky top-0" style={{ background: "var(--bg-card)" }}>
                  <tr className="border-b" style={{ borderColor: "var(--border)", color: "var(--text-muted)" }}>
                    <th className="text-left px-3 py-2 font-semibold">Página</th>
                    <th className="text-left px-3 py-2 font-semibold">Quem tem acesso</th>
                  </tr>
                </thead>
                <tbody>
                  {carregando ? (
                    <tr><td colSpan={2} className="px-3 py-6 text-center" style={{ color: "var(--text-muted)" }}>Carregando…</td></tr>
                  ) : d && d.nao_acessadas.length ? (
                    d.nao_acessadas.map((r) => (
                      <tr key={r.page} className="border-b align-top" style={{ borderColor: "var(--border)" }}>
                        <td className="px-3 py-2 whitespace-nowrap"><Pagina page={r.page} /></td>
                        <td className="px-3 py-2">
                          <div className="flex flex-wrap gap-1">
                            {r.users_with_access.length ? (
                              r.users_with_access.map((u) => <Selo key={u.username ?? ""} u={u} />)
                            ) : (
                              <span className="text-[10px] italic text-amber-600 dark:text-amber-500">
                                Ninguém tem permissão · página órfã
                              </span>
                            )}
                          </div>
                          <div className="text-[10px] mt-1" style={{ color: "var(--text-muted)" }}>
                            {r.users_with_access.length} usuário(s) com acesso
                          </div>
                        </td>
                      </tr>
                    ))
                  ) : (
                    <tr><td colSpan={2} className="px-3 py-6 text-center" style={{ color: "var(--text-muted)" }}>Todas as páginas foram acessadas no período.</td></tr>
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </>
      )}
    </div>
  );
}
