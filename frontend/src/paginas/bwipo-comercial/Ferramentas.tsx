import { useQuery } from "@tanstack/react-query";
import type { Acoes } from "./acoes";
import { apiOk, enviar, FONTE, num, textoErro } from "./formato";
import { ChaveValor, Modal, TituloSecao, Vazio } from "./Modal";
import type { Diagnostico, Duplicatas, Linha, SemData } from "./tipos";

type Base = { linhas: Linha[]; acoes: Acoes; onFechar: () => void };

/** RGMs únicos do recorte, na ordem das linhas (_bwcRgmsRecorte). */
export function rgmsRecorte(linhas: Linha[]): string[] {
  return [...new Set(linhas.map((l) => l.rgm))];
}

function porRgm(linhas: Linha[]): Record<string, Linha> {
  const m: Record<string, Linha> = {};
  for (const l of linhas) m[l.rgm] = l;
  return m;
}

const BOTAO_RGM = "font-bold underline decoration-dotted";

/** Duplicatas no Bwipo (bwcAbrirDuplicatas). */
export function ModalDuplicatas({ linhas, acoes, onFechar }: Base) {
  const titulo = "Duplicatas no Bwipo";
  const q = useQuery({
    queryKey: ["bwipo", "duplicatas"],
    queryFn: ({ signal }) => enviar<Duplicatas>("/api/bwipo/painel/duplicatas", "POST", { rgms: rgmsRecorte(linhas) }, signal),
    gcTime: 0,
    retry: false,
  });
  const lin = porRgm(linhas);
  const sub = q.isPending
    ? "Carregando…"
    : q.isError
      ? "Erro"
      : `${num(q.data.total)} RGMs do recorte com mais de um negócio no Pipeline Principal`;
  return (
    <Modal titulo={titulo} sub={sub} onFechar={onFechar}>
      {q.isError && <Vazio erro>{textoErro(q.error)}</Vazio>}
      {q.data &&
        (q.data.duplicatas.length ? (
          q.data.duplicatas.map((g) => (
            <div key={g.rgm} style={{ borderBottom: "1px solid var(--border)" }}>
              <div className="px-4 pt-2">
                <button type="button" className={BOTAO_RGM} onClick={() => acoes.consultarRgm(g.rgm)}>
                  {g.rgm}
                </button>{" "}
                {lin[g.rgm]?.nome || ""} <span style={{ color: "var(--text-muted)" }}>· {g.count} negócios</span>
              </div>
              {g.negocios.map((n) => (
                <div key={n.id} className="px-6 py-1 flex justify-between gap-3">
                  <span>{`#${n.number || "—"} ${n.title}`}</span>
                  <span style={{ color: "var(--text-muted)" }}>
                    {n.agente} ·{" "}
                    <span className={n.ganho ? "text-emerald-600 dark:text-emerald-400" : n.perdido ? "text-rose-600 dark:text-rose-400" : ""}>
                      {n.etapa}
                    </span>
                  </span>
                </div>
              ))}
            </div>
          ))
        ) : (
          <Vazio>Nenhum RGM do recorte com mais de um negócio no Pipeline Principal.</Vazio>
        ))}
    </Modal>
  );
}

/** Matrículas sem data no Bwipo (bwcAbrirSemData). */
export function ModalSemData({ linhas, acoes, onFechar }: Base) {
  const q = useQuery({
    queryKey: ["bwipo", "sem-data"],
    queryFn: ({ signal }) => enviar<SemData>("/api/bwipo/painel/sem-data", "POST", { rgms: rgmsRecorte(linhas) }, signal),
    gcTime: 0,
    retry: false,
  });
  const lin = porRgm(linhas);
  const sub = q.isPending ? "Carregando…" : q.isError ? "Erro" : "Matrícula do recorte (relatório SIAA) cruzada com o negócio no Bwipo";
  const item = (rgm: string, dir: string, chave: string) => (
    <div key={chave} className="px-4 py-1.5 flex justify-between gap-3" style={{ borderBottom: "1px solid var(--border-subtle)" }}>
      <span>
        <button type="button" className={BOTAO_RGM} onClick={() => acoes.consultarRgm(rgm)}>
          {rgm}
        </button>{" "}
        {lin[rgm]?.nome || ""}
      </span>
      <span style={{ color: "var(--text-muted)" }}>{dir}</span>
    </div>
  );
  return (
    <Modal titulo="Matrículas sem data no Bwipo" sub={sub} onFechar={onFechar}>
      {q.isError && <Vazio erro>{textoErro(q.error)}</Vazio>}
      {q.data && (
        <>
          <p className="px-4 py-2 font-semibold">{`Negócio no Bwipo sem data de matrícula (${num(q.data.sem_data.length)})`}</p>
          {q.data.sem_data.length ? (
            q.data.sem_data.map((s) => item(s.rgm, `#${s.number || "—"} · ${s.agente} · ${s.etapa}`, `d-${s.rgm}`))
          ) : (
            <p className="px-4 py-2" style={{ color: "var(--text-muted)" }}>Nenhum.</p>
          )}
          <p className="px-4 py-2 font-semibold" style={{ borderTop: "1px solid var(--border)" }}>
            {`Matrícula sem negócio no Bwipo (${num(q.data.sem_negocio.length)})`}
          </p>
          {q.data.sem_negocio.length ? (
            q.data.sem_negocio.map((r) => item(r, `${lin[r]?.agente || ""} · ${FONTE[lin[r]?.fonte || ""] || ""}`, `n-${r}`))
          ) : (
            <p className="px-4 py-2" style={{ color: "var(--text-muted)" }}>Nenhuma.</p>
          )}
        </>
      )}
    </Modal>
  );
}

/** Diagnóstico Kommo ↔ Bwipo (bwcAbrirDiagnostico). */
export function ModalDiagnostico({ linhas, onFechar }: Omit<Base, "acoes">) {
  const q = useQuery({
    queryKey: ["bwipo", "diagnostico"],
    queryFn: ({ signal }) => apiOk<Diagnostico>("/api/bwipo/painel/diagnostico", signal),
    gcTime: 0,
    retry: false,
  });
  const sub = q.isPending ? "Carregando…" : q.isError ? "Erro" : "Saúde da ligação entre os dois CRMs";
  const fontes: Record<string, number> = {};
  for (const l of linhas) fontes[l.fonte] = (fontes[l.fonte] || 0) + 1;
  const n = q.data?.numeros ?? {};
  return (
    <Modal titulo="Diagnóstico Kommo ↔ Bwipo" sub={sub} onFechar={onFechar}>
      {q.isError && <Vazio erro>{textoErro(q.error)}</Vazio>}
      {q.data && (
        <div className="pb-3">
          <TituloSecao>{`Quem ficou com as vendas deste recorte (${num(linhas.length)})`}</TituloSecao>
          {Object.entries(fontes).map(([k, v]) => (
            <ChaveValor key={k} chave={FONTE[k] || k} valor={num(v)} />
          ))}
          <TituloSecao>Espelho Bwipo</TituloSecao>
          <ChaveValor chave="Negócios" valor={num(n.negocios)} />
          <ChaveValor chave="No Pipeline Principal" valor={num(n.negocios_principal)} />
          <ChaveValor chave="Com RGM" valor={num(n.negocios_com_rgm)} />
          <ChaveValor chave="Sem responsável" valor={num(n.negocios_sem_dono)} />
          <ChaveValor chave="Responsável sem depara com o Kommo" valor={num(n.negocios_dono_sem_depara)} />
          <TituloSecao>Depara</TituloSecao>
          <ChaveValor chave="Leads Kommo casados" valor={num(n.depara_leads)} />
          <ChaveValor chave="Negócios Bwipo casados" valor={num(n.depara_negocios)} />
          {Object.entries(q.data.depara_por_tipo || {}).map(([k, v]) => (
            <ChaveValor key={k} chave={`· por ${k}`} valor={num(v)} />
          ))}
          <ChaveValor chave="Consultores ligados" valor={num(n.depara_consultores)} />
          <ChaveValor chave="Consultores tombados" valor={num(n.consultores_tombados)} />
          <ChaveValor chave="Decisões manuais de venda" valor={num(n.decisoes_manuais)} />
          <TituloSecao>Responsáveis do Bwipo sem depara</TituloSecao>
          {(q.data.donos_sem_depara || []).length ? (
            q.data.donos_sem_depara.map((x) => <ChaveValor key={x.agente} chave={x.agente} valor={`${num(x.negocios)} negócios`} />)
          ) : (
            <p className="px-4 py-1" style={{ color: "var(--text-muted)" }}>Nenhum.</p>
          )}
          <TituloSecao>Últimos syncs</TituloSecao>
          {(q.data.syncs || []).map((s) => (
            <ChaveValor
              key={s.entidade}
              chave={
                <>
                  {s.entidade}{" "}
                  <span style={{ color: "var(--text-muted)" }}>{`${s.status || ""}${s.erro ? " · " + s.erro : ""}`}</span>
                </>
              }
              valor={s.em ? new Date(s.em).toLocaleString("pt-BR") : "—"}
            />
          ))}
        </div>
      )}
    </Modal>
  );
}
