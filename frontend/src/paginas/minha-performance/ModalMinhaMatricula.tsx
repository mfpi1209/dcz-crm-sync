import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../../api";
import { TEXTO_FRACO } from "./comum";
import { data, pedirComCorpo, textoErro } from "./formato";
import { Modal } from "./Modal";
import type { MatriculaOficial, MinhaMatricula } from "./tipos";

type Campo = "nome" | "rgm" | "curso" | "polo" | "data" | "ciclo" | "nivel" | "kommo" | "obs";
type Form = Record<Campo, string>;

/** Obrigatórios na mesma ordem da antiga (_mpSaveMinhaMatricula). */
const OBRIGATORIOS: [Campo, string][] = [
  ["nome", "Nome do aluno"],
  ["rgm", "RGM"],
  ["curso", "Curso"],
  ["polo", "Polo"],
  ["data", "Data matrícula"],
  ["ciclo", "Ciclo"],
  ["nivel", "Nível"],
  ["kommo", "Nº do negócio"],
];

interface Prefill {
  nome?: string;
  rgm?: string;
  curso?: string;
  polo?: string;
  data_matricula?: string;
  ciclo?: string;
  nivel?: string;
  kommo_lead_id?: string | number;
  lead_id?: string | number;
}

interface RespostaSync {
  ok?: boolean;
  error?: string;
  msg?: string;
  prefill?: Prefill;
  deals?: { number: number; etapa?: string }[];
}

function formDe(m: MinhaMatricula | null): Form {
  return {
    nome: m?.nome || "",
    rgm: m?.rgm || "",
    curso: m?.curso || "",
    polo: m?.polo || "",
    data: m?.data_matricula ? data(m.data_matricula) : "",
    ciclo: m?.ciclo || "",
    nivel: m?.nivel || "",
    kommo: m?.kommo_lead_id != null ? String(m.kommo_lead_id) : "",
    obs: m?.observacao || "",
  };
}

/**
 * Modal "Adicionar/Editar matrícula" da Minha lista de vendas: escolher nas oficiais,
 * buscar no Bwipo ou preencher à mão; mesmas validações e mensagens da antiga.
 */
export function ModalMinhaMatricula({
  editando,
  minhas,
  oficiais,
  oficiaisCarregando,
  ciclosPainel,
  onFechar,
  onSalvo,
}: {
  editando: MinhaMatricula | null;
  minhas: MinhaMatricula[];
  oficiais: MatriculaOficial[];
  oficiaisCarregando: boolean;
  ciclosPainel: Record<string, string>;
  onFechar: () => void;
  onSalvo: (editou: boolean) => void;
}) {
  const [f, setF] = useState<Form>(() => formDe(editando));
  const [fonte, setFonte] = useState<"oficial" | "sync" | "manual">("oficial");
  const [erro, setErro] = useState("");
  const [busca, setBusca] = useState("");
  const [escolhido, setEscolhido] = useState<number | null>(null);
  const [syncLead, setSyncLead] = useState("");
  const [syncRgm, setSyncRgm] = useState("");
  const [syncMsg, setSyncMsg] = useState<{ cor: "erro" | "aviso" | "ok"; texto: string } | null>(null);
  const [negocios, setNegocios] = useState<{ number: number; etapa?: string }[]>([]);
  const [sincronizando, setSincronizando] = useState(false);

  const mudar = (c: Campo, v: string) => setF((x) => ({ ...x, [c]: v }));

  const q = busca.trim().toLowerCase();
  const lista = [...oficiais]
    .sort((a, b) => String(a.nome || "").localeCompare(String(b.nome || ""), "pt-BR"))
    .filter((m) => !q || [m.nome, m.rgm, m.curso, m.polo, m.situacao].map((x) => String(x || "").toLowerCase()).join(" ").includes(q));

  /** _mpPickOficialMatricula. */
  function escolher(i: number) {
    const m = lista[i];
    if (!m) return;
    setEscolhido(i);
    const ciclo = ciclosPainel[m.nivel || ""] || ciclosPainel["Graduação"] || "";
    setF((x) => ({
      ...x,
      nome: m.nome || "",
      rgm: m.rgm || "",
      curso: m.curso || "",
      polo: m.polo || "",
      data: m.data_matricula ? data(m.data_matricula) : "",
      nivel: m.nivel || "",
      ciclo: ciclo || x.ciclo,
      obs: x.obs.trim() ? x.obs : "Importado das matrículas oficiais",
    }));
  }

  /** _mpApplyPrefill: só os campos que vieram. */
  function aplicarPrefill(p: Prefill) {
    setF((x) => ({
      ...x,
      nome: p.nome || x.nome,
      rgm: p.rgm || x.rgm,
      curso: p.curso || x.curso,
      polo: p.polo || x.polo,
      data: p.data_matricula || x.data,
      ciclo: p.ciclo || x.ciclo,
      nivel: p.nivel || x.nivel,
      kommo: p.kommo_lead_id || p.lead_id ? String(p.kommo_lead_id || p.lead_id) : x.kommo,
      obs: x.obs.trim() ? x.obs : "Preenchido via sync Bwipo",
    }));
  }

  /** _mpSyncLeadPrefill. */
  async function sincronizar(forcado?: number) {
    const corpo: { number?: number; rgm?: string } = {};
    if (forcado != null) {
      corpo.number = forcado;
      setSyncLead(String(forcado));
    } else {
      const id = syncLead.trim().replace(/\D/g, "");
      const rgm = syncRgm.trim().replace(/\D/g, "");
      if (id) corpo.number = parseInt(id, 10);
      else if (rgm.length === 8) corpo.rgm = rgm;
      else {
        setSyncMsg({ cor: "erro", texto: "Informe o número do negócio ou um RGM com 8 dígitos." });
        return;
      }
    }
    setNegocios([]);
    setSincronizando(true);
    try {
      const r = await pedirComCorpo<RespostaSync>("/api/minha-performance/sync-bwipo-prefill", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(corpo),
      });
      const d = r.corpo;
      if (r.status === 409 && d.deals?.length) {
        setSyncMsg({ cor: "aviso", texto: d.error || "Vários negócios com esse RGM. Escolha um:" });
        setNegocios(d.deals);
        return;
      }
      if (!r.ok || !d.ok) {
        setSyncMsg({ cor: "erro", texto: d.error || "Falha no sync" });
        return;
      }
      aplicarPrefill(d.prefill || {});
      setSyncMsg({ cor: "ok", texto: `${d.msg || "OK"} RGM ${d.prefill?.rgm || "—"}.` });
    } catch (e) {
      setSyncMsg({ cor: "erro", texto: `Erro: ${textoErro(e)}` });
    } finally {
      setSincronizando(false);
    }
  }

  const salvar = useMutation({
    mutationFn: (corpo: Record<string, string | null>) =>
      api<{ ok?: boolean; error?: string }>(editando ? `/api/minha-performance/minhas-matriculas/${editando.id}` : "/api/minha-performance/minhas-matriculas", {
        method: editando ? "PUT" : "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(corpo),
      }),
    onSuccess: (d) => {
      if (d && d.ok === false) {
        setErro(`Erro ao salvar: ${d.error || "Erro ao salvar"}`);
        return;
      }
      onSalvo(!!editando);
    },
    onError: (e) => setErro(`Erro ao salvar: ${textoErro(e)}`),
  });

  function enviar() {
    for (const [c, rotulo] of OBRIGATORIOS) {
      if (!f[c].trim()) {
        setErro(`Preencha o campo obrigatório: ${rotulo}.`);
        document.getElementById(`mp-mm-${c}`)?.focus();
        return;
      }
    }
    const rgm = f.rgm.replace(/\D/g, "");
    if (!rgm) {
      setErro("Informe um RGM válido.");
      return;
    }
    const dup = minhas.find((m) => {
      const outro = String(m.rgm || "").replace(/\D/g, "");
      if (!outro || outro !== rgm) return false;
      return !(editando && String(m.id) === String(editando.id));
    });
    if (dup) {
      setErro(`RGM ${rgm} já está na sua lista de vendas. Não é permitido cadastrar o mesmo RGM duas vezes.`);
      return;
    }
    setErro("");
    salvar.mutate({
      nome: f.nome.trim(),
      rgm,
      curso: f.curso.trim(),
      polo: f.polo.trim(),
      data_matricula: f.data.trim() || null,
      ciclo: f.ciclo.trim(),
      nivel: f.nivel.trim(),
      kommo_lead_id: f.kommo.trim(),
      observacao: f.obs.trim(),
    });
  }

  const campo = (c: Campo, rotulo: string, extra: { placeholder?: string; inputMode?: "numeric" } = {}) => (
    <label className="flex flex-col gap-1 text-xs" style={{ color: "var(--text-secondary)" }}>
      <span>
        {rotulo} <span className="text-red-600">*</span>
      </span>
      <input id={`mp-mm-${c}`} aria-label={rotulo} className="controle font-normal w-full" value={f[c]} onChange={(e) => mudar(c, e.target.value)} {...extra} />
    </label>
  );

  const corMsg = { erro: "var(--danger)", aviso: "#b45309", ok: "#047857" } as const;

  return (
    <Modal titulo={editando ? "Editar matrícula" : "Adicionar matrícula"} onFechar={onFechar}>
      {!editando && (
        <div className="flex flex-col gap-3">
          <div className="segmentado self-start">
            {(
              [
                ["oficial", "Matrículas oficiais"],
                ["sync", "Sync Bwipo"],
                ["manual", "Preencher manual"],
              ] as const
            ).map(([k, r]) => (
              <button key={k} type="button" aria-pressed={fonte === k} onClick={() => setFonte(k)}>
                {r}
              </button>
            ))}
          </div>
          {fonte === "oficial" && (
            <div className="flex flex-col gap-2">
              <input
                type="search"
                className="controle font-normal w-full"
                aria-label="Buscar nas suas matrículas oficiais"
                placeholder="Nome, RGM, curso ou polo…"
                value={busca}
                onChange={(e) => {
                  setBusca(e.target.value);
                  setEscolhido(null);
                }}
              />
              <div className="max-h-56 overflow-y-auto rounded-lg border" style={{ borderColor: "var(--border)" }}>
                {oficiaisCarregando && !oficiais.length ? (
                  <p className="text-xs px-3 py-4 text-center" style={TEXTO_FRACO}>
                    Carregando matrículas oficiais…
                  </p>
                ) : !lista.length ? (
                  <p className="text-xs px-3 py-4 text-center" style={TEXTO_FRACO}>
                    Nenhuma matrícula neste filtro
                  </p>
                ) : (
                  lista.map((m, i) => (
                    <button
                      key={`${m.rgm}-${i}`}
                      type="button"
                      onClick={() => escolher(i)}
                      aria-pressed={escolhido === i}
                      className="w-full text-left px-3 py-2 hover:bg-[var(--surface-variant)] aria-pressed:bg-[var(--primary-light)]"
                      style={{ borderBottom: "1px solid var(--border-subtle)" }}
                    >
                      <div className="text-xs font-semibold truncate">{m.nome || "—"}</div>
                      <div className="mt-0.5 flex flex-wrap gap-x-3 text-[11px]" style={TEXTO_FRACO}>
                        <span className="font-mono">{`RGM ${m.rgm || "—"}`}</span>
                        <span>{m.polo || "—"}</span>
                        <span>{m.data_matricula ? data(m.data_matricula) : "—"}</span>
                        <span>{(m.situacao || "").toUpperCase() || "—"}</span>
                      </div>
                      <div className="text-[11px] truncate" style={TEXTO_FRACO}>
                        {m.curso || "—"}
                      </div>
                    </button>
                  ))
                )}
              </div>
              <p className="text-[11px]" style={TEXTO_FRACO}>
                {lista.length ? `${lista.length} aluno(s) · clique para preencher` : "0 resultado(s)"}
              </p>
            </div>
          )}
          {fonte === "sync" && (
            <div className="flex flex-col gap-2">
              <p className="text-xs" style={TEXTO_FRACO}>
                Busca o negócio no Bwipo e preenche com o nome, RGM, curso, polo e data de matrícula do CRM.
              </p>
              <div className="flex flex-wrap items-end gap-2">
                <input className="controle font-normal w-36" aria-label="Número do negócio no Bwipo" placeholder="Nº do negócio" value={syncLead} onChange={(e) => setSyncLead(e.target.value)} />
                <span className="text-xs pb-2" style={TEXTO_FRACO}>
                  ou
                </span>
                <input className="controle font-normal w-36" aria-label="RGM no Bwipo" placeholder="RGM (8 dígitos)" maxLength={12} value={syncRgm} onChange={(e) => setSyncRgm(e.target.value)} />
                <button type="button" className="controle controle-primario" disabled={sincronizando} onClick={() => sincronizar()}>
                  Buscar e preencher
                </button>
              </div>
              {negocios.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {negocios.map((n) => (
                    <button key={n.number} type="button" className="controle" onClick={() => sincronizar(n.number)}>
                      {`#${n.number}${n.etapa ? ` · ${n.etapa}` : ""}`}
                    </button>
                  ))}
                </div>
              )}
              {syncMsg && (
                <p className="text-xs" style={{ color: corMsg[syncMsg.cor] }}>
                  {syncMsg.texto}
                </p>
              )}
            </div>
          )}
        </div>
      )}
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        {campo("nome", "Nome do aluno")}
        {campo("rgm", "RGM")}
        {campo("curso", "Curso")}
        {campo("polo", "Polo")}
        {campo("data", "Data matrícula", { placeholder: "dd/mm/aaaa", inputMode: "numeric" })}
        {campo("ciclo", "Ciclo", { placeholder: "Ex: 2026/2" })}
        {campo("nivel", "Nível")}
        {campo("kommo", "Nº do negócio")}
        <label className="flex flex-col gap-1 text-xs sm:col-span-2" style={{ color: "var(--text-secondary)" }}>
          <span>Observação</span>
          <textarea aria-label="Observação" rows={2} className="rounded-[10px] border px-3 py-2 text-sm bg-transparent" style={{ borderColor: "var(--border)", color: "var(--text-primary)" }} value={f.obs} onChange={(e) => mudar("obs", e.target.value)} />
        </label>
      </div>
      {erro && (
        <p role="alert" className="text-sm" style={{ color: "var(--danger)" }}>
          {erro}
        </p>
      )}
      <div className="flex justify-end gap-2">
        <button type="button" className="controle" onClick={onFechar}>
          Cancelar
        </button>
        <button type="button" className="controle controle-primario" disabled={salvar.isPending} onClick={enviar}>
          Salvar
        </button>
      </div>
    </Modal>
  );
}
