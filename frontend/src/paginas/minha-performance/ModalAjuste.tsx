import { useMutation } from "@tanstack/react-query";
import { useState } from "react";
import { api } from "../../api";
import { textoErro } from "./formato";
import { Modal } from "./Modal";

export const TIPO_AJUSTE: Record<string, string> = {
  matricula_nao_computada: "Matrícula não computada",
  dados_incorretos: "Dados incorretos",
  evasao_indevida: "Evasão indevida",
};

/** Modal "Nova solicitação de ajuste" (_mpOpenAjusteModal / _mpSaveAjuste). */
export function ModalAjuste({ onFechar, onEnviado }: { onFechar: () => void; onEnviado: () => void }) {
  const [f, setF] = useState({ tipo: "matricula_nao_computada", nome: "", rgm: "", curso: "", polo: "", data: "", kommo: "", desc: "" });
  const [erro, setErro] = useState("");
  const mudar = (c: keyof typeof f, v: string) => setF((x) => ({ ...x, [c]: v }));

  const enviar = useMutation({
    mutationFn: (corpo: Record<string, string | null>) =>
      api<{ ok?: boolean; error?: string }>("/api/minha-performance/ajustes", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(corpo),
      }),
    onSuccess: (d) => {
      if (d && d.ok === false) {
        setErro(`Erro ao enviar: ${d.error || "Erro ao enviar"}`);
        return;
      }
      onEnviado();
    },
    onError: (e) => setErro(`Erro ao enviar: ${textoErro(e)}`),
  });

  function salvar() {
    const kommo = f.kommo.trim();
    const desc = f.desc.trim();
    if (!kommo) return setErro("Lead Kommo ID é obrigatório");
    if (!desc) return setErro("Justificativa é obrigatória");
    setErro("");
    enviar.mutate({
      tipo: f.tipo,
      nome_aluno: f.nome,
      rgm: f.rgm,
      curso: f.curso,
      polo: f.polo,
      data_matricula: f.data || null,
      kommo_lead_id: kommo,
      descricao: desc,
    });
  }

  const estiloRotulo = { color: "var(--text-secondary)" };
  const campo = (c: "nome" | "rgm" | "curso" | "polo" | "kommo", rotulo: string, obrigatorio = false) => (
    <label className="flex flex-col gap-1 text-xs" style={estiloRotulo}>
      <span>
        {rotulo}
        {obrigatorio && <span className="text-red-600"> *</span>}
      </span>
      <input aria-label={rotulo} className="controle font-normal w-full" value={f[c]} onChange={(e) => mudar(c, e.target.value)} />
    </label>
  );

  return (
    <Modal titulo="Nova solicitação de ajuste" onFechar={onFechar}>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <label className="flex flex-col gap-1 text-xs sm:col-span-2" style={estiloRotulo}>
          <span>Tipo</span>
          <select aria-label="Tipo" className="controle font-normal" value={f.tipo} onChange={(e) => mudar("tipo", e.target.value)}>
            {Object.entries(TIPO_AJUSTE).map(([k, r]) => (
              <option key={k} value={k}>
                {r}
              </option>
            ))}
          </select>
        </label>
        {campo("nome", "Nome do aluno")}
        {campo("rgm", "RGM")}
        {campo("curso", "Curso")}
        {campo("polo", "Polo")}
        <label className="flex flex-col gap-1 text-xs" style={estiloRotulo}>
          <span>Data matrícula</span>
          <input type="date" aria-label="Data matrícula" className="controle font-normal w-full" value={f.data} onChange={(e) => mudar("data", e.target.value)} />
        </label>
        {campo("kommo", "Lead Kommo ID", true)}
        <label className="flex flex-col gap-1 text-xs sm:col-span-2" style={estiloRotulo}>
          <span>
            Justificativa<span className="text-red-600"> *</span>
          </span>
          <textarea
            aria-label="Justificativa"
            rows={3}
            placeholder="Descreva o motivo do ajuste..."
            className="rounded-[10px] border px-3 py-2 text-sm bg-transparent"
            style={{ borderColor: "var(--border)", color: "var(--text-primary)" }}
            value={f.desc}
            onChange={(e) => mudar("desc", e.target.value)}
          />
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
        <button type="button" className="controle controle-primario" disabled={enviar.isPending} onClick={salvar}>
          Enviar solicitação
        </button>
      </div>
    </Modal>
  );
}
