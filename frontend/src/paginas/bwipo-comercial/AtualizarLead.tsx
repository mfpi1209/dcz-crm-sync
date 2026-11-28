import { useState } from "react";
import { digitos, enviar, textoErro } from "./formato";
import type { MiniSync, NegocioPick } from "./tipos";

export const ERRO_LEAD = "Informe o número do card ou um RGM de 8 dígitos.";

/**
 * "Atualizar lead" (bwcAtualizarLead): busca o card na API do Bwipo e grava no espelho
 * (POST /api/bwipo/painel/mini-sync). Número do card tem prioridade; senão RGM de 8 dígitos.
 * Como na antiga, não há confirmação; se a venda foi fixada, o painel é atualizado.
 */
export function AtualizarLead({
  onFechar,
  onAviso,
  onFixou,
}: {
  onFechar: () => void;
  onAviso: (tipo: "erro" | "sucesso", texto: string, ms?: number) => void;
  onFixou: () => void;
}) {
  const [numero, setNumero] = useState("");
  const [rgm, setRgm] = useState("");
  const [ocupado, setOcupado] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [escolha, setEscolha] = useState<NegocioPick[]>([]);

  async function atualizar(dealId?: string) {
    const n = digitos(numero);
    const r = digitos(rgm);
    let corpo: Record<string, string>;
    if (dealId) corpo = { deal_id: dealId };
    else if (n) corpo = { number: n };
    else if (r.length === 8) corpo = { rgm: r };
    else {
      onAviso("erro", ERRO_LEAD);
      return;
    }
    setOcupado(true);
    setMsg("Buscando no Bwipo…");
    if (!dealId) setEscolha([]);
    try {
      const d = await enviar<MiniSync>("/api/bwipo/painel/mini-sync", "POST", corpo);
      setMsg(d.msg || "Atualizado.");
      if (d.pick && d.pick.length) setEscolha(d.pick);
      if (d.pinned) onFixou();
      onAviso("sucesso", d.msg || "Atualizado.");
    } catch (e) {
      setMsg(textoErro(e));
      onAviso("erro", textoErro(e), 7000);
    } finally {
      setOcupado(false);
    }
  }

  return (
    <section className="cartao p-5 flex flex-col gap-3" aria-label="Atualizar negócio na base Bwipo">
      <div className="flex items-center justify-between gap-3">
        <h3 className="text-sm">Atualizar negócio na base Bwipo</h3>
        <button type="button" className="botao-icone" aria-label="Fechar atualizar lead" onClick={onFechar}>
          <span className="material-symbols-outlined" aria-hidden>close</span>
        </button>
      </div>
      <p className="text-xs" style={{ color: "var(--text-secondary)" }}>
        Busca o card na API do Bwipo e grava no espelho. Informe o <strong>número do card</strong> (o # da URL) ou o{" "}
        <strong>RGM</strong> de 8 dígitos. Se o negócio estiver em Ganho com responsável ligado ao consultor, a venda fica fixada nele.
      </p>
      <div className="flex flex-wrap items-end gap-3">
        <label className="flex flex-col gap-1">
          <span className="rotulo">Número do card (Bwipo)</span>
          <input
            type="text"
            inputMode="numeric"
            className="controle font-normal w-40"
            placeholder="Ex: 4997"
            value={numero}
            onChange={(e) => setNumero(e.target.value)}
            autoFocus
          />
        </label>
        <span className="text-xs pb-2" style={{ color: "var(--text-muted)" }}>ou</span>
        <label className="flex flex-col gap-1">
          <span className="rotulo">RGM (8 dígitos)</span>
          <input
            type="text"
            inputMode="numeric"
            maxLength={12}
            className="controle font-normal w-40"
            placeholder="Ex: 48411612"
            value={rgm}
            onChange={(e) => setRgm(e.target.value)}
          />
        </label>
        <button type="button" className="controle controle-primario" disabled={ocupado} onClick={() => atualizar()}>
          Buscar e atualizar
        </button>
      </div>
      {msg && (
        <div className="text-xs p-3 rounded-lg" style={{ border: "1px solid var(--border)", background: "var(--bg-elevated)" }}>
          {msg}
        </div>
      )}
      {escolha.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {escolha.map((c) => (
            <button key={c.id} type="button" className="controle font-normal" disabled={ocupado} onClick={() => atualizar(c.id)}>
              {`#${c.number} ${c.title} · ${c.etapa || ""}`}
            </button>
          ))}
        </div>
      )}
    </section>
  );
}
