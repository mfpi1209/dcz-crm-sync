/** Datas no formato AAAA-MM-DD, no fuso do navegador (igual ao painel atual). */

function iso(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

export function hojeIso(agora: Date = new Date()): string {
  return iso(agora);
}

export function somarDias(dataIso: string, dias: number): string {
  const [a, m, d] = dataIso.split("-").map(Number);
  const dt = new Date(a, m - 1, d);
  dt.setDate(dt.getDate() + dias);
  return iso(dt);
}

export function numero(n: unknown): string {
  return Number(n || 0).toLocaleString("pt-BR");
}
