import type { ItemMenu } from "./menu";

/** Minúsculas e sem acento, para "inscricao" achar "Inscrições". */
export function normalizar(texto: string): string {
  return texto.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().trim();
}

/**
 * Telas cujo título tem o texto. Vazio lista tudo na ordem original; quem
 * começa com o texto vem antes de quem só contém (a ordem original se mantém).
 */
export function filtrarTelas<T extends ItemMenu>(itens: T[], texto: string): T[] {
  const q = normalizar(texto);
  if (!q) return itens;
  const comecam: T[] = [];
  const contem: T[] = [];
  for (const i of itens) {
    const t = normalizar(i.titulo);
    if (t.startsWith(q)) comecam.push(i);
    else if (t.includes(q)) contem.push(i);
  }
  return [...comecam, ...contem];
}
