import { describe, expect, it } from "vitest";
import { filtrarTelas } from "./busca";
import type { ItemMenu } from "./menu";

const it_ = (pagina: string, titulo: string): ItemMenu => ({ pagina, titulo, href: `/#${pagina}`, nova: false });
const itens = [
  it_("dashboard", "Dashboard"),
  it_("uso", "Uso do Dashboard"),
  it_("inscricao", "Inscrições"),
  it_("repasse", "Repasse"),
];

describe("filtrarTelas", () => {
  it("ignora acento", () => {
    expect(filtrarTelas(itens, "inscricoes").map((i) => i.pagina)).toEqual(["inscricao"]);
    expect(filtrarTelas(itens, "INSCRIÇÕ").map((i) => i.pagina)).toEqual(["inscricao"]);
  });

  it("começa com vem antes", () => {
    expect(filtrarTelas(itens, "dash").map((i) => i.pagina)).toEqual(["dashboard", "uso"]);
    // "uso" fica antes na lista original, mas só contém "dash": quem começa vem primeiro
    expect(filtrarTelas([itens[1], itens[0]], "dash").map((i) => i.pagina)).toEqual(["dashboard", "uso"]);
  });

  it("vazio lista tudo", () => {
    expect(filtrarTelas(itens, "")).toEqual(itens);
    expect(filtrarTelas(itens, "   ")).toEqual(itens);
  });

  it("não acha o que não existe", () => {
    expect(filtrarTelas(itens, "zzz")).toEqual([]);
  });
});
