import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App, FUTURO_ROUTER } from "../App";
import type { Me } from "../me";
import { registrarAcesso } from "../rastreio";
import type { Agentes, Detalhe, Filtros, Matriculados, Planilha } from "./repasse/tipos";
import { dinheiro, iniciais } from "./repasse/formato";

vi.mock("../rastreio", () => ({ registrarAcesso: vi.fn() }));

/** Saída crua do Intl: "R$" + espaço sem quebra (U+00A0). */
const R_INTL = "R$\u00a0";
/** Na tela, o Testing Library troca o espaço sem quebra por espaço comum. */
const R = "R$ ";

const FILTROS: Filtros = {
  ok: true,
  ciclos: ["2026.2", "2026.1"],
  tipos: ["Mensalidade"],
  turmas_por_ciclo: { "2026.2": ["T1", "T2"], "2026.1": ["T9"] },
};

const AGENTES: Agentes = {
  ok: true,
  is_admin: true,
  agentes: [
    { id: 111, nome: "Ana Souza", owner_id: null, qtd_alunos: 2, total_valor: 1000, media_por_aluno: 500 },
    { id: "bw:Bruno Lima", nome: "Bruno Lima", owner_id: "77", qtd_alunos: 1, total_valor: 500, media_por_aluno: 500 },
  ],
  totais: { valor: 1500, alunos: 3, agentes: 2 },
};

const DETALHE: Detalhe = {
  ok: true,
  alunos: [
    { rgm: "12345678", valor: 600, tipo_pagamento: "Mensalidade", turma: "T1", ciclo: "2026.2" },
    { rgm: "87654321", valor: 400, tipo_pagamento: "Mensalidade 2", turma: "T2", ciclo: "2026.2" },
  ],
  total: 1000,
};

const PLANILHA: Planilha = {
  ok: true,
  page: 1,
  pages: 3,
  total: 120,
  valor: 9999.5,
  rows: [{ rgm: "11112222", valor: 10, turma: "T1", beleza: "Mensalidade", ciclo: "2026.2" }],
};

const MATRICULADOS: Matriculados = {
  ok: true,
  page: 1,
  pages: 2,
  total: 60,
  pagos: 40,
  valor: 1234.56,
  filtros: {
    ciclos: ["2026.2", "2026.1"],
    niveis: ["GRADUAÇÃO", "PÓS"],
    meses: [{ id: "2026-10", label: "outubro/2026" }],
    ciclo: "2026.2",
  },
  rows: [
    {
      rgm: "99990000",
      polo: "Polo Centro",
      nivel: "GRADUAÇÃO",
      modalidade: "EAD",
      data_matricula: "01/10/2026",
      ciclo: "2026.2",
      turma: "T1",
      valor: 0,
      tipo_pagamento: "",
      pagou: false,
    },
  ],
};

function me(admin = true): Me {
  return {
    user_id: admin ? 1 : 5,
    username: admin ? "adm" : "ana.souza",
    role: admin ? "admin" : "viewer",
    categoria: null,
    display_name: admin ? "Adm" : "Ana Souza",
    nav: {
      is_admin: admin,
      is_comercial: !admin,
      is_suporte_comercial: false,
      pagina_inicial: "dashboard",
      paginas: ["dashboard", "repasse"],
      secoes: { comercial: true },
      grupos: { comercial: ["repasse"] },
    },
  };
}

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

interface Chamada {
  url: string;
  metodo: string;
  corpo?: string;
}

let chamadas: Chamada[];

type Rota = (url: string, init?: RequestInit) => Response;

function abrir(
  opcoes: {
    admin?: boolean;
    agentes?: Rota;
    detalhe?: Rota;
    planilha?: Rota;
    matriculados?: Rota;
    taxaPut?: Rota;
  } = {},
) {
  chamadas = [];
  const admin = opcoes.admin ?? true;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    const metodo = (init?.method || "GET").toUpperCase();
    chamadas.push({ url, metodo, corpo: init?.body as string | undefined });
    if (url === "/api/me") return json(me(admin));
    if (url === "/api/me/fixadas") return json({ fixadas: [], mais_usadas: [] });
    if (url === "/api/avisos/nao-lidos") return json({ count: 0 });
    if (url === "/api/repasse/filtros") return json(FILTROS);
    if (url === "/api/repasse/taxa" && metodo === "PUT") {
      return (opcoes.taxaPut ?? ((_u, i) => json({ ok: true, taxa: JSON.parse(String(i?.body)).taxa })))(url, init);
    }
    if (url === "/api/repasse/taxa") return json({ taxa: 20 });
    if (url.startsWith("/api/repasse/agentes"))
      return (opcoes.agentes ?? (() => json(admin ? AGENTES : { ...AGENTES, is_admin: false, agentes: [AGENTES.agentes[0]], totais: { valor: 1000, alunos: 2, agentes: 1 } })))(url, init);
    if (url.startsWith("/api/repasse/detalhe")) return (opcoes.detalhe ?? (() => json(DETALHE)))(url, init);
    if (url.startsWith("/api/repasse/planilha")) return (opcoes.planilha ?? (() => json(PLANILHA)))(url, init);
    if (url.startsWith("/api/repasse/matriculados"))
      return (opcoes.matriculados ?? (() => json(MATRICULADOS)))(url, init);
    return json({}, 404);
  });
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(
    <QueryClientProvider client={cliente}>
      <MemoryRouter initialEntries={["/repasse"]} future={FUTURO_ROUTER}>
        <App />
      </MemoryRouter>
    </QueryClientProvider>,
  );
}

function urls(prefixo: string) {
  return chamadas.filter((c) => c.metodo === "GET" && c.url.startsWith(prefixo)).map((c) => c.url);
}
const ultima = (prefixo: string) => urls(prefixo).slice(-1)[0];

const kpi = (nome: string) => within(screen.getByRole("group", { name: nome }));
const regiao = (nome: string) => within(screen.getByRole("region", { name: nome }));

async function esperarAgentes() {
  await screen.findByRole("button", { name: /Ana Souza/ });
}

beforeEach(() => {
  vi.mocked(registrarAcesso).mockClear();
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

describe("formato", () => {
  it("dinheiro em reais com 2 casas", () => {
    expect(dinheiro(1234.5)).toBe(`${R_INTL}1.234,50`);
    expect(dinheiro(0)).toBe(`${R_INTL}0,00`);
    expect(dinheiro(undefined)).toBe(`${R_INTL}0,00`);
  });

  it("iniciais como na tela antiga", () => {
    expect(iniciais("Ana Souza Lima")).toBe("AS");
    expect(iniciais("bruno")).toBe("BR");
    expect(iniciais("")).toBe("?");
  });
});

describe("Repasse (admin)", () => {
  it("carrega filtros, KPIs e agentes com Mensalidade por padrão", async () => {
    abrir();
    await esperarAgentes();
    expect(urls("/api/repasse/agentes")[0]).toBe("/api/repasse/agentes?tipo=Mensalidade");
    expect((screen.getByLabelText("Tipo de pagamento") as HTMLSelectElement).value).toBe("Mensalidade");
    expect(kpi("Recebido").getByText(`${R}1.500,00`)).toBeTruthy();
    expect(kpi("Total repasse").getByText(`${R}300,00`)).toBeTruthy();
    expect(kpi("Com pagamento").getByText("3")).toBeTruthy();
    expect(kpi("Consultores").getByText("2")).toBeTruthy();
    // cartões: iniciais, alunos e repasse (taxa 20%)
    const ana = within(screen.getByRole("button", { name: /Ana Souza/ }));
    expect(ana.getByText("AS")).toBeTruthy();
    expect(ana.getByText("2 alunos")).toBeTruthy();
    expect(ana.getByText(`${R}1.000,00`)).toBeTruthy();
    expect(ana.getByText(`${R}200,00`)).toBeTruthy();
    expect(within(screen.getByRole("button", { name: /Bruno Lima/ })).getByText(`${R}100,00`)).toBeTruthy();
    expect(registrarAcesso).toHaveBeenCalledWith("repasse");
  });

  it("não repete o título da tela", async () => {
    abrir();
    await esperarAgentes();
    expect(screen.queryByText("Repasse de Recebimentos")).toBeNull();
  });

  it("selecionar agente abre o detalhe com as linhas", async () => {
    abrir();
    await esperarAgentes();
    fireEvent.click(screen.getByRole("button", { name: /Ana Souza/ }));
    const det = within(await screen.findByRole("region", { name: "Detalhe de Ana Souza" }));
    expect(await det.findByText("12345678")).toBeTruthy();
    expect(ultima("/api/repasse/detalhe")).toBe("/api/repasse/detalhe?kommo_uid=111&tipo=Mensalidade");
    expect(det.getByText("87654321")).toBeTruthy();
    expect(det.getByText(`${R}600,00`)).toBeTruthy(); // recebido (admin)
    expect(det.getByText(`${R}120,00`)).toBeTruthy(); // repasse da linha
    expect(det.getByText(`${R}200,00`)).toBeTruthy(); // repasse total
    expect(det.getAllByText("Repasse (20%)").length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: /Ana Souza/ }).getAttribute("aria-pressed")).toBe("true");
    // busca no detalhe filtra as linhas
    fireEvent.change(det.getByLabelText("Buscar no detalhe"), { target: { value: "8765" } });
    expect(det.queryByText("12345678")).toBeNull();
    expect(det.getByText("87654321")).toBeTruthy();
    // fechar
    fireEvent.click(det.getByRole("button", { name: "Fechar detalhe" }));
    expect(screen.queryByRole("region", { name: "Detalhe de Ana Souza" })).toBeNull();
  });

  it("agente do Bwipo manda o id como veio", async () => {
    abrir();
    await esperarAgentes();
    fireEvent.click(screen.getByRole("button", { name: /Bruno Lima/ }));
    await screen.findByRole("region", { name: "Detalhe de Bruno Lima" });
    expect(ultima("/api/repasse/detalhe")).toBe("/api/repasse/detalhe?kommo_uid=bw%3ABruno%20Lima&tipo=Mensalidade");
  });

  it("busca de agente filtra a lista no cliente", async () => {
    abrir();
    await esperarAgentes();
    const antes = urls("/api/repasse/agentes").length;
    fireEvent.change(screen.getByLabelText("Buscar consultor"), { target: { value: "bru" } });
    expect(screen.queryByRole("button", { name: /Ana Souza/ })).toBeNull();
    expect(screen.getByRole("button", { name: /Bruno Lima/ })).toBeTruthy();
    expect(urls("/api/repasse/agentes").length).toBe(antes);
  });

  it("trocar ciclo atualiza turmas; a busca só vai no Buscar", async () => {
    abrir();
    await esperarAgentes();
    const antes = urls("/api/repasse/agentes").length;
    const turma = screen.getByLabelText("Turma") as HTMLSelectElement;
    expect([...turma.options].map((o) => o.value)).toEqual(["", "T1", "T2", "T9"]);
    fireEvent.change(screen.getByLabelText("Ciclo"), { target: { value: "2026.1" } });
    expect([...turma.options].map((o) => o.value)).toEqual(["", "T9"]);
    fireEvent.change(turma, { target: { value: "T9" } });
    // mexer nos selects sozinho não chama a rota pesada de agentes nem a planilha
    await new Promise((r) => setTimeout(r, 50));
    expect(urls("/api/repasse/agentes").length).toBe(antes);
    expect(urls("/api/repasse/planilha").some((u) => u.includes("ciclo="))).toBe(false);
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    await waitFor(() =>
      expect(ultima("/api/repasse/agentes")).toBe("/api/repasse/agentes?ciclo=2026.1&tipo=Mensalidade&turma=T9"),
    );
    expect(urls("/api/repasse/agentes").length).toBe(antes + 1);
    // a planilha segue os mesmos filtros aplicados
    await waitFor(() =>
      expect(ultima("/api/repasse/planilha")).toBe("/api/repasse/planilha?page=1&ciclo=2026.1&tipo=Mensalidade&turma=T9"),
    );
  });

  it("Buscar com o mesmo filtro busca de novo agentes, planilha e matriculados", async () => {
    abrir();
    await esperarAgentes();
    await regiao("Matriculados").findByText("99990000");
    const n = (p: string) => urls(p).length;
    const antes = { ag: n("/api/repasse/agentes"), pl: n("/api/repasse/planilha"), mt: n("/api/repasse/matriculados") };
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    await waitFor(() => expect(n("/api/repasse/agentes")).toBe(antes.ag + 1));
    await waitFor(() => expect(n("/api/repasse/planilha")).toBe(antes.pl + 1));
    await waitFor(() => expect(n("/api/repasse/matriculados")).toBe(antes.mt + 1));
    expect(ultima("/api/repasse/agentes")).toBe("/api/repasse/agentes?tipo=Mensalidade");
  });

  it("Buscar fecha o detalhe aberto; mexer só no select não fecha", async () => {
    abrir();
    await esperarAgentes();
    fireEvent.click(screen.getByRole("button", { name: /Ana Souza/ }));
    await screen.findByRole("region", { name: "Detalhe de Ana Souza" });
    fireEvent.change(screen.getByLabelText("Ciclo"), { target: { value: "2026.2" } });
    expect(screen.getByRole("region", { name: "Detalhe de Ana Souza" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    expect(screen.queryByRole("region", { name: "Detalhe de Ana Souza" })).toBeNull();
  });

  it("Atualizar busca agentes, planilha e matriculados de novo, sem filtros nem taxa", async () => {
    abrir();
    await esperarAgentes();
    await regiao("Matriculados").findByText("99990000");
    const n = (p: string) => urls(p).length;
    const antes = {
      ag: n("/api/repasse/agentes"),
      pl: n("/api/repasse/planilha"),
      mt: n("/api/repasse/matriculados"),
      fi: n("/api/repasse/filtros"),
      tx: n("/api/repasse/taxa"),
    };
    // rascunho não aplicado não entra no Atualizar
    fireEvent.change(screen.getByLabelText("Ciclo"), { target: { value: "2026.1" } });
    fireEvent.click(screen.getByRole("button", { name: "Atualizar" }));
    await waitFor(() => expect(n("/api/repasse/agentes")).toBe(antes.ag + 1));
    await waitFor(() => expect(n("/api/repasse/planilha")).toBe(antes.pl + 1));
    await waitFor(() => expect(n("/api/repasse/matriculados")).toBe(antes.mt + 1));
    expect(ultima("/api/repasse/agentes")).toBe("/api/repasse/agentes?tipo=Mensalidade");
    expect(n("/api/repasse/filtros")).toBe(antes.fi);
    expect(n("/api/repasse/taxa")).toBe(antes.tx);
  });

  it("Limpar volta os filtros ao padrão, aplica e zera planilha e matriculados", async () => {
    abrir();
    await esperarAgentes();
    const pl = regiao("Planilha de recebimentos");
    const mt = regiao("Matriculados");
    await mt.findByText("99990000");
    // filtros de cima aplicados
    fireEvent.change(screen.getByLabelText("Ciclo"), { target: { value: "2026.1" } });
    fireEvent.change(screen.getByLabelText("Tipo de pagamento"), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    await waitFor(() => expect(ultima("/api/repasse/agentes")).toBe("/api/repasse/agentes?ciclo=2026.1"));
    fireEvent.change(screen.getByLabelText("Buscar consultor"), { target: { value: "bru" } });
    // painéis com busca e filtros
    fireEvent.change(pl.getByLabelText("Buscar RGM na planilha"), { target: { value: "1111" } });
    fireEvent.change(mt.getByLabelText("Buscar RGM nos matriculados"), { target: { value: "9999" } });
    fireEvent.change(mt.getByLabelText("Pagamento"), { target: { value: "sim" } });
    fireEvent.change(mt.getByLabelText("Mês de ingresso"), { target: { value: "2026-10" } });
    fireEvent.change(mt.getByLabelText("Dia"), { target: { value: "2026-10-01" } });
    fireEvent.change(mt.getByLabelText("Nível"), { target: { value: "PÓS" } });
    fireEvent.change(mt.getByLabelText("Tipo de matrícula"), { target: { value: "RETORNO" } });
    await waitFor(() => expect(ultima("/api/repasse/matriculados")).toContain("q=9999"));

    fireEvent.click(screen.getByRole("button", { name: "Limpar" }));
    expect((screen.getByLabelText("Ciclo") as HTMLSelectElement).value).toBe("");
    expect((screen.getByLabelText("Tipo de pagamento") as HTMLSelectElement).value).toBe("Mensalidade");
    expect((screen.getByLabelText("Buscar consultor") as HTMLInputElement).value).toBe("");
    const pl2 = regiao("Planilha de recebimentos");
    const mt2 = regiao("Matriculados");
    expect((pl2.getByLabelText("Buscar RGM na planilha") as HTMLInputElement).value).toBe("");
    expect((mt2.getByLabelText("Buscar RGM nos matriculados") as HTMLInputElement).value).toBe("");
    expect((mt2.getByLabelText("Pagamento") as HTMLSelectElement).value).toBe("");
    expect((mt2.getByLabelText("Mês de ingresso") as HTMLSelectElement).value).toBe("");
    expect((mt2.getByLabelText("Dia") as HTMLInputElement).value).toBe("");
    expect((mt2.getByLabelText("Nível") as HTMLSelectElement).value).toBe("");
    expect((mt2.getByLabelText("Tipo de matrícula") as HTMLSelectElement).value).toBe("NOVA MATRICULA");
    // padrão aplicado de novo
    await waitFor(() => expect(ultima("/api/repasse/agentes")).toBe("/api/repasse/agentes?tipo=Mensalidade"));
    await waitFor(() => expect(ultima("/api/repasse/planilha")).toBe("/api/repasse/planilha?page=1&tipo=Mensalidade"));
    await waitFor(() =>
      expect(ultima("/api/repasse/matriculados")).toBe("/api/repasse/matriculados?page=1&tipo_mat=NOVA%20MATRICULA"),
    );
  });

  it("mês e nível que somem das opções do servidor saem do filtro", async () => {
    const semOpcoes = { ...MATRICULADOS, filtros: { ...MATRICULADOS.filtros, meses: [], niveis: [] } };
    abrir({ matriculados: (url) => json(url.includes("tipo_mat=RECOMPRA") ? semOpcoes : MATRICULADOS) });
    await esperarAgentes();
    const mt = regiao("Matriculados");
    await mt.findByText("99990000");
    fireEvent.change(mt.getByLabelText("Mês de ingresso"), { target: { value: "2026-10" } });
    fireEvent.change(mt.getByLabelText("Nível"), { target: { value: "PÓS" } });
    await waitFor(() => expect(ultima("/api/repasse/matriculados")).toContain("nivel=P%C3%93S"));
    fireEvent.change(mt.getByLabelText("Tipo de matrícula"), { target: { value: "RECOMPRA" } });
    // a resposta sem as opções derruba mês e nível: o próximo pedido já vai sem eles
    await waitFor(() =>
      expect(ultima("/api/repasse/matriculados")).toBe("/api/repasse/matriculados?page=1&tipo_mat=RECOMPRA"),
    );
    expect((mt.getByLabelText("Mês de ingresso") as HTMLSelectElement).value).toBe("");
    expect((mt.getByLabelText("Nível") as HTMLSelectElement).value).toBe("");
  });

  it("taxa: admin edita, a tela recalcula na hora e salva com PUT {taxa}", async () => {
    abrir();
    await esperarAgentes();
    const taxa = screen.getByLabelText("Taxa de repasse (%)") as HTMLInputElement;
    expect(taxa.value).toBe("20");
    expect(taxa.readOnly).toBe(false);
    fireEvent.change(taxa, { target: { value: "25" } });
    expect(kpi("Total repasse").getByText(`${R}375,00`)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Salvar taxa" }));
    await screen.findByText("Taxa salva.");
    const put = chamadas.filter((c) => c.metodo === "PUT");
    expect(put).toHaveLength(1);
    expect(put[0].url).toBe("/api/repasse/taxa");
    expect(JSON.parse(String(put[0].corpo))).toEqual({ taxa: 25 });
  });

  it("taxa: Enter salva", async () => {
    abrir();
    await esperarAgentes();
    const taxa = screen.getByLabelText("Taxa de repasse (%)");
    fireEvent.change(taxa, { target: { value: "12.5" } });
    fireEvent.keyDown(taxa, { key: "Enter" });
    await screen.findByText("Taxa salva.");
    expect(JSON.parse(String(chamadas.find((c) => c.metodo === "PUT")?.corpo))).toEqual({ taxa: 12.5 });
  });

  it("taxa inválida mostra erro na tela e não faz PUT", async () => {
    abrir();
    await esperarAgentes();
    const taxa = screen.getByLabelText("Taxa de repasse (%)");
    for (const v of ["150", "-1", ""]) {
      fireEvent.change(taxa, { target: { value: v } });
      fireEvent.click(screen.getByRole("button", { name: "Salvar taxa" }));
      expect(screen.getByText("Taxa inválida. Use um valor entre 0 e 100.")).toBeTruthy();
    }
    expect(chamadas.filter((c) => c.metodo === "PUT")).toHaveLength(0);
    // os valores continuam com a última taxa válida
    expect(kpi("Total repasse").getByText(`${R}300,00`)).toBeTruthy();
  });

  it("erro do servidor ao salvar a taxa aparece", async () => {
    abrir({ taxaPut: () => json({ error: "Taxa inválida" }, 400) });
    await esperarAgentes();
    fireEvent.change(screen.getByLabelText("Taxa de repasse (%)"), { target: { value: "30" } });
    fireEvent.click(screen.getByRole("button", { name: "Salvar taxa" }));
    expect((await screen.findByRole("alert")).textContent).toContain("Taxa inválida");
  });

  it("planilha pagina e busca com os filtros", async () => {
    abrir();
    await esperarAgentes();
    const pl = regiao("Planilha de recebimentos");
    expect(await pl.findByText("11112222")).toBeTruthy();
    expect(urls("/api/repasse/planilha")[0]).toBe("/api/repasse/planilha?page=1&tipo=Mensalidade");
    expect(pl.getByText(`120 linhas · ${R}9.999,50 pagos`)).toBeTruthy();
    expect(pl.getByText("Página 1 de 3")).toBeTruthy();
    expect((pl.getByRole("button", { name: "Página anterior" }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.click(pl.getByRole("button", { name: "Próxima página" }));
    await waitFor(() => expect(ultima("/api/repasse/planilha")).toBe("/api/repasse/planilha?page=2&tipo=Mensalidade"));
    fireEvent.change(pl.getByLabelText("Buscar RGM na planilha"), { target: { value: "1111" } });
    await waitFor(() =>
      expect(ultima("/api/repasse/planilha")).toBe("/api/repasse/planilha?page=1&tipo=Mensalidade&q=1111"),
    );
    expect(pl.getByRole("link", { name: /Baixar CSV/ }).getAttribute("href")).toBe(
      "/api/repasse/planilha.csv?tipo=Mensalidade&q=1111",
    );
  });

  it("busca da planilha espera parar de digitar (debounce)", async () => {
    abrir();
    await esperarAgentes();
    const pl = regiao("Planilha de recebimentos");
    await pl.findByText("11112222");
    const campo = pl.getByLabelText("Buscar RGM na planilha");
    fireEvent.change(campo, { target: { value: "1" } });
    fireEvent.change(campo, { target: { value: "12" } });
    fireEvent.change(campo, { target: { value: "123" } });
    await waitFor(() => expect(ultima("/api/repasse/planilha")).toContain("q=123"));
    expect(urls("/api/repasse/planilha").some((u) => /q=1(2)?$/.test(u))).toBe(false);
  });

  it("matriculados paginam, filtram e buscam", async () => {
    abrir();
    await esperarAgentes();
    const mt = regiao("Matriculados");
    expect(await mt.findByText("99990000")).toBeTruthy();
    expect(urls("/api/repasse/matriculados")[0]).toBe("/api/repasse/matriculados?page=1&tipo_mat=NOVA%20MATRICULA");
    expect(mt.getByText(`60 ingressantes · 40 pagaram · ${R}1.234,56`)).toBeTruthy();
    expect(mt.getByText("Polo Centro")).toBeTruthy();
    expect(mt.getByText("Não")).toBeTruthy();
    expect(mt.getByText("Página 1 de 2")).toBeTruthy();
    // opções vindas da resposta; ciclo escolhido pelo servidor
    expect((mt.getByLabelText("Ciclo dos matriculados") as HTMLSelectElement).value).toBe("2026.2");
    expect(mt.getByRole("option", { name: "outubro/2026" })).toBeTruthy();
    expect(mt.getByRole("option", { name: "PÓS" })).toBeTruthy();

    fireEvent.click(mt.getByRole("button", { name: "Próxima página" }));
    await waitFor(() =>
      expect(ultima("/api/repasse/matriculados")).toBe("/api/repasse/matriculados?page=2&tipo_mat=NOVA%20MATRICULA"),
    );
    fireEvent.change(mt.getByLabelText("Pagamento"), { target: { value: "sim" } });
    await waitFor(() =>
      expect(ultima("/api/repasse/matriculados")).toBe(
        "/api/repasse/matriculados?page=1&pago=sim&tipo_mat=NOVA%20MATRICULA",
      ),
    );
    fireEvent.change(mt.getByLabelText("Mês de ingresso"), { target: { value: "2026-10" } });
    fireEvent.change(mt.getByLabelText("Dia"), { target: { value: "2026-10-01" } });
    fireEvent.change(mt.getByLabelText("Ciclo dos matriculados"), { target: { value: "2026.1" } });
    fireEvent.change(mt.getByLabelText("Nível"), { target: { value: "PÓS" } });
    fireEvent.change(mt.getByLabelText("Tipo de matrícula"), { target: { value: "todos" } });
    fireEvent.change(mt.getByLabelText("Buscar RGM nos matriculados"), { target: { value: "9999" } });
    const esperado =
      "q=9999&pago=sim&mes=2026-10&dia=2026-10-01&ciclo_mat=2026.1&nivel=P%C3%93S&tipo_mat=todos";
    await waitFor(() => expect(ultima("/api/repasse/matriculados")).toBe(`/api/repasse/matriculados?page=1&${esperado}`));
    expect(mt.getByRole("link", { name: /Baixar CSV/ }).getAttribute("href")).toBe(
      `/api/repasse/matriculados.csv?${esperado}`,
    );
  });

  it("matriculados não dependem dos filtros de cima", async () => {
    abrir();
    await esperarAgentes();
    await regiao("Matriculados").findByText("99990000");
    fireEvent.change(screen.getByLabelText("Ciclo"), { target: { value: "2026.1" } });
    fireEvent.click(screen.getByRole("button", { name: "Buscar" }));
    await waitFor(() => expect(ultima("/api/repasse/agentes")).toContain("ciclo=2026.1"));
    // o Buscar recarrega os matriculados (repLoad), mas com os filtros deles
    await waitFor(() => expect(urls("/api/repasse/matriculados").length).toBeGreaterThan(1));
    expect(urls("/api/repasse/matriculados").every((u) => !u.includes("ciclo=2026.1"))).toBe(true);
  });

  it("erro do servidor nos agentes aparece na tela", async () => {
    abrir({ agentes: () => json({ ok: false, error: "banco fora" }, 500) });
    expect((await screen.findByRole("alert")).textContent).toContain("banco fora");
  });

  it("403 nos agentes mostra sem permissão", async () => {
    abrir({ agentes: () => json({ error: "Sem permissão" }, 403) });
    expect((await screen.findByRole("alert")).textContent).toContain("Você não tem permissão para esta tela.");
  });

  it("erro da planilha aparece dentro dela", async () => {
    abrir({ planilha: () => json({ ok: false, error: "planilha quebrou" }, 500) });
    await esperarAgentes();
    expect(await regiao("Planilha de recebimentos").findByText(/planilha quebrou/)).toBeTruthy();
  });

  it("filtro sem agentes mostra vazio", async () => {
    abrir({ agentes: () => json({ ok: true, agentes: [], totais: { valor: 0, alunos: 0 } }) });
    expect(await screen.findByText("Nenhum consultor encontrado neste filtro.")).toBeTruthy();
  });

  it("detalhe vazio e erro no detalhe", async () => {
    abrir({ detalhe: () => json({ ok: false, error: "detalhe falhou" }, 500) });
    await esperarAgentes();
    fireEvent.click(screen.getByRole("button", { name: /Ana Souza/ }));
    const det = within(await screen.findByRole("region", { name: "Detalhe de Ana Souza" }));
    expect(await det.findByText(/detalhe falhou/)).toBeTruthy();
  });
});

describe("Repasse (consultor)", () => {
  it("abre direto no próprio detalhe, sem planilha nem matriculados, taxa só leitura", async () => {
    abrir({ admin: false });
    const det = within(await screen.findByRole("region", { name: "Detalhe de Ana Souza" }));
    expect(await det.findByText("12345678")).toBeTruthy();
    expect(ultima("/api/repasse/detalhe")).toBe("/api/repasse/detalhe?kommo_uid=111&tipo=Mensalidade");
    // banner com o nome
    expect(screen.getByText("Seus dados:")).toBeTruthy();
    // sem valor recebido para o consultor
    expect(det.queryByText(`${R}600,00`)).toBeNull();
    expect(det.getByText(`${R}120,00`)).toBeTruthy();
    expect(screen.queryByRole("group", { name: "Recebido" })).toBeNull();
    expect(screen.queryByRole("group", { name: "Consultores" })).toBeNull();
    expect(kpi("Total repasse").getByText(`${R}200,00`)).toBeTruthy();
    // sem busca de agente, planilha, matriculados, CSV
    expect(screen.queryByLabelText("Buscar consultor")).toBeNull();
    expect(screen.queryByRole("region", { name: "Planilha de recebimentos" })).toBeNull();
    expect(screen.queryByRole("region", { name: "Matriculados" })).toBeNull();
    expect(screen.queryAllByRole("link", { name: /Baixar CSV/ })).toHaveLength(0);
    expect(urls("/api/repasse/planilha")).toEqual([]);
    expect(urls("/api/repasse/matriculados")).toEqual([]);
    // taxa só leitura
    const taxa = screen.getByLabelText("Taxa de repasse (%)") as HTMLInputElement;
    expect(taxa.readOnly).toBe(true);
    expect(screen.queryByRole("button", { name: "Salvar taxa" })).toBeNull();
    fireEvent.keyDown(taxa, { key: "Enter" });
    expect(chamadas.filter((c) => c.metodo === "PUT")).toHaveLength(0);
  });
});
