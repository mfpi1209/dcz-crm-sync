import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ERRO_FIXADAS, useFixadas } from "./fixadas";

type Dados = { fixadas: string[]; mais_usadas: string[] };

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

/**
 * Fetch falso que roteia por método; devolve a lista de PUTs feitos.
 * Como o servidor de verdade, guarda a lista quando o PUT dá certo (o hook relê depois).
 */
function falso(inicial: Dados, put: (corpo: { paginas: string[] }) => Response | Promise<Response>) {
  const puts: { paginas: string[] }[] = [];
  let salvo = inicial;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    if (url === "/api/me/fixadas" && init?.method === "PUT") {
      const corpo = JSON.parse(String(init.body));
      puts.push(corpo);
      const resposta = await put(corpo);
      if (resposta.ok) salvo = { ...salvo, fixadas: corpo.paginas };
      return resposta;
    }
    if (url === "/api/me/fixadas") return json(salvo);
    return json({}, 404);
  });
  return puts;
}

function montar() {
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={cliente}>{children}</QueryClientProvider>
  );
  return { ...renderHook(() => useFixadas(), { wrapper }), cliente };
}

/** Promessa que o teste resolve quando quiser (simula o servidor demorando). */
function adiada<T>() {
  let resolver!: (v: T) => void;
  const promessa = new Promise<T>((r) => (resolver = r));
  return { promessa, resolver };
}

describe("useFixadas", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("alternar adiciona no fim e envia PUT", async () => {
    const puts = falso({ fixadas: ["a"], mais_usadas: ["x"] }, (c) => json({ fixadas: c.paginas }));
    const { result } = montar();
    await waitFor(() => expect(result.current.dados).not.toBeNull());
    act(() => result.current.alternar("b"));
    await waitFor(() => expect(result.current.fixada("b")).toBe(true)); // otimista
    await waitFor(() => expect(puts).toEqual([{ paginas: ["a", "b"] }]));
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(["a", "b"]));
    expect(result.current.dados?.mais_usadas).toEqual(["x"]);
  });

  it("alternar remove", async () => {
    const puts = falso({ fixadas: ["a", "b"], mais_usadas: [] }, (c) => json({ fixadas: c.paginas }));
    const { result } = montar();
    await waitFor(() => expect(result.current.fixada("a")).toBe(true));
    act(() => result.current.alternar("a"));
    await waitFor(() => expect(result.current.fixada("a")).toBe(false));
    await waitFor(() => expect(puts).toEqual([{ paginas: ["b"] }]));
  });

  it("erro no PUT volta a lista", async () => {
    const puts = falso({ fixadas: ["a"], mais_usadas: [] }, () => json({ error: "falhou" }, 400));
    const { result } = montar();
    await waitFor(() => expect(result.current.dados).not.toBeNull());
    act(() => result.current.alternar("b"));
    await waitFor(() => expect(puts.length).toBe(1));
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(["a"]));
    expect(result.current.fixada("b")).toBe(false);
  });

  it("limite de 8 não envia", async () => {
    const oito = ["p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8"];
    const puts = falso({ fixadas: oito, mais_usadas: [] }, (c) => json({ fixadas: c.paginas }));
    const { result } = montar();
    await waitFor(() => expect(result.current.dados?.fixadas.length).toBe(8));
    act(() => result.current.alternar("p9"));
    await new Promise((r) => setTimeout(r, 20));
    expect(puts).toEqual([]);
    expect(result.current.dados?.fixadas).toEqual(oito);
  });

  it("erro do GET deixa dados nulos", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(json({ error: "x" }, 500));
    const { result } = montar();
    await new Promise((r) => setTimeout(r, 20));
    expect(result.current.dados).toBeNull();
    expect(result.current.fixada("a")).toBe(false);
  });

  it("erro 400 mostra a mensagem do servidor e a lista volta", async () => {
    falso({ fixadas: ["a"], mais_usadas: [] }, () => json({ error: "Tela não permitida" }, 400));
    const { result } = montar();
    await waitFor(() => expect(result.current.dados).not.toBeNull());
    expect(result.current.erro).toBeNull();
    act(() => result.current.alternar("b"));
    await waitFor(() => expect(result.current.erro).toBe("Tela não permitida"));
    expect(result.current.dados?.fixadas).toEqual(["a"]);
  });

  it("erro 500 sem mensagem mostra o aviso padrão; a próxima ação limpa", async () => {
    let falhar = true;
    falso({ fixadas: ["a"], mais_usadas: [] }, (c) =>
      falhar ? new Response("", { status: 500 }) : json({ fixadas: c.paginas }),
    );
    const { result } = montar();
    await waitFor(() => expect(result.current.dados).not.toBeNull());
    act(() => result.current.alternar("b"));
    await waitFor(() => expect(result.current.erro).toBe(ERRO_FIXADAS));
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(["a"]));
    falhar = false;
    act(() => result.current.alternar("c"));
    await waitFor(() => expect(result.current.erro).toBeNull());
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(["a", "c"]));
    expect(result.current.erro).toBeNull();
  });

  it("cliques seguidos: o segundo PUT só sai depois do primeiro; erro no primeiro → vale o servidor", async () => {
    let servidor: Dados = { fixadas: ["a"], mais_usadas: [] };
    const primeiro = adiada<Response>();
    const puts = falso(servidor, (c) => {
      if (puts.length === 1) return primeiro.promessa;
      servidor = { ...servidor, fixadas: c.paginas };
      return json({ fixadas: c.paginas });
    });
    // GET devolve sempre o estado atual do "servidor".
    const original = vi.mocked(globalThis.fetch).getMockImplementation()!;
    let gets = 0;
    vi.mocked(globalThis.fetch).mockImplementation(async (entrada, init) => {
      if (String(entrada) === "/api/me/fixadas" && init?.method !== "PUT") {
        gets++;
        return json(servidor);
      }
      return original(entrada, init);
    });
    const { result } = montar();
    await waitFor(() => expect(result.current.dados).not.toBeNull());
    act(() => result.current.alternar("b"));
    act(() => result.current.alternar("c"));
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(["a", "b", "c"])); // otimista
    await new Promise((r) => setTimeout(r, 30));
    expect(puts).toEqual([{ paginas: ["a", "b"] }]); // o segundo espera
    await act(async () => primeiro.resolver(json({ error: "falhou" }, 400)));
    await waitFor(() => expect(puts).toEqual([{ paginas: ["a", "b"] }, { paginas: ["a", "b", "c"] }]));
    await waitFor(() => expect(gets).toBeGreaterThan(1)); // releu do servidor
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(servidor.fixadas));
    expect(servidor.fixadas).toEqual(["a", "b", "c"]);
  });

  it("GET 200 fora do formato deixa dados nulos", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(json({}));
    const { result } = montar();
    await new Promise((r) => setTimeout(r, 20));
    expect(result.current.dados).toBeNull();
    expect(result.current.fixada("a")).toBe(false);
  });

  it("releitura que falha mantém os dados em cache", async () => {
    let falhar = false;
    vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
      falhar ? json({ error: "fora" }, 500) : json({ fixadas: ["a"], mais_usadas: [] }),
    );
    const { result, cliente } = montar();
    await waitFor(() => expect(result.current.dados?.fixadas).toEqual(["a"]));
    falhar = true;
    await act(async () => {
      await cliente.refetchQueries({ queryKey: ["fixadas"] });
    });
    await waitFor(() => expect(cliente.getQueryState(["fixadas"])?.status).toBe("error"));
    expect(result.current.dados?.fixadas).toEqual(["a"]);
  });
});
