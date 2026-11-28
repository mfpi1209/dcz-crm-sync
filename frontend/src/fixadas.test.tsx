import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useFixadas } from "./fixadas";

type Dados = { fixadas: string[]; mais_usadas: string[] };

function json(corpo: unknown, status = 200) {
  return new Response(JSON.stringify(corpo), { status, headers: { "Content-Type": "application/json" } });
}

/** Fetch falso que roteia por método; devolve a lista de PUTs feitos. */
function falso(inicial: Dados, put: (corpo: { paginas: string[] }) => Response | Promise<Response>) {
  const puts: { paginas: string[] }[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (entrada, init) => {
    const url = String(entrada);
    if (url === "/api/me/fixadas" && init?.method === "PUT") {
      const corpo = JSON.parse(String(init.body));
      puts.push(corpo);
      return put(corpo);
    }
    if (url === "/api/me/fixadas") return json(inicial);
    return json({}, 404);
  });
  return puts;
}

function montar() {
  const cliente = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={cliente}>{children}</QueryClientProvider>
  );
  return renderHook(() => useFixadas(), { wrapper });
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
});
