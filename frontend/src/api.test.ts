import { afterEach, describe, expect, it, vi } from "vitest";
import { api, ErroApi, navegador, SEM_PERMISSAO } from "./api";

function resposta(status: number, corpo: unknown) {
  return new Response(JSON.stringify(corpo), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

afterEach(() => vi.restoreAllMocks());

describe("api", () => {
  it("devolve o JSON e manda o cookie da sessão", async () => {
    const f = vi.spyOn(globalThis, "fetch").mockResolvedValue(resposta(200, { ok: 1 }));
    await expect(api("/api/x")).resolves.toEqual({ ok: 1 });
    const init = f.mock.calls[0][1]!;
    expect(init.credentials).toBe("same-origin");
    expect((init.headers as Record<string, string>).Accept).toBe("application/json");
  });

  it("mantém o Accept quando a chamada passa outros cabeçalhos", async () => {
    const f = vi.spyOn(globalThis, "fetch").mockResolvedValue(resposta(200, {}));
    await api("/api/x", { method: "POST", headers: { "Content-Type": "application/json" } });
    const h = f.mock.calls[0][1]!.headers as Record<string, string>;
    expect(h.Accept).toBe("application/json");
    expect(h["Content-Type"]).toBe("application/json");
    expect(f.mock.calls[0][1]!.method).toBe("POST");
  });

  it("401 leva ao login", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(resposta(401, { error: "Não autenticado" }));
    const ir = vi.spyOn(navegador, "irPara").mockImplementation(() => {});
    await expect(api("/api/x")).rejects.toMatchObject({ status: 401 });
    expect(ir).toHaveBeenCalledWith("/login");
  });

  it("403 vira erro de permissão, sem sair da tela", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(resposta(403, { ok: false, error: "Sem permissão para esta ação." }));
    const ir = vi.spyOn(navegador, "irPara").mockImplementation(() => {});
    const erro = (await api("/api/x").catch((e: unknown) => e)) as ErroApi;
    expect(erro).toBeInstanceOf(ErroApi);
    expect(erro.status).toBe(403);
    expect(erro.message).toBe(SEM_PERMISSAO);
    expect(ir).not.toHaveBeenCalled();
  });

  it("outros erros trazem a mensagem do servidor", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(resposta(500, { error: "banco fora" }));
    await expect(api("/api/x")).rejects.toMatchObject({ status: 500, message: "banco fora" });
  });

  it("erro sem JSON não quebra", async () => {
    vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response("<html>", { status: 502 }));
    await expect(api("/api/x")).rejects.toMatchObject({ status: 502, message: "Erro 502" });
  });
});
