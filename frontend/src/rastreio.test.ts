import { afterEach, describe, expect, it, vi } from "vitest";
import { registrarAcesso } from "./rastreio";

afterEach(() => {
  vi.restoreAllMocks();
  vi.unstubAllGlobals();
});

describe("registrarAcesso", () => {
  it("usa sendBeacon com o mesmo corpo do painel atual", async () => {
    const beacon = vi.fn().mockReturnValue(true);
    vi.stubGlobal("navigator", { sendBeacon: beacon });
    registrarAcesso("page_views");
    expect(beacon).toHaveBeenCalledTimes(1);
    const [url, blob] = beacon.mock.calls[0];
    expect(url).toBe("/api/track-page-view");
    const texto = await new Promise<string>((ok) => {
      const leitor = new FileReader();
      leitor.onload = () => ok(String(leitor.result));
      leitor.readAsText(blob as Blob);
    });
    expect(texto).toBe('{"page":"page_views"}');
    expect((blob as Blob).type).toBe("application/json");
  });

  it("sem sendBeacon, cai no fetch com keepalive", () => {
    vi.stubGlobal("navigator", {});
    const f = vi.spyOn(globalThis, "fetch").mockResolvedValue(new Response(null, { status: 204 }));
    registrarAcesso("page_views");
    expect(f).toHaveBeenCalledWith("/api/track-page-view", expect.objectContaining({ method: "POST", keepalive: true }));
  });

  it("erro no registro não sobe para a tela", () => {
    vi.stubGlobal("navigator", { sendBeacon: () => { throw new Error("x"); } });
    expect(() => registrarAcesso("page_views")).not.toThrow();
  });

  it("página vazia não registra", () => {
    const beacon = vi.fn();
    vi.stubGlobal("navigator", { sendBeacon: beacon });
    registrarAcesso("");
    expect(beacon).not.toHaveBeenCalled();
  });
});
