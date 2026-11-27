import { describe, expect, it } from "vitest";
import { hojeIso, numero, somarDias } from "./datas";

describe("datas", () => {
  it("hoje no fuso do navegador", () => {
    expect(hojeIso(new Date(2026, 10, 27, 23, 59))).toBe("2026-11-27");
  });
  it("soma e subtrai dias atravessando mês e ano", () => {
    expect(somarDias("2026-11-27", -29)).toBe("2026-10-29");
    expect(somarDias("2026-01-01", -1)).toBe("2025-12-31");
    expect(somarDias("2028-02-28", 1)).toBe("2028-02-29");
  });
  it("número no formato brasileiro, vazio vira zero", () => {
    expect(numero(1234567)).toBe("1.234.567");
    expect(numero(undefined)).toBe("0");
  });
});
