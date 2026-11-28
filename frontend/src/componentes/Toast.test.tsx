import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { Aviso } from "../fixadas";
import { Toast } from "./Toast";

afterEach(() => {
  cleanup();
  vi.useRealTimers();
});

const ERRO: Aviso = { tipo: "erro", texto: "Tela não permitida" };
const OK: Aviso = { tipo: "sucesso", texto: "Tela fixada no menu." };

describe("Toast", () => {
  it("sem aviso: só a região de status vazia, nada visível", () => {
    render(<Toast aviso={null} onFechar={() => {}} />);
    expect(screen.getByRole("status").textContent).toBe("");
    expect(screen.queryByRole("button", { name: "Fechar aviso" })).toBeNull();
  });

  it("erro: título, texto no status e animação de entrada", () => {
    const { container } = render(<Toast aviso={ERRO} onFechar={() => {}} />);
    expect(screen.getByText("Não deu certo")).toBeTruthy();
    expect(screen.getByRole("status").textContent).toBe("Tela não permitida");
    const toast = container.querySelector(".toast")!;
    expect(toast.className).toContain("toast-erro");
    expect(toast.className).toContain("toast-entra");
  });

  it("sucesso usa o visual de sucesso", () => {
    const { container } = render(<Toast aviso={OK} onFechar={() => {}} />);
    expect(screen.getByText("Pronto")).toBeTruthy();
    expect(container.querySelector(".toast")!.className).toContain("toast-sucesso");
  });

  it("X chama onFechar", () => {
    const fechar = vi.fn();
    render(<Toast aviso={OK} onFechar={fechar} />);
    fireEvent.click(screen.getByRole("button", { name: "Fechar aviso" }));
    expect(fechar).toHaveBeenCalledTimes(1);
  });

  it("aviso some: roda a saída e depois sai da página", () => {
    vi.useFakeTimers();
    const { container, rerender } = render(<Toast aviso={OK} onFechar={() => {}} />);
    rerender(<Toast aviso={null} onFechar={() => {}} />);
    expect(container.querySelector(".toast")!.className).toContain("toast-sai");
    act(() => {
      vi.advanceTimersByTime(250);
    });
    expect(container.querySelector(".toast")).toBeNull();
    expect(screen.getByRole("status").textContent).toBe("");
  });
});
