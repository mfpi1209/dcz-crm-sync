/**
 * Cliente das APIs do Flask (mesmo endereço, cookie de sessão enviado sozinho).
 *
 * - 401: a sessão caiu. Vai para o login do painel e volta para esta tela depois.
 * - 403: sem permissão. `permissoes.exige_pagina` responde {"error": "..."}.
 * - Outros erros: ErroApi com a mensagem do servidor, quando houver.
 */

export class ErroApi extends Error {
  constructor(
    public readonly status: number,
    mensagem: string,
  ) {
    super(mensagem);
    this.name = "ErroApi";
  }
}

export const SEM_PERMISSAO = "Você não tem permissão para esta tela.";

/** Troca de página do navegador; separada para os testes. */
export const navegador = {
  irPara(url: string) {
    window.location.assign(url);
  },
};

async function mensagemDoErro(r: Response): Promise<string> {
  try {
    const corpo = await r.json();
    return corpo?.error || corpo?.erro || corpo?.message || `Erro ${r.status}`;
  } catch {
    return `Erro ${r.status}`;
  }
}

export async function api<T>(caminho: string, init?: RequestInit): Promise<T> {
  const r = await fetch(caminho, {
    ...init,
    credentials: "same-origin",
    headers: { Accept: "application/json", ...((init?.headers as Record<string, string>) || {}) },
  });
  if (r.status === 401) {
    navegador.irPara("/login");
    throw new ErroApi(401, "Sessão expirada. Entre de novo.");
  }
  if (r.status === 403) {
    throw new ErroApi(403, SEM_PERMISSAO);
  }
  if (!r.ok) {
    throw new ErroApi(r.status, await mensagemDoErro(r));
  }
  return (await r.json()) as T;
}

/** Regra de novas tentativas do QueryClient de produção (main.tsx): 401/403 não melhoram tentando de novo. */
export function tentarDeNovo(tentativas: number, erro: unknown): boolean {
  return !(erro instanceof ErroApi && (erro.status === 401 || erro.status === 403)) && tentativas < 2;
}
