import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "./api";

/** Telas fixadas pelo usuário e as mais usadas (a API já filtra pelo que o menu mostra). */
export interface DadosFixadas {
  fixadas: string[];
  mais_usadas: string[];
}

/** Mesmo limite do servidor. */
export const MAX_FIXADAS = 8;

const CHAVE = ["fixadas"] as const;

export function useFixadas(): {
  dados: DadosFixadas | null;
  alternar: (pagina: string) => void;
  fixada: (pagina: string) => boolean;
} {
  const cliente = useQueryClient();
  const consulta = useQuery({
    queryKey: CHAVE,
    queryFn: () => api<DadosFixadas>("/api/me/fixadas"),
    retry: false,
  });

  const salvar = useMutation({
    mutationFn: ({ paginas }: { paginas: string[]; anterior: DadosFixadas }) =>
      api<{ fixadas: string[] }>("/api/me/fixadas", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ paginas }),
      }),
    // Erro: volta para a lista de antes do clique.
    onError: (_erro, { anterior }) => cliente.setQueryData(CHAVE, anterior),
    onSuccess: (resposta) => {
      // A resposta do servidor é a verdade; as mais usadas não mudam.
      const atual = cliente.getQueryData<DadosFixadas>(CHAVE);
      if (atual) cliente.setQueryData<DadosFixadas>(CHAVE, { ...atual, fixadas: resposta.fixadas });
    },
  });

  /** Atualiza a tela na hora (otimista) e só depois envia. */
  function enviar(atual: DadosFixadas, paginas: string[]) {
    cliente.setQueryData<DadosFixadas>(CHAVE, { ...atual, fixadas: paginas });
    salvar.mutate({ paginas, anterior: atual });
  }

  const dados = consulta.isError ? null : (consulta.data ?? null);

  return {
    dados,
    fixada: (pagina) => dados?.fixadas.includes(pagina) ?? false,
    alternar: (pagina) => {
      const atual = cliente.getQueryData<DadosFixadas>(CHAVE);
      if (!atual) return;
      if (atual.fixadas.includes(pagina)) {
        enviar(atual, atual.fixadas.filter((p) => p !== pagina));
      } else if (atual.fixadas.length < MAX_FIXADAS) {
        enviar(atual, [...atual.fixadas, pagina]);
      }
    },
  };
}
