import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ErroApi } from "./api";

/** Telas fixadas pelo usuário e as mais usadas (a API já filtra pelo que o menu mostra). */
export interface DadosFixadas {
  fixadas: string[];
  mais_usadas: string[];
}

/** Mesmo limite do servidor. */
export const MAX_FIXADAS = 8;

/** Aviso quando o servidor não informa o motivo da falha. */
export const ERRO_FIXADAS = "Não deu para salvar as fixadas. Tente de novo.";

/** Quanto tempo o aviso de erro fica na tela. */
export const TEMPO_AVISO_MS = 4000;
/** Confirmação de sucesso some mais rápido. */
export const TEMPO_SUCESSO_MS = 2500;

const CHAVE = ["fixadas"] as const;
/** Fora do prefixo ["fixadas"] para não ser invalidada junto com a lista. */
const CHAVE_ERRO = ["aviso-fixadas"] as const;

/** Aviso compartilhado entre a barra de cima e o menu (cada um usa o hook). */
export interface Aviso {
  tipo: "erro" | "sucesso";
  texto: string;
}

const listaDeTextos = (v: unknown): v is string[] => Array.isArray(v) && v.every((p) => typeof p === "string");

/** Só aceita a resposta no formato esperado; senão o menu segue sem a seção. */
export function validarFixadas(corpo: unknown): DadosFixadas | null {
  if (!corpo || typeof corpo !== "object") return null;
  const { fixadas, mais_usadas } = corpo as Record<string, unknown>;
  return listaDeTextos(fixadas) && listaDeTextos(mais_usadas) ? { fixadas, mais_usadas } : null;
}

/** Mensagem do servidor quando houver; "Erro 500" e afins viram o aviso padrão. */
function textoDoErro(erro: unknown): string {
  if (erro instanceof ErroApi && erro.message && !/^Erro \d+$/.test(erro.message)) return erro.message;
  return ERRO_FIXADAS;
}

export function useFixadas(): {
  dados: DadosFixadas | null;
  erro: string | null;
  aviso: Aviso | null;
  fecharAviso: () => void;
  alternar: (pagina: string) => void;
  fixada: (pagina: string) => boolean;
} {
  const cliente = useQueryClient();
  const consulta = useQuery({
    queryKey: CHAVE,
    queryFn: async () => validarFixadas(await api<unknown>("/api/me/fixadas")),
    retry: false,
  });
  const aviso = useQuery<Aviso | null>({
    queryKey: CHAVE_ERRO,
    queryFn: () => null,
    initialData: null,
    enabled: false,
    staleTime: Infinity,
  });

  function limparAviso() {
    if (cliente.getQueryData(CHAVE_ERRO)) cliente.setQueryData(CHAVE_ERRO, null);
  }

  /** Mostra o aviso e o tira sozinho depois de `ms` (se outro não tiver chegado). */
  function mostrarAviso(novo: Aviso, ms: number) {
    cliente.setQueryData(CHAVE_ERRO, novo);
    setTimeout(() => {
      if (cliente.getQueryData(CHAVE_ERRO) === novo) cliente.setQueryData(CHAVE_ERRO, null);
    }, ms);
  }

  const salvar = useMutation({
    mutationKey: CHAVE,
    // Um clique de cada vez: o PUT seguinte só sai depois que o anterior responder.
    scope: { id: "fixadas" },
    mutationFn: ({ paginas }: { paginas: string[]; anterior: DadosFixadas; mensagem: string }) =>
      api<{ fixadas: string[] }>("/api/me/fixadas", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ paginas }),
      }),
    onError: (erro, { anterior }) => {
      // Volta para a lista de antes do clique e mostra um aviso discreto.
      cliente.setQueryData(CHAVE, anterior);
      mostrarAviso({ tipo: "erro", texto: textoDoErro(erro) }, TEMPO_AVISO_MS);
    },
    onSuccess: (resposta, { mensagem }) => {
      mostrarAviso({ tipo: "sucesso", texto: mensagem }, TEMPO_SUCESSO_MS);
      // A resposta do servidor é a verdade; as mais usadas não mudam.
      const atual = cliente.getQueryData<DadosFixadas | null>(CHAVE);
      if (atual) cliente.setQueryData<DadosFixadas>(CHAVE, { ...atual, fixadas: resposta.fixadas });
    },
    onSettled: () => {
      // Último clique da fila respondeu: relê do servidor para ele sempre ganhar.
      if (cliente.isMutating({ mutationKey: CHAVE }) <= 1) {
        return cliente.invalidateQueries({ queryKey: CHAVE });
      }
    },
  });

  /** Atualiza a tela na hora (otimista) e só depois envia. */
  function enviar(atual: DadosFixadas, paginas: string[], mensagem: string) {
    // Cancela uma leitura em andamento para ela não apagar o otimista
    // (o cancelamento é imediato; a promessa só avisa quando terminou).
    void cliente.cancelQueries({ queryKey: CHAVE });
    cliente.setQueryData<DadosFixadas>(CHAVE, { ...atual, fixadas: paginas });
    salvar.mutate({ paginas, anterior: atual, mensagem });
  }

  // Dado em cache continua valendo se uma releitura falhar.
  const dados = consulta.data ?? null;

  return {
    dados,
    erro: aviso.data?.tipo === "erro" ? aviso.data.texto : null,
    aviso: aviso.data ?? null,
    fecharAviso: limparAviso,
    fixada: (pagina) => dados?.fixadas.includes(pagina) ?? false,
    alternar: (pagina) => {
      limparAviso();
      const atual = cliente.getQueryData<DadosFixadas | null>(CHAVE);
      if (!atual) return;
      if (atual.fixadas.includes(pagina)) {
        enviar(atual, atual.fixadas.filter((p) => p !== pagina), "Tela removida das fixadas.");
      } else if (atual.fixadas.length < MAX_FIXADAS) {
        enviar(atual, [...atual.fixadas, pagina], "Tela fixada no menu.");
      }
    },
  };
}
