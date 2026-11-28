import { useQuery } from "@tanstack/react-query";
import { api } from "./api";

/**
 * Quantos avisos o usuário ainda não leu (GET /api/avisos/nao-lidos, o mesmo do
 * sino do painel atual). Atualiza a cada 2 min; erro ou resposta estranha = 0,
 * porque o sino nunca pode atrapalhar a tela.
 */
export function useAvisosNaoLidos(): number {
  const consulta = useQuery({
    queryKey: ["avisos-nao-lidos"],
    queryFn: () => api<{ count?: unknown }>("/api/avisos/nao-lidos"),
    refetchInterval: 120000,
    retry: false,
  });
  const n = consulta.isError ? 0 : Number(consulta.data?.count);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : 0;
}
