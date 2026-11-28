import { useQuery } from "@tanstack/react-query";
import { api } from "./api";

/** Bloco `nav` do /api/me (navegacao.Menu.para_api no servidor). */
export interface Nav {
  is_admin: boolean;
  is_comercial: boolean;
  is_suporte_comercial: boolean;
  pagina_inicial: string;
  paginas: string[];
  secoes: Record<string, boolean>;
  grupos: Record<string, string[]>;
}

export interface Me {
  user_id: number;
  username: string;
  role: string;
  categoria: string | null;
  display_name: string;
  nav: Nav;
  /** Campos antigos do /api/me que algumas telas ainda leem. */
  pages?: string[];
  kommo_user_id?: number | null;
  is_suporte_comercial?: boolean;
}

export function useMe() {
  return useQuery({
    queryKey: ["me"],
    queryFn: () => api<Me>("/api/me"),
    staleTime: 5 * 60 * 1000,
    retry: false,
  });
}
