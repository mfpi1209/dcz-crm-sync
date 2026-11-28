/** Ações da tela que os pedaços chamam (as que gravam pedem confirmação, como na antiga). */
export interface Acoes {
  admin: boolean;
  consultarRgm: (rgm: string) => void;
  fixarVenda: (rgm: string, userId: number, nome: string, jaConfirmou?: boolean) => void;
  desfazerVenda: (rgm: string) => void;
  contarVenda: (rgm: string, contar: boolean) => void;
  syncCandidato: (dealId: string, rgm: string) => void;
}
