import { Component, type ErrorInfo, type ReactNode } from "react";

/**
 * Segura o erro de uma tela (ex.: o pacote dela não carregou depois de um deploy)
 * sem derrubar a casca: o menu continua e a pessoa pode recarregar.
 * No App.tsx vai com `key` da rota, para zerar ao trocar de tela.
 */
export class ErroTela extends Component<{ children: ReactNode }, { erro: boolean }> {
  state = { erro: false };

  static getDerivedStateFromError() {
    return { erro: true };
  }

  componentDidCatch(erro: Error, info: ErrorInfo) {
    console.error("Erro na tela", erro, info.componentStack);
  }

  render() {
    if (!this.state.erro) return this.props.children;
    return (
      <div role="alert" className="cartao p-8 flex flex-col items-center gap-3 text-center max-w-xl">
        <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
          Não deu para abrir esta tela.
        </p>
        <button type="button" className="controle controle-primario" onClick={() => window.location.reload()}>
          Recarregar
        </button>
      </div>
    );
  }
}
