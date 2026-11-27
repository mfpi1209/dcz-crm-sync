import { Link } from "react-router-dom";

export function NaoEncontrada({ mensagem }: { mensagem?: string }) {
  return (
    <div className="max-w-xl">
      <h1 className="text-xl">Tela não encontrada</h1>
      <p className="mt-2 text-sm" style={{ color: "var(--text-secondary)" }}>
        {mensagem ?? "Esta tela não existe no front novo ou você não tem acesso a ela."}
      </p>
      <Link to="/" className="inline-block mt-4 underline text-primary">Voltar ao início</Link>
    </div>
  );
}
