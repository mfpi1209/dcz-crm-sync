import { Suspense, useEffect } from "react";
import { Route, Routes, useParams } from "react-router-dom";
import { registrarAcesso } from "./rastreio";
import { ErroTela } from "./componentes/ErroTela";
import { Estrutura } from "./componentes/Estrutura";
import { useMe } from "./me";
import { Inicio } from "./paginas/Inicio";
import { NaoEncontrada } from "./paginas/NaoEncontrada";
import { TELAS_NOVAS } from "./telas";

/** Comportamento do React Router 7 já ligado, para a troca de versão não mudar nada. */
export const FUTURO_ROUTER = { v7_startTransition: true, v7_relativeSplatPath: true } as const;

/** /app/<pagina>: abre a tela migrada se o /api/me disser que o usuário a vê. */
function Tela() {
  const { pagina = "" } = useParams();
  const me = useMe();
  const Componente = TELAS_NOVAS[pagina];
  const pode = !!Componente && !!me.data?.nav.paginas.includes(pagina);
  // Mesmo registro de uso do painel atual, uma vez por tela aberta.
  useEffect(() => {
    if (pode) registrarAcesso(pagina);
  }, [pode, pagina]);
  if (!pode) {
    return <NaoEncontrada />;
  }
  // Telas carregadas sob demanda: o Suspense espera o pacote; o ErroTela (zerado a cada
  // rota) segura a falha sem derrubar o menu.
  return (
    <ErroTela key={pagina}>
      <Suspense fallback={<p className="py-16 text-center text-sm" style={{ color: "var(--text-muted)" }}>Carregando a tela...</p>}>
        <Componente />
      </Suspense>
    </ErroTela>
  );
}

export function App() {
  return (
    <Routes>
      <Route element={<Estrutura />}>
        <Route index element={<Inicio />} />
        <Route path=":pagina" element={<Tela />} />
        <Route path="*" element={<NaoEncontrada />} />
      </Route>
    </Routes>
  );
}
