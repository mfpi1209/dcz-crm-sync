import { Route, Routes, useParams } from "react-router-dom";
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
  if (!Componente || !me.data?.nav.paginas.includes(pagina)) {
    return <NaoEncontrada />;
  }
  return <Componente />;
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
