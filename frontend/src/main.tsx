import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { App, FUTURO_ROUTER } from "./App";
import "./estilo.css";

const cliente = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      // Como o painel antigo, que nunca repetia pedidos: um erro aparece na hora, e rotas
      // pesadas não são chamadas até 3 vezes. Uma tela que queira repetir pede no próprio useQuery.
      retry: false,
    },
  },
});

createRoot(document.getElementById("raiz")!).render(
  <StrictMode>
    <QueryClientProvider client={cliente}>
      <BrowserRouter basename="/app" future={FUTURO_ROUTER}>
        <App />
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
