import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { tentarDeNovo } from "./api";
import { App, FUTURO_ROUTER } from "./App";
import "./estilo.css";

const cliente = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      // 401/403 não melhoram tentando de novo.
      retry: tentarDeNovo,
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
