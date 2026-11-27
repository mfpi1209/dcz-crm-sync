import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

// Servido pelo Flask em /app/ (routes/front_novo.py), no mesmo endereço do painel:
// mesmo cookie de sessão, mesmas permissões, sem CORS.
// Em `npm run dev`, as chamadas vão para o Flask local na 5001.
const FLASK = "http://127.0.0.1:5001";

export default defineConfig({
  base: "/app/",
  plugins: [react()],
  build: { outDir: "dist", emptyOutDir: true, sourcemap: false },
  server: {
    port: 5173,
    proxy: {
      "/api": FLASK,
      "/login": FLASK,
      "/logout": FLASK,
      "/static": FLASK,
    },
  },
  test: {
    environment: "jsdom",
    globals: false,
  },
});
