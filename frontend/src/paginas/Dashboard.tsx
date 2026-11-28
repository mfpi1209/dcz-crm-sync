import { useEffect } from "react";
import { navegador } from "../api";
import { useMe } from "../me";
import { item } from "../menu";
import { NOMES_TELAS_NOVAS } from "../telas";
import { DashboardAcademico } from "./dashboard/Academico";

/**
 * Dashboard (página `dashboard`), a home de quem não é comercial. Escolhe o painel
 * pela categoria do usuário, como o loadDashboard antigo (static/js/dashboard.js):
 *
 * - supervisor comercial / supervisor acadêmico: o painel próprio deles ainda não foi
 *   migrado; a tela leva ao painel atual e não busca nada da home;
 * - acadêmico: vai direto para Meus Atendimentos (se tiver a página); senão, atalhos;
 * - os demais: Dashboard Acadêmico (funil, matrículas, evolução e ciclos).
 */

function categoria(c: string | null | undefined) {
  return (c || "").toLowerCase().trim();
}

export function Dashboard() {
  const me = useMe();
  if (!me.data) return null;
  const cat = categoria(me.data.categoria);
  if (cat === "supervisor comercial" || cat === "supervisor acadêmico" || cat === "supervisor academico") {
    return <PainelSupervisor />;
  }
  if (cat === "acadêmico" || cat === "academico") {
    return <AcademicoSimples username={me.data.username} paginas={me.data.nav.paginas} />;
  }
  return <DashboardAcademico />;
}

function PainelSupervisor() {
  return (
    <section className="cartao p-5 max-w-3xl flex flex-col gap-3">
      <h2 className="text-base">Painel do supervisor</h2>
      <p className="text-sm" style={{ color: "var(--text-secondary)" }}>
        O painel do supervisor ainda não chegou ao front novo. Ele continua no painel atual, com o mesmo login.
      </p>
      <p>
        <a className="controle controle-primario" href={item("dashboard", new Set()).href}>Abrir no painel atual</a>
      </p>
    </section>
  );
}

const ATALHOS = [
  { pagina: "distribuicao", titulo: "Distribuição Acadêmica", texto: "Acompanhe e gerencie a fila de distribuição.", icone: "share" },
  { pagina: "feedback", titulo: "Feedback Acadêmico", texto: "Registre e acompanhe feedbacks dos alunos.", icone: "forum" },
  { pagina: "inadimplencia", titulo: "Inadimplência", texto: "Veja a saúde financeira e alunos inadimplentes.", icone: "account_balance_wallet" },
  { pagina: "recadastros", titulo: "Recadastros", texto: "Atualize dados cadastrais dos alunos.", icone: "how_to_reg" },
];

function AcademicoSimples({ username, paginas }: { username: string; paginas: string[] }) {
  const podeMA = paginas.includes("meus_atendimentos");
  const destino = item("meus_atendimentos", NOMES_TELAS_NOVAS).href;
  useEffect(() => {
    if (podeMA) navegador.irPara(destino);
  }, [podeMA, destino]);
  if (podeMA) {
    return <p className="text-sm" style={{ color: "var(--text-muted)" }}>Abrindo Meus Atendimentos…</p>;
  }
  const nm = (username || "").split("@")[0].split(".")[0];
  const nome = nm ? nm.charAt(0).toUpperCase() + nm.slice(1) : "";
  return (
    <div className="max-w-5xl flex flex-col gap-6">
      <div>
        <h2 className="text-2xl">Bem-vindo(a) {nome}!</h2>
        <p className="text-sm mt-1" style={{ color: "var(--text-muted)" }}>Selecione um dos atalhos abaixo para iniciar.</p>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {ATALHOS.map((a) => (
          <a key={a.pagina} href={item(a.pagina, NOMES_TELAS_NOVAS).href} className="cartao-agente p-5 flex items-start gap-4">
            <span className="material-symbols-outlined text-[28px]" aria-hidden style={{ color: "var(--primary)" }}>{a.icone}</span>
            <span className="flex-1 min-w-0">
              <span className="block text-base font-bold">{a.titulo}</span>
              <span className="block text-xs mt-1" style={{ color: "var(--text-muted)" }}>{a.texto}</span>
            </span>
            <span className="material-symbols-outlined" aria-hidden style={{ color: "var(--text-muted)" }}>arrow_forward</span>
          </a>
        ))}
      </div>
    </div>
  );
}
