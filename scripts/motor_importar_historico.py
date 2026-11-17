"""Importa o histórico de pagamento (ciclos já fechados) para `motor_aluno_ciclo`.

    python scripts/motor_importar_historico.py historico.csv            # só confere e mostra o resumo
    python scripts/motor_importar_historico.py historico.csv --aplicar  # grava

Formato do CSV em `motor/historico.py`. Usa o banco do painel (mesmas variáveis
DB_* do painel). Nunca sobrescreve linha que já existe: o que o motor registrou
vale. Não fala com o CRM. Em produção, só rodar com autorização.
"""
from __future__ import annotations

import sys
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(RAIZ))


def main(argv: list[str]) -> int:
    args = [a for a in argv if not a.startswith("--")]
    if len(args) != 1:
        print(__doc__)
        return 2
    from motor.calendario import agora_brt
    from motor.historico import ler_csv, resumo

    linhas, fora = ler_csv(Path(args[0]).read_text(encoding="utf-8"), hoje=agora_brt().date())
    for ciclo, r in resumo(linhas).items():
        print(f"{ciclo}: {r['alunos']} alunos, {r['com_baixa']} com baixa")
    for motivo, n in sorted(fora.items()):
        print(f"fora ({motivo}): {n}")
    if "--aplicar" not in argv:
        print("Conferência apenas. Para gravar, repita com --aplicar.")
        return 0

    import psycopg2.extras

    from db import get_conn

    conn = get_conn()
    try:
        with conn, conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                "INSERT INTO motor_aluno_ciclo (contact_id, ciclo, aberto_em, baixa_dia10_em, controle) VALUES %s "
                "ON CONFLICT (contact_id, ciclo) DO NOTHING",
                [(l["contact_id"], l["ciclo"], l["aberto_em"], l["baixa_dia10_em"], False) for l in linhas],
                page_size=2000,
            )
            gravadas = cur.rowcount
            cur.execute(
                "INSERT INTO motor_diario (tipo, autor, texto) VALUES ('carga', 'usuario:script', %s)",
                (f"Histórico importado: {len(linhas)} linhas lidas, ciclos {', '.join(resumo(linhas))}. Linhas que já existiam foram mantidas.",),
            )
    finally:
        conn.close()
    print(f"Gravado. Última página: {gravadas} linhas novas (as que já existiam foram mantidas).")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
