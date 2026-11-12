"""
Confere, só com GET, se o CRM acadêmico já tem o que a coleta do NPS precisa.

Não imprime token nem dado de aluno: só nomes de campos, chaves do JSON e contagens.
Usa EDUIT_CRM_TOKEN e NPS_CRM_BASE_URL do .env.

Uso:
    python scripts/nps_check.py
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
sys.path.insert(0, _ROOT)
os.chdir(_ROOT)

from dotenv import load_dotenv

load_dotenv()

from nps.coleta import ColetaErro, extrair, itens, montar_mapa, nomes_dos_campos
from nps.crm import CrmErro, CrmLeitura


def main() -> int:
    crm = CrmLeitura()
    if not crm.configurado():
        print("EDUIT_CRM_TOKEN não configurado: nada foi consultado.")
        return 1
    try:
        campos = itens(crm.campos())
        print(f"Campos personalizados lidos: {len(campos)}")
        try:
            mapa = montar_mapa(campos)
        except ColetaErro as e:
            print(f"FALTA: {e}")
            print("Esperados no negócio:", ", ".join(nomes_dos_campos().values()))
            return 2
        faltam = [nome for papel, nome in nomes_dos_campos().items() if papel not in mapa]
        print("Campos do NPS encontrados:", ", ".join(sorted(mapa)))
        if faltam:
            print("Opcionais ausentes:", ", ".join(faltam))

        pagina = itens(crm.negocios(1, 50, (os.getenv("NPS_PIPELINE_ID") or "").strip()))
        print(f"Negócios na 1ª página (50): {len(pagina)}")
        if pagina:
            print("Chaves do negócio:", ", ".join(sorted(pagina[0].keys())))
        from datetime import datetime, timezone

        agora = datetime.now(timezone.utc)
        pesquisas = [p for p in (extrair(d, mapa, agora) for d in pagina) if p]
        print(f"Com pesquisa enviada nessa página: {len(pesquisas)}; com resposta: {sum(1 for p in pesquisas if p.nota is not None)}")
        com_conversa = next((p for p in pesquisas if p.conversa_origem), None)
        if com_conversa:
            mensagens = itens(crm.mensagens(com_conversa.conversa_origem))
            print(f"Mensagens da conversa de origem: {len(mensagens)}")
            if mensagens:
                print("Chaves da mensagem:", ", ".join(sorted(mensagens[0].keys())))
    except CrmErro as e:
        print(f"ERRO: {e}")
        return 3
    return 0


if __name__ == "__main__":
    sys.exit(main())
