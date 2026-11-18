"""Textos dos avisos: propostas por mensagem e variante, e a verificação de qualidade.

Os textos abaixo são PROPOSTAS (`aprovado=False`): nenhum foi cadastrado na Meta
nem no CRM. O envio real continua usando o nome do template que está no
ambiente (`MOTOR_TEMPLATE_<MENSAGEM>`); este módulo serve para revisar o texto
antes de cadastrar e para o resultado por variante ter um texto ao lado.

Variáveis do corpo: {{1}} primeiro nome, {{2}} mês da mensalidade, {{3}} prazo
efetivo (dd/mm). Reconhecimento, recuperação e firme não levam prazo.

`avaliar` aponta o que derruba aprovação na Meta ou piora a experiência:
variável faltando ou fora de ordem, texto começando ou terminando em variável,
tamanho, CAIXA ALTA, pontuação repetida, palavra de ameaça fora da mensagem
firme, botão longo e botão que o motor não sabe ler (resposta por botão é
classificada sem IA; botão desconhecido cai na classificação por texto).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from motor.classificador import classificar_botao
from motor.modelos import Mensagem

TAMANHO_MAX = 1024          # limite da Meta para o corpo
TAMANHO_BOM = 550           # acima disso o texto não cabe na tela sem rolar
BOTAO_MAX = 25              # limite da Meta para resposta rápida
BOTOES_MAX = 3
SIGLAS = frozenset({"PIX", "EAD", "RGM", "CPF", "AVA", "TCC"})
AMEACA = ("negativa", "protesto", "serasa", "spc", "judicial", "cobranca extrajudicial", "cobrança extrajudicial", "juridico", "jurídico")
COM_PRAZO = frozenset({Mensagem.LEMBRETE_DIA10.value, Mensagem.AVISO_CALOURO.value, Mensagem.LEMBRETE_DIA25.value})


@dataclass(frozen=True)
class Texto:
    mensagem: str
    variante: str
    angulo: str                      # o que esta variante testa
    corpo: str
    botoes: tuple[str, ...] = ()
    aprovado: bool = False           # só vira True depois de aprovado pelo Marcelo e pela Meta
    problemas: tuple[str, ...] = field(default=(), compare=False)


def variaveis(corpo: str) -> list[int]:
    return [int(n) for n in re.findall(r"\{\{(\d+)\}\}", corpo)]


def avaliar(mensagem: str, corpo: str, botoes: tuple[str, ...] = ()) -> list[str]:
    """Problemas do texto, em português. Lista vazia = pode seguir para cadastro."""
    problemas: list[str] = []
    usadas = variaveis(corpo)
    esperadas = 3 if mensagem in COM_PRAZO else 1
    if sorted(set(usadas)) != list(range(1, esperadas + 1)):
        problemas.append(f"variáveis esperadas: {{{{1}}}} a {{{{{esperadas}}}}}; o texto usa {sorted(set(usadas)) or 'nenhuma'}")
    limpo = corpo.strip()
    if re.match(r"^\{\{\d+\}\}", limpo) or re.search(r"\{\{\d+\}\}$", limpo):
        problemas.append("o texto não pode começar nem terminar com variável (a Meta recusa)")
    if re.search(r"\{\{\d+\}\}\s*\{\{\d+\}\}", limpo):
        problemas.append("duas variáveis seguidas, sem texto entre elas")
    if len(limpo) > TAMANHO_MAX:
        problemas.append(f"{len(limpo)} caracteres: passa do limite de {TAMANHO_MAX}")
    elif len(limpo) > TAMANHO_BOM:
        problemas.append(f"{len(limpo)} caracteres: acima de {TAMANHO_BOM} o aluno precisa rolar a tela")
    gritos = [p for p in re.findall(r"\b[A-ZÁÉÍÓÚÂÊÔÃÕÇ]{4,}\b", limpo) if p not in SIGLAS]
    if gritos:
        problemas.append("palavra em CAIXA ALTA: " + ", ".join(sorted(set(gritos))))
    if re.search(r"[!?]{2,}", limpo) or limpo.count("!") > 1:
        problemas.append("pontuação de ênfase repetida")
    if mensagem != Mensagem.FIRME.value:
        baixo = limpo.lower()
        usadas_ameaca = [a for a in AMEACA if a in baixo]
        if usadas_ameaca:
            problemas.append("palavra de ameaça fora da mensagem firme: " + ", ".join(usadas_ameaca))
    if "\n\n\n" in corpo:
        problemas.append("mais de uma linha em branco seguida (a Meta recusa)")
    if len(botoes) > BOTOES_MAX:
        problemas.append(f"{len(botoes)} botões: o máximo é {BOTOES_MAX}")
    for b in botoes:
        if len(b) > BOTAO_MAX:
            problemas.append(f"botão \"{b}\" tem {len(b)} caracteres (máximo {BOTAO_MAX})")
        if classificar_botao(b) is None:
            problemas.append(f"botão \"{b}\" não está entre os que o motor lê sem IA")
    return problemas


def _t(mensagem: Mensagem, variante: str, angulo: str, corpo: str, botoes: tuple[str, ...]) -> Texto:
    return Texto(mensagem.value, variante, angulo, corpo, botoes, False, tuple(avaliar(mensagem.value, corpo, botoes)))


PAGAR = ("Quero pagar", "Já paguei", "Não quero lembretes")
AJUDA = ("Quero negociar", "Já paguei", "Preciso de ajuda")

PROPOSTAS: tuple[Texto, ...] = (
    _t(Mensagem.LEMBRETE_DIA10, "A", "direto: prazo e desconto",
       "Oi, {{1}}. A mensalidade de {{2}} tem desconto para pagamento até {{3}}.\n\nSe já pagou, pode desconsiderar: a baixa leva até 3 dias úteis para aparecer.", PAGAR),
    _t(Mensagem.LEMBRETE_DIA10, "B", "serviço: oferece o caminho para pagar",
       "Oi, {{1}}. Passando para lembrar que o desconto da mensalidade de {{2}} vale até {{3}}.\n\nQuer que eu te mostre onde pegar o boleto ou o código do PIX?", PAGAR),
    _t(Mensagem.AVISO_CALOURO, "A", "acolhimento: primeira mensalidade",
       "Oi, {{1}}, tudo bem? Esta é a sua primeira mensalidade, a de {{2}}. O prazo de pagamento vai até {{3}}.\n\nSe tiver qualquer dúvida sobre boleto ou PIX, é só responder aqui.", ("Quero pagar", "Já paguei", "Preciso de ajuda")),
    _t(Mensagem.AVISO_CALOURO, "B", "passo a passo curto",
       "Oi, {{1}}. Sua primeira mensalidade, a de {{2}}, pode ser paga até {{3}}.\n\nO boleto e o PIX ficam na área do aluno. Posso te mandar o caminho?", ("Quero pagar", "Já paguei", "Preciso de ajuda")),
    _t(Mensagem.RECONHECIMENTO, "A", "agradece e pergunta se quer continuar recebendo",
       "Oi, {{1}}. Você tem pago a mensalidade em dia, obrigado por isso.\n\nQuer continuar recebendo o lembrete todo mês ou prefere que a gente não avise mais?", ("Continuar lembrando", "Não precisa me lembrar")),
    _t(Mensagem.LEMBRETE_DIA25, "A", "direto: vencimento",
       "Oi, {{1}}. A mensalidade de {{2}} vence em {{3}} e ainda não consta como paga.\n\nSe já pagou, pode desconsiderar: a baixa leva até 3 dias úteis para aparecer.", PAGAR),
    _t(Mensagem.LEMBRETE_DIA25, "B", "ajuda: abre a porta para negociar antes do vencimento",
       "Oi, {{1}}. A mensalidade de {{2}} vence em {{3}}.\n\nSe estiver difícil pagar este mês, me avisa por aqui que a gente vê uma saída antes do vencimento.", ("Quero pagar", "Quero negociar", "Já paguei")),
    _t(Mensagem.RECUPERACAO, "A", "conversa: pergunta o que houve",
       "Oi, {{1}}. Vi que a mensalidade deste mês ficou em aberto. Aconteceu alguma coisa?\n\nSe quiser, a gente conversa sobre a melhor forma de acertar.", AJUDA),
    _t(Mensagem.FIRME, "A", "firme, sem ameaça: consequência acadêmica e porta para negociar",
       "Oi, {{1}}. Há mensalidades em aberto no seu cadastro e ainda não conseguimos falar com você.\n\nPara não ter problema com a rematrícula, vale resolver agora. Responda aqui que um atendente te ajuda.", AJUDA),
)


def catalogo() -> list[dict]:
    """Propostas com os problemas de cada uma, para a página."""
    return [
        {"mensagem": t.mensagem, "variante": t.variante, "angulo": t.angulo, "corpo": t.corpo, "botoes": list(t.botoes),
         "aprovado": t.aprovado, "caracteres": len(t.corpo), "problemas": list(t.problemas)}
        for t in PROPOSTAS
    ]
