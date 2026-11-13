"""Classificação das respostas dos alunos.

  - clique em botão é tratado em código, pelo próprio botão, sem classificador
  - texto livre vai para o Jev (TypeSafe), sempre mascarado; se o Jev falhar ou
    não houver chave, valem as regras por palavra-chave
  - confiança abaixo de 0,7 vai para revisão humana

Antes de sair do processo, todo texto passa por `anonimizar`: o Jev só recebe
texto sem nome, telefone, CPF, RGM e e-mail. O texto nunca vai para log nem
para mensagem de erro.
"""
from __future__ import annotations

import json
import os
import re
import time
import unicodedata
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Optional, Protocol

ROTULOS = (
    "ja_paguei", "vai_pagar", "como_pagar", "desconto_valor_prazo", "negociar", "cancelar_trancar",
    "pediu_parar", "reclamou", "numero_errado", "outro_assunto", "sem_conteudo",
)
# Contam para a trava de experiência (pedidos para parar e reclamações por 1.000 entregues).
ROTULOS_DE_EXPERIENCIA = frozenset({"pediu_parar", "reclamou"})
CONFIANCA_MINIMA = 0.7
JEV_URL = "https://api.typesafe.ai/v1/systemone"

BOTOES = {
    "ja paguei": "ja_paguei",
    "quero pagar": "como_pagar",
    "2a via": "como_pagar",
    "preciso de ajuda": "outro_assunto",
    "quero negociar": "negociar",
    "quero trancar ou cancelar": "cancelar_trancar",
    "nao quero lembretes": "pediu_parar",
    "parar de receber": "pediu_parar",
    "nao precisa me lembrar": "silenciar",
    "continuar lembrando": "continuar",
}


@dataclass(frozen=True)
class Classificacao:
    rotulo: str
    confianca: float
    origem: str  # "botao" | "regras" | "jev"

    @property
    def precisa_de_revisao(self) -> bool:
        return self.origem != "botao" and self.confianca < CONFIANCA_MINIMA


def _fold(s: str) -> str:
    n = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in n if not unicodedata.combining(c)).lower().strip()


_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
# Qualquer sequência com 5 ou mais dígitos (telefone, CPF, RGM, código de barras),
# com ou sem pontuação no meio. Valores e datas curtas ("R$ 350,00", "10/11") ficam.
_NUMERO = re.compile(r"\+?\d(?:[\s().\-/]*\d){4,}")
# "meu nome é Maria Souza", "aqui é o Carlos", "sou a Ana": tira o nome que vem depois.
_APRESENTACAO = re.compile(
    r"\b((?i:meu nome [ée]|me chamo|aqui [ée](?: o| a)?|sou(?: o| a)?|falo com(?: o| a)?)\s+)((?:[A-ZÀ-Ý][\wÀ-ÿ']+\s?){1,4})"
)


def anonimizar(texto: str, nomes: Iterable[str] = ()) -> str:
    """Tira e-mail, números longos, nomes informados e nomes em apresentação. Devolve o texto pronto para sair.

    Limite conhecido: nome escrito em minúsculas no meio da frase, sem estar
    entre os nomes informados, não é reconhecido.
    """
    limpo = _EMAIL.sub("[email]", texto or "")
    limpo = _NUMERO.sub("[numero]", limpo)
    limpo = _APRESENTACAO.sub(lambda m: m.group(1) + "[nome] ", limpo)
    partes = {p for nome in nomes for p in _fold(nome).split() if len(p) >= 3}
    if partes:
        def troca(m: re.Match) -> str:
            return "[nome]" if _fold(m.group(0)) in partes else m.group(0)

        limpo = re.sub(r"[^\W\d_]+", troca, limpo)
    return re.sub(r"[ \t]{2,}", " ", limpo).strip()


def classificar_botao(texto: str) -> Optional[Classificacao]:
    rotulo = BOTOES.get(_fold(texto).replace("ª", "a"))
    return Classificacao(rotulo, 1.0, "botao") if rotulo else None


class Classificador(Protocol):
    nome: str

    def classificar(self, texto_anonimo: str) -> Classificacao: ...


_REGRAS = (
    ("numero_errado", ("numero errado", "nao sou aluno", "nao sou aluna", "nao conheco", "engano", "nao estudo ai", "nunca estudei")),
    ("pediu_parar", ("pare de mandar", "parem de mandar", "para de mandar", "nao quero receber", "nao me mande", "nao mandem mais", "sair da lista", "me tira", "me tire", "bloquear", "nao quero mais mensag")),
    ("reclamou", ("absurdo", "procon", "denunciar", "denuncia", "reclamacao", "reclamar", "vergonha", "desrespeito", "palhacada", "processar")),
    ("cancelar_trancar", ("cancelar", "cancelamento", "trancar", "trancamento", "desistir", "desisti")),
    ("ja_paguei", ("ja paguei", "ja foi pago", "ja esta pago", "ja efetuei", "ja fiz o pagamento", "paguei", "comprovante", "ja quitei")),
    ("negociar", ("negociar", "negociacao", "parcelar", "parcelamento", "acordo", "nao tenho condicoes", "sem condicoes")),
    ("vai_pagar", ("vou pagar", "pago amanha", "pago hoje", "pago dia", "irei pagar", "pagarei", "vou efetuar", "ate sexta", "semana que vem")),
    ("como_pagar", ("boleto", "segunda via", "2 via", "pix", "codigo de barras", "como pago", "como pagar", "link", "onde pago")),
    ("desconto_valor_prazo", ("desconto", "valor", "vencimento", "vence", "prazo")),
)


class ClassificadorRegras:
    """Palavras-chave. No teste de 12/11 acertou de 74% a 80%; serve de reserva sem chave."""

    nome = "regras"

    def classificar(self, texto_anonimo: str) -> Classificacao:
        t = _fold(texto_anonimo)
        for rotulo, termos in _REGRAS:
            if any(termo in t for termo in termos):
                return Classificacao(rotulo, 0.6, "regras")
        return Classificacao("sem_conteudo" if len(t) < 12 else "outro_assunto", 0.3, "regras")


JEV_PERGUNTA = {
    "tema": {
        "type": "choice",
        "instructions": (
            "O texto é a resposta de um aluno de faculdade a distância a um aviso de pagamento de mensalidade "
            "enviado por WhatsApp. Qual é o assunto principal da resposta?"
        ),
        "criteria": {
            "ja_paguei": "O aluno afirma que já pagou a mensalidade, enviou ou vai enviar comprovante, ou diz que a cobrança já está paga.",
            "vai_pagar": "O aluno diz que ainda vai pagar, promete pagar em uma data ou avisa quando pretende pagar.",
            "como_pagar": "O aluno pede boleto, segunda via, fatura, Pix, código de barras ou pergunta como ou onde pagar.",
            "desconto_valor_prazo": "O aluno tem dúvida sobre valor, desconto, data de vencimento ou prazo da mensalidade.",
            "negociar": "O aluno diz que não consegue pagar, está em dificuldade financeira, quer negociar, parcelar ou fazer acordo.",
            "cancelar_trancar": "O aluno quer cancelar ou trancar o curso, ou diz que já cancelou, trancou ou não estuda mais.",
            "pediu_parar": "O aluno pede para parar de receber mensagens ou lembretes, ou diz que não quer mais ser avisado.",
            "reclamou": "O aluno reclama do contato, da cobrança ou do atendimento, demonstra irritação ou ameaça denunciar.",
            "numero_errado": "A pessoa diz que não é aluna, que o número está errado ou que não conhece a instituição.",
            "outro_assunto": "Assunto que não é pagamento: acesso à plataforma, senha, aulas, provas, documentos, estágio, troca de curso.",
            "sem_conteudo": "Apenas saudação, agradecimento, ok, emoji ou texto sem pedido identificável.",
        },
    }
}

TransporteJev = Callable[[str, dict, dict], tuple[int, Any]]


def _transporte_jev(url: str, headers: dict, corpo: dict) -> tuple[int, Any]:
    req = urllib.request.Request(url, data=json.dumps(corpo).encode("utf-8"), headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "null")
    except urllib.error.HTTPError as e:
        return e.code, None


class ClassificadorJev:
    """Jev (TypeSafe System One): `POST /v1/systemone` com uma pergunta de escolha.

    Recebe só texto já mascarado. Em qualquer falha (sem chave, rede, 4xx/5xx,
    resposta fora do formato) devolve a classificação das regras, que por ter
    confiança abaixo de 0,7 vai para revisão. No teste de 12/11 (520 respostas)
    o Jev acertou de 87% a 94%, com todos os erros abaixo de 0,7 de confiança.
    """

    nome = "jev"

    def __init__(
        self,
        chave: Optional[str] = None,
        *,
        modelo: Optional[str] = None,
        transporte: TransporteJev = _transporte_jev,
        reserva: Optional[Classificador] = None,
        tentativas: int = 3,
        dormir: Callable[[float], None] = time.sleep,
    ):
        self._chave = (chave if chave is not None else os.getenv("TYPESAFE_API_KEY") or "").strip()
        self.modelo = (modelo or os.getenv("MOTOR_JEV_MODELO") or "jev-latest").strip()
        self._transporte = transporte
        self.reserva = reserva or ClassificadorRegras()
        self.tentativas = max(1, tentativas)
        self._dormir = dormir
        self.falhas = 0

    @property
    def configurado(self) -> bool:
        return bool(self._chave)

    def classificar(self, texto_anonimo: str) -> Classificacao:
        if not self._chave or not texto_anonimo.strip():
            return self.reserva.classificar(texto_anonimo)
        headers = {"Authorization": f"Bearer {self._chave}", "Content-Type": "application/json"}
        corpo = {"model": self.modelo, "state": texto_anonimo[:4000], "questions": JEV_PERGUNTA}
        for tentativa in range(self.tentativas):
            try:
                status, dados = self._transporte(JEV_URL, headers, corpo)
            except Exception:  # rede, tempo esgotado, JSON inválido
                status, dados = 0, None
            if status in (429, 529) and tentativa + 1 < self.tentativas:
                self._dormir(0.5 * 2 ** tentativa)
                continue
            resposta = ((dados or {}).get("answers") or {}).get("tema") if isinstance(dados, dict) else None
            if status == 200 and isinstance(resposta, dict) and resposta.get("choice") in ROTULOS:
                try:
                    confianca = max(0.0, min(1.0, float(resposta.get("confidence"))))
                except (TypeError, ValueError):
                    confianca = 0.0
                return Classificacao(resposta["choice"], confianca, "jev")
            break
        self.falhas += 1
        return self.reserva.classificar(texto_anonimo)


def criar_classificador() -> Classificador:
    """Jev quando há chave; senão as regras."""
    jev = ClassificadorJev()
    return jev if jev.configurado else ClassificadorRegras()


def classificar_resposta(texto: str, classificador: Classificador, nomes: Iterable[str] = ()) -> Classificacao:
    """Botão primeiro; texto livre só depois de anonimizado."""
    botao = classificar_botao(texto)
    if botao:
        return botao
    return classificador.classificar(anonimizar(texto, nomes))
