"""Classificação das respostas dos alunos.

  - clique em botão é tratado em código, pelo próprio botão, sem classificador
  - texto livre vai para um `Classificador`: regras por palavra-chave (sem
    chave nenhuma) ou o Jev da TypeSafe (ponto de extensão)
  - confiança abaixo de 0,7 vai para revisão humana

Antes de sair do processo, todo texto passa por `anonimizar`: o Jev só recebe
texto sem nome, telefone, CPF, RGM e e-mail. O uso do Jev em produção com
mensagens de alunos depende do aval de quem responde pela LGPD.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Iterable, Optional, Protocol

ROTULOS = ("ja_paguei", "vai_pagar", "como_pagar", "negociar", "cancelar_trancar", "outro")
CONFIANCA_MINIMA = 0.7

BOTOES = {
    "ja paguei": "ja_paguei",
    "quero pagar": "como_pagar",
    "2a via": "como_pagar",
    "preciso de ajuda": "outro",
    "quero negociar": "negociar",
    "quero trancar ou cancelar": "cancelar_trancar",
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


def anonimizar(texto: str, nomes: Iterable[str] = ()) -> str:
    """Tira e-mail, números longos e os nomes informados. Devolve o texto pronto para sair."""
    limpo = _EMAIL.sub("[email]", texto or "")
    limpo = _NUMERO.sub("[numero]", limpo)
    partes = {p for nome in nomes for p in _fold(nome).split() if len(p) >= 3}
    if partes:
        def troca(m: re.Match) -> str:
            return "[nome]" if _fold(m.group(0)) in partes else m.group(0)

        limpo = re.sub(r"[^\W\d_]+", troca, limpo)
    return limpo


def classificar_botao(texto: str) -> Optional[Classificacao]:
    rotulo = BOTOES.get(_fold(texto).replace("ª", "a"))
    return Classificacao(rotulo, 1.0, "botao") if rotulo else None


class Classificador(Protocol):
    nome: str

    def classificar(self, texto_anonimo: str) -> Classificacao: ...


_REGRAS = (
    ("cancelar_trancar", ("cancelar", "cancelamento", "trancar", "trancamento", "desistir", "desisti")),
    ("ja_paguei", ("ja paguei", "ja foi pago", "ja esta pago", "ja efetuei", "ja fiz o pagamento", "paguei", "comprovante", "ja quitei")),
    ("negociar", ("negociar", "negociacao", "parcelar", "parcelamento", "acordo", "desconto maior", "nao tenho condicoes", "sem condicoes")),
    ("vai_pagar", ("vou pagar", "pago amanha", "pago hoje", "pago dia", "irei pagar", "pagarei", "vou efetuar", "ate sexta", "semana que vem")),
    ("como_pagar", ("boleto", "segunda via", "2 via", "pix", "codigo de barras", "como pago", "como pagar", "link", "onde pago")),
)


class ClassificadorRegras:
    """Palavras-chave. No teste de 12/11 acertou de 74% a 80%; serve de reserva sem chave."""

    nome = "regras"

    def classificar(self, texto_anonimo: str) -> Classificacao:
        t = _fold(texto_anonimo)
        for rotulo, termos in _REGRAS:
            if any(termo in t for termo in termos):
                return Classificacao(rotulo, 0.6, "regras")
        return Classificacao("outro", 0.3, "regras")


class ClassificadorJev:
    """Ponto de extensão do Jev (TypeSafe, `POST https://api.typesafe.ai/v1/systemone`).

    Ainda não chama a API. Entra quando houver a chave `TYPESAFE_API_KEY` e o
    aval de LGPD. Contrato esperado: uma pergunta de escolha com `ROTULOS`,
    resposta com rótulo e confiança; abaixo de 0,7 vai para revisão. No teste
    de 12/11 (520 respostas) acertou de 87% a 94%, com todos os erros abaixo
    de 0,7 de confiança.
    """

    nome = "jev"

    def classificar(self, texto_anonimo: str) -> Classificacao:
        raise NotImplementedError("classificador Jev ainda não implementado")


def classificar_resposta(texto: str, classificador: Classificador, nomes: Iterable[str] = ()) -> Classificacao:
    """Botão primeiro; texto livre só depois de anonimizado."""
    botao = classificar_botao(texto)
    if botao:
        return botao
    return classificador.classificar(anonimizar(texto, nomes))
