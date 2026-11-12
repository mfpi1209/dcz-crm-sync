"""Cálculo do NPS e leitura por recorte. Python puro, sem banco e sem rede.

NPS = % de promotores (9 e 10) menos % de detratores (0 a 6), de -100 a +100.
A margem é o intervalo de 95% da diferença de duas proporções da mesma amostra.
"""
from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional

# Abaixo de N_MIN_PUBLICAR o número não é mostrado; abaixo de N_MIN_COMPARAR
# ele aparece, mas não serve para comparar dois recortes (margem perto de ±15).
N_MIN_PUBLICAR = 30
N_MIN_COMPARAR = 100
Z95 = 1.96

RESOLVIDO = ("sim", "parte", "nao")
_NOTA_RE = re.compile(r"(?<!\d)(10|[0-9])(?!\d)")
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_DIGITOS_RE = re.compile(r"\d{5,}")


def _fold(s: Any) -> str:
    n = unicodedata.normalize("NFKD", str(s or ""))
    return "".join(c for c in n if not unicodedata.combining(c)).lower().strip()


def ler_nota(valor: Any) -> Optional[int]:
    """Nota de 0 a 10 como veio do formulário ("9", "9 - Muito provável", "nota_9"). Sem nota = None."""
    if isinstance(valor, bool):
        return None
    if isinstance(valor, (int, float)):
        return int(valor) if float(valor).is_integer() and 0 <= valor <= 10 else None
    achado = _NOTA_RE.search(str(valor or ""))
    return int(achado.group(1)) if achado else None


def ler_resolvido(valor: Any) -> Optional[str]:
    """"Sim" / "Em parte" / "Não" do formulário. O que não der para ler fica None (não adivinha)."""
    v = _fold(valor)
    if not v:
        return None
    if "parte" in v or "parcial" in v:
        return "parte"
    if re.search(r"\bnao\b", v.replace("_", " ")):
        return "nao"
    if re.search(r"\bsim\b", v.replace("_", " ")):
        return "sim"
    return None


def mascarar(texto: Any, limite: int = 1000) -> str:
    """Tira e-mail e qualquer sequência de 5 ou mais dígitos do comentário do aluno."""
    t = _EMAIL_RE.sub("[e-mail]", str(texto or "").strip())
    return _DIGITOS_RE.sub("[número]", t)[:limite]


def classe(nota: int) -> str:
    if nota >= 9:
        return "promotor"
    if nota >= 7:
        return "neutro"
    return "detrator"


def leitura(n: int) -> str:
    if n < N_MIN_PUBLICAR:
        return "insuficiente"
    if n < N_MIN_COMPARAR:
        return "indicativo"
    return "comparavel"


def margem(promotores: int, detratores: int, n: int) -> Optional[float]:
    """Meia-largura do intervalo de 95% do NPS, em pontos."""
    if n <= 0:
        return None
    p, d = promotores / n, detratores / n
    variancia = max(p + d - (p - d) ** 2, 0.0)
    return round(100 * Z95 * math.sqrt(variancia / n), 1)


@dataclass(frozen=True)
class Resumo:
    envios: int
    respostas: int
    promotores: int
    neutros: int
    detratores: int
    resolvido_sim: int
    resolvido_parte: int
    resolvido_nao: int

    @property
    def nps(self) -> Optional[float]:
        if self.respostas < N_MIN_PUBLICAR:
            return None
        return round(100 * (self.promotores - self.detratores) / self.respostas, 1)

    def como_dict(self) -> dict:
        n = self.respostas
        declarados = self.resolvido_sim + self.resolvido_parte + self.resolvido_nao
        publica = n >= N_MIN_PUBLICAR
        return {
            "envios": self.envios,
            "respostas": n,
            "taxa_resposta": round(100 * n / self.envios, 1) if self.envios else None,
            "leitura": leitura(n),
            "nps": self.nps,
            "margem": margem(self.promotores, self.detratores, n) if publica else None,
            "promotores_pct": round(100 * self.promotores / n, 1) if publica else None,
            "neutros_pct": round(100 * self.neutros / n, 1) if publica else None,
            "detratores_pct": round(100 * self.detratores / n, 1) if publica else None,
            "resolvido_respostas": declarados,
            "resolvido_sim_pct": round(100 * self.resolvido_sim / declarados, 1) if declarados >= N_MIN_PUBLICAR else None,
            "resolvido_nao_pct": round(100 * self.resolvido_nao / declarados, 1) if declarados >= N_MIN_PUBLICAR else None,
        }


def resumir(linhas: Iterable[Mapping[str, Any]]) -> Resumo:
    """Cada linha é uma pesquisa enviada: `nota` (0–10 ou None) e `resolvido` (sim/parte/nao ou None)."""
    envios = respostas = prom = neu = det = 0
    res = {k: 0 for k in RESOLVIDO}
    for linha in linhas:
        envios += 1
        nota = linha.get("nota")
        if nota is None:
            continue
        respostas += 1
        c = classe(int(nota))
        prom += c == "promotor"
        neu += c == "neutro"
        det += c == "detrator"
        if linha.get("resolvido") in res:
            res[linha["resolvido"]] += 1
    return Resumo(envios, respostas, prom, neu, det, res["sim"], res["parte"], res["nao"])


def por_recorte(linhas: Iterable[Mapping[str, Any]], chave: str, vazio: str = "(sem informação)") -> list[dict]:
    """Agrupa por um campo da linha e devolve os recortes, do que tem mais respostas para o que tem menos."""
    grupos: dict[str, list] = {}
    for linha in linhas:
        grupos.setdefault(str(linha.get(chave) or vazio), []).append(linha)
    saida = [{"recorte": nome, **resumir(itens).como_dict()} for nome, itens in grupos.items()]
    return sorted(saida, key=lambda r: (-r["respostas"], r["recorte"]))


def diferenca_e_real(a: Mapping[str, Any], b: Mapping[str, Any]) -> Optional[bool]:
    """Dois recortes diferem de verdade? Só responde quando os dois têm amostra para comparar."""
    if a.get("leitura") != "comparavel" or b.get("leitura") != "comparavel":
        return None
    folga = math.sqrt(a["margem"] ** 2 + b["margem"] ** 2)
    return abs(a["nps"] - b["nps"]) > folga
