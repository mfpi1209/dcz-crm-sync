"""Sorteio determinístico do grupo de controle.

5% de cada público não recebe nada no ciclo. O sorteio é uma função do
identificador do aluno e do ciclo: a mesma entrada dá sempre a mesma resposta,
em qualquer máquina, sem guardar semente. Como é uniforme por aluno, qualquer
recorte do público (frente, estado, calouro) fica com cerca de 5% de controle.

Depois que o livro-razão grava o resultado do ciclo, vale o que está gravado:
trocar o sal não muda o controle de um ciclo já aberto.
"""
from __future__ import annotations

import hashlib

_FAIXAS = 10_000


def faixa(aluno_id: str, ciclo: str, sal: str) -> int:
    """Número estável de 0 a 9999 para o par (aluno, ciclo)."""
    h = hashlib.sha256(f"{sal}|{ciclo}|{aluno_id}".encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") % _FAIXAS


def no_controle(aluno_id: str, ciclo: str, sal: str, pct: float = 5.0) -> bool:
    if not aluno_id or pct <= 0:
        return False
    return faixa(aluno_id, ciclo, sal) < round(pct * _FAIXAS / 100)


def ordem_estavel(aluno_id: str, chave: str) -> int:
    """Ordem pseudoaleatória e reproduzível dentro de um lote (sem viés por id)."""
    h = hashlib.sha256(f"ordem|{chave}|{aluno_id}".encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big")
