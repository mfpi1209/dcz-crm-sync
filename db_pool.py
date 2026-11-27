"""Reaproveitamento de conexões com o Postgres (Modernização, Fase 2).

Antes, cada `get_conn()` abria uma conexão nova (TCP + autenticação) e o
`conn.close()` a derrubava. Aqui o `close()` devolve a conexão para uma pilha
por DSN e o próximo `conectar()` a reaproveita.

Por que assim, e não `psycopg2.pool`:
- o código já chama `conn.close()` em ~450 lugares; o proxy mantém essa forma
  e nenhuma chamada precisa mudar;
- não existe limite de conexões abertas: com a pilha vazia, abre uma nova,
  exatamente como antes. Só as conexões ociosas têm teto (`DB_POOL_MAX_OCIOSAS`).
  Assim não há fila nem `PoolError` novo dentro de request.

Garantias ao devolver:
- `conn.reset()` do psycopg2: desfaz transação aberta e roda `RESET ALL`
  (statement_timeout, search_path etc. voltam ao padrão);
- `autocommit` volta para False e o `readonly`/isolamento ao padrão;
- conexão fechada, quebrada ou com erro no reset é descartada.

Ao pegar: conexão parada há mais de `DB_POOL_PING_S` recebe um `SELECT 1`;
conexão com mais de `DB_POOL_VIDA_S` de vida é trocada; conexão de outro
processo (fork) nunca é usada.

`DB_POOL=0` desliga tudo: `conectar()` vira `psycopg2.connect()` puro.
"""
from __future__ import annotations

import logging
import os
import threading
import time

import psycopg2
import psycopg2.extensions

logger = logging.getLogger(__name__)


def _env_int(nome: str, padrao: int) -> int:
    try:
        return int(os.getenv(nome, str(padrao)))
    except ValueError:
        return padrao


def ligado() -> bool:
    return os.getenv("DB_POOL", "1") != "0"


MAX_OCIOSAS = _env_int("DB_POOL_MAX_OCIOSAS", 8)   # por DSN
PING_S = _env_int("DB_POOL_PING_S", 30)
VIDA_S = _env_int("DB_POOL_VIDA_S", 1800)
LOG_S = _env_int("DB_POOL_LOG_S", 600)

_trava = threading.Lock()
_pilhas: dict[tuple, list[tuple]] = {}   # chave -> [(conn, criada_em, devolvida_em)]
_pid = os.getpid()
_est = {"abertas": 0, "reusadas": 0, "descartadas": 0, "ms_abrir": 0.0}
_ultimo_log = time.monotonic()


def _chave(kwargs: dict) -> tuple:
    return tuple(sorted((k, str(v)) for k, v in kwargs.items()))


def _fechar(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass


def _abrir(kwargs: dict):
    t0 = time.perf_counter()
    conn = psycopg2.connect(**kwargs)
    ms = (time.perf_counter() - t0) * 1000
    with _trava:
        _est["abertas"] += 1
        _est["ms_abrir"] += ms
    return conn


def _talvez_logar() -> None:
    global _ultimo_log
    agora = time.monotonic()
    if LOG_S <= 0 or agora - _ultimo_log < LOG_S:
        return
    _ultimo_log = agora
    e = estatisticas()
    logger.info("[db_pool] abertas=%s reusadas=%s descartadas=%s ociosas=%s média_abrir=%.1fms",
                e["abertas"], e["reusadas"], e["descartadas"], e["ociosas"], e["media_ms_abrir"])


def _pegar_da_pilha(chave: tuple):
    """Conexão ociosa e saudável desta chave, ou None."""
    global _pid
    agora = time.monotonic()
    while True:
        with _trava:
            if os.getpid() != _pid:          # processo filho: nada herdado serve
                _pilhas.clear()
                _pid = os.getpid()
            pilha = _pilhas.get(chave)
            if not pilha:
                return None
            conn, criada, devolvida = pilha.pop()   # LIFO: a mais quente primeiro
        if conn.closed or agora - criada > VIDA_S:
            _descartar(conn)
            continue
        if agora - devolvida > PING_S:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                conn.rollback()
            except Exception:
                _descartar(conn)
                continue
        return conn, criada


def _descartar(conn) -> None:
    _fechar(conn)
    with _trava:
        _est["descartadas"] += 1


def _devolver(chave: tuple, conn, criada: float) -> None:
    if conn.closed or os.getpid() != _pid:
        _descartar(conn)
        return
    try:
        conn.rollback()                    # fora de transação antes de mexer na sessão
        conn.reset()                       # ROLLBACK + RESET ALL + SET SESSION AUTHORIZATION DEFAULT
        if (conn.autocommit or conn.readonly is not None or conn.deferrable is not None
                or conn.isolation_level is not None):
            conn.set_session(isolation_level="DEFAULT", readonly="DEFAULT",
                             deferrable="DEFAULT", autocommit=False)
        if conn.status != psycopg2.extensions.STATUS_READY:
            raise RuntimeError("conexão não voltou ao estado pronto")
    except Exception:
        _descartar(conn)
        return
    with _trava:
        pilha = _pilhas.setdefault(chave, [])
        if len(pilha) < MAX_OCIOSAS:
            pilha.append((conn, criada, time.monotonic()))
            return
    _descartar(conn)


class ConexaoReusavel:
    """Proxy da conexão psycopg2: tudo é repassado, menos `close()`, que devolve à pilha."""

    __slots__ = ("_conn", "_chave", "_criada", "_devolvida")

    def __init__(self, conn, chave: tuple, criada: float):
        self._conn = conn
        self._chave = chave
        self._criada = criada
        self._devolvida = False

    def __getattr__(self, nome):
        if nome in ConexaoReusavel.__slots__:   # slot ainda vazio: não recursar
            raise AttributeError(nome)
        return getattr(self._conn, nome)

    def __setattr__(self, nome, valor):
        if nome in ConexaoReusavel.__slots__:
            object.__setattr__(self, nome, valor)
        else:
            setattr(self._conn, nome, valor)

    @property
    def closed(self):
        return 1 if self._devolvida else self._conn.closed

    def cursor(self, *a, **k):
        if self._devolvida:
            raise psycopg2.InterfaceError("connection already closed")
        return self._conn.cursor(*a, **k)

    def close(self):
        if self._devolvida:
            return
        self._devolvida = True
        _devolver(self._chave, self._conn, self._criada)

    # `with conn:` do psycopg2 faz commit/rollback e NÃO fecha; mantém igual.
    def __enter__(self):
        self._conn.__enter__()
        return self

    def __exit__(self, *exc):
        return self._conn.__exit__(*exc)

    def __del__(self):
        # Esquecida sem close(): fecha de vez, como o GC fazia antes (sem travar nada aqui).
        try:
            if not self._devolvida:
                self._devolvida = True
                self._conn.close()
        except Exception:
            pass


def conectar(**kwargs):
    """Igual a `psycopg2.connect(**kwargs)`, reaproveitando conexões ociosas do mesmo DSN."""
    if not ligado():
        return psycopg2.connect(**kwargs)
    chave = _chave(kwargs)
    achada = _pegar_da_pilha(chave)
    if achada:
        conn, criada = achada
        with _trava:
            _est["reusadas"] += 1
    else:
        conn, criada = _abrir(kwargs), time.monotonic()
    _talvez_logar()
    return ConexaoReusavel(conn, chave, criada)


def estatisticas() -> dict:
    with _trava:
        abertas = _est["abertas"]
        return {
            "ligado": ligado(),
            "abertas": abertas,
            "reusadas": _est["reusadas"],
            "descartadas": _est["descartadas"],
            "ociosas": sum(len(p) for p in _pilhas.values()),
            "media_ms_abrir": round(_est["ms_abrir"] / abertas, 1) if abertas else 0.0,
        }


def esvaziar() -> None:
    """Fecha todas as ociosas (testes e desligamento)."""
    with _trava:
        todas = [c for p in _pilhas.values() for c, _, _ in p]
        _pilhas.clear()
    for c in todas:
        _fechar(c)
