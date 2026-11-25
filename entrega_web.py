"""Entrega do painel ao navegador: compressão e cache dos estáticos (Fase 0).

Medição em produção (25/11/2026): o HTML de 1,8 MB e os 55 JS saíam sem
compressão, pelas mesmas 4 threads do Gunicorn que atendem a API, e o `?v=`
aleatório do index.html obrigava a baixar todos os JS a cada abertura.

- HTML, JSON, CSS e JS dinâmicos: comprimidos pelo Flask-Compress (br/gzip).
- /static: servido pelo WhiteNoise, antes do Flask, com as versões .gz/.br
  geradas no build da imagem (`python -m whitenoise.compress static`).
- Cache: o pedido com `?v=` igual à versão do conteúdo deste processo fica em
  cache por 1 ano (`immutable`). Qualquer outro pedido (sem `?v=`, versão de
  outro deploy, modo de desenvolvimento) revalida a cada vez (`no-cache` + ETag),
  que é o comportamento de antes. Assim, num deploy em que o container velho e o
  novo respondem ao mesmo tempo, um conteúdo antigo nunca fica preso no cache
  sob a versão nova.
"""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from urllib.parse import parse_qs

from flask import Flask
from flask_compress import Compress
from whitenoise import WhiteNoise

UM_ANO_S = 31_536_000
CACHE_LONGO = f"public, max-age={UM_ANO_S}, immutable"
CACHE_REVALIDAR = "no-cache"

COMPRESS_MIMETYPES = [
    "text/html",
    "application/json",
    "text/css",
    "application/javascript",
    "text/javascript",
]

_SUFIXOS_COMPRIMIDOS = (".gz", ".br")


def versao_estaticos(static_dir: str | Path) -> str:
    """Hash do conteúdo de static/ (muda só quando algum arquivo muda)."""
    raiz = Path(static_dir)
    h = hashlib.sha1()
    for p in sorted(raiz.rglob("*")):
        if not p.is_file() or p.suffix in _SUFIXOS_COMPRIMIDOS:
            continue
        h.update(p.relative_to(raiz).as_posix().encode("utf-8"))
        h.update(b"\0")
        h.update(p.read_bytes())
    return h.hexdigest()[:12]


def autorefresh_padrao() -> bool:
    """Desenvolvimento relê os arquivos a cada pedido; a imagem Docker desliga (STATIC_AUTOREFRESH=0)."""
    return os.getenv("STATIC_AUTOREFRESH", "1") != "0"


class _CachePorVersao:
    """Define o Cache-Control de /static/ conforme o `?v=` do pedido."""

    def __init__(self, wsgi_app, versao: str, prefixo: str, cache_longo: bool):
        self.wsgi_app = wsgi_app
        self.versao = versao
        self.prefixo = prefixo
        self.cache_longo = cache_longo

    def _cabecalho(self, environ) -> str:
        if not self.cache_longo:
            return CACHE_REVALIDAR
        v = (parse_qs(environ.get("QUERY_STRING", "")).get("v") or [""])[0]
        # O index.html usa ?v=<versão>-<sufixo manual>, ex.: ?v=ab12cd34ef56-hide-crgm
        if v == self.versao or v.startswith(self.versao + "-"):
            return CACHE_LONGO
        return CACHE_REVALIDAR

    def __call__(self, environ, start_response):
        if not environ.get("PATH_INFO", "").startswith(self.prefixo):
            return self.wsgi_app(environ, start_response)
        valor = self._cabecalho(environ)

        def _start(status, headers, exc_info=None):
            headers = [(k, v) for k, v in headers if k.lower() != "cache-control"]
            headers.append(("Cache-Control", valor))
            return start_response(status, headers, exc_info)

        return self.wsgi_app(environ, _start)


def configurar_entrega(app: Flask, static_dir: str | Path, versao: str,
                       autorefresh: bool | None = None) -> None:
    """Liga a compressão e o WhiteNoise no app. Chamar uma vez, logo após criar o Flask."""
    if autorefresh is None:
        autorefresh = autorefresh_padrao()

    app.config.setdefault("COMPRESS_ALGORITHM", ["br", "gzip"])
    app.config.setdefault("COMPRESS_MIMETYPES", COMPRESS_MIMETYPES)
    app.config.setdefault("COMPRESS_MIN_SIZE", 500)
    app.config.setdefault("COMPRESS_BR_LEVEL", 4)
    app.config.setdefault("COMPRESS_LEVEL", 6)
    Compress(app)

    servidor = WhiteNoise(
        app.wsgi_app,
        root=str(static_dir),
        prefix="static/",
        autorefresh=autorefresh,
        max_age=None,  # quem define o Cache-Control é o _CachePorVersao
    )
    app.wsgi_app = _CachePorVersao(servidor, versao, "/static/", cache_longo=not autorefresh)
