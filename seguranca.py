"""Segurança da sessão e das senhas (Fase 1 da modernização).

- `chave_da_sessao`: a SECRET_KEY assina o cookie de sessão, onde ficam usuário e
  papel. Sem a variável, o painel usava uma chave fixa publicada no GitHub, com a
  qual qualquer pessoa forjava uma sessão de admin. Agora, sem a variável (ou com
  o valor antigo), gera uma chave aleatória por processo e avisa no log. O painel
  continua subindo; o custo é todo mundo logar de novo a cada reinício.
- `configurar_cookies`: HttpOnly, SameSite=Lax e Secure (este por variável,
  para o localhost em http continuar funcionando).
- `gerar_hash` / `conferir_senha`: senha com hash lento e com sal (scrypt do
  Werkzeug). O hash antigo (SHA-256 sem sal, 64 hex) continua aceito e é trocado
  pelo novo no próximo login de cada usuário.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import re
import secrets

from werkzeug.security import check_password_hash, generate_password_hash

logger = logging.getLogger(__name__)

CHAVE_ANTIGA_PUBLICA = "dcz-sync-default-key-change-me"
_HASH_ANTIGO = re.compile(r"^[0-9a-f]{64}$")


def chave_da_sessao() -> str:
    chave = (os.getenv("SECRET_KEY") or "").strip()
    if chave and chave != CHAVE_ANTIGA_PUBLICA:
        return chave
    logger.warning(
        "SECRET_KEY ausente (ou com o valor público antigo): usando chave aleatória deste processo. "
        "As sessões caem a cada reinício e NÃO funcionam com mais de um worker. Defina SECRET_KEY no ambiente."
    )
    return secrets.token_hex(32)


def configurar_cookies(app) -> None:
    app.config["SESSION_COOKIE_HTTPONLY"] = True
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_SECURE"] = os.getenv("SESSION_COOKIE_SECURE", "0") == "1"


def gerar_hash(senha: str) -> str:
    return generate_password_hash(senha)


def _hash_antigo(senha: str) -> str:
    return hashlib.sha256(senha.encode("utf-8")).hexdigest()


def eh_hash_antigo(hash_salvo: str | None) -> bool:
    return bool(hash_salvo) and bool(_HASH_ANTIGO.match(hash_salvo))


def conferir_senha(senha: str, hash_salvo: str | None) -> tuple[bool, bool]:
    """Devolve (senha_confere, precisa_regravar_o_hash)."""
    if not hash_salvo or senha is None:
        return False, False
    if eh_hash_antigo(hash_salvo):
        ok = hmac.compare_digest(_hash_antigo(senha), hash_salvo)
        return ok, ok
    try:
        return check_password_hash(hash_salvo, senha), False
    except (ValueError, TypeError):
        return False, False
