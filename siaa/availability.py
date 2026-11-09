"""Consulta GET /availability. Não sonda o SIAA e não carrega cookies."""

from __future__ import annotations


def consultar_availability(url: str, token: str, get=None, timeout: float = 6) -> dict:
    """404 é API antiga. Falha de rede fica desconhecida e não declara o SIAA indisponível."""
    base = (url or "").strip().rstrip("/")
    segredo = (token or "").strip()
    if not base or not segredo:
        return {"estado": "nao_configurado", "motivo": ""}
    pedido = get
    if pedido is None:
        import requests

        def pedido(endereco, **kwargs):
            return requests.get(endereco, **kwargs)

    try:
        resposta = pedido(
            f"{base}/availability",
            headers={"Authorization": f"Bearer {segredo}"},
            timeout=timeout,
            allow_redirects=False,
        )
    except Exception:
        return {"estado": "desconhecido", "motivo": "falha_session_api"}
    status = getattr(resposta, "status_code", 0)
    if status == 404:
        return {"estado": "api_antiga", "motivo": "sem_availability"}
    if status != 200:
        return {"estado": "desconhecido", "motivo": "falha_session_api"}
    try:
        corpo = resposta.json()
    except Exception:
        return {"estado": "desconhecido", "motivo": "falha_session_api"}
    if not isinstance(corpo, dict) or "estado" not in corpo:
        return {"estado": "desconhecido", "motivo": "falha_session_api"}
    return corpo


def decidir_consulta(situacao: dict) -> str:
    """seguir consulta o SIAA. preservar devolve o dado anterior."""
    estado = str((situacao or {}).get("estado") or "")
    if estado in {"nao_configurado", "api_antiga", "disponivel"}:
        return "seguir"
    return "preservar"


def mensagem_preservada(situacao: dict, data: str | None) -> str:
    quando = data or "data indisponivel"
    estado = str((situacao or {}).get("estado") or "")
    motivo = str((situacao or {}).get("motivo") or "")
    if estado == "pausa" and motivo == "indisponivel_siaa":
        return f"SIAA em pausa. Exibindo captura de {quando}."
    if estado == "expirada" or motivo == "sessao_expirada":
        return f"Sessao expirada. Exibindo captura de {quando}."
    if estado == "desconhecido":
        return (
            f"Falha ao consultar a session-api. Ultima captura de {quando}. "
            "O SIAA nao foi consultado."
        )
    return f"Consulta ao vivo adiada. Exibindo captura de {quando}."


def data_da_captura(captura: dict | None) -> str | None:
    if not isinstance(captura, dict):
        return None
    for chave in ("capturado_em", "created_at"):
        valor = captura.get(chave)
        if valor:
            return str(valor)
    return None
