"""
Cliente HTTP para a API Kommo v4.
Implementa rate limiting (token bucket), retry com backoff exponencial,
e paginação automática.
"""

import time
import logging
import threading
import requests
from collections import deque

from config import (
    KOMMO_BASE_URL,
    KOMMO_TOKEN,
    RATE_LIMIT_REQUESTS,
    RATE_LIMIT_PERIOD_SECONDS,
    PAGE_SIZE,
)

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Rate limiter usando sliding window.
    Garante no máximo RATE_LIMIT_REQUESTS requests em RATE_LIMIT_PERIOD_SECONDS.
    Thread-safe.
    """

    def __init__(self, max_requests: int, period_seconds: int):
        self.max_requests = max_requests
        self.period_seconds = period_seconds
        self.timestamps: deque = deque()
        self.lock = threading.Lock()
        # Margem de segurança: usar 90% da capacidade
        self.effective_max = int(max_requests * 0.90)
        logger.info(
            "Rate limiter configurado: %d req/%ds (efetivo: %d)",
            max_requests, period_seconds, self.effective_max
        )

    def wait_if_needed(self):
        """Espera se necessário para respeitar o rate limit."""
        with self.lock:
            now = time.monotonic()

            # Remove timestamps fora da janela
            while self.timestamps and (now - self.timestamps[0]) > self.period_seconds:
                self.timestamps.popleft()

            if len(self.timestamps) >= self.effective_max:
                # Calcula quanto tempo esperar
                oldest = self.timestamps[0]
                wait_time = self.period_seconds - (now - oldest) + 0.5  # +0.5s de margem
                if wait_time > 0:
                    logger.debug("Rate limit atingido. Aguardando %.1fs...", wait_time)
                    time.sleep(wait_time)

                # Limpa timestamps expirados após espera
                now = time.monotonic()
                while self.timestamps and (now - self.timestamps[0]) > self.period_seconds:
                    self.timestamps.popleft()

            self.timestamps.append(time.monotonic())


class KommoAPIClient:
    """
    Cliente para a API Kommo v4.
    Features:
    - Rate limiting automático (120 req/min com margem de segurança)
    - Retry com backoff exponencial para erros transitórios
    - Paginação automática
    - Logging detalhado
    """

    def __init__(self):
        self.base_url = KOMMO_BASE_URL.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {KOMMO_TOKEN}",
        })
        self.rate_limiter = RateLimiter(RATE_LIMIT_REQUESTS, RATE_LIMIT_PERIOD_SECONDS)
        self.total_requests = 0

    def _request(self, method: str, endpoint: str, params: dict = None,
                 max_retries: int = 5) -> dict | None:
        """
        Executa uma requisição HTTP com rate limiting e retry.
        
        Retorna o JSON da resposta ou None se não houver dados.
        Levanta exceção em caso de erro irrecuperável.
        """
        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        for attempt in range(max_retries):
            # Rate limiting
            self.rate_limiter.wait_if_needed()

            try:
                self.total_requests += 1
                response = self.session.request(method, url, params=params, timeout=30)

                # Log da requisição
                logger.debug(
                    "[#%d] %s %s -> %d",
                    self.total_requests, method.upper(), url, response.status_code
                )

                # Sucesso
                if response.status_code == 200:
                    return response.json()

                # Sem conteúdo (lista vazia)
                if response.status_code == 204:
                    logger.info("Sem dados retornados (204) para %s", endpoint)
                    return None

                # Rate limited pelo servidor
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 5))
                    logger.warning(
                        "Rate limited pelo servidor (429). Aguardando %ds... (tentativa %d/%d)",
                        retry_after, attempt + 1, max_retries
                    )
                    time.sleep(retry_after)
                    continue

                # Erro de autenticação
                if response.status_code == 401:
                    logger.error("Token expirado ou inválido (401). Verifique KOMMO_TOKEN.")
                    raise PermissionError("Token de autenticação inválido ou expirado.")

                # Erro do servidor (5xx) - retry
                if response.status_code >= 500:
                    wait = min(2 ** attempt, 30)
                    logger.warning(
                        "Erro do servidor (%d). Retry em %ds... (tentativa %d/%d)",
                        response.status_code, wait, attempt + 1, max_retries
                    )
                    time.sleep(wait)
                    continue

                # Outros erros (4xx) - não faz retry
                logger.error(
                    "Erro na API (%d): %s - %s",
                    response.status_code, url, response.text[:500]
                )
                response.raise_for_status()

            except requests.exceptions.ConnectionError as e:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Erro de conexão: %s. Retry em %ds... (tentativa %d/%d)",
                    str(e)[:200], wait, attempt + 1, max_retries
                )
                time.sleep(wait)

            except requests.exceptions.Timeout:
                wait = min(2 ** attempt, 30)
                logger.warning(
                    "Timeout na requisição. Retry em %ds... (tentativa %d/%d)",
                    wait, attempt + 1, max_retries
                )
                time.sleep(wait)

        logger.error("Máximo de retries (%d) atingido para %s", max_retries, url)
        raise ConnectionError(f"Falha após {max_retries} tentativas: {url}")

    def get(self, endpoint: str, params: dict = None) -> dict | None:
        """Executa GET request."""
        return self._request("GET", endpoint, params=params)

    def get_all_pages(self, endpoint: str, params: dict = None,
                      embedded_key: str = None) -> list[dict]:
        """
        Pagina automaticamente por todos os resultados de um endpoint.
        
        Args:
            endpoint: Endpoint da API (ex: "leads")
            params: Parâmetros de query adicionais
            embedded_key: Chave dentro de _embedded (ex: "leads", "contacts")
                         Se None, tenta inferir do endpoint.
        
        Returns:
            Lista com todos os registros de todas as páginas.
        """
        if params is None:
            params = {}

        if embedded_key is None:
            # Inferir do endpoint: "leads" -> "leads", "contacts" -> "contacts"
            embedded_key = endpoint.split("/")[-1].split("?")[0]

        params["limit"] = PAGE_SIZE
        page = 1
        all_records = []

        while True:
            params["page"] = page
            logger.info(
                "Buscando %s - página %d (acumulado: %d registros)...",
                endpoint, page, len(all_records)
            )

            data = self.get(endpoint, params=params)

            if data is None:
                break

            embedded = data.get("_embedded", {})
            records = embedded.get(embedded_key, [])

            if not records:
                break

            all_records.extend(records)
            logger.info(
                "Página %d: %d registros recebidos (total acumulado: %d)",
                page, len(records), len(all_records)
            )

            # Verificar se há próxima página
            links = data.get("_links", {})
            if "next" not in links:
                break

            page += 1

        logger.info(
            "Paginação completa para %s: %d registros totais em %d páginas",
            endpoint, len(all_records), page
        )
        return all_records

    def get_stats(self) -> dict:
        """Retorna estatísticas do cliente."""
        return {
            "total_requests": self.total_requests,
        }
