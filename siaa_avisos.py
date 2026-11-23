"""Incidentes SIAA -> Avisos do painel (mesma tabela, mesma visibilidade, mesmo popup).

Fonte: ``GET /alertas`` da session-api (só leitura do estado gravado pelo serviço ``alertas``; nada
consulta o SIAA nem dispara renovação). Um job do APScheduler do painel chama ``sincronizar`` a cada
``INTERVALO_S``, sem depender de alguém com a página aberta. Ligado só com
``SIAA_AVISOS_INCIDENTES=1`` (padrão desligado).

Regras:
- Um aviso por ocorrência (``ocorrencia`` da API), compartilhado pelos destinatários; a leitura é
  individual (``aviso_lido``, como nos avisos manuais). A tabela ``siaa_incidente_aviso`` guarda o
  vínculo ocorrência -> aviso com chave primária: polling, reinício ou dois processos não duplicam
  (trava ``pg_advisory_xact_lock`` + chave única, aviso e vínculo na mesma transação).
- Destinatários exclusivos (``DESTINATARIOS``), resolvidos em ``app_users.username``. Ausente ->
  erro no log e no estado; nunca vira aviso para todos (lista vazia = nenhum aviso).
- Recuperação só quando a API a declara (lista ``recuperados``): o mesmo aviso é reescrito como
  recuperado (prioridade normal, expira em ``EXPIRA_RECUPERADO``), sem apagar a leitura de ninguém:
  quem já leu não recebe popup de novo; quem não leu vê o texto atualizado, não a falha antiga.
- API indisponível, 503 ou resposta inesperada: nada é criado nem encerrado.
- Textos escapados (o corpo do aviso é renderizado como HTML); sem cookie, token, cURL, ViewState
  ou dado de aluno: só título, módulo, motivo e horários de Brasília vindos da API.
"""
from __future__ import annotations

import html
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

DESTINATARIOS = ("fabio@eduit.com.br", "marcelo@eduit.com.br")
INTERVALO_S = 60
EXPIRA_RECUPERADO = timedelta(days=7)
TRAVA = 7_310_455_001  # pg_advisory_xact_lock: serializa a sincronização entre processos
FUSO = ZoneInfo("America/Sao_Paulo")
TIPOS = {"renovacao", "executor", "keepalive", "estado_ilegivel", "status_ilegivel"}
_OCORRENCIA = re.compile(r"^[0-9a-f]{20}$")
_TOKENISH = re.compile(r"JSESSIONID|ViewState|Bearer|curl\s|cookie|[A-Za-z0-9+/_=-]{40,}", re.I)
_LINK = ('<a href="#conexao_siaa" onclick="var o=document.getElementById(\'avisos-popup-overlay\');'
         'if(o)o.remove();" class="underline font-semibold">Abrir Conexão SIAA</a>')

DDL = """
CREATE TABLE IF NOT EXISTS siaa_incidente_aviso (
    ocorrencia     TEXT NOT NULL,            -- id da API
    geracao        INTEGER NOT NULL DEFAULT 1, -- reabertura do mesmo id depois de recuperado
    tipo           TEXT NOT NULL,
    aviso_id       INTEGER REFERENCES avisos(id) ON DELETE SET NULL,
    estado         TEXT NOT NULL,            -- aberto | recuperado | recuperado_sem_aviso
    titulo         TEXT NOT NULL,
    modulo         TEXT NOT NULL DEFAULT '',
    motivo         TEXT NOT NULL DEFAULT '',
    aberto_em      TIMESTAMPTZ,
    recuperado_em  TIMESTAMPTZ,
    criado_em      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    atualizado_em  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ocorrencia, geracao)
)
"""


def ativo() -> bool:
    return (os.environ.get("SIAA_AVISOS_INCIDENTES") or "").strip() == "1"


# ---------------------------------------------------------------------------
# Validação da resposta da API (lista branca)
# ---------------------------------------------------------------------------

def _texto(v, n=400) -> str:
    if not isinstance(v, str):
        return ""
    v = " ".join(v.split())[:n]
    return "" if _TOKENISH.search(v) else v


def _quando(v) -> datetime | None:
    if not isinstance(v, str):
        return None
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else None


def _item(d, campos_data) -> dict | None:
    if not isinstance(d, dict):
        return None
    oc, tipo = d.get("ocorrencia"), d.get("tipo")
    if not isinstance(oc, str) or not _OCORRENCIA.match(oc) or tipo not in TIPOS:
        return None
    item = {"ocorrencia": oc, "tipo": tipo, "titulo": _texto(d.get("titulo"), 120) or "Incidente SIAA",
            "modulo": _texto(d.get("modulo"), 160), "motivo": _texto(d.get("motivo"))}
    for c in campos_data:
        item[c] = _quando(d.get(c))
    return item


def validar(dados) -> dict | None:
    """Resposta de ``GET /alertas`` saneada, ou None (não confiável: não abre nem fecha nada)."""
    if not isinstance(dados, dict) or dados.get("ok") is not True:
        return None
    if not isinstance(dados.get("abertos"), list) or not isinstance(dados.get("recuperados"), list):
        return None
    abertos = [i for i in (_item(d, ("aberto_em",)) for d in dados["abertos"]) if i]
    rec = [i for i in (_item(d, ("aberto_em", "recuperado_em")) for d in dados["recuperados"]) if i]
    return {"existe": bool(dados.get("existe")), "avaliado_em": _quando(dados.get("avaliado_em")),
            "abertos": abertos, "recuperados": [r for r in rec if r["recuperado_em"]]}


# ---------------------------------------------------------------------------
# Texto dos avisos
# ---------------------------------------------------------------------------

def hora_brt(dt: datetime | None) -> str:
    return dt.astimezone(FUSO).strftime("%d/%m/%Y %H:%M") if dt else "—"


def aviso_aberto(inc: dict) -> tuple[str, str]:
    e = html.escape
    linhas = [f"<b>Módulo:</b> {e(inc['modulo'] or '—')}",
              f"<b>Motivo:</b> {e(inc['motivo'] or 'sem detalhe')}",
              f"<b>Desde:</b> {hora_brt(inc.get('aberto_em'))} (horário de Brasília)",
              _LINK]
    return f"SIAA: {e(inc['titulo'])}", "\n".join(linhas)


def aviso_recuperado(vinc: dict, rec: dict) -> tuple[str, str]:
    e = html.escape
    titulo = vinc.get("titulo") or rec["titulo"]
    linhas = [f"<b>Módulo:</b> {e(vinc.get('modulo') or rec['modulo'] or '—')}",
              f"<b>Situação:</b> normalizado em {hora_brt(rec['recuperado_em'])} (horário de Brasília).",
              f"<b>Incidente aberto em:</b> {hora_brt(vinc.get('aberto_em') or rec.get('aberto_em'))}.",
              f"<b>Motivo original:</b> {e(vinc.get('motivo') or '—')}",
              _LINK]
    return f"SIAA recuperado: {e(titulo)}", "\n".join(linhas)


# ---------------------------------------------------------------------------
# Banco
# ---------------------------------------------------------------------------

def garantir_tabela(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.commit()


def resolver_destinatarios(cur) -> tuple[list[int], list[str]]:
    """IDs dos destinatários exclusivos e e-mails não encontrados."""
    cur.execute("SELECT id, lower(username) FROM app_users WHERE lower(username) = ANY(%s)",
                (list(DESTINATARIOS),))
    achados = {nome: uid for uid, nome in cur.fetchall()}
    ids = sorted({achados[n] for n in DESTINATARIOS if n in achados})
    ausentes = [n for n in DESTINATARIOS if n not in achados]
    return ids, ausentes


def _criar_aviso(cur, ids, titulo, corpo, prioridade):
    from helpers import criar_aviso_para_usuarios
    return criar_aviso_para_usuarios(ids, titulo, corpo, prioridade=prioridade, cur=cur)


# Erro de destinatário: no log quando muda ou a cada hora (o job roda a cada 60 s).
_LOG_ERRO: dict = {}
LOG_ERRO_REPETE = timedelta(hours=1)


def _log_erro(chave: str, msg: str, agora: datetime, *args) -> None:
    ultimo = _LOG_ERRO.get(chave)
    if ultimo is None or agora - ultimo >= LOG_ERRO_REPETE:
        _LOG_ERRO[chave] = agora
        logger.error(msg, *args)


def _ultimo_vinculo(cur, ocorrencia):
    cur.execute("""SELECT geracao, aviso_id, estado, titulo, modulo, motivo, aberto_em, recuperado_em
                     FROM siaa_incidente_aviso WHERE ocorrencia = %s
                    ORDER BY geracao DESC LIMIT 1""", (ocorrencia,))
    r = cur.fetchone()
    if r is None:
        return None
    return dict(zip(("geracao", "aviso_id", "estado", "titulo", "modulo", "motivo", "aberto_em",
                     "recuperado_em"), r))


def sincronizar(conn, dados: dict, agora: datetime | None = None) -> dict:
    """Aplica a visão validada (``validar``) aos avisos. Uma transação; idempotente.

    Vínculo por (ocorrência, geração). A mesma ocorrência listada como aberta depois de recuperada
    (``aberto_em`` posterior ao ``recuperado_em``) é uma reabertura -- ex.: o estado da API se perdeu
    e a nova falha repetiu o id -- e vira um aviso novo (geração seguinte)."""
    agora = agora or datetime.now(timezone.utc)
    res = {"criados": 0, "recuperados": 0, "ja_existentes": 0, "destinatarios": 0, "ausentes": [],
           "erro": None}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(%s)", (TRAVA,))
            ids, ausentes = resolver_destinatarios(cur)
            res["destinatarios"], res["ausentes"] = len(ids), ausentes
            if ausentes:
                _log_erro("ausentes:" + ",".join(ausentes),
                          "siaa_avisos: destinatário não encontrado em app_users: %s", agora, ", ".join(ausentes))
            for inc in dados["abertos"]:
                v = _ultimo_vinculo(cur, inc["ocorrencia"])
                geracao = 1
                if v is not None:
                    reaberta = (v["estado"] != "aberto" and inc["aberto_em"] is not None
                                and v["recuperado_em"] is not None and inc["aberto_em"] > v["recuperado_em"])
                    if not reaberta:
                        res["ja_existentes"] += 1
                        if v["estado"] == "aberto" and v["aviso_id"] and ids:
                            # destinatário cadastrado depois: só acrescenta, nunca remove nem esvazia
                            cur.execute("""UPDATE avisos SET target_user_ids = (
                                               SELECT ARRAY(SELECT DISTINCT unnest(target_user_ids || %s::int[]) ORDER BY 1))
                                           WHERE id = %s AND NOT (target_user_ids @> %s::int[])""",
                                        (ids, v["aviso_id"], ids))
                        continue
                    geracao = v["geracao"] + 1
                if not ids:
                    res["erro"] = "sem_destinatarios"
                    _log_erro("sem_destinatarios", "siaa_avisos: nenhum destinatário encontrado; aviso não criado",
                              agora)
                    continue  # tenta de novo no próximo ciclo; nunca cria aviso para todos
                titulo, corpo = aviso_aberto(inc)
                aviso_id = _criar_aviso(cur, ids, titulo, corpo, "urgente")
                cur.execute("""INSERT INTO siaa_incidente_aviso
                                   (ocorrencia, geracao, tipo, aviso_id, estado, titulo, modulo, motivo, aberto_em)
                               VALUES (%s, %s, %s, %s, 'aberto', %s, %s, %s, %s)""",
                            (inc["ocorrencia"], geracao, inc["tipo"], aviso_id, inc["titulo"], inc["modulo"],
                             inc["motivo"], inc["aberto_em"]))
                res["criados"] += 1
            for rec in dados["recuperados"]:
                v = _ultimo_vinculo(cur, rec["ocorrencia"])
                if v is None:
                    # nunca virou aviso (abriu e fechou entre dois ciclos): só o registro
                    cur.execute("""INSERT INTO siaa_incidente_aviso
                                       (ocorrencia, geracao, tipo, aviso_id, estado, titulo, modulo, aberto_em,
                                        recuperado_em)
                                   VALUES (%s, 1, %s, NULL, 'recuperado_sem_aviso', %s, %s, %s, %s)
                                   ON CONFLICT DO NOTHING""",
                                (rec["ocorrencia"], rec["tipo"], rec["titulo"], rec["modulo"],
                                 rec["aberto_em"], rec["recuperado_em"]))
                    continue
                if v["estado"] != "aberto":
                    continue
                if v["aberto_em"] is not None and rec["recuperado_em"] < v["aberto_em"]:
                    continue  # recuperação de uma geração anterior: não fecha a atual
                if v["aviso_id"]:
                    titulo, corpo = aviso_recuperado(v, rec)
                    cur.execute("""UPDATE avisos SET titulo = %s, corpo = %s, prioridade = 'normal',
                                          expires_at = %s
                                    WHERE id = %s""",
                                (titulo, corpo, rec["recuperado_em"] + EXPIRA_RECUPERADO, v["aviso_id"]))
                cur.execute("""UPDATE siaa_incidente_aviso SET estado = 'recuperado', recuperado_em = %s,
                                      atualizado_em = NOW() WHERE ocorrencia = %s AND geracao = %s""",
                            (rec["recuperado_em"], rec["ocorrencia"], v["geracao"]))
                res["recuperados"] += 1
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    return res


# ---------------------------------------------------------------------------
# Job (APScheduler) e leitura para a Conexão SIAA
# ---------------------------------------------------------------------------

_ULTIMO: dict = {"em": None, "resultado": None, "erro": None}


def _chamar_alertas():
    """(dados_validos|None, mensagem_de_erro|None) a partir de ``GET /alertas`` da session-api."""
    import requests
    base = (os.environ.get("SIAA_SESSION_URL", "") or "").rstrip("/")
    token = os.environ.get("SIAA_SESSION_TOKEN", "") or ""
    if not base or not token:
        return None, "Conexão com a session-api não configurada no painel."
    try:
        resp = requests.get(f"{base}/alertas", timeout=(3, 8),
                            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"})
        status = resp.status_code
        try:
            dados = resp.json()
        except Exception:
            dados = None
    except Exception as exc:  # sem URL/token no log
        logger.warning("siaa_avisos: falha de rede (%s)", type(exc).__name__)
        return None, "Não foi possível falar com a session-api."
    if status in (401, 403):
        return None, "O painel não está autorizado na session-api."
    if status == 503:
        return None, "Estado dos alertas ilegível na session-api."
    if status == 404:
        return None, "A session-api ainda não oferece /alertas (versão desatualizada)."
    if status != 200:
        return None, "A session-api respondeu de forma inesperada."
    v = validar(dados)
    if v is None:
        return None, "A session-api respondeu de forma inesperada."
    return v, None


def executar_job() -> None:
    if not ativo():
        return
    agora = datetime.now(timezone.utc)
    dados, erro = _chamar_alertas()
    _ULTIMO.update(em=agora, erro=erro, resultado=None)
    if dados is None:
        logger.warning("siaa_avisos: sem dados de alertas (%s); nada alterado", erro)
        return
    from db import get_conn
    conn = get_conn()
    try:
        res = sincronizar(conn, dados, agora)
        _ULTIMO["resultado"] = res
        if res["criados"] or res["recuperados"]:
            logger.info("siaa_avisos: %s aviso(s) criado(s), %s recuperado(s)", res["criados"], res["recuperados"])
    except Exception as exc:
        _ULTIMO["erro"] = "Falha ao gravar os avisos no banco."
        logger.error("siaa_avisos: falha ao sincronizar (%s)", type(exc).__name__)
    finally:
        conn.close()


def register_siaa_avisos_job(scheduler) -> None:
    scheduler.add_job(executar_job, "interval", seconds=INTERVALO_S, id="siaa_avisos_incidentes",
                      max_instances=1, coalesce=True, replace_existing=True)


def ultimos_vinculos(conn, limite: int = 30) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute("""SELECT v.ocorrencia, v.tipo, v.estado, v.titulo, v.modulo, v.motivo, v.aberto_em,
                              v.recuperado_em, v.aviso_id,
                              (SELECT count(*) FROM aviso_lido l WHERE l.aviso_id = v.aviso_id) AS lidos,
                              COALESCE(array_length(a.target_user_ids, 1), 0) AS destinatarios
                         FROM siaa_incidente_aviso v LEFT JOIN avisos a ON a.id = v.aviso_id
                        ORDER BY COALESCE(v.recuperado_em, v.aberto_em, v.criado_em) DESC
                        LIMIT %s""", (limite,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
