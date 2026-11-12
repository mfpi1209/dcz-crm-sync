"""Calendário do motor: horário de Brasília, dias úteis, feriados e ciclo mensal."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

BRT = timezone(timedelta(hours=-3))


def agora_brt() -> datetime:
    return datetime.now(BRT)


def para_brt(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(BRT)


def _pascoa(ano: int) -> date:
    """Domingo de Páscoa (algoritmo de Meeus/Butcher)."""
    a = ano % 19
    b, c = divmod(ano, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    mes = (h + l - 7 * m + 114) // 31
    dia = (h + l - 7 * m + 114) % 31 + 1
    return date(ano, mes, dia)


def feriados_nacionais(ano: int) -> frozenset[date]:
    """Feriados nacionais fixos, mais Carnaval (seg/ter), Sexta-feira Santa e Corpus Christi."""
    pascoa = _pascoa(ano)
    fixos = [(1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (11, 20), (12, 25)]
    dias = {date(ano, m, d) for m, d in fixos}
    dias |= {
        pascoa - timedelta(days=48),  # segunda de Carnaval
        pascoa - timedelta(days=47),  # terça de Carnaval
        pascoa - timedelta(days=2),   # Sexta-feira Santa
        pascoa + timedelta(days=60),  # Corpus Christi
    }
    return frozenset(dias)


def eh_feriado(d: date, extras: frozenset[date] = frozenset()) -> bool:
    return d in extras or d in feriados_nacionais(d.year)


def eh_dia_util(d: date, extras: frozenset[date] = frozenset()) -> bool:
    return d.weekday() < 5 and not eh_feriado(d, extras)


def dia_util_ate(d: date, extras: frozenset[date] = frozenset()) -> date:
    """O próprio dia, se útil; senão o dia útil imediatamente anterior."""
    while not eh_dia_util(d, extras):
        d -= timedelta(days=1)
    return d


def somar_dias_uteis(d: date, n: int, extras: frozenset[date] = frozenset()) -> date:
    """Avança n dias úteis a partir de d (n=0 devolve d)."""
    passo = 1 if n >= 0 else -1
    restante = abs(n)
    while restante:
        d += timedelta(days=passo)
        if eh_dia_util(d, extras):
            restante -= 1
    return d


def dias_uteis_entre(inicio: date, fim: date, extras: frozenset[date] = frozenset()) -> int:
    """Quantos dias úteis existem depois de `inicio` até `fim`, inclusive."""
    n = 0
    d = inicio
    while d < fim:
        d += timedelta(days=1)
        if eh_dia_util(d, extras):
            n += 1
    return n


def ciclo_de(d: date) -> str:
    """Ciclo mensal de cobrança no formato AAAA-MM."""
    return f"{d.year:04d}-{d.month:02d}"


def ciclo_anterior(ciclo: str, n: int = 1) -> str:
    ano, mes = (int(x) for x in ciclo.split("-"))
    total = ano * 12 + (mes - 1) - n
    return f"{total // 12:04d}-{total % 12 + 1:02d}"


def semestre_de(ciclo: str) -> str:
    """2026-11 → 2026/2. O aprendizado por aluno recomeça a cada semestre."""
    ano, mes = (int(x) for x in ciclo.split("-"))
    return f"{ano}/{1 if mes <= 6 else 2}"


def data_do_ciclo(ciclo: str, dia: int) -> date:
    """Dia do mês do ciclo; dia além do fim do mês cai no último dia."""
    ano, mes = (int(x) for x in ciclo.split("-"))
    prox = date(ano + (mes == 12), mes % 12 + 1, 1)
    ultimo = (prox - timedelta(days=1)).day
    return date(ano, mes, min(dia, ultimo))


def parse_feriados(texto: str) -> frozenset[date]:
    """'2026-11-20, 2027-01-25' → conjunto de datas. Entradas inválidas são ignoradas."""
    dias = set()
    for parte in (texto or "").replace(";", ",").split(","):
        parte = parte.strip()
        if not parte:
            continue
        try:
            dias.add(date.fromisoformat(parte))
        except ValueError:
            continue
    return frozenset(dias)
