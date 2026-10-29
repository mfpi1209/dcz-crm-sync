"""Extrai o header Cookie de um "Copy as cURL (bash)" e salva em cookies/<modulo>.txt.

Uso (macOS, com o cURL copiado na area de transferencia):
    pbpaste | python3 scripts/siaa_cookie_from_curl.py            # -> cookies/academico.txt
    pbpaste | python3 scripts/siaa_cookie_from_curl.py financeiro # -> cookies/financeiro.txt
"""
import re
import sys
from pathlib import Path

modulo = (sys.argv[1] if len(sys.argv) > 1 else "academico").lower()
raw = sys.stdin.read()

m = (re.search(r"-H\s+'[Cc]ookie:\s*([^']*)'", raw)
     or re.search(r'-H\s+"[Cc]ookie:\s*([^"]*)"', raw)
     or re.search(r"-b\s+'([^']*)'", raw)
     or re.search(r'-b\s+"([^"]*)"', raw)
     or re.search(r"^[Cc]ookie:\s*(.+)$", raw.strip(), re.M))
if not m or not m.group(1).strip():
    sys.exit("Nenhum header Cookie encontrado. Cole o 'Copy as cURL (bash)' ou a linha 'Cookie: ...'.")

cookie = m.group(1).strip()
if "JSESSIONID" not in cookie:
    print("aviso: cookie sem JSESSIONID — confira se pegou uma requisicao de siaa.cruzeirodosul.edu.br", file=sys.stderr)

dest = Path(__file__).resolve().parents[1] / "cookies" / f"{modulo}.txt"
dest.parent.mkdir(parents=True, exist_ok=True)
dest.write_text(cookie, encoding="utf-8")
print(f"ok: {len(cookie)} chars, {cookie.count('=')} cookies -> {dest}")
