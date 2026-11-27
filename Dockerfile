# Fase 4 (27/11): build do front novo (frontend/) numa etapa separada.
# A imagem final leva só o resultado (frontend/dist); Node não vai para produção.
FROM node:22-slim AS front
WORKDIR /front
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

FROM python:3.14-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
COPY --from=front /front/dist ./frontend/dist

# Versões .gz/.br dos estáticos, servidas pelo WhiteNoise (entrega_web.py).
# Na imagem os arquivos não mudam, então o WhiteNoise não relê o disco a cada pedido.
RUN python -m whitenoise.compress static
ENV STATIC_AUTOREFRESH=0
# Cookie de sessão só por HTTPS (produção é servida em https). Localhost em http fica sem (seguranca.py).
ENV SESSION_COOKIE_SECURE=1

RUN mkdir -p /app/logs /app/reports

VOLUME ["/app/reports", "/app/logs"]

EXPOSE 5001

# AGENDADOR=worker roda os jobs (worker.py); senão, o site. Ver iniciar.sh.
# O tr tira CR caso o arquivo venha de um checkout no Windows.
RUN tr -d '\r' < iniciar.sh > /tmp/iniciar.sh && mv /tmp/iniciar.sh iniciar.sh && chmod +x iniciar.sh
CMD ["./iniciar.sh"]
