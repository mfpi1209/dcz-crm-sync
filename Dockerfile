FROM python:3.14-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

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
