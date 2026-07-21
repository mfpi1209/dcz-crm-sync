"""Server de teste com Waitress (WSGI de producao, robusto com uploads grandes)."""
from app import app
from waitress import serve

if __name__ == "__main__":
    print("* Serving on http://0.0.0.0:5001 via Waitress")
    serve(app, host="0.0.0.0", port=5001, threads=8, max_request_body_size=200*1024*1024)
