import os
from dotenv import load_dotenv

# Carrega as variáveis do seu arquivo .env
load_dotenv()

# Puxa o token do .env
GOBI_TOKEN = os.getenv("GOBI_TOKEN")

# Adicione a URL base da sua API aqui (ou coloque no .env também)
GOBI_BASE_URL = os.getenv("GOBI_BASE_URL", "https://gobi-api.lineaalimentos.com.br/v1/reports") 

# Define o limite de requisições simultâneas
MAX_CONCURRENCY_API = int(os.getenv("MAX_CONCURRENCY_API", 5))