import requests
import os


API_URL = os.getenv("API_URL", "http://localhost:8000") #Caso queira substituir a URL da API e adicionar outra url para outros endpoints múltiplos sistemas (ARRENDONDADA)

def buscar_filas():
    try:
        response = requests.get(f"{API_URL}/notificacoes", timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erro ao buscar dados da API: {e}")
        return []

def atualizar_status(num_notificacao: str):
    try:
        response = requests.patch(f"{API_URL}/notificacoes/{num_notificacao}", timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erro ao atualizar status da notificação {num_notificacao}: {e}")
        return False