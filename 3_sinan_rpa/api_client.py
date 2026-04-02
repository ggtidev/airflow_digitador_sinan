# File: ggtidev/digitador_sinan/digitador_sinan-019bd2e1c350031cc7df9724bcb329bef76767b8/3_sinan_rpa/api_client.py

import requests
import os


API_URL = os.getenv("API_URL", "http://localhost:8000") 

def buscar_filas():
    try:
        response = requests.get(f"{API_URL}/notificacoes", timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Erro ao buscar dados da API: {e}")
        return []

def atualizar_status(num_notificacao: str, novo_status: str = "concluido"):
    """
    Atualiza o status da notificação na API, enviando o status no corpo da requisição PATCH.
    O novo_status padrão é "concluido" para compatibilidade com o uso anterior.
    """
    try:
        # Envia o novo status no corpo da requisição (PATCH), assumindo que a API aceita JSON.
        payload = {"status": novo_status}
        response = requests.patch(f"{API_URL}/notificacoes/{num_notificacao}", json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erro ao atualizar status da notificação {num_notificacao} para '{novo_status}': {e}")
        return False

def registrar_erro(num_notificacao: str, erro_pergunta: str = None):
    """
    Função dedicada para registrar um erro de digitação na API,
    definindo o status da notificação como "erro_digitacao".
    Opcionalmente, registra qual campo/pergunta estava sendo preenchido quando o erro ocorreu.
    """
    try:
        payload = {"status": "erro_digitacao"}
        if erro_pergunta:
            payload["erro_pergunta"] = erro_pergunta
        response = requests.patch(f"{API_URL}/notificacoes/{num_notificacao}", json=payload, timeout=10)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Erro ao registrar erro da notificação {num_notificacao}: {e}")
        return False