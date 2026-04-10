import pyautogui
import time
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from logger import log_debug

load_dotenv()

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        return json.load(file)

def wait_and_click(image_path, timeout=10, intervalo=0.5, confidence=0.9):
    start_time = time.time()
    while True:
        location = pyautogui.locateCenterOnScreen(image_path, confidence=confidence)
        if location:
            pyautogui.click(location)
            log_debug(f"Encontrou e clicou na imagem: {image_path}")
            return True  # Retorna explicitamente True após clicar
        if time.time() - start_time > timeout:
            log_debug(f"Timeout ao procurar imagem: {image_path}")
            return False  # Retorna False se não encontrar no timeout
        time.sleep(intervalo)

def get_usuario_ativo():
    chave = os.getenv("USUARIO_LOGIN", "USUARIO1").upper()
    username = os.getenv(f"{chave}_USERNAME")
    password = os.getenv(f"{chave}_PASSWORD")
    return username, password

def formatar_unidade_saude(valor):
    if not valor:
        return ""
    partes = valor.strip().split()
    ultimas_duas = partes[-2:] if len(partes) >= 2 else partes
    return f"%{' '.join(ultimas_duas)}%"

def calcular_idade_formatada(data_nascimento_str: str) -> int:
    try:
        nascimento = datetime.strptime(data_nascimento_str, "%d%m%Y")
        hoje = datetime.today()
        idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
        return idade
    except Exception:
        return 0
    
# criar um arquivo de log com as posicoes do mouse.    
    
