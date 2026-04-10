import pyautogui
import time
import json
import os
import keyboard
import threading
from datetime import datetime
from dotenv import load_dotenv
from logger import log_debug, log_erro, log_info

# load_dotenv() # <--- REMOVA OU COMENTE ESTA LINHA

# --- INÍCIO: Mecanismo de Parada de Emergência ---
STOP_AUTOMATION = False

def emergency_stop_listener():
    """Função que fica escutando o atalho (Ctrl+Esc) para parar a automação."""
    global STOP_AUTOMATION
    keyboard.wait('ctrl+esc')
    STOP_AUTOMATION = True
    log_info("!! PARADA DE EMERGÊNCIA SOLICITADA PELO USUÁRIO !!")

def start_stop_listener():
    """Inicia o listener da tecla de parada em uma thread separada."""
    listener_thread = threading.Thread(target=emergency_stop_listener, daemon=True)
    listener_thread.start()
    log_info("Atalho de parada (Ctrl+Esc) está ativo.")

def stop_requested():
    """Verifica se a parada de emergência foi solicitada."""
    return STOP_AUTOMATION

# --- FIM do Mecanismo de Parada ---

def take_screenshot(prefix="erro"):
    """
    Tira um screenshot da tela e salva em uma pasta 'erros' com timestamp.
    Retorna o caminho do arquivo salvo.
    """
    try:
        erros_dir = os.path.join(os.path.dirname(__file__), 'erros')
        os.makedirs(erros_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        screenshot_path = os.path.join(erros_dir, f"{prefix}_{timestamp}.png")
        
        pyautogui.screenshot(screenshot_path)
        log_erro(f"Screenshot de erro salvo em: {screenshot_path}")
        return screenshot_path
    except Exception as e:
        log_erro(f"Falha ao tirar screenshot: {e}")
        return None

def wait_for_image(image_path, timeout=10, confidence=0.9):
    """
    (Verificação de Estado) Apenas espera por uma imagem para aparecer.
    Retorna True se encontrar, False se der timeout.
    """
    log_debug(f"Verificando estado: aguardando pela imagem '{image_path}'...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        if stop_requested(): return False
        
        if pyautogui.locateOnScreen(image_path, confidence=confidence):
            log_debug(f"Verificação de estado bem-sucedida: Imagem '{image_path}' encontrada.")
            return True
        time.sleep(0.5)
    
    log_erro(f"Timeout! A tela esperada com a imagem '{image_path}' não apareceu.")
    take_screenshot(f"tela_inesperada_{os.path.basename(image_path).split('.')[0]}")
    return False

def find_and_click(image_path, timeout=10, confidence=0.9):
    """
    (Mecanismo de Recuperação) Procura por uma imagem e clica. Se falhar, tira screenshot.
    Retorna True se clicou, False se deu timeout.
    """
    log_debug(f"Procurando para clicar na imagem: {image_path}")
    start_time = time.time()
    while time.time() - start_time < timeout:
        if stop_requested(): return False

        try:
            location = pyautogui.locateCenterOnScreen(image_path, confidence=confidence)
            if location:
                pyautogui.click(location)
                log_debug(f"Encontrou e clicou na imagem: {image_path}")
                return True
        except Exception as e:
            log_erro(f"Erro inesperado ao procurar imagem '{image_path}': {e}")
        
        time.sleep(0.5)

    log_erro(f"Timeout ao procurar imagem para clicar: {image_path}")
    take_screenshot(f"erro_clique_{os.path.basename(image_path).split('.')[0]}")
    return False

# --- Funções existentes (mantidas como estão) ---
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


def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        return json.load(file)

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
    try:
        nascimento = datetime.strptime(data_nascimento_str, "%d%m%Y")
        hoje = datetime.today()
        idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
        return idade
    except Exception:
        return 0
    
# criar um arquivo de log com as posicoes do mouse.    
    
