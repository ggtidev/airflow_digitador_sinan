# File: ggtidev/digitador_sinan/digitador_sinan-019bd2e1c350031cc7df9724bcb329bef76767b8/3_sinan_rpa/utils.py (Versão Final Corrigida)

import pyautogui
import time
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from logger import log_debug, log_erro, log_info 
from api_client import registrar_erro 

load_dotenv()

# Define o caminho absoluto para a pasta de imagens
IMAGENS_RPA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "imagens"))

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        return json.load(file)

def wait_and_click(image_path, timeout=10, intervalo=0.5, confidence=0.9):
    start_time = time.time()
    while True:
        try:
            location = pyautogui.locateCenterOnScreen(image_path, confidence=confidence)
            if location:
                pyautogui.click(location)
                log_debug(f"Encontrou e clicou na imagem: {image_path}")
                return True
        except Exception as e:
            log_erro(f"PyAutoGUI encontrou um erro ao processar a imagem {image_path}: {e}")
            return False

        if time.time() - start_time > timeout:
            log_debug(f"Timeout ao procurar imagem: {image_path}")
            return False
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
    ultimas_duas = partes[-3:] if len(partes) >= 3 else partes
    return f"%{' '.join(ultimas_duas)}%"

def calcular_idade_formatada(data_nascimento_str: str) -> int:
    try:
        nascimento = datetime.strptime(data_nascimento_str, "%d%m%Y")
        hoje = datetime.today()
        idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
        return idade
    except Exception:
        return 0

# --- FUNÇÃO NOVA SOLICITADA PARA REABERTURA DE AGRAVO ---
def selecionar_agravo_atual(nome_agravo: str):
    """
    Reabre a tela de Notificação Individual após um erro, 
    presumindo que o agravo já foi selecionado.
    """
    # 1. Clicar na área do menu Notificação Individual (x=72, y=59)
    pyautogui.click(x=72, y=59)
    
    log_debug(f"Focado no menu principal para reabrir Notificação Individual do Agravo: {nome_agravo}")
    
    # 2. Pressionar ENTER duas vezes (Selecionar Agravo e Abrir Nova Notificação)
    pyautogui.press("enter")
    time.sleep(2)
    pyautogui.press("enter")
    time.sleep(6)
    log_info("Reaberta a tela de Notificação Individual.")

# --- FIM DA FUNÇÃO NOVA ---

def verificar_erros_popup(erros_config, imagem_ok):
    """
    Verifica se alguma imagem de erro conhecida está na tela.
    Se encontrar, fecha o pop-up e retorna True.
    """
    time.sleep(1) # Pequena pausa para a janela de erro aparecer
    for nome_erro, caminho_imagem_erro in erros_config.items():
        try:
            if pyautogui.locateOnScreen(caminho_imagem_erro, confidence=0.8):
                log_erro(f"ERRO DETECTADO: Pop-up '{nome_erro}' encontrado na tela.")
                # Tira um screenshot do erro para análise
                pasta_erros = os.path.join(os.path.dirname(__file__), "erros")
                os.makedirs(pasta_erros, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = os.path.join(pasta_erros, f"erro_popup_{nome_erro}_{timestamp}.png")
                pyautogui.screenshot(screenshot_path)
                log_erro(f"Screenshot do erro salvo em: {screenshot_path}")

                # Tenta fechar o pop-up de erro clicando em OK
                log_info("Tentando fechar o pop-up de erro...")
                wait_and_click(imagem_ok, timeout=5)
                time.sleep(1)
                # Confirma que um erro foi encontrado e tratado
                return True 
        except Exception as e:
            log_erro(f"Ocorreu um erro ao verificar a imagem do pop-up '{nome_erro}': {e}")
            continue # Continua para a próxima imagem de erro
            
    # Se o loop terminar sem encontrar nenhum erro
    return False

def verificar_e_tratar_erro(num_notificacao: str, agravo: str):
    """
    Verifica se algum pop-up de erro conhecido está na tela.
    Se encontrar, trata o erro (fecha a digitação, registra erro na API), 
    reabre a tela de agravo e retorna True.

    Args:
        num_notificacao (str): O número da notificação atual que está sendo digitada.
        agravo (str): O nome/código do agravo (usado para reabertura).

    Returns:
        bool: True se um erro foi encontrado e tratado, False caso contrário.
    """
    path_sair = os.path.join(IMAGENS_RPA_DIR, "sair.png")
    path_nao = os.path.join(IMAGENS_RPA_DIR, "nao.png")

    ERROS_TEMPLATES = [
        os.path.join(IMAGENS_RPA_DIR, 'erro-01-atencao.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-02-atencao.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-03-informacoes.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-04-popup.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-05-intem_ja_cadastrado.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-05-popup.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-06-opcao-invalida.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-07-atencao_uf.png'),
        os.path.join(IMAGENS_RPA_DIR, 'erro-08-atencao_so_recebe_valores_numericos.png'),
    ]

    for template in ERROS_TEMPLATES:
        try:
            posicao = pyautogui.locateOnScreen(template, confidence=0.8)
            
            if posicao:
                log_erro(f"ERRO DE DIGITAÇÃO DETECTADO: Pop-up/Alerta encontrado com a imagem: {os.path.basename(template)}.")
                
                # 1. Registrar Erro na API (primeira ação)
                log_info(f"Atualizando status da notificação {num_notificacao} para 'erro_digitacao' na API.")
                if registrar_erro(num_notificacao):
                    log_info(f"Status da notificação {num_notificacao} atualizado com sucesso na API.")
                else:
                    log_erro(f"FALHA ao atualizar status da notificação {num_notificacao} na API.")

                # 2. Limpar Tela de Erro (ESC / Sair / Não)
                log_info("Iniciando sequência de fechamento de tela: ESC / Sair / Não Salvar.")
                
                # ESC (para fechar pop-ups simples)
                pyautogui.press('esc')
                time.sleep(1) 
                
                if wait_and_click(path_sair, timeout=5):
                    log_info("Clicado no botão 'Sair'.")
                    time.sleep(1)
                    
                    # Tela de Confirmação: Clicar em "NÃO" para descartar alterações
                    if wait_and_click(path_nao, timeout=5):
                        log_info("Clicado em 'Não' para descartar alterações.")
                    else:
                        log_debug("Tela de confirmação 'Não' não encontrada ou não foi resolvida.")
                else:
                    log_debug("Botão 'Sair' não encontrado, tentando apenas ESC.")
                
                # 3. Reabrir a tela do Agravo (Selecionar Agravo Atual)
                # Passo solicitado: Vai abrir a tela de Notificação Individual, ja com o nome do agravo
                selecionar_agravo_atual(agravo) 
                
                # 4. Retornar True para que o fluxo principal inicie o preenchimento da próxima ficha
                return True 
        
        except Exception as e:
            log_erro(f"Exceção ao verificar ou tratar erro do template {template}: {e}")
            continue

    return False # Nenhum erro encontrado


# criar um arquivo de log com as posicoes do mouse.    
def log_posicoes_mouse(log_filepath, intervalo=1, duracao=60):
    """
    Registra as posições do mouse em um arquivo de log por um determinado período.
    
    Args:
        log_filepath (str): Caminho do arquivo de log onde as posições serão salvas.
        intervalo (int): Intervalo em segundos entre cada registro de posição.
        duracao (int): Duração total em segundos para registrar as posições.
    """
    end_time = time.time() + duracao
    with open(log_filepath, 'w') as log_file:
        log_file.write("Timestamp, X, Y\n")
        while time.time() < end_time:
            x, y = pyautogui.position()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write(f"{timestamp}, {x}, {y}\n")
            time.sleep(intervalo)