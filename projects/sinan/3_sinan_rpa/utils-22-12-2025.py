# ==========================================================
# File: utils.py (VERSÃO ROBUSTA COM FALLBACK)
# Autor: André Bezerra
# Data: 24/11/2025
# ==========================================================

import pyautogui
import time
import json
import os
import cv2
import mss
import numpy as np
import psutil # BIBLIOTECA NOVA PARA MONITORAR SISTEMA
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
from logger import log_debug, log_erro, log_info
from api_client import registrar_erro

# --- CONFIGURAÇÕES GERAIS ---
pyautogui.FAILSAFE = True
# Ajustamos o PAUSE para um valor um pouco maior para compensar o processador U-series
pyautogui.PAUSE = 0.5 
pyautogui.MINIMUM_CONFIDENCE = 0.8

load_dotenv()

# Caminhos absolutos
IMAGENS_RPA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "imagens"))
PASTA_ERROS = os.path.abspath(os.path.join(os.path.dirname(__file__), "erros"))
RPA_LOG = os.path.abspath(os.path.join(os.path.dirname(__file__), "rpa_log.txt"))

SAIR_IMG = os.path.join(IMAGENS_RPA_DIR, "sair.png")
NAO_IMG = os.path.join(IMAGENS_RPA_DIR, "nao.png")

os.makedirs(PASTA_ERROS, exist_ok=True)

# ==========================================================
# FUNÇÃO NOVA: MONITORAMENTO DE RECURSOS (CPU/RAM)
# ==========================================================
def monitorar_recursos():
    """
    Captura e loga o uso atual de CPU e Memória RAM.
    Útil para diagnosticar lentidão ou travamentos.
    """
    try:
        # CPU: Pega a porcentagem de uso (intervalo curto para não atrasar o robô)
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        # MEMÓRIA: Pega detalhes da RAM
        memory = psutil.virtual_memory()
        mem_percent = memory.percent
        mem_used_gb = round(memory.used / (1024 ** 3), 2) # Converte bytes para GB
        mem_total_gb = round(memory.total / (1024 ** 3), 2)
        
        # Loga no formato solicitado
        log_info(f"📊 [SISTEMA] CPU: {cpu_percent}% | RAM: {mem_percent}% ({mem_used_gb}GB usados de {mem_total_gb}GB)")
        
        # Alerta se o uso estiver muito crítico (Opcional)
        if mem_percent > 90:
            log_erro("⚠️ ALERTA: Uso de Memória RAM acima de 90%! Risco de travamento.")
            
    except Exception as e:
        log_erro(f"Erro ao monitorar recursos do sistema: {e}")
        
# ==========================================================
# FUNÇÕES AUXILIARES (Dados, Mouse, Sincronização)
# ==========================================================

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
            log_erro(f"Erro pyautogui ao processar {image_path}: {e}")
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

# ==========================================================
# FUNÇÃO DE ABERTURA DO AGRAVO (MANTIDA)
# ==========================================================

def selecionar_agravo_atual(nome_agravo: str):
    """
    Reabre a tela de Notificação Individual clicando no menu lateral e pressionando ENTER duas vezes.
    Usado no fluxo de reabertura após sucesso ou erro.
    """
    pyautogui.moveTo(x=72, y=59, duration=1)
    pyautogui.click(x=72, y=59)
    time.sleep(1)
    log_debug(f"Focado no menu para reabrir Notificação do Agravo: {nome_agravo}")
    pyautogui.press("enter", clicks=2)
    time.sleep(6)
    log_info("Tela de Notificação Individual reaberta.")


# ==========================================================
# FUNÇÃO DE LOCALIZAÇÃO DE TEMPLATE (MANTIDA)
# ==========================================================

def localizar_template_rapido_pos(template_path, confidence=0.8):
    """
    USA OpenCV + MSS: Localiza o template e retorna (x, y, w, h) se encontrado.
    Retorna None se não encontrar ou se houver erro.
    """
    try:
        with mss.mss() as sct:
            monitor = sct.monitors[1]
            screenshot = np.array(sct.grab(monitor))
            screenshot_gray = cv2.cvtColor(screenshot, cv2.COLOR_BGR2GRAY)
            template = cv2.imread(template_path, cv2.IMREAD_GRAYSCALE)
            if template is None:
                log_erro(f"Template {template_path} não pôde ser lido (arquivo ausente ou corrompido).")
                return None
            res = cv2.matchTemplate(screenshot_gray, template, cv2.TM_CCOEFF_NORMED)
            _, max_val, _, max_loc = cv2.minMaxLoc(res)
            if max_val >= confidence:
                h, w = template.shape
                return (max_loc[0], max_loc[1], w, h)
    except Exception as e:
        # Não loga aqui, a função chamadora fará o log do erro de exceção
        pass 
    return None


# ==========================================================
# FUNÇÃO DE FECHAMENTO DE TELA DE ERRO (MANTIDA)
# ==========================================================

def fechar_tela_erro():
    """
    Executa a sequência completa de descarte após encontrar um erro:
    ESC → SAIR → NÃO → clique fixo → ENTER ×2 → espera 3s
    Esta função reabre a ficha para o próximo item.
    """
    log_info("🧩 Executando sequência de descarte e reabertura: ESC → SAIR → NÃO → REINICIAR DIGITAÇÃO")
    pyautogui.press('esc')
    time.sleep(1)

    # Nota: Usamos locateCenterOnScreen (PyAutoGUI puro) aqui pois ele é mais confiável
    # para botões fixos como SAIR e NÃO após o pop-up ser fechado.

    sair = pyautogui.locateCenterOnScreen(SAIR_IMG, confidence=0.8)

    if sair:
        pyautogui.click(sair)
        log_info("   ✅ Botão 'Sair' clicado.")
        time.sleep(1)
    else:
        log_debug("   ⚠️ Botão 'Sair' não encontrado. Continuando fluxo...")

    nao = pyautogui.locateCenterOnScreen(NAO_IMG, confidence=0.8)

    if nao:
        pyautogui.click(nao)
        log_info("   ✅ Botão 'Não' clicado (descartar alterações).")
        time.sleep(1)

        # Reabertura da ficha (preparação para o próximo item da fila)
        pyautogui.click(x=395, y=229) 
        log_info("   🖱️ Clique fixo em (395, 229) para focar na tela de notificação individual.")

        pyautogui.press('enter', presses=2, interval=0.5)
        time.sleep(3)
        log_info("   ✅ Nova notificação aberta. Tela pronta para o próximo item.\n")
    else:
        log_debug("   ⚠️ Botão 'Não' não encontrado. Nenhuma alteração descartada.")


# ==========================================================
# FUNÇÃO PRINCIPAL DE VERIFICAÇÃO E TRATAMENTO DE ERRO (NOVA LÓGICA)
# ==========================================================

def _executar_tratamento_completo(num_notificacao, template, metodo_deteccao):
    """
    Executa os passos de tratamento (Screenshot, Log, API, Fechamento)
    após a detecção do erro por qualquer método.
    """
    template_nome = os.path.basename(template)
    log_info(f"🚨 ERRO DETECTADO ({metodo_deteccao}): {template_nome}. Iniciando tratamento.")

    # 1. SCREENSHOT
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    screenshot_filename = f"erro_{num_notificacao}_{template_nome}_{timestamp}_{metodo_deteccao}.png"
    screenshot_path = os.path.join(PASTA_ERROS, screenshot_filename)
    pyautogui.screenshot(screenshot_path)
    log_info(f"📸 Screenshot salva em: {screenshot_path}")

    # 2. REGISTRO NA API
    registrar_erro(num_notificacao)
    log_info(f"Status do item {num_notificacao} atualizado para 'erro_digitacao' na API.")

    # 3. FECHAMENTO DA TELA DE ERRO (Descarte + Reabertura de Ficha)
    fechar_tela_erro()

    # 4. Retorna True para interromper o fluxo principal
    return True


def verificar_e_tratar_erro(num_notificacao: str, agravo: str):
    """
    Detecta pop-ups de erro usando OpenCV/MSS (Prioridade) e PyAutoGUI puro (Fallback).
    Se encontrado, trata o erro, reabre a ficha e força a interrupção.
    """
    ERROS_TEMPLATES = [
        os.path.join(IMAGENS_RPA_DIR, f) for f in [
            'erro-01-atencao.png','erro-02-atencao.png','erro-03-informacoes.png',
            'erro-04-popup.png','erro-05-intem_ja_cadastrado.png','erro-05-popup.png',
            'erro-06-opcao-invalida.png','erro-07-atencao_uf.png', 'erro-07-atencao_uf.jpg','erro-08-atencao_so_recebe_valores_numericos.png',
            'erro-10-dt_nascimento_ou_idade_obrigatorio.png','erro-11-dt_invalida.png','erro-12-idade_inferior_ou_superior.png','erro-12-_idade_inferior_ou_superior.jpg',
            'erro-13-_preenchimento_obrigatorio.jpg','erro-13-sinan-nao-responde.jpg',
            'erro-14-_data_encerramento_deve_ser_maior_igual_data_notificacao.jpg',
            'erro-15-categoria_nao_permitida.jpg'
        ]
    ]

    for template in ERROS_TEMPLATES:
        
        # --- 1. TENTATIVA COM OPEN CV / MSS (PRIORIDADE) ---
        try:
            pos_opencv = localizar_template_rapido_pos(template, confidence=0.8)
            
            if pos_opencv:
                # Se for encontrado, clique no centro para focar/fechar o pop-up
                x, y, w, h = pos_opencv
                pyautogui.click(x + w // 2, y + h // 2)
                
                # Executa o tratamento completo e retorna True para interrupção
                return _executar_tratamento_completo(num_notificacao, template, "OPENCV")
        
        except Exception as e:
            # Se o OpenCV/MSS falhar ou levantar uma exceção (problema de dependência/compilação),
            # o fluxo continua para o Fallback.
            log_erro(f"Falha na detecção [ OpenCV ] do template {os.path.basename(template)}: {e}")
            pass # Continua para a próxima etapa: Fallback

        
        # --- 2. TENTATIVA COM PYAUTOGUI PURO (FALLBACK) ---
        try:
            # Usando PyAutoGUI padrão: mais lento, mas não depende de OpenCV/MSS.
            pos_pyautogui = pyautogui.locateOnScreen(template, confidence=0.7, grayscale=True)
            
            if pos_pyautogui:
                # Se for encontrado, clique no centro para focar/fechar o pop-up
                x, y, w, h = pos_pyautogui
                pyautogui.click(x + w // 2, y + h // 2)
                
                # Executa o tratamento completo e retorna True para interrupção
                return _executar_tratamento_completo(num_notificacao, template, "FALLBACK")
        
        except Exception as e:
            # Captura exceções do PyAutoGUI puro (pode ser arquivo corrompido, por exemplo)
            log_erro(f"Falha na detecção [ PyAutoGUI ] FALLBACK do template {os.path.basename(template)}: {e}")
            continue # Continua para o próximo template

    return False # Nenhum erro encontrado

def get_cnes(unidade : str):
    # URL e token de autorização
    url = f"https://vigilanciaemsaude.recife.pe.gov.br/api-sinan/unidade?descricao={unidade}"
    auth = ("sevs_user", "YrQKvg82DKfLooYT")
    headers = {}
    
    # Fazendo a requisição
    response = requests.get(url, auth=auth, headers=headers)

    data = None
    resultados = []

    # Verificando o status da resposta
    if response.status_code == 200:
        # Itera sobre cada dicionário na lista de entrada (dados_json)
        for estabelecimento in response.json():
            # Cria um novo dicionário contendo apenas as chaves desejadas
            novo_registro = {
                "ds_estabelecimento": estabelecimento.get("ds_estabelecimento"),
                "co_cnes": estabelecimento.get("co_cnes")
            }
            # Adiciona o novo dicionário à lista de resultados
            resultados.append(novo_registro)
            
        return resultados