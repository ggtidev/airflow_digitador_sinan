# =========================================================
#  ARQUIVO: utils.py
#  PROJETO: SINAN-RPA (Automação do SINAN)
# =========================================================
#  DESCRIÇÃO:
#      Módulo utilitário contendo funções de apoio para o
#      fluxo do RPA do SINAN, incluindo detecção de erros,
#      automação de tela e controle de tempo.
#
#  ATUALIZAÇÕES NESTA VERSÃO:
#      ✅ Função localizar_template_rapido() com MSS + OpenCV
#         para detecção de imagem até 4x mais rápida.
#      ✅ verificar_e_tratar_erro() reescrita para usar a nova
#         busca otimizada, com logs detalhados no rpa_log.txt.
#      ✅ Loga nome do template, caminho e tempo de detecção.
#
#  AUTOR: Andre Bezerra
#  DATA: 11/11/2025
# =========================================================

import pyautogui
import cv2
import os
import time
import json
import mss
import numpy as np
from datetime import datetime

# Caminhos padrão usados pelo sistema
RPA_LOG = r"C:\Users\aluisr\Documents\GitHub\sinan\3_sinan_rpa\rpa_log.txt"
IMAGENS_DIR = r"C:\Users\aluisr\Documents\GitHub\sinan\3_sinan_rpa\imagens"

# =========================================================
#  FUNÇÃO: registrar_log
# =========================================================
def registrar_log(mensagem: str):
    """Registra mensagens no arquivo de log principal do RPA."""
    try:
        with open(RPA_LOG, "a", encoding="utf-8") as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{timestamp} [LOG] - {mensagem}\n")
    except Exception as e:
        print(f"Erro ao registrar log: {e}")

# =========================================================
#  FUNÇÃO: localizar_template_rapido
# =========================================================
def localizar_template_rapido(template_path: str, confidence: float = 0.8):
    """
    Localiza o template na tela de forma otimizada usando MSS + OpenCV.
    Retorna (x, y, w, h) se encontrado, ou None caso contrário.
    """
    inicio = time.time()

    # Captura de tela completa (muito mais rápida que pyautogui)
    with mss.mss() as sct:
        screenshot = np.array(sct.grab(sct.monitors[0]))
        screenshot = cv2.cvtColor(screenshot, cv2.COLOR_BGRA2BGR)

    # Carrega o template
    template = cv2.imread(template_path)
    if template is None:
        registrar_log(f"❌ Erro ao carregar template: {template_path}")
        return None

    # Busca do template na imagem da tela
    resultado = cv2.matchTemplate(screenshot, template, cv2.TM_CCOEFF_NORMED)
    loc = np.where(resultado >= confidence)
    duracao = round(time.time() - inicio, 2)

    # Log de desempenho
    registrar_log(f"Template verificado: {os.path.basename(template_path)} | "
                  f"Confiança={confidence} | Tempo={duracao}s")

    # Se encontrar, retorna posição
    if len(loc[0]) > 0:
        y, x = loc[0][0], loc[1][0]
        h, w = template.shape[:2]
        return (x, y, w, h)
    return None

# =========================================================
#  FUNÇÃO: verificar_e_tratar_erro
# =========================================================
def verificar_e_tratar_erro(ERROS_TEMPLATES: list, path_nova_notificacao: str):
    """
    Verifica se algum pop-up de erro está visível e executa o fluxo
    de tratamento e reinício de notificação conforme a lógica definida.

    FLUXO:
    1️⃣ Verifica se há pop-ups na tela (usando templates).
    2️⃣ Se encontrar → fecha com ESC.
    3️⃣ Clica em 'Sair' → 'Não' (descarta alterações).
    4️⃣ Pressiona duas vezes ENTER (reabre nova ficha).
    5️⃣ Verifica se a tela corresponde a nova notificação.
    6️⃣ Retorna True se pronto para continuar o preenchimento.
    """
    try:
        for template in ERROS_TEMPLATES:
            nome_arquivo = os.path.basename(template)
            registrar_log(f"🔎 Verificando template: {nome_arquivo}")

            inicio = time.time()
            posicao = localizar_template_rapido(template, confidence=0.8)
            duracao = round(time.time() - inicio, 2)

            if posicao:
                registrar_log(f"⚠️ Erro detectado: {nome_arquivo} | Tempo de detecção: {duracao}s")
                pyautogui.press("esc")
                time.sleep(0.5)

                # Saindo da ficha atual
                if os.path.exists(IMAGENS_DIR):
                    pyautogui.press("f10")  # equivalente a clicar em "Sair"
                    time.sleep(0.8)
                    pyautogui.press("right")  # seleciona "Não"
                    pyautogui.press("enter")
                    registrar_log("🧩 Saída da ficha confirmada (clicado 'Não').")
                    time.sleep(2)

                # Reabrindo nova notificação
                pyautogui.press("enter")
                time.sleep(0.5)
                pyautogui.press("enter")
                registrar_log("🔁 Tentando abrir nova notificação (2x ENTER).")
                time.sleep(3)

                # Verifica se voltou para a tela inicial de nova notificação
                nova_posicao = localizar_template_rapido(path_nova_notificacao, confidence=0.8)
                if nova_posicao:
                    registrar_log("✅ Nova notificação detectada na tela. Pronto para continuar.")
                    return True
                else:
                    registrar_log("⚠️ Não foi possível confirmar a tela de nova notificação.")
                    return False

            else:
                registrar_log(f"🔹 Nenhum erro detectado para o template: {nome_arquivo} "
                              f"({duracao}s)")

        return False

    except Exception as e:
        registrar_log(f"❌ Exceção ao verificar/tratar erro: {str(e)}")
        return False
