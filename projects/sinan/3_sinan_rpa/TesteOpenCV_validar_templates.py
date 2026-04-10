r"""
=========================================================
      TESTE DE VALIDAÇÃO DE IMAGENS (TEMPLATES)
=========================================================

📌 OBJETIVO:
    Validar todos os templates (.png) usados no SINAN-RPA.
    Testa leitura, visibilidade na tela e performance de
    detecção usando a função localizar_template_rapido_pos()
    (implementada internamente com OpenCV + MSS).

🧠 FUNCIONALIDADES:
    1️⃣ Varre a pasta de imagens configurada.
    2️⃣ Testa se o arquivo é legível pelo OpenCV.
    3️⃣ Usa MSS + OpenCV para localizar o template na tela.
    4️⃣ Ao encontrar, clica no centro da imagem e pressiona ESC.
    5️⃣ Executa fechamento ESC → SAIR → NÃO → Clique fixo → ENTER ×2.
    6️⃣ Registra tempo de detecção e salva screenshot.
    7️⃣ Exporta relatório CSV detalhado com resultados.
    8️⃣ Registra log multilinha padronizado em rpa_log.txt.

📂 SAÍDA:
    - Prints → pasta /erros
    - CSV → /erros/validacao_resultados.csv
    - Log → rpa_log.txt (formato multilinha padronizado)

🧑‍💻 AUTOR: André Bezerra
🗓️ DATA: 17/11/2025
=========================================================
"""

import cv2
import os
import time
import pandas as pd
import pyautogui
import numpy as np
import mss
from datetime import datetime

# --- CONFIGURAÇÕES GERAIS ---
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
IMAGENS_DIR = os.path.join(BASE_DIR, "imagens")
ERROS_DIR = os.path.join(BASE_DIR, "erros")
LOG_PATH = os.path.join(BASE_DIR, "rpa_log.txt")

SAIR_IMG = os.path.join(IMAGENS_DIR, "sair.png")
NAO_IMG = os.path.join(IMAGENS_DIR, "nao.png")

os.makedirs(ERROS_DIR, exist_ok=True)
os.makedirs(IMAGENS_DIR, exist_ok=True)

print("===== INICIANDO VALIDAÇÃO DE IMAGENS DO SINAN RPA =====")
print(f"OpenCV: {cv2.__version__} | PyAutoGUI: {pyautogui.__version__}\n")

# ---------------------------------------------------------------
def registrar_log(template_nome, caminho, tempo, resultado):
    """Registra informações detalhadas no arquivo rpa_log.txt."""
    with open(LOG_PATH, 'a', encoding='utf-8') as log_file:
        log_file.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [INFO] - Template: {template_nome}\n")
        log_file.write(f"   Caminho: {caminho}\n")
        log_file.write(f"   Tempo de processamento: {tempo}s\n")
        log_file.write(f"   Resultado: {resultado}\n")

# ---------------------------------------------------------------
def localizar_template_rapido_pos(template_path, confidence=0.8):
    """
    Usa OpenCV + MSS para localizar o template e retorna (x, y, w, h)
    se encontrado, ou None caso contrário.
    """
    try:
        with mss.mss() as sct:
            monitor = sct.monitors[1]
            screenshot = np.array(sct.grab(monitor))
            screenshot_gray = cv2.cvtColor(screenshot, cv2.COLOR_BGR2GRAY)
            template = cv2.imread(template_path, cv2.IMREAD_GRAYSCALE)
            if template is None:
                return None
            res = cv2.matchTemplate(screenshot_gray, template, cv2.TM_CCOEFF_NORMED)
            _, max_val, _, max_loc = cv2.minMaxLoc(res)
            if max_val >= confidence:
                h, w = template.shape
                return (max_loc[0], max_loc[1], w, h)
    except Exception as e:
        print(f"Erro ao localizar template {template_path}: {e}")
    return None

# ---------------------------------------------------------------
def fechar_tela_erro():
    """
    Executa a sequência completa após encontrar um erro:
    1️⃣ Pressiona ESC para sair do pop-up.
    2️⃣ Clica no botão SAIR.
    3️⃣ Clica no botão NÃO.
    4️⃣ Clica em (395, 229) para focar a tela de notificação.
    5️⃣ Pressiona ENTER duas vezes.
    6️⃣ Aguarda 3 segundos antes de continuar.
    """
    print("🧩 Executando sequência de fechamento: ESC → SAIR → NÃO → REINICIAR DIGITAÇÃO")
    pyautogui.press('esc')
    time.sleep(1)

    try:
        sair = pyautogui.locateCenterOnScreen(SAIR_IMG, confidence=0.8)
    except pyautogui.ImageNotFoundException:
        sair = None

    if sair:
        pyautogui.click(sair)
        print("   ✅ Botão 'Sair' clicado.")
        time.sleep(1)
    else:
        print("   ⚠️ Botão 'Sair' não encontrado. Continuando fluxo...")

    try:
        nao = pyautogui.locateCenterOnScreen(NAO_IMG, confidence=0.8)
    except pyautogui.ImageNotFoundException:
        nao = None

    if nao:
        pyautogui.click(nao)
        print("   ✅ Botão 'Não' clicado (descartar alterações).")
        time.sleep(1)

        # 1️⃣ Clique fixo para garantir foco na tela da Notificação Individual
        pyautogui.click(x=395, y=229)
        print("   🖱️ Clique fixo em (395, 229) para focar na tela de notificação individual.")

        # 2️⃣ Pressiona duas vezes ENTER
        pyautogui.press('enter', presses=2, interval=0.5)
        print("   ⌨️ Pressionado ENTER duas vezes para iniciar nova notificação.")

        # 3️⃣ Espera 3 segundos
        time.sleep(3)
        print("   ⏳ Aguardando 3 segundos para carregamento da nova ficha...")
        print("   ✅ Pronto para iniciar a próxima notificação.\n")

    else:
        print("   ⚠️ Botão 'Não' não encontrado. Continuando normalmente.")

# ---------------------------------------------------------------
def validar_templates():
    """Varre e testa todos os templates PNG."""
    resultados = []
    arquivos = [f for f in os.listdir(IMAGENS_DIR) if f.lower().endswith('.png')]

    if not arquivos:
        print("⚠️ Nenhum arquivo PNG encontrado na pasta de imagens.")
        return

    print(f"🔍 {len(arquivos)} arquivos PNG encontrados. Iniciando varredura...\n")
    time.sleep(1)

    for arquivo in arquivos:
        caminho = os.path.join(IMAGENS_DIR, arquivo)
        status = {"arquivo": arquivo, "resultado": "", "detalhes": "", "tempo_segundos": ""}

        print(f"➡️ Verificando: {arquivo}")

        if not os.path.exists(caminho):
            print("  ❌ Arquivo ausente.")
            status["resultado"] = "AUSENTE"
            resultados.append(status)
            continue

        img = cv2.imread(caminho)
        if img is None:
            print("  ❌ Imagem corrompida ou ilegível pelo OpenCV.")
            status["resultado"] = "CORROMPIDA"
            status["detalhes"] = "Falha na leitura pelo OpenCV"
            resultados.append(status)
            continue
        print("  ✅ Imagem carregada com sucesso.")

        inicio = time.time()
        pos = localizar_template_rapido_pos(caminho, confidence=0.8)
        duracao = round(time.time() - inicio, 2)
        status["tempo_segundos"] = duracao
        resultado = "ENCONTRADO" if pos else "NÃO ENCONTRADO"

        registrar_log(arquivo, caminho, duracao, resultado)

        if pos:
            x, y, w, h = pos
            centro_x, centro_y = x + w // 2, y + h // 2
            pyautogui.click(centro_x, centro_y)
            print(f"🖱️ Clique no centro da imagem detectada em ({centro_x}, {centro_y})")
            time.sleep(0.5)
            pyautogui.press('esc')
            print("⌨️ Tecla ESC pressionada para fechar alerta.")

            nome_screenshot = f"achado_{os.path.splitext(arquivo)[0]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
            caminho_screenshot = os.path.join(ERROS_DIR, nome_screenshot)
            pyautogui.screenshot(caminho_screenshot)
            print(f"📸 Screenshot salva em: {caminho_screenshot}")

            fechar_tela_erro()

            status["resultado"] = "ENCONTRADO"
            status["detalhes"] = f"Screenshot salva em {nome_screenshot}"
        else:
            print(f"⚠️ Template não encontrado ({duracao}s).")
            status["resultado"] = "NÃO ENCONTRADO"
            status["detalhes"] = "Imagem válida, mas não visível na tela."

        resultados.append(status)
        print("-" * 70)
        time.sleep(0.5)

    df = pd.DataFrame(resultados)
    csv_path = os.path.join(ERROS_DIR, "validacao_resultados.csv")
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    print("\n===== RESUMO FINAL =====")
    for r in resultados:
        print(f"{r['arquivo']:<50} → {r['resultado']} ({r['tempo_segundos']}s)")

    print(f"\n📂 Relatório CSV salvo em: {csv_path}")
    print(f"📸 Screenshots salvas em: {ERROS_DIR}")
    print("===== FIM DA VERIFICAÇÃO =====")

# ---------------------------------------------------------------
if __name__ == "__main__":
    validar_templates()
