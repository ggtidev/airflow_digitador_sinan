import pyautogui
import cv2
import os

print("===== TESTE DE TEMPLATE COM OPENCV + PYAUTOGUI =====")
print("OpenCV:", cv2.__version__)
print("PyAutoGUI:", pyautogui.__version__)

# Caminho absoluto da imagem que você quer testar
template = r"C:\Users\aluisr\Documents\GitHub\sinan\3_sinan_rpa\imagens\erro-12-_idade_inferior_ou_superior.png"

print(f"\n🔍 Verificando arquivo:\n{template}")

# 1️⃣ Verifica se o arquivo existe
if not os.path.exists(template):
    print("❌ ERRO: Arquivo não encontrado nesse caminho. Verifique o nome e extensão.")
else:
    print("✅ Arquivo encontrado. Tentando carregar...")

    # 2️⃣ Testa leitura direta pelo OpenCV (garante integridade do arquivo)
    img = cv2.imread(template)
    if img is None:
        print("❌ ERRO: OpenCV não conseguiu ler a imagem. Arquivo pode estar corrompido ou com formato incompatível.")
    else:
        print("✅ OpenCV conseguiu ler a imagem com sucesso.")

        # 3️⃣ Tenta localizar a imagem na tela com PyAutoGUI
        print("\n🖥️ Tentando localizar a imagem na tela (confidence=0.8)...")
        try:
            resultado = pyautogui.locateOnScreen(template, confidence=0.8)
            if resultado:
                print(f"✅ Imagem encontrada na tela! Região: {resultado}")
            else:
                print("⚠️ Imagem NÃO encontrada na tela (mas leitura e comparação funcionaram).")
        except Exception as e:
            print(f"❌ ERRO AO EXECUTAR locateOnScreen: {e}")

print("\n===== FIM DO TESTE =====")
