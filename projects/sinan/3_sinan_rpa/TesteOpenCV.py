import pyautogui
import cv2
import os

print("OpenCV:", cv2.__version__)
print("PyAutoGUI:", pyautogui.__version__)

# Caminho real para um template de erro
# Procurar a Imagem na tela
template = os.path.abspath(r"C:\Users\aluisr\Documents\GitHub\sinan\3_sinan_rpa\imagens\erro-12-_idade_inferior_ou_superior.png")
print(f"Tentando localizar: {template}")

if not os.path.exists(template):
    print("❌ Arquivo não encontrado no caminho informado!")
else:
    result = pyautogui.locateOnScreen(template, confidence=0.8)
    print("Resultado:", result)
