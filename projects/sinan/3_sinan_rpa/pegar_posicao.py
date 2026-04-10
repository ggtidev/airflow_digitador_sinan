import pyautogui
import time

print("Posicione o mouse em 5 segundos...")
time.sleep(5)

posicao = pyautogui.position()
print(f"Posição atual do mouse: {posicao}")

#  implemente uma variavel de posição do mouse, salvar um arquivo com todas as posições
#with open("posicao_mouse.txt", "w") as file:
#    file.write(f"{posicao.x},{posicao.y}")

