# File: ggtidev/digitador_sinan/digitador_sinan-019bd2e1c350031cc7df9724bcb329bef76767b8/3_sinan_rpa/utils.py

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

# --- DEFINIÇÃO DE FOCO (COORDENADAS FORNECIDAS) ---
# Coordenadas fixas para clicar e garantir que a aplicação SINAN esteja em foco.
APLICACAO_FOCO_X = 958 
APLICACAO_FOCO_Y = 534 
# ------------------------------------------------

def load_json(filepath):
    with open(filepath, 'r', encoding='utf-8') as file:
        return json.load(file)

# --- FUNÇÃO wait_and_click MODIFICADA ---
# Aceita uma string (caminho único) ou uma lista de strings (caminhos múltiplos).
def wait_and_click(image_path_or_list, timeout=10, intervalo=0.5, confidence=0.9):
    """
    Aguarda e clica no centro da imagem na tela. Suporta um único caminho ou uma lista de caminhos.

    Args:
        image_path_or_list (str | list): Caminho único ou lista de caminhos de imagem a serem procurados.
    """
    start_time = time.time()
    
    # Converte o caminho único para lista, se necessário
    image_paths = [image_path_or_list] if isinstance(image_path_or_list, str) else image_path_or_list

    while True:
        for image_path in image_paths:
            try:
                # Tenta localizar o centro da imagem na tela
                location = pyautogui.locateCenterOnScreen(image_path, confidence=confidence)
                if location:
                    pyautogui.click(location)
                    log_debug(f"Encontrou e clicou na imagem: {image_path}")
                    return True
            except Exception as e:
                # Loga o erro, mas continua tentando com as outras imagens
                log_erro(f"PyAutoGUI encontrou um erro ao processar a imagem {image_path}: {e}")
                continue # Tenta a próxima imagem na lista

        if time.time() - start_time > timeout:
            log_debug(f"Timeout ao procurar imagem(s): {image_paths}")
            return False
        time.sleep(intervalo)
# --- FIM DA FUNÇÃO wait_and_click MODIFICADA ---

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

# --- FUNÇÃO PARA REABERTURA DE AGRAVO ---
def selecionar_agravo_atual(nome_agravo: str):
    """
    Reabre a tela de Notificação Individual após um erro.
    
    Este é o passo final do fluxo de erro: Enter / Enter para abrir nova ficha.
    """
    # 1. Clicar na área do menu Notificação Individual (x=72, y=59)
    #pyautogui.click(x=72, y=59)
    #pyautogui.click(x=357, y=231)
    
    log_debug(f"Focado no menu principal para reabrir Notificação Individual do Agravo: {nome_agravo}")
    log_info("ENTROU no: selecionar_agravo_atual ")
    
    # 2. Pressionar ENTER duas vezes (Selecionar Agravo e Abrir Nova Notificação)
    pyautogui.press("enter")
    time.sleep(1)
    pyautogui.press("enter")
    time.sleep(5)
    log_info("Reaberta a tela de Notificação Individual.")

# --- FIM DA FUNÇÃO DE REABERTURA ---

def verificar_erros_popup(erros_config, imagem_ok):
    """
    Verifica se alguma imagem de erro conhecida está na tela.
    Se encontrar, fecha o pop-up e retorna True.
    """
    # ... Código da função verificar_erros_popup (mantido para compatibilidade)
    time.sleep(1) 
    for nome_erro, caminho_imagem_erro in erros_config.items():
        try:
            if pyautogui.locateOnScreen(caminho_imagem_erro, confidence=0.8):
                log_erro(f"ERRO DETECTADO: Pop-up '{nome_erro}' encontrado na tela.")
                pasta_erros = os.path.join(os.path.dirname(__file__), "erros")
                os.makedirs(pasta_erros, exist_ok=True)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                screenshot_path = os.path.join(pasta_erros, f"erro_popup_{nome_erro}_{timestamp}.png")
                pyautogui.screenshot(screenshot_path)
                log_erro(f"Screenshot do erro salvo em: {screenshot_path}")

                log_info("Tentando fechar o pop-up de erro...")
                wait_and_click(imagem_ok, timeout=5) 
                time.sleep(1)
                return True 
        except Exception as e:
            log_erro(f"Ocorreu um erro ao verificar a imagem do pop-up '{nome_erro}': {e}")
            continue
    return False

def verificar_e_tratar_erro(num_notificacao: str, agravo: str):
    """
    Verifica se há algum erro na tela (pop-up) e executa a sequência completa de recuperação:
    1. Fecha o pop-up.
    2. Sai da tela atual sem salvar (clicando em 'Sair' e depois 'Não').
    3. Reabre automaticamente o Agravo atual.

    Essa função é essencial para evitar travamentos do robô e manter o fluxo automatizado estável.
    """
    # Caminhos fixos para botões
    path_sair = os.path.join(IMAGENS_RPA_DIR, "sair.png")
    path_nao = os.path.join(IMAGENS_RPA_DIR, "nao.png")

    # Lista de possíveis variações do botão OK
    path_ok_list = [
        os.path.join(IMAGENS_RPA_DIR, "ok.png"),
        os.path.join(IMAGENS_RPA_DIR, "ok_listra.png")
    ]

    # Templates de erro conhecidos
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

    # Percorre todos os templates conhecidos para detectar erros na tela
    for template in ERROS_TEMPLATES:
        try:
            posicao = pyautogui.locateOnScreen(template, confidence=0.8)

            if posicao:
                template_basename = os.path.basename(template)
                log_erro(f"ERRO DE DIGITAÇÃO DETECTADO: {template_basename}")

                # 1️⃣ Atualiza o status na API
                log_info(f"Atualizando status da notificação {num_notificacao} para 'erro_digitacao' na API.")
                if registrar_erro(num_notificacao):
                    log_info("Status atualizado com sucesso na API.")
                else:
                    log_erro("Falha ao atualizar status da notificação na API.")

                pop_up_closed = False  # Controle interno

                # 2️⃣ FECHAMENTO DO POP-UP (regras específicas por tipo)
                if template_basename in ('erro-04-popup.png', 'erro-05-popup.png'):
                    # Regra A: Fechar via ESC
                    log_info("Regra A (Pop-up): Fechando com ESC.")
                    pyautogui.press('esc')
                    time.sleep(1)
                    pop_up_closed = True

                elif template_basename in (
                    'erro-01-atencao.png', 'erro-02-atencao.png', 'erro-03-informacoes.png',
                    'erro-06-opcao-invalida.png', 'erro-07-atencao_uf.png',
                    'erro-08-atencao_so_recebe_valores_numericos.png',
                    'erro-09-campo_preenchiento_obrigatorio.png'
                ):
                    # Regra B: Tenta OK e, se falhar, usa ESC
                    log_info("Regra B (Validação): Tentando fechar Pop-up com OK/ESC.")
                    if wait_and_click(path_ok_list, timeout=2):
                        log_info("Pop-up fechado com OK.")
                        pop_up_closed = True
                    else:
                        log_info("Botão OK não encontrado. Fechando com ESC.")
                        pyautogui.press('esc')
                        time.sleep(1)
                        pop_up_closed = True

                elif template_basename == 'erro-05-intem_ja_cadastrado.png':
                    # Regra C: Item já cadastrado → botão 'Não'
                    log_info("Regra C (Item Cadastrado): Fechando diálogo com 'Não'.")
                    if wait_and_click(path_nao, timeout=5):
                        pop_up_closed = True
                    else:
                        log_info("Botão 'Não' não encontrado. Fechando com ESC.")
                        pyautogui.press('esc')
                        time.sleep(1)
                        pop_up_closed = True

                # 3️⃣ SEQUÊNCIA PADRÃO DE DESCARTE E REABERTURA
                if pop_up_closed:
                    log_info("Pop-up fechado. Iniciando sequência de descarte: Sair / Não / Reabrir Agravo.")

                    # --- SAIR ---
                    if wait_and_click(path_sair, timeout=5):
                        log_info("Clicado no botão 'Sair'.")
                        time.sleep(1)

                        # --- NÃO (descartar alterações) ---
                        if wait_and_click(path_nao, timeout=5):
                            log_info("Clicado em 'Não' para descartar alterações.")
                            # ✅ GARANTIA: Só reabrir depois do clique em “Não” bem-sucedido
                            log_info("Reabrindo tela do agravo atual...")
                            selecionar_agravo_atual(agravo)
                        else:
                            # ⚠️ Caso não encontre “Não”, ainda reabre, mas loga o alerta
                            log_erro("Botão 'Não' não encontrado após 'Sair'. Reabrindo mesmo assim.")
                            log_info("Reabrindo tela do agravo atual...[IF ELSE 2]")
                            selecionar_agravo_atual(agravo)

                    else:
                        # ⚠️ Falhou ao clicar em “Sair” → usa ESC e reabre
                        log_erro("Botão 'Sair' não encontrado. Tentando ESC e reabrindo agravo.")
                        pyautogui.press('esc')
                        time.sleep(1)
                        log_info("Reabrindo tela do agravo atual...[ELSE 3]")
                        selecionar_agravo_atual(agravo)

                    # ✅ Fluxo completo
                    return True

                else:
                    # Caso o pop-up não tenha sido fechado corretamente
                    log_erro("Aviso: Pop-up detectado, mas falha ao fechar. Ficha interrompida.")
                    return True

        except Exception as e:
            log_erro(f"Exceção ao tratar erro do template {template}: {e}")
            continue

    # Nenhum erro detectado
    return False



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