import pyautogui
import time
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from logger import log_debug, log_erro

load_dotenv()

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

# --- INÍCIO DA NOVA FUNÇÃO DE TRATAMENTO DE ERRO ---

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

# --- FIM DA NOVA FUNÇÃO ---

#Criar uma função para analisar erro e passa para a próxima ficha

 # 1- Buscar Imagens de possíveis Erros.

 # 2- erro 01( Atenção )AMARELO) Escolaridade incompatível com a idade preenchida

 # 3- erro 02 ( Atenção )AMARELO) Campo de preenchimento Obrigatório: Município de ocorrência

 # 4- erro 03 ( Informação )AZUL) Escolaridade incompatível com a idade preenchida

 # 5- erro 04 ( POPUP)) Ao buscar o campo ele vem com o CNES e UNIDADE vazia.   
 # 

def verificar_e_tratar_erro(rpa_service, log_service):
    """
    Verifica se algum pop-up de erro conhecido está na tela.
    Se encontrar, trata o erro (fecha a digitação) e retorna True.
    """
    # Lista de caminhos para os templates de erro
    ERROS_TEMPLATES = [
        'imagens/erro-01-atencao.png',
        'imagens/erro-02-atencao.png',
        'imagens/erro-03-informacoes.png',
        'imagens/erro-04-popup.png',
        'imagens/erro-05-intem_ja_cadastrado.png',
        'imagens/erro-05-popup.png',
    ]

    for template in ERROS_TEMPLATES:
        try:
            # Use o método do seu RPA para buscar a imagem na tela
            posicao = rpa_service.find_image_on_screen(template, confidence=0.8)
            
            if posicao:
                # Log do erro encontrado
                log_service.log_erro_rpa(f"Erro Visual Detectado: {template}. Fechando notificação atual.")
                
                # Ação de Fechar a Digitação (depende da sua interface)
                # 1. Clicar no botão 'OK' ou 'Não' (se for pop-up)
                if 'popup' in template or 'atencao' in template:
                    # Assumindo que a função resolve o pop-up (clica em OK/Não)
                    rpa_service.handle_popup_error(template) 
                
                # 2. Clicar no botão 'Sair' ou 'Cancelar' da tela principal da notificação
                rpa_service.clicar_no_botao_sair() 
                
                # 3. Confirma o descarte da notificação
                rpa_service.clicar_em_descartar_alteracoes() 
                
                return True  # Erro tratado, deve começar uma nova notificação
        
        except Exception as e:
            # Continua buscando, mas registra a exceção
            log_service.log_warning(f"Exceção ao buscar template {template}: {e}")

    return False # Nenhum erro encontrado    



# criar um arquivo de log com as posicoes do mouse.    
    