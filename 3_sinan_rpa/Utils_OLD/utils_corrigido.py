import pyautogui
import time
import json
import os
from datetime import datetime
from dotenv import load_dotenv
from logger import log_debug, log_erro, log_info 
from api_client import registrar_erro 

# -------------------------------------------------------------------------
# utils_corrigido.py
# -------------------------------------------------------------------------
# ✅ Revisado para corrigir:
#  - Tratamento incorreto do erro "erro-06-opcao-invalida.png"
#  - Reset do estado pop_up_closed entre notificações
#  - Foco agressivo antes do fechamento do pop-up
#  - Logs mais claros e consistentes
# -------------------------------------------------------------------------

load_dotenv()

# Define o caminho absoluto para a pasta de imagens
IMAGENS_RPA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "imagens"))

# Coordenadas padrão de foco (usadas para trazer a janela ativa)
APLICACAO_FOCO_X = 958 
APLICACAO_FOCO_Y = 534 

# -------------------------------------------------------------------------
# Funções utilitárias básicas
# -------------------------------------------------------------------------
def load_json(filepath):
    """Carrega um arquivo JSON e retorna como dicionário."""
    with open(filepath, 'r', encoding='utf-8') as file:
        return json.load(file)


def wait_and_click(image_path_or_list, timeout=10, intervalo=0.5, confidence=0.9):
    """
    Aguarda e clica no centro da imagem na tela. 
    Suporta um único caminho ou uma lista de caminhos.
    Retorna True se encontrou e clicou, False se não.
    """
    start_time = time.time()
    image_paths = [image_path_or_list] if isinstance(image_path_or_list, str) else image_path_or_list

    while True:
        for image_path in image_paths:
            try:
                location = pyautogui.locateCenterOnScreen(image_path, confidence=confidence)
                if location:
                    pyautogui.click(location)
                    log_debug(f"Encontrou e clicou na imagem: {image_path}")
                    return True
            except Exception as e:
                log_erro(f"PyAutoGUI encontrou um erro ao processar a imagem {image_path}: {e}")
                continue

        if time.time() - start_time > timeout:
            log_debug(f"Timeout ao procurar imagem(s): {image_paths}")
            return False
        time.sleep(intervalo)


def get_usuario_ativo():
    """Obtém as credenciais do usuário ativo a partir das variáveis de ambiente."""
    chave = os.getenv("USUARIO_LOGIN", "USUARIO1").upper()
    username = os.getenv(f"{chave}_USERNAME")
    password = os.getenv(f"{chave}_PASSWORD")
    return username, password


def formatar_unidade_saude(valor):
    """Formata o nome da unidade de saúde para o padrão de busca do SINAN."""
    if not valor:
        return ""
    partes = valor.strip().split()
    ultimas_duas = partes[-3:] if len(partes) >= 3 else partes
    return f"%{' '.join(ultimas_duas)}%"


def calcular_idade_formatada(data_nascimento_str: str) -> int:
    """Calcula a idade com base em uma string de data de nascimento no formato DDMMYYYY."""
    try:
        nascimento = datetime.strptime(data_nascimento_str, "%d%m%Y")
        hoje = datetime.today()
        idade = hoje.year - nascimento.year - ((hoje.month, hoje.day) < (nascimento.month, nascimento.day))
        return idade
    except Exception:
        return 0


# -------------------------------------------------------------------------
# Reabertura de Agravo
# -------------------------------------------------------------------------
def selecionar_agravo_atual(nome_agravo: str):
    """
    Reabre a tela de Notificação Individual após um erro.
    """
    pyautogui.click(x=72, y=59)
    log_debug(f"Focado no menu principal para reabrir Notificação Individual do Agravo: {nome_agravo}")
    pyautogui.press("enter")
    time.sleep(2)
    pyautogui.press("enter")
    time.sleep(6)
    log_info("Reaberta a tela de Notificação Individual.")


# -------------------------------------------------------------------------
# Verificação simples de erros com imagem OK (mantido do original)
# -------------------------------------------------------------------------
def verificar_erros_popup(erros_config, imagem_ok):
    """
    Verifica se alguma imagem de erro conhecida está na tela.
    Se encontrar, fecha o pop-up e retorna True.
    """
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


# -------------------------------------------------------------------------
# Função principal revisada
# -------------------------------------------------------------------------
def verificar_e_tratar_erro(num_notificacao: str, agravo: str):
    """
    Verifica se um erro está na tela, força o foco na aplicação (clicando no pop-up)
    e aplica a sequência de descarte e reabertura.
    Revisado para corrigir a Regra B (erro-06-opcao-invalida) e o reset do estado pop_up_closed.
    """
    path_sair = os.path.join(IMAGENS_RPA_DIR, "sair.png")
    path_nao = os.path.join(IMAGENS_RPA_DIR, "nao.png")
    path_ok_list = [
        os.path.join(IMAGENS_RPA_DIR, "ok.png"),
        os.path.join(IMAGENS_RPA_DIR, "ok_listra.png"),
    ]

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
        pop_up_closed = False  # Reinicia o estado a cada iteração
        try:
            posicao = pyautogui.locateOnScreen(template, confidence=0.8)
            if not posicao:
                continue

            template_basename = os.path.basename(template)
            log_erro(f"ERRO DETECTADO: {template_basename} para {num_notificacao}")

            # Atualiza status na API
            log_info(f"Atualizando status da notificação {num_notificacao} para 'erro_digitacao'.")
            registrar_erro(num_notificacao)

            # Foco agressivo no centro do pop-up
            centro_posicao = pyautogui.center(posicao)
            pyautogui.click(centro_posicao)
            log_info("Foco agressivo aplicado no centro da imagem de erro.")
            time.sleep(0.5)

            # ---------------- REGRAS ----------------
            # Regra A - Pop-ups simples (fechados apenas com ESC)
            if template_basename in ('erro-04-popup.png', 'erro-05-popup.png'):
                log_info("Regra A: Pop-up simples -> Fechando com ESC.")
                pyautogui.press('esc')
                pop_up_closed = True

            # Regra B - Pop-ups de Atenção/Validação (com foco forçado e botão OK/ESC)
            elif template_basename in (
                'erro-01-atencao.png', 'erro-02-atencao.png', 'erro-03-informacoes.png',
                'erro-06-opcao-invalida.png',  # Corrigido: Agora pertence SOMENTE à Regra B
                'erro-07-atencao_uf.png', 'erro-08-atencao_so_recebe_valores_numericos.png'
            ):
                log_info("Regra B: Validação/Atenção -> Tentando fechar com OK ou ESC.")
                if wait_and_click(path_ok_list, timeout=5):
                    pop_up_closed = True
                else:
                    pyautogui.press('esc')
                    pop_up_closed = True

            # Regra C - Item já cadastrado (botão 'Não' no próprio diálogo)
            elif template_basename == 'erro-05-intem_ja_cadastrado.png':
                log_info("Regra C: Item já cadastrado -> Fechando com 'Não'.")
                if wait_and_click(path_nao, timeout=5):
                    pop_up_closed = True
                else:
                    pyautogui.press('esc')
                    pop_up_closed = True

            # ---------------- PÓS-TRATAMENTO ----------------
            if pop_up_closed:
                log_info("Pop-up fechado. Executando sequência de descarte: Sair / Não / Reabrir.")
                if wait_and_click(path_sair, timeout=10):
                    log_info("Clicado em 'Sair'.")
                    if wait_and_click(path_nao, timeout=10):
                        log_info("Clicado em 'Não' para descartar alterações.")
                else:
                    log_erro("Botão 'Sair' não encontrado. Tentando ESC.")
                    pyautogui.press('esc')

                selecionar_agravo_atual(agravo)
                return True

            else:
                log_erro("Falha ao fechar pop-up. Encerrando ficha atual.")
                return True

        except Exception as e:
            log_erro(f"Exceção ao processar {template}: {e}")
            continue

    return False


# -------------------------------------------------------------------------
# Ferramenta auxiliar: registrar posições do mouse
# -------------------------------------------------------------------------
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
        log_file.write("Timestamp, X, Y\\n")
        while time.time() < end_time:
            x, y = pyautogui.position()
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_file.write(f"{timestamp}, {x}, {y}\\n")
            time.sleep(intervalo)
