import sys
import os
import pyautogui
import time
import requests

pyautogui.FailSafeException = True
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils import wait_and_click, get_usuario_ativo, formatar_unidade_saude, calcular_idade_formatada, verificar_e_tratar_erro, monitorar_recursos, get_cnes
from api_client import atualizar_status, registrar_erro
from logger import log_info, log_debug, log_erro
from unidades.buscar_unidades import buscar_estabelecimento

IMAGENS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "imagens"))
primeira_execucao = True
pyautogui.PAUSE = 0.3

UNIDADES_ESPECIAIS_CNES = {
    "SECRETARIA DE SAUDE", "SECRETARIA DE SAUDE DO RECIFE",
    "CENTRO DE REFERENCIA CLARICE LISPECTOR",
    "UNIDADE DESCENTRALIZADA CLARICE LISPECTOR NO COMPAZ EDUARDO CAMPOS",
    "UNIDADE DESCENTRALIZADA CLARICE LISPECTOR",
    "COMPAZ GOVERNADOR EDUARDO CAMPOS", "COMPAZ GOVERNADOR MIGUEL ARRAES",
    "COMPAZ PROF. PAULO FREIRE", "COMPAZ DOM HELDER CÂMARA",
    "COMPAZ ESCRITOR ARIANO SUASSUNA"
}
CNES_ESPECIAL = "6468918"


def executar_creutzfeldt_jakob(item, reaproveitar_sessao=False, tem_proxima=False):
    """Função principal - Doença de Creutzfeldt-Jakob. Baseado em: toxoplasmose.py"""
    num_notificacao = item.get("num_notificacao")
    agravo_nome = "%DOENCA DE CREUTZFELDT-JAKOB%"
    try:
        monitorar_recursos()
        if not reaproveitar_sessao:
            abrir_sinan()
            username, password = get_usuario_ativo()
            login(username, password)
            time.sleep(6)
            selecionar_agravo(agravo_nome)

        log_info(f"Iniciando preenchimento: {num_notificacao}")
        idade = preencher_bloco_notificacao(item["notificacao"], num_notificacao)
        log_info("Notificação preenchida. Iniciando investigação.")
        preencher_bloco_investigacao(item["investigacao"], idade, num_notificacao)

        if verificar_e_tratar_erro(num_notificacao, agravo_nome):
            log_erro(f"Erro em Bloco Investigação para {num_notificacao}.")
            return

        monitorar_recursos()
        time.sleep(2)
        if not wait_and_click(os.path.join(IMAGENS_DIR, "salvar.png"), timeout=15):
            raise Exception("Botão 'Salvar' não encontrado.")
        time.sleep(2)
        if not wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=10):
            raise Exception("Não encontrou primeira janela 'ok'")
        if verificar_e_tratar_erro(num_notificacao, agravo_nome): return

        time.sleep(2)
        try: wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=5)
        except: pass

        time.sleep(4)
        if pyautogui.locateOnScreen(os.path.join(IMAGENS_DIR, "novo_ou_nao.png"), confidence=0.8):
            btn = "sim.png" if tem_proxima else "nao.png"
            if not wait_and_click(os.path.join(IMAGENS_DIR, btn), timeout=3):
                raise Exception(f"Botão '{btn}' não encontrado.")
        else:
            pyautogui.screenshot(f"erro_nova_notificacao_{num_notificacao}.png")
            raise Exception("Não encontrou tela 'Deseja incluir nova notificação?'")

        global primeira_execucao
        primeira_execucao = False
    except Exception as e:
        log_erro(f"Erro FATAL creutzfeldt_jakob {num_notificacao}: {e}")
        if num_notificacao: registrar_erro(num_notificacao)
        raise e
    else:
        if num_notificacao: atualizar_status(num_notificacao)


def abrir_sinan():
    pyautogui.press("win"); time.sleep(3)
    pyautogui.write("sinan"); time.sleep(3)
    pyautogui.press("enter"); time.sleep(6)

def login(usuario, senha):
    pyautogui.write(usuario); pyautogui.press("tab")
    pyautogui.write(senha); pyautogui.press("enter"); time.sleep(8)

def selecionar_agravo(nome_agravo):
    pyautogui.click(x=72, y=59)
    pyautogui.write(nome_agravo); pyautogui.press("enter")
    time.sleep(2); pyautogui.press("enter"); time.sleep(5)


def preencher_bloco_notificacao(campos, num_notificacao):
    """Bloco Notificação P1-P30 - Padrão SINAN"""
    agravo = "%DOENCA DE CREUTZFELDT-JAKOB%"
    pyautogui.write(num_notificacao); pyautogui.press("tab")
    pyautogui.write(campos['data_notificacao']); pyautogui.press("tab", presses=3)

    un = campos.get('unidade_notificadora', '')
    if un in ['2','3','4','5','6']:
        if verificar_e_tratar_erro(num_notificacao, agravo):
            raise Exception(f"Unidade Notificadora inválida ({un}).")
    pyautogui.write(campos['unidade_notificadora']); pyautogui.press("tab")

    if campos['unidade_notificadora'] in {"2","3","4","5","6"}:
        pyautogui.press("tab", presses=2)
        if verificar_e_tratar_erro(num_notificacao, agravo):
            raise Exception("Erro em P06.")

    nome_bruto, nome_destino = "", ""
    if campos['unidade_notificadora'] == "7":
        nome_bruto = campos.get('nome_unidade_notificadora', '')
        nome_destino = f"{nome_bruto} DO RECIFE"
    elif campos['unidade_notificadora'] == "1":
        val = campos.get('nome_unidade_saude', '')
        try: nome_bruto = buscar_estabelecimento(int(val))
        except (ValueError, TypeError): nome_bruto = val
        nome_destino = formatar_unidade_saude(nome_bruto)

    nome_comp = nome_bruto.upper().strip()
    if nome_comp in UNIDADES_ESPECIAIS_CNES:
        pyautogui.press("tab"); pyautogui.write(CNES_ESPECIAL)
    elif campos['unidade_notificadora'] == "1":
        cnes_temp = get_cnes(nome_comp)
        if cnes_temp and cnes_temp[0].get("co_cnes"):
            pyautogui.press("tab"); pyautogui.write(cnes_temp[0]["co_cnes"])
        else: pyautogui.write(nome_destino)
    else: pyautogui.write(nome_destino)

    pyautogui.press("tab")
    if verificar_e_tratar_erro(num_notificacao, agravo): raise Exception("Erro P07/08.")

    pyautogui.click(x=654, y=301); time.sleep(2)
    pyautogui.write(campos.get('data_primeiros_sintomas', '')); pyautogui.press("tab")
    pyautogui.write(campos['nome_paciente']); pyautogui.press("tab")

    idade = 0
    if campos.get('data_nascimento'):
        pyautogui.write(campos['data_nascimento'])
        idade = calcular_idade_formatada(campos['data_nascimento'])
        pyautogui.press("tab")
        if verificar_e_tratar_erro(num_notificacao, agravo): raise Exception("Erro P09.")
    else:
        pyautogui.press("tab")
        idade = int(campos.get('idade_calculada_notificador', 0))
        pyautogui.write(str(idade)); pyautogui.press("tab")
        if verificar_e_tratar_erro(num_notificacao, agravo): raise Exception("Erro P10.")
        pyautogui.write("4"); pyautogui.press("tab")

    pyautogui.write(campos['sexo'])
    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.press("tab"); pyautogui.write(campos.get('gestante', '9'))
    pyautogui.press("tab")
    if campos.get('raca'): pyautogui.write(campos['raca'])
    pyautogui.press("tab")

    if idade >= 7 and campos.get('escolaridade'):
        m = {'1':'0','2':'1','3':'2','4':'3','5':'4','6':'5','7':'6','8':'7','9':'8','10':'10','99':'9'}
        pyautogui.write(m.get(campos['escolaridade'], '9')); pyautogui.press("tab")
        if verificar_e_tratar_erro(num_notificacao, agravo): raise Exception("Erro P14.")

    time.sleep(0.5)
    if campos.get('cartao_sus'): pyautogui.write(campos['cartao_sus'])
    pyautogui.press("tab")
    if campos.get('nome_mae'): pyautogui.write(campos['nome_mae'])
    pyautogui.press("tab"); time.sleep(4)

    if campos.get('uf_residencia'): pyautogui.write(campos['uf_residencia'])
    pyautogui.press("tab")
    if campos.get('municipio_residencia'): pyautogui.write(campos['municipio_residencia'])
    pyautogui.press("tab")
    if campos.get('municipio_residencia','').upper() == 'RECIFE' and campos.get('distrito_residencia'):
        pyautogui.write(f"%{campos['distrito_residencia']}%")
    pyautogui.press("tab")
    if campos.get('municipio_residencia','').upper() == 'RECIFE' and campos.get('bairro_residencia'):
        pyautogui.write(campos['bairro_residencia'])
    pyautogui.press("tab")
    if campos.get('endereco_residencia'): pyautogui.write(campos['endereco_residencia'])
    pyautogui.click(x=685, y=507); time.sleep(0.5); pyautogui.press("tab")
    if campos.get('numero_residencia'): pyautogui.write(campos['numero_residencia'])
    pyautogui.press("tab")
    if campos.get('complemento_residencia'): pyautogui.write(campos['complemento_residencia'])
    pyautogui.press("tab"); pyautogui.press("tab"); pyautogui.press("tab")
    if campos.get('ponto_referencia'): pyautogui.write(campos['ponto_referencia'])
    pyautogui.press("tab")
    if campos.get('cep_residencia'): pyautogui.write(campos['cep_residencia'])
    pyautogui.press("tab")
    tel = campos.get('telefone', '')
    if tel and len(tel) >= 3:
        pyautogui.write(tel[:2]); pyautogui.press("tab"); pyautogui.write(tel[2:]); pyautogui.press("tab")
    else: pyautogui.press("tab", presses=2)
    if campos.get('zona'): pyautogui.write(campos['zona'])
    pyautogui.press("tab")
    if campos.get('pais_residencia'): pyautogui.write(campos['pais_residencia'])
    pyautogui.press("tab")
    return idade


def preencher_bloco_investigacao(campos, idade, num_notificacao):
    """Bloco Investigação P31-P43. TODO: Ajustar conforme ficha de Creutzfeldt-Jakob."""
    agravo = "%DOENCA DE CREUTZFELDT-JAKOB%"
    if campos.get('data_investigacao'): pyautogui.write(campos['data_investigacao'])
    pyautogui.press("tab")
    if campos.get('classificacao_final'): pyautogui.write(campos['classificacao_final'])
    pyautogui.press("tab")
    if campos.get('criterio_confirmacao'): pyautogui.write(campos['criterio_confirmacao'])
    pyautogui.press("tab")
    if campos.get('caso_autoctone'): pyautogui.write(campos['caso_autoctone'])
    pyautogui.press("tab")
    if campos.get('uf_autoctone'): pyautogui.write(campos['uf_autoctone'])
    pyautogui.press("tab")
    if campos.get('pais_autoctone'): pyautogui.write(campos['pais_autoctone'])
    pyautogui.press("tab")
    if campos.get('municipio_autoctone'): pyautogui.write(campos['municipio_autoctone'])
    pyautogui.press("tab"); pyautogui.press("tab")
    if campos.get('distrito_autoctone'): pyautogui.write(campos['distrito_autoctone'])
    pyautogui.press("tab")
    if campos.get('bairro_autoctone'): pyautogui.write(campos['bairro_autoctone'])
    pyautogui.press("tab")
    pyautogui.write(campos.get('doenca_trabalho', '9')); pyautogui.press("tab")
    ev = campos.get('evolucao_caso', '9')
    pyautogui.write(ev); pyautogui.press("tab")
    if ev in ['2','3'] and campos.get('data_obito'): pyautogui.write(campos['data_obito'])
    pyautogui.press("tab")
    enc = campos.get('data_encerramento', '').strip()
    if enc: pyautogui.write(enc)
    pyautogui.press("tab")
    if verificar_e_tratar_erro(num_notificacao, agravo): raise Exception("Erro P43.")
    if campos.get('observacoes'): pyautogui.write(campos['observacoes'])
