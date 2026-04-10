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

# Caminho absoluto da pasta de imagens
IMAGENS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "imagens")
)
print("Pasta de imagens usada:", IMAGENS_DIR)

primeira_execucao = True
pyautogui.PAUSE = 0.3

UNIDADES_ESPECIAIS_CNES = {
    "SECRETARIA DE SAUDE", 
    "SECRETARIA DE SAUDE DO RECIFE",
    "CENTRO DE REFERENCIA CLARICE LISPECTOR",
    "UNIDADE DESCENTRALIZADA CLARICE LISPECTOR NO COMPAZ EDUARDO CAMPOS",
    "UNIDADE DESCENTRALIZADA CLARICE LISPECTOR",
    "COMPAZ GOVERNADOR EDUARDO CAMPOS",
    "COMPAZ GOVERNADOR MIGUEL ARRAES",
    "COMPAZ PROF. PAULO FREIRE",
    "COMPAZ DOM HELDER CÂMARA",
    "COMPAZ ESCRITOR ARIANO SUASSUNA"
}
CNES_ESPECIAL = "6468918"


def executar_varicela(item, reaproveitar_sessao=False, tem_proxima=False):
    """
    Função principal para executar a digitação de uma notificação de
    Varicela no SINAN NET via RPA.
    
    Baseado em: toxoplasmose.py
    """
    num_notificacao = item.get("num_notificacao")
    agravo_nome = "%VARICELA%"
    try:
        monitorar_recursos() 

        if not reaproveitar_sessao:
            abrir_sinan()
            username, password = get_usuario_ativo()
            login(username, password)
            time.sleep(6)
            selecionar_agravo(agravo_nome)

        log_info(f"Iniciando preenchimento da notificação: {num_notificacao}")
        idade = preencher_bloco_notificacao(item["notificacao"], num_notificacao)
        
        log_info("Notificação preenchida. Iniciando investigação.")
        preencher_bloco_investigacao(item["investigacao"], idade, num_notificacao)

        if verificar_e_tratar_erro(num_notificacao, agravo_nome):
            log_erro(f"Erro de digitação encontrado em Bloco Investigação para {num_notificacao}. Interrompendo e prosseguindo para a próxima.")
            return

        log_info("Preenchimento completo. Tentando salvar formulário.")
        monitorar_recursos()
        time.sleep(2)

        if wait_and_click(os.path.join(IMAGENS_DIR, "salvar.png"), timeout=15):
            log_info("Clicado em salvar. Aguardando confirmação.")
        else:
            log_erro("Não conseguiu clicar em salvar.")
            raise Exception("Botão 'Salvar' não encontrado.")
            
        time.sleep(2)
        if wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=10):
            log_info("Primeiro 'ok' clicado com sucesso.")
        else:
            log_erro("Não encontrou a primeira janela 'ok'.")
            raise Exception("Não encontrou primeira janela 'ok'")

        if verificar_e_tratar_erro(num_notificacao, agravo_nome):
            log_erro(f"Erro encontrado após 1º OK para {num_notificacao}. Interrompendo e prosseguindo para a próxima.")
            return

        time.sleep(2)
        log_info("Verificando existência da segunda confirmação ('ok').")
        try:
            if wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=5):
                log_info("Segundo 'ok' clicado com sucesso.")
            else:
                log_info("Segunda janela 'ok' não apareceu. Continuando sem clicar.")
        except Exception:
            log_info("Erro leve ao verificar segundo 'ok'. Prosseguindo mesmo assim.")
        
        time.sleep(4)
        log_info("Aguardando janela 'Deseja incluir nova notificação deste agravo?'.")
        
        if pyautogui.locateOnScreen(os.path.join(IMAGENS_DIR, "novo_ou_nao.png"), confidence=0.8):
            log_info("Encontrada tela 'Deseja incluir nova notificação deste agravo?'.")
            if tem_proxima:
                log_info("Clicando em 'Sim' para novo formulário.")
                if not wait_and_click(os.path.join(IMAGENS_DIR, "sim.png"), timeout=3):
                    raise Exception("Botão 'Sim' não encontrado na janela de confirmação.")
            else:
                log_info("Clicando em 'Não' para fechar formulário.")
                if not wait_and_click(os.path.join(IMAGENS_DIR, "nao.png"), timeout=3):
                    raise Exception("Botão 'Não' não encontrado na janela de confirmação.")
        else:
            screenshot_nome = f"erro_nova_notificacao_{item['num_notificacao']}.png"
            pyautogui.screenshot(screenshot_nome)
            log_erro("Não encontrou a tela 'Deseja incluir nova notificação?'.")
            log_erro(f"Screenshot salvo como {screenshot_nome}")
            raise Exception("Não encontrou tela 'Deseja incluir nova notificação?'")

        global primeira_execucao
        primeira_execucao = False
        
    except Exception as e:
        log_erro(f"Erro FATAL durante execução do script varicela para {num_notificacao}: {e}")
        if num_notificacao:
            registrar_erro(num_notificacao) 
            log_info(f"Status da notificação {num_notificacao} atualizado para 'erro_digitacao' devido a erro fatal.")
        raise e
        
    else:
        if num_notificacao:
            atualizar_status(num_notificacao)
            log_info(f"Status atualizado na API para a notificação {num_notificacao}.")

def abrir_sinan():
    pyautogui.press("win")
    time.sleep(3)
    pyautogui.write("sinan")
    time.sleep(3)
    pyautogui.press("enter")
    time.sleep(6)

def login(usuario, senha):
    log_info(f"Realizando login com o usuário: {usuario}")
    log_info(f"Senha do usuário: {senha}")
    pyautogui.write(usuario)
    pyautogui.press("tab")
    pyautogui.write(senha)
    pyautogui.press("enter")
    time.sleep(8)

def selecionar_agravo(nome_agravo):
    pyautogui.click(x=72, y=59)
    pyautogui.write(nome_agravo)
    pyautogui.press("enter")
    time.sleep(2)
    pyautogui.press("enter")
    time.sleep(5)


# ====================================================================
# BLOCO NOTIFICAÇÃO (Campos 1 a 30) - Padrão SINAN
# ====================================================================
def preencher_bloco_notificacao(campos, num_notificacao):
    """
    Preenche os campos da aba 'Notificação' do SINAN NET.
    """
    log_debug(f"Campos notificação: {campos}")
    pyautogui.write(num_notificacao)
    pyautogui.press("tab")  
    pyautogui.write(campos['data_notificacao'])
    pyautogui.press("tab", presses=3)
    
    unidade_notificadora = campos.get('unidade_notificadora', '')
    if unidade_notificadora in ['2', '3', '4', '5', '6']:
        log_info(f"[REGRA-UNIDADE] Unidade Notificadora '{unidade_notificadora}' requer validação imediata de erro.")
        if verificar_e_tratar_erro(num_notificacao, "%VARICELA%"):
            raise Exception(f"Erro de validação automática após detectar Unidade Notificadora inválida (Valor: {unidade_notificadora}).")
    else:
        log_info(f"[REGRA-UNIDADE] Unidade Notificadora '{unidade_notificadora}' permitida, seguindo fluxo normalmente.") 
    
    pyautogui.write(campos['unidade_notificadora'])
    log_debug(f"[P-06] - Valor recebido para Unidade Notificadora: {campos.get('unidade_notificadora', '')}")
    pyautogui.press("tab")

    log_info("Iniciando preenchimento da Pergunta 07 e 08 - Unidade de Saúde / Nome da Unidade.")
    log_debug(f"[P-07] - Valor recebido: {campos.get('nome_unidade_saude', '')}")
    log_debug(f"[P-08] - Valor recebido: {campos.get('unidade_notificadora', '')}")

    if campos['unidade_notificadora'] in {"2", "3", "4", "5", "6"}:
        pyautogui.press("tab", presses=2)
        if verificar_e_tratar_erro(num_notificacao, "%VARICELA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após Pergunta 06")
            raise Exception(f"Erro de digitação em Pergunta 06. Interrupção forçada.")
    
    nome_da_unidade_bruto = ""
    nome_campo_destino = ""

    if campos['unidade_notificadora'] == "7":
        nome_da_unidade_bruto = campos.get('nome_unidade_notificadora', '')
        nome_completo = f"{nome_da_unidade_bruto} DO RECIFE"
        nome_campo_destino = nome_completo
    elif campos['unidade_notificadora'] == "1":
        valor_unidade_saude = campos.get('nome_unidade_saude', '')
        try:
            codigo_unidade = int(valor_unidade_saude)
            nome_da_unidade_bruto = buscar_estabelecimento(codigo_unidade)
        except (ValueError, TypeError):
            nome_da_unidade_bruto = valor_unidade_saude
        nome_campo_destino = formatar_unidade_saude(nome_da_unidade_bruto)
    
    nome_para_comparacao = nome_da_unidade_bruto.upper().strip()    
    
    log_debug("---Iniciando lógica de substituição por CNES (se aplicável).----")
    log_debug(f"Nome da Unidade de Saúde (Para Comparação): {nome_para_comparacao}")
    log_debug("---FINALIZANDO lógica de substituição por CNES (se aplicável).----")
    
    if nome_para_comparacao in UNIDADES_ESPECIAIS_CNES:
        log_info(f"UNIDADE ESPECIAL DETECTADA ({nome_para_comparacao}). SUBSTITUINDO NOME por CNES {CNES_ESPECIAL}.")
        pyautogui.press("tab") 
        pyautogui.write(CNES_ESPECIAL)
    elif campos['unidade_notificadora'] == "1":
        cnes_temp = get_cnes(nome_para_comparacao)
        if cnes_temp:
            cnes_encontrado = cnes_temp[0].get("co_cnes")
            nome_unidade_api = cnes_temp[0].get("ds_estabelecimento")
            if cnes_encontrado:
                log_info(f"CNES ENCONTRADO na API ({nome_unidade_api}). SUBSTITUINDO por CNES {cnes_encontrado}.")
                pyautogui.press("tab") 
                pyautogui.write(cnes_encontrado)
            else:
                log_info(f"CNES da API vazio. Digitando nome formatado: {nome_campo_destino}")
                pyautogui.write(nome_campo_destino)
        else:
            log_info(f"CNES NÃO ENCONTRADO na API. Digitando nome formatado: {nome_campo_destino}")
            pyautogui.write(nome_campo_destino)
    else:
        log_debug(f"Preenchendo Nome da Unidade: {nome_campo_destino}")
        pyautogui.write(nome_campo_destino)

    pyautogui.press("tab")

    erro_contexto = f"Pergunta 07/08 - Unidade/Nome: {nome_para_comparacao}"
    if verificar_e_tratar_erro(num_notificacao, "%VARICELA%"):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")

    log_info("Ajustando foco via clique para P07 (Data dos Primeiros Sintomas).")
    pyautogui.click(x=654, y=301)
    time.sleep(2)
    pyautogui.write(campos.get('data_primeiros_sintomas', ''))
    pyautogui.press("tab")

    pyautogui.write(campos['nome_paciente'])
    pyautogui.press("tab")

    idade = 0

    if campos.get('data_nascimento'):
        data_nascimento = campos['data_nascimento']
        log_debug(f"[P-09] Data de Nascimento: {data_nascimento}")
        pyautogui.write(data_nascimento)
        idade = calcular_idade_formatada(data_nascimento)
        pyautogui.press("tab")
        if verificar_e_tratar_erro(num_notificacao, "%VARICELA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após P09 - Data de Nascimento")
            raise Exception(f"Erro de digitação em P09. Interrupção forçada.")
    else:
        pyautogui.press("tab")
        idade = int(campos.get('idade_calculada_notificador', 0))
        pyautogui.write(str(idade))
        pyautogui.press("tab")
        if verificar_e_tratar_erro(num_notificacao, "%VARICELA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após P10 - Idade")
            raise Exception(f"Erro de digitação em P10. Interrupção forçada.")
        pyautogui.write("4")
        pyautogui.press("tab")

    pyautogui.write(campos['sexo'])

    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.press("tab")
        pyautogui.write(campos.get('gestante', '9'))
    pyautogui.press("tab")

    if campos.get('raca'):
        pyautogui.write(campos['raca'])
    pyautogui.press("tab")

    valor_sinan = "N/A"
    if idade >= 7 and campos.get('escolaridade'):
        mapeamento_escolaridade = {
            '1': '0', '2': '1', '3': '2', '4': '3', '5': '4',
            '6': '5', '7': '6', '8': '7', '9': '8', '10': '10', '99': '9'
        }
        valor_redcap = campos.get('escolaridade')
        valor_sinan = mapeamento_escolaridade.get(valor_redcap, '9')
        log_debug(f"[P14] Escolaridade: RedCap='{valor_redcap}' → SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
        pyautogui.press("tab")
        if verificar_e_tratar_erro(num_notificacao, "%VARICELA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após P14 - Escolaridade")
            raise Exception(f"Erro de digitação em P14. Interrupção forçada.")
    else:
        log_debug("[P14] Escolaridade pulada.")

    time.sleep(0.5)
    if campos.get('cartao_sus'):
        pyautogui.write(campos['cartao_sus'])
    pyautogui.press("tab")

    if campos.get('nome_mae'):
        pyautogui.write(campos['nome_mae']) 
    pyautogui.press("tab")

    time.sleep(4)
    
    if campos.get('uf_residencia'):
        pyautogui.write(campos['uf_residencia'])
    pyautogui.press("tab")
    
    if campos.get('municipio_residencia'):
        pyautogui.write(campos['municipio_residencia'])
    pyautogui.press("tab")

    if campos.get('municipio_residencia', '').upper() == 'RECIFE':
        if campos.get('distrito_residencia'):
            pyautogui.write(f"%{campos['distrito_residencia']}%")
    pyautogui.press("tab")

    if campos.get('municipio_residencia', '').upper() == 'RECIFE':
        if campos.get('bairro_residencia'):
            pyautogui.write(campos['bairro_residencia'])
    pyautogui.press("tab")

    if campos.get('endereco_residencia'): 
        pyautogui.write(campos['endereco_residencia'])
    
    pyautogui.click(x=685, y=507)
    time.sleep(0.5)
    pyautogui.press("tab")
    
    if campos.get('numero_residencia'):
        pyautogui.write(campos['numero_residencia'])
    pyautogui.press("tab")

    if campos.get('complemento_residencia'):
        pyautogui.write(campos['complemento_residencia'])
    pyautogui.press("tab")

    pyautogui.press("tab")  # Geocampo1
    pyautogui.press("tab")  # Geocampo2
    
    if campos.get('ponto_referencia'):
        pyautogui.write(campos['ponto_referencia'])
    pyautogui.press("tab")

    if campos.get('cep_residencia'):
        pyautogui.write(campos['cep_residencia'])
    pyautogui.press("tab") 

    telefone = campos.get('telefone', '')
    if telefone and len(telefone) >= 3:
        pyautogui.write(telefone[:2])
        pyautogui.press("tab")
        pyautogui.write(telefone[2:])
        pyautogui.press("tab")
    else:
        pyautogui.press("tab", presses=2)

    if campos.get('zona'):
        pyautogui.write(campos['zona'])
    pyautogui.press("tab")

    if campos.get('pais_residencia'):
        pyautogui.write(campos['pais_residencia'])
    pyautogui.press("tab")

    log_debug(f"Idade calculada/fornecida: {idade}")
    return idade


# ====================================================================
# BLOCO INVESTIGAÇÃO (Campos 31 a 43) - Varicela
# ====================================================================
def preencher_bloco_investigacao(campos, idade, num_notificacao):
    """
    Preenche os campos da aba 'Investigação' do SINAN NET para Varicela.
    Campos 31-43: Data Investigação, Classificação Final, Critério,
    Autóctone, UF, País, Município, Distrito, Bairro,
    Doença do Trabalho, Evolução, Data Óbito, Data Encerramento, Observações.
    
    TODO: Ajustar a navegação e campos conforme a ficha específica de Varicela no SINAN.
    """
    log_debug(f"Campos investigação: {campos}")
    log_debug(f"Idade recebida para investigação: {idade}")

    agravo_nome = "%VARICELA%"

    if campos.get('data_investigacao'):
        pyautogui.write(campos['data_investigacao'])
        log_debug(f"[P31] Data da Investigação: {campos['data_investigacao']}")
    pyautogui.press("tab")

    if campos.get('classificacao_final'):
        pyautogui.write(campos['classificacao_final'])
        log_debug(f"[P32] Classificação Final: {campos['classificacao_final']}")
    pyautogui.press("tab")

    if campos.get('criterio_confirmacao'):
        pyautogui.write(campos['criterio_confirmacao'])
        log_debug(f"[P33] Critério de Confirmação: {campos['criterio_confirmacao']}")
    pyautogui.press("tab")

    if campos.get('caso_autoctone'):
        pyautogui.write(campos['caso_autoctone'])
        log_debug(f"[P34] Caso Autóctone: {campos['caso_autoctone']}")
    pyautogui.press("tab")

    if campos.get('uf_autoctone'):
        pyautogui.write(campos['uf_autoctone'])
        log_debug(f"[P35] UF Local Provável: {campos['uf_autoctone']}")
    pyautogui.press("tab")

    if campos.get('pais_autoctone'):
        pyautogui.write(campos['pais_autoctone'])
        log_debug(f"[P36] País: {campos['pais_autoctone']}")
    pyautogui.press("tab")

    if campos.get('municipio_autoctone'):
        pyautogui.write(campos['municipio_autoctone'])
        log_debug(f"[P37] Município: {campos['municipio_autoctone']}")
    pyautogui.press("tab")

    pyautogui.press("tab")  # Código IBGE

    if campos.get('distrito_autoctone'):
        pyautogui.write(campos['distrito_autoctone'])
        log_debug(f"[P38] Distrito: {campos['distrito_autoctone']}")
    pyautogui.press("tab")

    if campos.get('bairro_autoctone'):
        pyautogui.write(campos['bairro_autoctone'])
        log_debug(f"[P39] Bairro: {campos['bairro_autoctone']}")
    pyautogui.press("tab")

    valor_doenca_trabalho = campos.get('doenca_trabalho', '9')
    pyautogui.write(valor_doenca_trabalho)
    log_debug(f"[P40] Doença Relacionada ao Trabalho: {valor_doenca_trabalho}")
    pyautogui.press("tab")

    valor_evolucao = campos.get('evolucao_caso', '9')
    pyautogui.write(valor_evolucao)
    log_debug(f"[P41] Evolução do Caso: {valor_evolucao}")
    pyautogui.press("tab")

    if valor_evolucao in ['2', '3'] and campos.get('data_obito'):
        pyautogui.write(campos['data_obito'])
        log_debug(f"[P42] Data do Óbito: {campos['data_obito']}")
    pyautogui.press("tab")

    data_encerramento = campos.get('data_encerramento', '').strip()
    if data_encerramento:
        pyautogui.write(data_encerramento)
        log_debug(f"[P43] Data do Encerramento: {data_encerramento}")
    pyautogui.press("tab")

    erro_contexto = f"Pergunta 43 - Data de Encerramento: {data_encerramento}"
    if verificar_e_tratar_erro(num_notificacao, agravo_nome):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")

    if campos.get('observacoes'):
        pyautogui.write(campos['observacoes'])
        log_debug(f"[Observações] {campos['observacoes']}")
