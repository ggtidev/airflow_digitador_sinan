import sys
import os
import pyautogui
import time
import requests

pyautogui.FailSafeException = True  # Ativar a exceção de segurança do PyAutoGUI

# Garante que o Python encontre os módulos da pasta raiz do projeto (3_sinan_rpa)
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

# Lista de Unidades de Saúde que exigem o CNES específico 6468918
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


def executar_toxoplasmose(item, reaproveitar_sessao=False, tem_proxima=False):
    """
    Função principal para executar a digitação de uma notificação de
    Toxoplasmose Congênita no SINAN NET via RPA.
    """
    num_notificacao = item.get("num_notificacao")
    agravo_nome = "%TOXOPLASMOSE CONGENITA%"  # Agravo que está sendo digitado
    try:
        # --- MONITORAMENTO DE RECURSOS ---
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

        # Checagem de erro após preencher o bloco de Investigação
        if verificar_e_tratar_erro(num_notificacao, agravo_nome):
            log_erro(f"Erro de digitação encontrado em Bloco Investigação para {num_notificacao}. Interrompendo e prosseguindo para a próxima.")
            return

        log_info("Preenchimento completo. Tentando salvar formulário.")
        monitorar_recursos()
        time.sleep(2)

        # Clicar em Salvar
        if wait_and_click(os.path.join(IMAGENS_DIR, "salvar.png"), timeout=15):
            log_info("Clicado em salvar. Aguardando confirmação.")
        else:
            log_erro("Não conseguiu clicar em salvar.")
            raise Exception("Botão 'Salvar' não encontrado.")
            
        time.sleep(2)
        # Primeiro OK
        if wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=10):
            log_info("Primeiro 'ok' clicado com sucesso.")
        else:
            log_erro("Não encontrou a primeira janela 'ok'.")
            raise Exception("Não encontrou primeira janela 'ok'")

        # Checar erro após o primeiro OK
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
        
        # VERIFICA SE TEM QUE ABRIR NOVA NOTIFICAÇÃO
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
        log_erro(f"Erro FATAL durante execução do script toxoplasmose para {num_notificacao}: {e}")
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
    Campos: Nº Notificação, Data da Notificação (3), UF/Município (4/5),
    Unidade de Saúde (6), Data Primeiros Sintomas (7), Nome do Paciente (8),
    Data de Nascimento (9), Idade (10), Sexo (11), Gestante (12),
    Raça/Cor (13), Escolaridade (14), Cartão SUS (15), Nome da Mãe (16),
    UF Residência (17), Município Residência (18), Distrito (19),
    Bairro (20), Logradouro (21), Número (22), Complemento (23),
    Geocampo1 (24), Geocampo2 (25), Ponto de Referência (26),
    CEP (27), Telefone (28), Zona (29), País (30).
    """
    log_debug(f"Campos notificação: {campos}")
    pyautogui.write(num_notificacao)
    pyautogui.press("tab")  
    pyautogui.write(campos['data_notificacao'])  # Pergunta 03
    pyautogui.press("tab", presses=3)
    
    # --- VALIDAÇÃO CONDICIONAL DA UNIDADE NOTIFICADORA (P06) ---
    unidade_notificadora = campos.get('unidade_notificadora', '')
    if unidade_notificadora in ['2', '3', '4', '5', '6']:
        log_info(f"[REGRA-UNIDADE] Unidade Notificadora '{unidade_notificadora}' requer validação imediata de erro.")
        if verificar_e_tratar_erro(num_notificacao, "%TOXOPLASMOSE CONGENITA%"):
            raise Exception(f"Erro de validação automática após detectar Unidade Notificadora inválida para este fluxo (Valor: {unidade_notificadora}).")
    else:
        log_info(f"[REGRA-UNIDADE] Unidade Notificadora '{unidade_notificadora}' permitida, seguindo fluxo normalmente.") 
    
    pyautogui.write(campos['unidade_notificadora'])  # Pergunta 06
    log_debug(f"[P-06] - Valor recebido para Unidade Notificadora: {campos.get('unidade_notificadora', '')}")
    pyautogui.press("tab")

    # --- Pergunta 07/08 - Unidade de Saúde / Nome da Unidade ---
    log_info("Iniciando preenchimento da Pergunta 07 e 08 - Unidade de Saúde / Nome da Unidade.")
    log_debug(f"[P-07] - Valor recebido para Unidade de Saúde / Nome da Unidade: {campos.get('nome_unidade_saude', '')}")
    log_debug(f"[P-08] - Valor recebido para Unidade Notificadora: {campos.get('unidade_notificadora', '')}")

    # --- VALIDAÇÃO CONDICIONAL ANTES DE PREENCHER P06 ---
    if campos['unidade_notificadora'] in {"2", "3", "4", "5", "6"}:
        pyautogui.press("tab", presses=2)
        if verificar_e_tratar_erro(num_notificacao, "%TOXOPLASMOSE CONGENITA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após Pergunta 06 - Unidade Notificadora")
            raise Exception(f"Erro de digitação em Pergunta 06 - Unidade Notificadora. Interrupção forçada.")
    
    nome_da_unidade_bruto = ""
    nome_campo_destino = ""

    # 1. Determinar o nome BRUTO e o nome FORMATADO para digitação
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
    
    # 2. Normalizar o nome BRUTO para comparação
    nome_para_comparacao = nome_da_unidade_bruto.upper().strip()    
    
    # 3. Lógica de SUBSTITUIÇÃO pelo CNES (SHORTCUT)
    log_debug("---Iniciando lógica de substituição por CNES (se aplicável).----")
    log_debug(f"Nome da Unidade de Saúde (Para Comparação): {nome_para_comparacao}")
    log_debug("---FINALIZANDO lógica de substituição por CNES (se aplicável).----")
    
    if nome_para_comparacao in UNIDADES_ESPECIAIS_CNES:
        log_info(f"UNIDADE ESPECIAL DETECTADA ({nome_para_comparacao}). SUBSTITUINDO NOME por CNES {CNES_ESPECIAL}.")
        pyautogui.press("tab") 
        pyautogui.write(CNES_ESPECIAL)

    # 4. Lógica de PESQUISA por CNES
    elif campos['unidade_notificadora'] == "1":
        cnes_temp = get_cnes(nome_para_comparacao)
        
        if cnes_temp:
            cnes_encontrado = cnes_temp[0].get("co_cnes")
            nome_unidade_api = cnes_temp[0].get("ds_estabelecimento")

            if cnes_encontrado:
                log_info(f"CNES ENCONTRADO na API ({nome_unidade_api}). SUBSTITUINDO NOME por CNES {cnes_encontrado}.")
                pyautogui.press("tab") 
                pyautogui.write(cnes_encontrado)
            else:
                log_info(f"CNES da API vazio. REGRA PADRÃO: Digitar o nome formatado: {nome_campo_destino}")
                pyautogui.write(nome_campo_destino)
        else:
            log_info(f"CNES NÃO ENCONTRADO na API. REGRA PADRÃO: Digitar o nome formatado: {nome_campo_destino}")
            pyautogui.write(nome_campo_destino)

    # 5. Lógica Padrão
    else:
        log_debug(f"Preenchendo Nome da Unidade: {nome_campo_destino}")
        pyautogui.write(nome_campo_destino)

    # 6. NAVEGAÇÃO FINAL
    pyautogui.press("tab")

    # --- VALIDAÇÃO DE ERRO APÓS PREENCHIMENTO DA UNIDADE/CNES ---
    erro_contexto = f"Pergunta 07/08 - Unidade/Nome: {nome_para_comparacao}"
    if verificar_e_tratar_erro(num_notificacao, "%TOXOPLASMOSE CONGENITA%"):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")

    # --- AJUSTE DE FOCO PARA P09 (Data dos Primeiros Sintomas) ---
    log_info("Ajustando foco via clique para P07 (Data dos Primeiros Sintomas).")
    pyautogui.click(x=654, y=301)

    time.sleep(2)
    pyautogui.write(campos.get('data_primeiros_sintomas', ''))  # Pergunta 07
    pyautogui.press("tab")

    # ====
    # BLOCO NOTIFICAÇÃO INDIVIDUAL
    # ====

    # (1) Pergunta 08 - Nome do Paciente
    pyautogui.write(campos['nome_paciente'])
    pyautogui.press("tab")

    idade = 0

    # (2) Pergunta 09 - Data de Nascimento
    if campos.get('data_nascimento'):
        data_nascimento = campos['data_nascimento']
        log_debug(f"[P-09] Data de Nascimento: {data_nascimento}")
        pyautogui.write(data_nascimento)
        idade = calcular_idade_formatada(data_nascimento)
        pyautogui.press("tab")

        erro_contexto = f"Pergunta 09 - Data de Nascimento: {data_nascimento}"
        if verificar_e_tratar_erro(num_notificacao, "%TOXOPLASMOSE CONGENITA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
            raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
    else:
        # (3) Pergunta 10 - Idade (quando não há data de nascimento)
        pyautogui.press("tab")
        idade = int(campos.get('idade_calculada_notificador', 0))
        pyautogui.write(str(idade))
        pyautogui.press("tab")

        erro_contexto_10 = f"Pergunta 10 - Idade: {idade}"
        if verificar_e_tratar_erro(num_notificacao, "%TOXOPLASMOSE CONGENITA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto_10}")
            raise Exception(f"Erro de digitação em {erro_contexto_10}. Interrupção forçada.")

        pyautogui.write("4")  # Tipo Idade: Anos
        pyautogui.press("tab")

    # (4) Pergunta 11 - Sexo
    pyautogui.write(campos['sexo'])

    # (5) Pergunta 12 - Gestante
    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.press("tab")
        pyautogui.write(campos.get('gestante', '9'))
    pyautogui.press("tab")

    # (6) Pergunta 13 - Raça/Cor
    if campos.get('raca'):
        pyautogui.write(campos['raca'])
    pyautogui.press("tab")

    # (7) Pergunta 14 - Escolaridade
    valor_sinan = "N/A"

    if idade >= 7 and campos.get('escolaridade'):
        mapeamento_escolaridade = {
            '1': '0',   # Analfabeto
            '2': '1',   # 1ª a 4ª série incompleta
            '3': '2',   # 4ª série completa
            '4': '3',   # 5ª à 8ª série incompleta
            '5': '4',   # Ensino fundamental completo
            '6': '5',   # Ensino médio incompleto
            '7': '6',   # Ensino médio completo
            '8': '7',   # Educação superior incompleta
            '9': '8',   # Educação superior completa
            '10': '10', # Não se aplica
            '99': '9'   # Ignorado
        }

        valor_redcap = campos.get('escolaridade')
        valor_sinan = mapeamento_escolaridade.get(valor_redcap, '9')

        log_debug(f"[P14] Escolaridade preenchida: RedCap='{valor_redcap}' → SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
        pyautogui.press("tab")
 
        if verificar_e_tratar_erro(num_notificacao, "%TOXOPLASMOSE CONGENITA%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após Pergunta 14 - Escolaridade")
            raise Exception(f"Erro de digitação em Pergunta 14 - Escolaridade. Interrupção forçada.")
    else:
        log_debug("[P14] Escolaridade pulada (campo fechado pelo sistema).")

    # (8) Pergunta 15 - Cartão SUS
    time.sleep(0.5)
    if campos.get('cartao_sus'):
        pyautogui.write(campos['cartao_sus'])
        log_debug(f"[Pergunta-15] Código do cartão SUS '{campos['cartao_sus']}'")
    else:
        log_debug("[Pergunta-15] Cartão SUS não informado.")
    pyautogui.press("tab")

    # (9) Pergunta 16 - Nome da Mãe
    if campos.get('nome_mae'):
        pyautogui.write(campos['nome_mae']) 
        log_debug(f"[Pergunta-16] Preenchendo Nome da Mãe: {campos['nome_mae']}")
    else:
        log_debug("[Pergunta-16] Nome da mãe não informado ou em branco.")
    pyautogui.press("tab")

    # ====
    # FIM BLOCO NOTIFICAÇÃO INDIVIDUAL
    # ====

    # ====
    # BLOCO DADOS DE RESIDÊNCIA (Perguntas 17 a 30)
    # ====
    time.sleep(4)
    
    # Pergunta 17 - UF de Residência
    if campos.get('uf_residencia'):
        pyautogui.write(campos['uf_residencia'])
    pyautogui.press("tab")
    
    # Pergunta 18 - Município de Residência
    if campos.get('municipio_residencia'):
        pyautogui.write(campos['municipio_residencia'])
    pyautogui.press("tab")

    # Pergunta 19 - Distrito
    if campos.get('municipio_residencia', '').upper() == 'RECIFE':
        if campos.get('distrito_residencia'):
            log_debug(f"Município é RECIFE, preenchendo Distrito: %{campos['distrito_residencia']}%")
            pyautogui.write(f"%{campos['distrito_residencia']}%")
    else:
        log_debug(f"Município não é RECIFE ({campos.get('municipio_residencia')}), pulando campo Distrito.")
    pyautogui.press("tab")

    # Pergunta 20 - Bairro
    if campos.get('municipio_residencia', '').upper() == 'RECIFE':
        if campos.get('bairro_residencia'):
            log_debug(f"Município é RECIFE, preenchendo Bairro: {campos['bairro_residencia']}")
            pyautogui.write(campos['bairro_residencia'])
    else:
        log_debug(f"Município não é RECIFE ({campos.get('municipio_residencia')}), pulando campo Bairro.")
    pyautogui.press("tab")

    # Pergunta 21 - Logradouro
    if campos.get('endereco_residencia'): 
        pyautogui.write(campos['endereco_residencia'])
    
    # Clique para focar no campo 'Código'
    log_info("Clicando para focar no campo 'Código' (x=685, y=507).")
    pyautogui.click(x=685, y=507)
    time.sleep(0.5)
    log_debug("Pulando campo Código (vazio).")
    pyautogui.press("tab")
    
    # Pergunta 22 - Número
    if campos.get('numero_residencia'):
        pyautogui.write(campos['numero_residencia'])
    pyautogui.press("tab")

    # Pergunta 23 - Complemento
    if campos.get('complemento_residencia'):
        pyautogui.write(campos['complemento_residencia'])
    pyautogui.press("tab")

    # Pergunta 24 - Geocampo1 (pula)
    log_debug("Pulando campo Geocampo1 (vazio).")
    pyautogui.press("tab")
    
    # Pergunta 25 - Geocampo2 (pula)
    log_debug("Pulando campo Geocampo2 (vazio).")
    pyautogui.press("tab")
    
    # Pergunta 26 - Ponto de Referência
    if campos.get('ponto_referencia'):
        pyautogui.write(campos['ponto_referencia'])
    pyautogui.press("tab")

    # Pergunta 27 - CEP
    if campos.get('cep_residencia'):
        pyautogui.write(campos['cep_residencia'])
    pyautogui.press("tab") 

    # Pergunta 28 - (DDD) Telefone
    telefone = campos.get('telefone', '')
    if telefone and len(telefone) >= 3:
        pyautogui.write(telefone[:2])
        pyautogui.press("tab")
        pyautogui.write(telefone[2:])
        pyautogui.press("tab")
    else:
        pyautogui.press("tab", presses=2)

    # Pergunta 29 - Zona
    if campos.get('zona'):
        log_info(f"Preenchimento da ZONA: {campos['zona']}")
        pyautogui.write(campos['zona'])
    pyautogui.press("tab")

    # Pergunta 30 - País (se residente fora do Brasil)
    # Campo normalmente vazio para residentes no Brasil
    if campos.get('pais_residencia'):
        pyautogui.write(campos['pais_residencia'])
    pyautogui.press("tab")

    log_debug(f"Idade calculada/fornecida: {idade}")
    return idade


# ====================================================================
# BLOCO INVESTIGAÇÃO (Campos 31 a 43) - Toxoplasmose Congênita
# ====================================================================
def preencher_bloco_investigacao(campos, idade, num_notificacao):
    """
    Preenche os campos da aba 'Investigação' do SINAN NET para Toxoplasmose Congênita.
    Campos: Data da Investigação (31), Classificação Final (32),
    Critério de Confirmação/Descarte (33), Caso Autóctone (34),
    UF (35), País (36), Município (37), Código IBGE (37.1),
    Distrito (38), Bairro (39), Doença Relacionada ao Trabalho (40),
    Evolução do Caso (41), Data do Óbito (42), Data do Encerramento (43),
    Observações adicionais.
    """
    log_debug(f"Campos investigação: {campos}")
    log_debug(f"Idade recebida para investigação: {idade}")

    agravo_nome = "%TOXOPLASMOSE CONGENITA%"

    # --- Pergunta 31: Data da Investigação ---
    if campos.get('data_investigacao'):
        pyautogui.write(campos['data_investigacao'])
        log_debug(f"[P31] Data da Investigação: {campos['data_investigacao']}")
    pyautogui.press("tab")

    # --- Pergunta 32: Classificação Final ---
    # 1 - Confirmado / 2 - Descartado
    if campos.get('classificacao_final'):
        pyautogui.write(campos['classificacao_final'])
        log_debug(f"[P32] Classificação Final: {campos['classificacao_final']}")
    pyautogui.press("tab")

    # --- Pergunta 33: Critério de Confirmação/Descarte ---
    # 1 - Laboratorial / 2 - Clínico-Epidemiológico
    if campos.get('criterio_confirmacao'):
        pyautogui.write(campos['criterio_confirmacao'])
        log_debug(f"[P33] Critério de Confirmação/Descarte: {campos['criterio_confirmacao']}")
    pyautogui.press("tab")

    # --- Pergunta 34: O caso é autóctone do município de residência? ---
    # 1 - Sim / 2 - Não / 3 - Indeterminado
    if campos.get('caso_autoctone'):
        pyautogui.write(campos['caso_autoctone'])
        log_debug(f"[P34] Caso Autóctone: {campos['caso_autoctone']}")
    pyautogui.press("tab")

    # --- Pergunta 35: UF (Local Provável da Fonte de Infecção) ---
    if campos.get('uf_autoctone'):
        pyautogui.write(campos['uf_autoctone'])
        log_debug(f"[P35] UF Local Provável: {campos['uf_autoctone']}")
    pyautogui.press("tab")

    # --- Pergunta 36: País ---
    if campos.get('pais_autoctone'):
        pyautogui.write(campos['pais_autoctone'])
        log_debug(f"[P36] País: {campos['pais_autoctone']}")
    pyautogui.press("tab")

    # --- Pergunta 37: Município ---
    if campos.get('municipio_autoctone'):
        pyautogui.write(campos['municipio_autoctone'])
        log_debug(f"[P37] Município Local Provável: {campos['municipio_autoctone']}")
    pyautogui.press("tab")

    # --- Código IBGE (campo automático, pula) ---
    pyautogui.press("tab")

    # --- Pergunta 38: Distrito ---
    if campos.get('distrito_autoctone'):
        pyautogui.write(campos['distrito_autoctone'])
        log_debug(f"[P38] Distrito: {campos['distrito_autoctone']}")
    pyautogui.press("tab")

    # --- Pergunta 39: Bairro ---
    if campos.get('bairro_autoctone'):
        pyautogui.write(campos['bairro_autoctone'])
        log_debug(f"[P39] Bairro: {campos['bairro_autoctone']}")
    pyautogui.press("tab")

    # --- Pergunta 40: Doença Relacionada ao Trabalho ---
    # 1 - Sim / 2 - Não / 9 - Ignorado
    valor_doenca_trabalho = campos.get('doenca_trabalho', '9')
    pyautogui.write(valor_doenca_trabalho)
    log_debug(f"[P40] Doença Relacionada ao Trabalho: {valor_doenca_trabalho}")
    pyautogui.press("tab")

    # --- Pergunta 41: Evolução do Caso ---
    # 1 - Cura / 2 - Óbito pelo agravo notificado / 3 - Óbito por outras causas / 9 - Ignorado
    valor_evolucao = campos.get('evolucao_caso', '9')
    pyautogui.write(valor_evolucao)
    log_debug(f"[P41] Evolução do Caso: {valor_evolucao}")
    pyautogui.press("tab")

    # --- Pergunta 42: Data do Óbito ---
    # Preenchido apenas se Evolução for 2 ou 3 (Óbito)
    if valor_evolucao in ['2', '3'] and campos.get('data_obito'):
        pyautogui.write(campos['data_obito'])
        log_debug(f"[P42] Data do Óbito: {campos['data_obito']}")
    pyautogui.press("tab")

    # --- Pergunta 43: Data do Encerramento ---
    data_encerramento = campos.get('data_encerramento', '').strip()
    if data_encerramento:
        pyautogui.write(data_encerramento)
        log_debug(f"[P43] Data do Encerramento: {data_encerramento}")
    pyautogui.press("tab")

    # --- VALIDAÇÃO DE ERRO APÓS DATA DE ENCERRAMENTO ---
    erro_contexto = f"Pergunta 43 - Data de Encerramento: {data_encerramento}"
    if verificar_e_tratar_erro(num_notificacao, agravo_nome):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")

    # --- Observações adicionais ---
    if campos.get('observacoes'):
        pyautogui.write(campos['observacoes'])
        log_debug(f"[Observações] {campos['observacoes']}")
