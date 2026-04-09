import sys
import os
import pyautogui
import time
import requests

pyautogui.FailSafeException = True  # DeAtivar a exceção de segurança do PyAutoGUI

# Garante que o Python encontre os módulos da pasta raiz do projeto (3_sinan_rpa)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# ATUALIZAÇÃO: 'verificar_e_tratar_erro' adicionada.
from utils import wait_and_click, get_usuario_ativo, formatar_unidade_saude, calcular_idade_formatada, verificar_e_tratar_erro, monitorar_recursos, get_cnes

# ATUALIZAÇÃO: 'registrar_erro' adicionada.
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


def executar_violencia(item, reaproveitar_sessao=False, tem_proxima=False):
    # --- ÁREA DE "LIMPEZA" / INICIALIZAÇÃO ---
    # Aqui você garante que as variáveis comecem zeradas para ESSA ficha
        #idade = 0 
        #erro_encontrado = False
        #lista_pendencias = []
    # -----------------------------------------
    num_notificacao = item.get("num_notificacao")
    agravo_nome = "%VIOLENC%" # Agravo que está sendo digitado
    try:
        # --- NOVO: MONITORAMENTO DE RECURSOS ---
        # Chama a função para logar o uso de CPU e RAM no início de cada execução
        # Isso ajuda a identificar se travamentos são causados por falta de memória
        monitorar_recursos() 
        # ---------------------------------------
        if not reaproveitar_sessao:
            abrir_sinan()
            username, password = get_usuario_ativo()
            login(username, password)
            time.sleep(6) #Tempo para teste (12/12/2025) Coloquei porque estava demorando para abrir o sinan
            selecionar_agravo(agravo_nome) # Seleciona o agravo de violência

        log_info(f"Iniciando preenchimento da notificação: {num_notificacao}")
        idade = preencher_bloco_notificacao(item["notificacao"], num_notificacao)
        
        # A validação de erro interna (verificar_e_tratar_erro) já levanta uma Exception se for o caso.
        
        log_info("Notificação preenchida. Iniciando investigação.")
        preencher_bloco_investigacao(item["investigacao"], idade, num_notificacao) # CORREÇÃO AQUI: Passando 'num_notificacao' como argumento

        # Checagem de erro após preencher o bloco de Investigação
        if verificar_e_tratar_erro(num_notificacao, agravo_nome):
            log_erro(f"Erro de digitação encontrado em Bloco Investigação para {num_notificacao}. Interrompendo e prosseguindo para a próxima.")
            return # Sai da função

        log_info("Preenchimento completo. Tentando salvar formulário.")
        # [OPCIONAL] Espiada no consumo antes de salvar (momento crítico)
        monitorar_recursos()
        time.sleep(2)

        # Usa caminho absoluto para salvar.png
        if wait_and_click(os.path.join(IMAGENS_DIR, "salvar.png"), timeout=15):
            log_info("Clicado em salvar. Aguardando confirmação.")
        else:
            log_erro("Não conseguiu clicar em salvar.")
            raise Exception("Botão 'Salvar' não encontrado.")

        # Checar erro após clicar em SALVAR (erros de validação pop-up)
        #if verificar_e_tratar_erro(num_notificacao, agravo_nome):
        #    log_erro(f"Erro de validação encontrado após salvar para {num_notificacao}. Interrompendo e prosseguindo para a próxima.")
        #    return # Sai da função.
            
        time.sleep(2)
        # Usa caminho absoluto para ok.png
        if wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=10):
            log_info("Primeiro 'ok' clicado com sucesso.")
        else:
            log_erro("Não encontrou a primeira janela 'ok'.")
            raise Exception("Não encontrou primeira janela 'ok'")

        # Checar erro após o primeiro OK
        if verificar_e_tratar_erro(num_notificacao, agravo_nome):
            log_erro(f"Erro encontrado após 1º OK para {num_notificacao}. Interrompendo e prosseguindo para a próxima.")
            return # Sai da função.

        time.sleep(2)
        log_info("Verificando existência da segunda confirmação ('ok').")
        try:
            if wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=5):
                log_info("Segundo 'ok' clicado com sucesso.")
            else:
                log_info("Segunda janela 'ok' não apareceu. Continuando sem clicar.")
        except Exception:
            log_info("Erro leve ao verificar segundo 'ok'. Prosseguindo mesmo assim.")
        
        # FINALMENTE, APÓS SALVAR, VERIFICA SE TEM QUE ABRIR NOVA NOTIFICAÇÃO
        time.sleep(4)
        log_info("Aguardando janela 'Deseja incluir nova notificação deste agravo?'.")
        
        # Etapa 1: Visualiza a imagem da janela, sem clicar
        if pyautogui.locateOnScreen(os.path.join(IMAGENS_DIR, "novo_ou_nao.png"), confidence=0.8):
            log_info("Encontrada tela 'Deseja incluir nova notificação deste agravo?'.")
            
            # Etapa 2: Decide se clica em SIM ou NÃO
            if tem_proxima:
                log_info("Clicando em 'Sim' para novo formulário.")
                if not wait_and_click(os.path.join(IMAGENS_DIR, "sim.png"), timeout=3):
                    raise Exception("Botão 'Sim' não encontrado na janela de confirmação.")
            else:
                log_info("Clicando em 'Não' para fechar formulário.")
                if not wait_and_click(os.path.join(IMAGENS_DIR, "nao.png"), timeout=3):
                    raise Exception("Botão 'Não' não encontrado na janela de confirmação.")
        else:
            # Se a janela de confirmação não aparecer, registra o erro e salva screenshot
            screenshot_nome = f"erro_nova_notificacao_{item['num_notificacao']}.png"
            pyautogui.screenshot(screenshot_nome)
            log_erro("Não encontrou a tela 'Deseja incluir nova notificação?'.")
            log_erro(f"Screenshot salvo como {screenshot_nome}")
            raise Exception("Não encontrou tela 'Deseja incluir nova notificação?'")

        global primeira_execucao
        primeira_execucao = False
        
    # **BLOCO EXCEPT Corrigido: Registra erro e re-lança a exceção**
    except Exception as e:
        log_erro(f"Erro FATAL durante execução do script violência para {num_notificacao}: {e}")
        # Se houve um erro FATAL, registra o status "erro_digitacao"
        if num_notificacao:
            # Chame registrar_erro (que atualiza para "erro_digitacao")
            registrar_erro(num_notificacao) 
            log_info(f"Status da notificação {num_notificacao} atualizado para 'erro_digitacao' devido a erro fatal.")
        raise e # Interrompe o fluxo e impede que o bloco 'else' seja executado.
        
    # **BLOCO ELSE: Executado SOMENTE se o 'try' for concluído sem exceções.**
    else:
        # Atualiza o STATUS NA API para 'concluido'
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
    log_info(f"Realizando login com o usuário: {usuario}") # Loga apenas o usuário
    log_info(f"Senha do usuário: {senha}") # senha
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

def preencher_bloco_notificacao(campos, num_notificacao):
    log_debug(f"Campos notificação: {campos}")
    pyautogui.write(num_notificacao)
    pyautogui.press("tab")  
    pyautogui.write(campos['data_notificacao']) # Pergunta 03
    pyautogui.press("tab", presses=3)
    
    pyautogui.write(campos['unidade_notificadora']) # Pergunta 06
    log_debug(f"[P-06] - Valor recebido para Unidade Notificadora: {campos.get('unidade_notificadora', '')}")
    pyautogui.press("tab")

    #Colocar uma regra para validar da unidade notificadora dependendo do valor da pergunta 06
    # Se (Unidade de Saúde) For 2,3,4,5 ou 6 , chamar a função de tratamento de erro (if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):)
    
    # Else se (Unidade de Saúde) For 1 ou 7, seguir o fluxo normal 


    # --- INÍCIO DA NOVA LÓGICA --- Pergunta 08
    log_info("Iniciando preenchimento da Pergunta 07 e 08 - Unidade de Saúde / Nome da Unidade.")
    log_debug(f"[P-07] - Valor recebido para Unidade de Saúde / Nome da Unidade: {campos.get('nome_unidade_saude', '')}")
    log_debug(f"[P-08.1] (us_vio) - Nome da Unidade de Saúde: {campos.get('nome_unidade_saude')}")
    log_debug(f"[P-08.1] (nm_un_vio)- Nome da Unidade Notificadora: {campos.get('nome_unidade_notificadora')}")
    log_debug(f"[P-08] - Valor recebido para Unidade Notificadora: {campos.get('unidade_notificadora', '')}")


    # ==============================================
    # Campo: Unidade Notificadora
    #
    # Valores possíveis:
    # 1 - Unidade de Saúde
    # 2 - Unidade de Assistência Social
    # 3 - Estabelecimento de Ensino
    # 4 - Conselho Tutelar
    # 5 - Unidade de Saúde Indígena
    # 6 - Centro Especializado de Atendimento à Mulher
    # 7 - Outros
    #
    # Mapeamento de percurso do sistema:
    # UNIDADE_NOTIFICADORA = {
    #     "1": "Pula para a pergunta 08",
    #       "2": "Pula para a pergunta 07",
    #       "3": "Pula para a pergunta 07",
    #       "4": "Pula para a pergunta 07",
    #       "5": "Pula para a pergunta 07",
    #       "6": "Pula para a pergunta 07",
    #     "7": "Pula para a pergunta 08"
    # }
    #
    # Observação:
    # Caso o valor seja "7" (Outros), o sistema preenche o nome informado manualmente.
    # ==============================================
    
    # ==============================================
    # Campo: Unidade Notificadora (P6)
    #
    # Mapeamento de percurso do sistema:
    # Caso o valor seja "7" (Outros), o sistema preenche o nome informado manualmente (P7).
    # Caso seja "1" (Unidade de Saúde), o sistema preenche o nome da unidade (P8).
    # ==============================================
    
    nome_da_unidade_bruto = "" # Variável para armazenar o nome bruto para comparação
    nome_campo_destino = ""    # Variável para armazenar o nome formatado para digitação (se não for especial)

    # 1. Determinar o nome BRUTO e o nome FORMATADO para digitação
    if campos['unidade_notificadora'] == "7":
        # P6 = "7" (Outros) -> O campo de nome é P7
        nome_da_unidade_bruto = campos['nome_unidade_notificadora']
        nome_completo = f"{nome_da_unidade_bruto} DO RECIFE"
        nome_campo_destino = nome_completo # Será digitado o nome completo se não for especial
        
    elif campos['unidade_notificadora'] == "1":
        # P6 = "1" (Unidade de Saúde) -> O campo de nome é P8
        valor_unidade_saude = campos.get('nome_unidade_saude', '')
        
        # Tenta converter o código para nome, ou usa o valor bruto
        try:
            codigo_unidade = int(valor_unidade_saude)
            nome_da_unidade_bruto = buscar_estabelecimento(codigo_unidade)
        except (ValueError, TypeError):
            nome_da_unidade_bruto = valor_unidade_saude
        
        nome_campo_destino = formatar_unidade_saude(nome_da_unidade_bruto)
    
    # 2. Normalizar o nome BRUTO para comparação com a lista de CNES Especiais
    nome_para_comparacao = nome_da_unidade_bruto.upper().strip()    
    
    # 3. Lógica de SUBSTITUIÇÃO pelo CNES (SHORTCUT)
    log_debug("---Iniciando lógica de substituição por CNES (se aplicável).--------")
    log_debug(f"Nome da Unidade de Saúde (Para Comparação): {nome_para_comparacao}")
    log_debug(f"[P-08.1] (us_vio) - Nome da Unidade de Saúde: {campos.get('nome_unidade_saude')}")
    log_debug("---FINALIZANDO lógica de substituição por CNES (se aplicável).--------")
    if nome_para_comparacao in UNIDADES_ESPECIAIS_CNES:
        
        # REGRA 1: Unidade Especial (Usa CNES fixo)
        log_info(f"UNIDADE ESPECIAL DETECTADA ({nome_para_comparacao}). SUBSTITUINDO NOME por CNES {CNES_ESPECIAL}.")
        pyautogui.press("tab") 
        pyautogui.write(CNES_ESPECIAL)

    # 4. Lógica de PESQUISA por CNES (Se não for especial, e for uma Unidade de Saúde)
    elif campos['unidade_notificadora'] == "1":
        
        # Buscar CNES no endpoint de CNES
        # O resultado é uma lista de dicionários [{ds_estabelecimento: nome, co_cnes: cnes}]
        cnes_temp = get_cnes(nome_para_comparacao)
        
        if cnes_temp:
            # Pega o CNES do primeiro resultado encontrado (ou o único)
            cnes_encontrado = cnes_temp[0].get("co_cnes")
            nome_unidade_api = cnes_temp[0].get("ds_estabelecimento")

            if cnes_encontrado:
                # REGRA 2: Unidade de Saúde VÁLIDA (Usa CNES retornado da API)
                log_info(f"CNES ENCONTRADO na API ({nome_unidade_api}). SUBSTITUINDO NOME por CNES {cnes_encontrado}.")
                pyautogui.press("tab") 
                pyautogui.write(cnes_encontrado)
            else:
                # Caso o CNES tenha vindo da API, mas o valor 'co_cnes' esteja vazio (improvável, mas seguro)
                log_info(f"CNES da API vazio. REGRA PADRÃO: Digitar o nome formatado: {nome_campo_destino}")
                pyautogui.write(nome_campo_destino)
        else:
            # Caso a API não encontre o estabelecimento (cnes_temp está vazio)
            log_info(f"CNES NÃO ENCONTRADO na API. REGRA PADRÃO: Digitar o nome formatado: {nome_campo_destino}")
            pyautogui.write(nome_campo_destino)

    # 5. Lógica Padrão (Se não se enquadrou em nenhuma das regras acima)
    # Isso cobre:
    # - Unidades P6="7" (Outros) que devem digitar o nome completo.
    # - Qualquer outro caso que falhou na busca.
    else:
        # REGRA PADRÃO: Digitar o nome formatado (ou como %BUSCA%)
        log_debug(f"Preenchendo Nome da Unidade: {nome_campo_destino}")
        pyautogui.write(nome_campo_destino)

    # 6. NAVEGAÇÃO FINAL (MOVE PARA P9)
    # Após digitar o Nome/CNES, o próximo TAB leva para a Data da Ocorrência (P9),
    # pulando o campo 'Código CNES' dedicado que viria depois do Nome.
    pyautogui.press("tab")
    


    # [ A FAZER ] Apos o TAB ele está voltando o foco para o Numero da Notificação (01), então é necessário. voltar o foco para p campo (Pergunta 09)( x=654, y=301) - Data da Ocorrência.

    # --- INCLUSÃO DA VALIDAÇÃO DE ERRO APÓS PREENCHIMENTO DA UNIDADE/CNES ---
    # O agravo é hardcoded como "%VIOLENC%" na função 'executar_violencia'.
    
    # 💡 Mensagem de erro contextualizada
    erro_contexto = f"Pergunta 07/08 - Unidade/Nome: {nome_para_comparacao}"
    
    if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}") # Loga o contexto
        # Forçamos a interrupção do preenchimento desta ficha.
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
    # --- FIM DA VALIDAÇÃO DE ERRO ---

    # --- NOVO: AJUSTE DE FOCO OBRIGATÓRIO PARA P09 (Data da Ocorrência) ---
    # Correção do desvio de foco após a validação da Unidade/CNES (P08)
    log_info("Ajustando foco via clique para P09 (Data da Ocorrência).")
    # Coordenadas do campo P09: (x=654, y=301)
    pyautogui.click(x=654, y=301)
    # --- FIM DO AJUSTE DE FOCO ---

    # --- FIM DA NOVA LÓGICA --- Pergunta 08




    time.sleep(2)
    pyautogui.write(campos['data_ocorrencia']) # Pergunta 09
    pyautogui.press("tab")    
  # ================================
    # BLOCO NOTIFICAÇÃO INDIVIDUAL
    # ================================

    # (1) Pergunta 10 - Nome do Paciente
    pyautogui.write(campos['nome_paciente'])
    pyautogui.press("tab")

    idade = 0

    # (2) Pergunta 11 - Data de Nascimento
    if campos.get('data_nascimento_completa'):
        data_nascimento = campos['data_nascimento_completa']
        log_debug(f"[P-11] Data de Nascimento: {data_nascimento}")
        pyautogui.write(data_nascimento)
        idade = calcular_idade_formatada(data_nascimento)
        pyautogui.press("tab")

        erro_contexto = f"Pergunta 11 - Data de Nascimento: {data_nascimento}"
        if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
            raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
    else:
        # (3) Pergunta 12 - Idade (quando não há data de nascimento)
        pyautogui.press("tab")
        idade = int(campos.get('idade_calculada_notificador', 0))
        pyautogui.write(str(idade))
        pyautogui.press("tab")

        erro_contexto_12 = f"Pergunta 12 - Idade: {idade}"
        if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto_12}")
            raise Exception(f"Erro de digitação em {erro_contexto_12}. Interrupção forçada.")

        pyautogui.write("4")  # Tipo Idade: Anos
        pyautogui.press("tab")

    # (4) Pergunta 13 - Sexo (REGRA MANTIDA)
    pyautogui.write(campos['sexo'])

    # (5) Pergunta 14 - Gestante (campo fechado na maioria dos casos)
    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.press("tab")
        pyautogui.write(campos.get('gestante', '9'))
    pyautogui.press("tab")

    # (6) Pergunta 15 - Raça/Cor
    if campos.get('raca'):
        pyautogui.write(campos['raca'])
    pyautogui.press("tab")

    # ------------------------------------------------
    # (7) Pergunta 16 - Escolaridade
    # REGRA 01 APLICADA AQUI
    # ------------------------------------------------

    # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO (ESCOLARIDADE) ---
    # Vamos ter que Comparar a idade dele com a escolaridade...

    valor_sinan = "N/A"

    if idade >= 7 and campos.get('escolaridade'):
        # ⚠️ Escolaridade SOMENTE quando idade >= 7

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

        log_debug(f"[P16] Escolaridade preenchida (idade < 6): RedCap='{valor_redcap}' → SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
        # Sai do campo de escolaridade
        pyautogui.press("tab")
 
    # Validação SOMENTE se o campo foi preenchido
   # if idade <= 6 and campos.get('escolaridade'):
    #    erro_contexto = f"[Pergunta 16] Escolaridade: {valor_sinan}"
        if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
            raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
    else:
        log_debug("[P16] Escolaridade pulada (campo fechado pelo sistema).")

    

    

    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO (ESCOLARIDADE) ---

    # (8) Pergunta 17 - Cartão SUS
    time.sleep(0.5)
    if campos.get('cartao_sus'):
        pyautogui.write(campos['cartao_sus'])
        log_debug(f"[Pergunta-17] Código do cartão SUS '{campos['cartao_sus']}'")
    else:
        log_debug("[Pergunta-17] Cartão SUS não informado.")
    pyautogui.press("tab")

    # (9) Pergunta 18 - Nome da Mãe
    if campos.get('nome_mae'): # pergunta 18
        pyautogui.write(campos['nome_mae']) 
        log_debug(f"[Pergunta-18] Preenchendo Nome da Mãe: {campos['nome_mae']}")
    else:
        log_debug("[Pergunta-18] Nome da mãe não informado ou em branco.")
    pyautogui.press("tab")

    # ================================
    # FIM BLOCO NOTIFICAÇÃO INDIVIDUAL
    # ================================



    time.sleep(4)
    # Bloco dados de residência
    pyautogui.write(campos['uf_residencia_vio']) # Pergunta 19
    pyautogui.press("tab")
    
    pyautogui.write(campos['municipio_residencia'])
    pyautogui.press("tab")

    # --- INÍCIO DA ATUALIZAÇÃO - 10/10/2025 ---
    # Se o município for RECIFE, preenche o distrito. Caso contrário, apenas pula o campo.
    if campos.get('municipio_residencia', '').upper() == 'RECIFE':
        if campos.get('distrito_residencia'):
            log_debug(f"Município é RECIFE, preenchendo Distrito: %{campos['distrito_residencia']}%")
            pyautogui.write(f"%{campos['distrito_residencia']}%")
    else:
        log_debug(f"Município não é RECIFE ({campos.get('municipio_residencia')}), pulando campo Distrito.")
    
    pyautogui.press("tab") # Garante a navegação para o próximo campo (Bairro)
    # --- FIM DA ATUALIZAÇÃO ---

    # Se o município for RECIFE, preenche o bairro. Caso contrário, apenas pula o campo.
    if campos.get('municipio_residencia', '').upper() == 'RECIFE':
        if campos.get('bairro_residencia'):
            log_debug(f"Município é RECIFE, preenchendo Bairro: {campos['bairro_residencia']}")
            pyautogui.write(campos['bairro_residencia']) # Pergunta 22
    else:
        log_debug(f"Município não é RECIFE ({campos.get('municipio_residencia')}), pulando campo Bairro.")

    pyautogui.press("tab") # Garante a navegação para o próximo campo (Endereço)
     # --- FIM DA ATUALIZAÇÃO ---

    if campos.get('endereco_residencia'): 
        pyautogui.write(campos['endereco_residencia']) # Pergunta 23.0
    
    # Ao inves de ser tab tem que ser um click(x=685, y=507) para ir para o campo Codigo
    log_info("Clicando para focar no campo 'Código' (x=685, y=507).")
    pyautogui.click(x=685, y=507)
    time.sleep(0.5)
    log_debug("Pulando campo Código (vazio).")
    pyautogui.press("tab")
    
    if campos.get('numero_residencia'):
        pyautogui.write(campos['numero_residencia']) # Pergunta 24
    pyautogui.press("tab")

    if campos.get('complemento_residencia'):
        pyautogui.write(campos['complemento_residencia'])
    pyautogui.press("tab") # Após o complemento, um TAB vai para Geocampo1

    # Pula os campos "Geocampo1" e "Geocampo2", que sempre vêm vazios
    log_debug("Pulando campo Geocampo1 (vazio).")
    pyautogui.press("tab")
    log_debug("Pulando campo Geocampo2 (vazio).")
    pyautogui.press("tab")
    
    if campos.get('ponto_referencia'):
        pyautogui.write(campos['ponto_referencia'])
    pyautogui.press("tab")

    if campos.get('cep_residencia'): # Pergunta 29
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
        log_info(f"Preenchimento da ZONA: {campos['zona']}")
        pyautogui.write(campos['zona']) # Pergunta 31
    pyautogui.press("tab")
    log_debug(f"Idade calculada/fornecida: {idade}")
    return idade

def preencher_bloco_investigacao(campos, idade, num_notificacao):
    log_debug(f"Campos investigação: {campos}")
    log_debug(f"Idade recebida para investigação: {idade}")
    
    #Verificar se a dados em ocupacao., ele faz a pesquisa via %NOME_OCUPACAO%
    #log_debug(f"Campos (34) Ocupação: {campos['ocupacao']}")
    #if campos.get('ocupacao'):
    #    valor_ocupacao = campos['ocupacao']
    #    log_debug(f"Preenchendo campo (34) Ocupação com valor recebido: '{valor_ocupacao}'")
    #    pyautogui.write(f"%{valor_ocupacao}%")
    #    pyautogui.press("tab")   # Avaliar necessidade
    #    pyautogui.press("enter") # Avaliar necessidade
    #else:
    #    log_debug("Campo (34) Ocupação está vazio ou ausente no JSON recebido.")
    pyautogui.press("tab")
    
    # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO - Pergunta( 35/36/37)
    if idade >= 10:
        
        # --- PERGUNTA 35 (ESTADO CIVIL) ---
        valor_sinan_35 = '9' # Padrão para Ignorado
        if campos.get('estado_civil'): # Pergunta 35
            mapeamento_estado_civil = {
                '1': '1',  # Solteiro
                '2': '2',  # Casado / união consensual
                '3': '3',  # Viúvo
                '4': '4',  # Separado
                '5': '8',  # Não se aplica
                '6': '9'   # Ignorado
            }
            valor_redcap = campos.get('estado_civil')
            valor_sinan_35 = mapeamento_estado_civil.get(valor_redcap, '9')
        
        log_debug(f"Preenchendo P35 Estado Civil: {valor_sinan_35}")
        pyautogui.write(valor_sinan_35)
        # ESTE TAB É OBRIGATÓRIO PARA IR PARA A P36
        pyautogui.press("tab")
        
        # --- PERGUNTA 36 (ORIENTAÇÃO SEXUAL) ---
        valor_sinan_36 = '9' # Padrão para Ignorado
        if campos.get('orientacao_sexual'): # Pergunta 36 
            mapeamento_orientacao = {
                '1': '1',  # Heterossexual
                '2': '2',  # Homossexual (gay/lésbica)
                '3': '3',  # Bissexual
                '4': '8',  # Não se aplica
                '5': '9'   # Ignorado
            }
            valor_redcap = campos.get('orientacao_sexual')
            valor_sinan_36 = mapeamento_orientacao.get(valor_redcap, '9')

        log_debug(f"Preenchendo P36 Orientação Sexual: {valor_sinan_36}")
        pyautogui.write(valor_sinan_36)
        # ESTE TAB É OBRIGATÓRIO PARA IR PARA A P37
        pyautogui.press("tab")
        
        # --- PERGUNTA 37 (IDENTIDADE DE GENERO) ---
        valor_sinan_37 = '9' # Padrão para Ignorado
        if campos.get('identidade_genero'): # Pergunta 37
            mapeamento_genero = {
                '1': '1',  # Travesti
                '2': '2',  # Mulher Transexual
                '3': '3',  # Homem Transexual
                '4': '8',  # Não se aplica
                '5': '9'   # Ignorado
            }
            valor_redcap = campos.get('identidade_genero')
            valor_sinan_37 = mapeamento_genero.get(valor_redcap, '9')
            
        log_debug(f"Preenchendo P37 Identidade de Gênero: {valor_sinan_37}")
        pyautogui.write(valor_sinan_37)
        
        # ESTE TAB É OBRIGATÓRIO PARA IR PARA A P38 (Deficiência)
        pyautogui.press("tab")
        
        # --- VALIDAÇÃO DE ERRO APÓS DADOS SOCIAIS (Removida a lógica de mapeamento duplicada) ---
        # Removido o bloco de validação de erro para simplificar o fluxo de TABs e prevenir erros de cursor.
        
    else:
        # Se idade for < 10:
        # Após a P34 (Ocupação), o cursor está no campo P35/Estado Civil (ou P36/Orientação, dependendo da tela)
        # Precisamos de 3 TABS para pular os campos P35, P36 e P37 e cair no P38 (Deficiência).
        log_debug("Idade < 10. Campos P35, P36 e P37 estão bloqueados. O cursor já está na P38.")
        # NENHUM 'pyautogui.press("tab")' ADICIONAL É NECESSÁRIO AQUI.
        pass

    
    # --- PERGUNTA 38 (DEFICIÊNCIA) (def_transt_vio) ---
    # O cursor está AGORA no campo P38, pronto para receber o valor.
    
    # Preenche P38 (1-Sim, 2-Não, 9-Ignorado)
    #if campos.get('deficiencia'):
    #    pyautogui.write(campos['deficiencia'])
    #log_debug(f"Campos (38) DEFICIENCIA: {campos.get('deficiencia')}")

    # --- Pergunta 38: Possui algum tipo de deficiência/transtorno? ---
    valor_deficiencia = campos.get('deficiencia', '').strip() or '9'  # Se vier vazio, assume '9' (Ignorado)
    pyautogui.write(valor_deficiencia)
    log_debug(f"Campos (38) DEFICIÊNCIA preenchido: {valor_deficiencia}")


        
    # LÓGICA CONDICIONAL:
    # Se for '1', entra na P39 e preenche os detalhes.
    # Se for '2' ou '9', pula o if, dá um TAB e cai direto na P40.
    
    if campos.get('deficiencia') == "1":
        # ... (Preenchimento dos sub-campos da P39 com TABs intermediários) ...
        # O último TAB dentro deste bloco é o que leva à P40 (UF Ocorrência)
               
       # O campo 38 é do tipo COMBO BOX, ou seja, ao digitar, ele precisa de um TAB
        # para ir para o primeiro campo de Deficiência (Se a Deficiência for '1' - Sim)
        
        pyautogui.press("tab") # Entra no primeiro campo de detalhe (Deficiência Física)
        
        # 1. Deficiência Física (39.1)
        pyautogui.write(campos.get('deficiencia_fisica', '9')) 
        pyautogui.press("tab")
        
        # 2. Deficiência Mental (39.2)
        pyautogui.write(campos.get('deficiencia_mental', '9')) 
        pyautogui.press("tab")
        
        # 3. Deficiência Visual (39.3)
        log_debug(f"Campos (39.3) DEFICIENCIA VISUAL: {campos.get('deficiencia_visual', '9')}")
        pyautogui.write(campos.get('deficiencia_visual', '9')) 
        pyautogui.press("tab")
        
        # 4. Deficiência Auditiva (39.4)
        log_debug(f"Campos (39.4) DEFICIENCIA AUDITIVA: {campos.get('deficiencia_auditiva', '9')}")
        pyautogui.write(campos.get('deficiencia_auditiva', '9')) 
        pyautogui.press("tab")
        
        # 5. Deficiência Intelectual (39.5) - [Transtorno Mental (SINAN)]  - viol_2 (RedCap)
        pyautogui.write(campos.get('deficiencia_intelectual', '9')) 
        pyautogui.press("tab")
        
        # 6. Transtorno de Comportamento (39.6) - viol_6 (RedCap)
        pyautogui.write(campos.get('transtorno_comportamento', '9')) 
        pyautogui.press("tab")
        
        # 7. Outras Deficiências (39.7)  - viol_7 (RedCap)
        valor_outras = campos.get('outras_deficiencias', '9')
        pyautogui.write(valor_outras)
        
        # Lógica para campo 'Outras' (P39) - Se Sim, descreve
        if valor_outras == "1":
            pyautogui.press("tab") # Entra no campo de descrição
            log_debug(f"Campos (39.7) DESCRIÇÃO OUTRAS DEFICIÊNCIAS: {campos.get('outra_deficiencia', '')}")
            pyautogui.write(campos.get('outra_deficiencia', ''))
            pyautogui.press("tab") # Sai do campo de descrição
            log_debug("(TAB 01 ) -Saindo do campo 'Outras Deficiências' para P40.")
        else:
            log_debug("(TAB 02 ) Não há descrição para 'Outras Deficiências'. Pulando campo de descrição.")
            pyautogui.press("tab") # Sai do campo 'Outras Deficiências' e vai para P40
            
    else:
        # Se 'deficiencia' é '2' (Não) ou '9' (Ignorado), apenas precisamos de um TAB 
        # para sair do campo P38 (combo box) e ir direto para P40.
        log_debug("(TAB 03 ) Deficiência marcada como 'Não' ou 'Ignorado'. Pulando detalhes da P39.")
        pyautogui.press("tab")

    # --- VALIDAÇÃO DE ERRO APÓS DEFICIÊNCIAS ---
    # Verifica se houve algum erro de preenchimento (ex: opção inválida em algum dos sub-campos)
    
    erro_contexto = f"Bloco Deficiências (P38/P39)"
    if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
        log_erro(f"(TAB 04 )ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}")
        raise Exception(f"Erro de digitação em {erro_contexto} para {num_notificacao}. Interrupção forçada.")
    

    # --- PERGUNTA 40 (UF OCORRÊNCIA) - (uf_ocor_vio) ---
    
    log_debug(f"Campos (40) UF OCORRÊNCIA - (Antes do IF): {campos['uf_ocorrencia']}")
    time.sleep(3)
    if campos.get('uf_ocorrencia'):
        pyautogui.write(campos['uf_ocorrencia'])
    log_debug(f"Campos (40) UF OCORRÊNCIA - (Dentro do IF): {campos.get('uf_ocorrencia')}")
    # --- PERGUNTA 40 (UF OCORRÊNCIA) --- (uf_ocor_vio)
    pyautogui.press("tab")
        
    # --- INCLUSÃO DA VALIDAÇÃO DE ERRO APÓS P40 ---
    # caso haja erro na digitação do campo UF Ocorrência (P40) ou campo vazio seguinte chamamos a função de verificação de erro
    uf_valor = campos.get('uf_ocorrencia', 'Vazio')
    erro_contexto = f"Pergunta 40 - UF Ocorrência: {uf_valor}"
    if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}") # Loga o contexto
        # Forçamos a interrupção do preenchimento desta ficha.
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
    # --- FIM DA VALIDAÇÃO DE ERRO ---
       
    # --- PERGUNTA 41 (MUNICIPIO OCORRENCIA) --- (ds_resid) 
    municipio_valor = f"%{campos['municipio_ocorrencia']}%"
    log_info(f"Preenchendo Município Ocorrência (P41): {municipio_valor}") # Log adicionado
    
    pyautogui.write(municipio_valor)
    pyautogui.press("tab", presses=2)

   # --- PERGUNTA 42 (DISTRITO)(ds_resid) ---
    if campos.get('distrito'):
        # Formata o valor com % ao redor (Ex: "1" vira "%1%")
        valor_distrito = f"%{campos['distrito']}%"
        
        log_debug(f"Preenchendo Distrito (P42) formatado: {valor_distrito}")
        pyautogui.write(valor_distrito)
        
    # Pressiona TAB para sair do campo Distrito (preenchido ou não) e ir para Bairro (P43)
    pyautogui.press("tab")
    
    if campos.get('bairro_ocorrencia'): #pergunta 43
        pyautogui.write(campos['bairro_ocorrencia'])
        #pyautogui.press("esc")
    pyautogui.press("tab")
    if campos.get('endereco_ocorrencia'): #pergunta 44
        pyautogui.write(campos['endereco_ocorrencia'])
    pyautogui.press("tab")
    pyautogui.press("esc")
    pyautogui.press("tab")
    if campos.get('codigo'):
        pyautogui.write(campos['codigo'])
    pyautogui.press("tab")
    if campos.get('numero'):
        pyautogui.write(campos['numero']) #pergunta 45
    pyautogui.press("tab")
    if campos.get('complemento'):
        pyautogui.write(campos['complemento'])
    pyautogui.press("tab", presses=3)
    if campos.get('ponto_referencia'):
        pyautogui.write(campos['ponto_referencia'])
    pyautogui.press("tab")
    if campos.get('zona'):
        log_info(f"Preenchimento da ZONA_02: {campos['zona']}")
        pyautogui.write(campos['zona'])
    pyautogui.press("tab")
    if campos.get('horario_ocorrencia'):
        pyautogui.write(campos['horario_ocorrencia'])
    pyautogui.press("tab")
    
    # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO (LOCAL DA OCORRÊNCIA) ---
    mapeamento_local = {
        '1': '01',  # Residência
        '2': '02',  # Habitação coletiva
        '3': '03',  # Escola
        '4': '04',  # Local de prática esportiva
        '5': '05',  # Bar ou similar
        '6': '06',  # Via pública
        '7': '07',  # Comércio / serviços
        '8': '08',  # Indústrias / construção
        '9': '09',  # Outro
        '10': '99'  # Ignorado
    }

    valor_redcap = campos.get('local_ocorrencia')
    valor_sinan = mapeamento_local.get(valor_redcap, '99') 
    
    log_debug(f"Mapeando Local da Ocorrência: RedCap='{valor_redcap}' -> SINAN='{valor_sinan}'")
    
    if valor_sinan == '09': # Corresponde ao '9' do RedCap
        pyautogui.write(valor_sinan)
        pyautogui.press("tab")
        log_debug("Preenchendo descrição de 'Outro local de ocorrência'.")
        pyautogui.write(campos.get('outro_local', ''))
        pyautogui.press("tab")
    else:
        pyautogui.write(valor_sinan)
        pyautogui.press("tab")
    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO ---

    #INCLUIR LISTA COMPARATIVA DE NUMEROS QUE VEM DO REDCAP
    #if campos.get('ocorreu_outras_vezes'): # pergunta 53
    #    pyautogui.write(campos['ocorreu_outras_vezes'])
    #pyautogui.press("tab")

    # --- Pergunta 53: Ocorreu outras vezes? (1-Sim, 2-Não, 9-Ignorado) ---
    valor_ocorreu_outras_vezes = campos.get('ocorreu_outras_vezes', '').strip() or '9'
    pyautogui.write(valor_ocorreu_outras_vezes)
    log_debug(f"Campos (53) Ocorreu outras vezes preenchido: {valor_ocorreu_outras_vezes}")
    pyautogui.press("tab")

    
    #INCLUIR LISTA COMPARATIVA DE NUMEROS QUE VEM DO REDCAP
    
    # Pergunta 54: Lesão Autoprovocada
    # Se 'lesao_autoprovocada' estiver ausente (None) ou em branco, .get() retornará '9'.
    pyautogui.write(campos.get('lesao_autoprovocada', '9')) 
    pyautogui.press("tab")
    
    pyautogui.write(campos['motivo_violencia']) # pergunta 55
    pyautogui.press("tab")
    
    # Bloco Tipo de Violência (Perguntas 56.1 a 56.10)
    # Regra: Se o campo estiver ausente ou em branco, preenche com '9' (Ignorado)

    # pergunta 56.1
    pyautogui.write(campos.get('fisica', '9')) 
    pyautogui.press("tab")

    # pergunta 56.2
    pyautogui.write(campos.get('moral_psicologica', '9')) 
    pyautogui.press("tab")

    # pergunta 56.3
    pyautogui.write(campos.get('tortura', '9')) 
    pyautogui.press("tab")

    # pergunta 56.4 (Violência Sexual - Valor '9' fixo/chumbado)
    pyautogui.write('9') 
    pyautogui.press("tab")

    # pergunta 56.5
    pyautogui.write(campos.get('trafico_pessoas', '9')) 
    pyautogui.press("tab")

    # pergunta 56.6
    pyautogui.write(campos.get('financeiro', '9')) 
    pyautogui.press("tab")

    # pergunta 56.7
    pyautogui.write(campos.get('negligencia_abandono', '9')) 
    pyautogui.press("tab")

    # pergunta 56.8
    pyautogui.write(campos.get('trabalho_infantil', '9')) 
    pyautogui.press("tab")

    # pergunta 56.9
    pyautogui.write(campos.get('intervencao_legal', '9')) 
    pyautogui.press("tab")

    # pergunta 56.10
    pyautogui.write(campos.get('outro_tipo_violencia', '9')) 
    pyautogui.press("tab")

    # Pergunta 56.10.1 (Somente se 'outro_tipo_violencia' for "1")
    if campos.get('outro_tipo_violencia') == "1": 
        # Para o campo de especificação, não se aplica a regra '9', pois ele só é preenchido se o campo anterior for '1'.
        # Aqui, garantimos que se o campo de especificação estiver ausente, ele não cause erro.
        pyautogui.write(campos.get('esp_outro_tipo_violencia', ''))
        pyautogui.press("tab")
    
    # Bloco Meio de Agressão (Perguntas 57.1 a 57.8)
    # Regra: Se o campo estiver ausente ou em branco, preenche com '9' (Ignorado)

    # pergunta 57.1
    pyautogui.write(campos.get('forca_corporal_espancamento', '9')) 
    pyautogui.press("tab")

    # pergunta 57.2
    pyautogui.write(campos.get('enforcamento', '9')) 
    pyautogui.press("tab")

    # pergunta 57.3
    pyautogui.write(campos.get('objeto_contundente', '9')) 
    pyautogui.press("tab")

    # pergunta 57.4
    pyautogui.write(campos.get('objeto_perfurante', '9')) 
    pyautogui.press("tab")

    # pergunta 57.5
    pyautogui.write(campos.get('objeto_quente', '9')) 
    pyautogui.press("tab")

    # pergunta 57.6
    pyautogui.write(campos.get('envenenamento', '9')) 
    pyautogui.press("tab")

    # pergunta 57.7
    pyautogui.write(campos.get('arma_fogo', '9')) 
    pyautogui.press("tab")

    # pergunta 57.8
    pyautogui.write(campos.get('ameaca', '9')) 
    pyautogui.press("tab")
   
   




    # --- INÍCIO DA LÓGICA ATUALIZADA PARA P57.9 e P57.10 ---
    time.sleep(1.5)

    # (1) Recuperar o valor de P57.9 - Outro Meio de Agressão
    valor_outro_meio = (campos.get('outro_meio_agressao', '9'))

    if valor_outro_meio == "1":
        # (2) Preencher o campo P57.9
        pyautogui.write(valor_outro_meio)
        log_info(f"[P57.9] Valor preenchido: {valor_outro_meio}")
        log_info("P57.9 Igual a '1' [Entrou no IF]")

        # (3) Avançar para o campo de especificação (P57.10)
        pyautogui.press("tab")

        # (4) Preencher a especificação
        valor_especificacao = (campos.get('esp_outro_meio_agressao', 'sem especificação'))
        pyautogui.write(valor_especificacao)
        log_info(f"[P57.10] Especificação preenchida: {valor_especificacao}")
        log_info("P57.10 Igual a '1' [Entrou no IF] - [Depois do TAB]")

        # (5) Validar se a especificação foi preenchida corretamente
        if not valor_especificacao:
            erro_contexto = f"Pergunta 57.10 - Especificação 'Outro Meio de Agressão'. Valor recebido: '{valor_especificacao}'"
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO: {erro_contexto}")
            if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
                raise Exception(f"Erro detectado na tela e campo vazio em {erro_contexto}. Interrupção forçada.")
            else:
                raise Exception(f"Campo obrigatório vazio em {erro_contexto}. Interrupção forçada.")

        # (6) Avançar para o campo 60 (somente 1 TAB)
        pyautogui.press("tab")
        pyautogui.click(x=694, y=507) # Click para garantir o foco correto (Primeira execução detectada: Realizando clique de ajuste em (671, 505). o sistema tem um erro de foco, que apos o TAB ele não )
        time.sleep(1.5)

    else:
        # (2) Preencher o campo P57.9
        pyautogui.write(valor_outro_meio)
        log_info(f"[P57.9] Valor preenchido: {valor_outro_meio}")
        log_info("P57.9 diferente de '1', pulando campo de especificação. [Entrou no ELSE]")

        # (3) Clique técnico para garantir foco (se necessário)
        #pyautogui.click(x=671, y=505)
        pyautogui.press("tab")
        pyautogui.click(x=671, y=505)
        pyautogui.press("tab")
        time.sleep(1.0)

        # (4) Avançar para o campo 60 com apenas 1 TAB
        #pyautogui.press("tab")

    # --- FIM DA LÓGICA ATUALIZADA ---
        time.sleep(1.5)

    
    # 1. Obter o valor, usando '9' como padrão se estiver vazio ou ausente ('')
    valor_numero_envolvidos = campos.get('numero_envolvidos', '9') # pergunta 60  # out_agres_vio
    log_info(f"[Pergunta 60 ] Número de envolvidos: {valor_numero_envolvidos}")

    # 2. Digitar o valor da variável que já tem o padrão '9'
    pyautogui.write(valor_numero_envolvidos) 
    pyautogui.press("tab")
    
    # Relação com a Vítima (Perguntas 61.1 a 61.17)
    # Regra: Se o campo estiver ausente ou em branco, preenche com '9' (Ignorado)

    # pergunta 61.1
    pyautogui.write(campos.get('pai', '9')) 
    pyautogui.press("tab")

    # pergunta 61.2
    pyautogui.write(campos.get('mae', '9')) 
    pyautogui.press("tab")

    # pergunta 61.3
    pyautogui.write(campos.get('padrasto', '9')) 
    pyautogui.press("tab")

    # pergunta 61.4
    pyautogui.write(campos.get('madrasta', '9')) 
    pyautogui.press("tab")

    # pergunta 61.5
    pyautogui.write(campos.get('conjuge_parceiro', '9')) 
    pyautogui.press("tab")

    # pergunta 61.6
    pyautogui.write(campos.get('ex_conjuge_parceiro', '9')) 
    pyautogui.press("tab")

    # pergunta 61.7
    pyautogui.write(campos.get('namorado', '9')) 
    pyautogui.press("tab")

    # pergunta 61.8
    pyautogui.write(campos.get('ex_namorado', '9')) 
    pyautogui.press("tab")

    # pergunta 61.9
    pyautogui.write(campos.get('filho', '9')) 
    pyautogui.press("tab")

    # pergunta 61.10
    pyautogui.write(campos.get('irmao', '9')) 
    pyautogui.press("tab")

    # pergunta 61.11
    pyautogui.write(campos.get('amigos_conhecidos', '9')) 
    pyautogui.press("tab")

    # pergunta 61.12
    pyautogui.write(campos.get('desconhecido', '9')) 
    pyautogui.press("tab")

    # pergunta 61.13
    pyautogui.write(campos.get('cuidador', '9')) 
    pyautogui.press("tab")

    # pergunta 61.14
    pyautogui.write(campos.get('patrao_chefe', '9')) 
    pyautogui.press("tab")

    # pergunta 61.15
    pyautogui.write(campos.get('pessoa_relacao_instituicao', '9')) 
    pyautogui.press("tab")

    # pergunta 61.16
    pyautogui.write(campos.get('policial_agente', '9')) 
    pyautogui.press("tab")

    # pergunta 61.17
    pyautogui.write(campos.get('propria_pessoa', '9')) 
    pyautogui.press("tab")

    # Vim um if aqui para verificar a condição e preencher o campo de descrição se necessário
    
    outros_envolvidos_valor = campos.get('outros_envolvidos', '2') # pergunta 61.18
    pyautogui.write(outros_envolvidos_valor)
    if outros_envolvidos_valor == "1":
        pyautogui.press("tab") # Entra no campo de descrição
        log_debug("Preenchendo descrição de 'Outros envolvidos'.")
        pyautogui.write(campos.get('esp_outros_envolvidos', ''))
        pyautogui.press("tab") # Sai do campo de descrição e vai para 'sexo_agressor'
    else:
        # Se for "2" ou "9", um TAB vai direto para 'sexo_agressor'
        pyautogui.press("tab")

    log_debug("Preenchendo o campo 'sexo_agressor'.")
    time.sleep(1.5)

    # Pergunta 62: Sexo do Agressor.
    # A regra é aplicada aqui: se 'sexo_agressor' não estiver no dicionário 
    # 'campos' ou for None/em branco (dependendo de como o .get() está configurado, 
    # mas geralmente usando o valor padrão '9'), ele preencherá com '9'.
    pyautogui.write(campos.get('sexo_agressor', '9'))
    pyautogui.press("tab") # Vai para o campo 'Suspeita de uso de álcool'
    
    # Pergunta 63: Suspeita de Uso de Álcool pelo Agressor
    # Se 'suspeita_alcool' estiver ausente, .get() retornará '9', aplicando a regra.
    pyautogui.write(campos.get('suspeita_alcool', '9')) 
    pyautogui.press("tab")

    # Pergunta 64: Ciclo de Vida do Autor/Agressor
    # Se 'ciclo_vida_autor' estiver ausente, .get() retornará '9', aplicando a regra.
    pyautogui.write(campos.get('ciclo_vida_autor', '9')) 
    pyautogui.press("tab")

    # Encaminhamentos (Perguntas 65.1 a 65.14)
    # Regra: Se o campo estiver ausente ou em branco, preenche com '9' (Ignorado)
    # pergunta 65.1
    pyautogui.write(campos.get('rede_saude', '9')) 
    pyautogui.press("tab")

    # pergunta 65.2
    pyautogui.write(campos.get('rede_assistencia_social', '9')) 
    pyautogui.press("tab")

    # pergunta 65.3
    pyautogui.write(campos.get('rede_educacao', '9')) 
    pyautogui.press("tab")

    # pergunta 65.4
    pyautogui.write(campos.get('rede_atendimento_mulher', '9')) 
    pyautogui.press("tab")

    # pergunta 65.5
    pyautogui.write(campos.get('conselho_tutelar', '9')) 
    pyautogui.press("tab")

    # pergunta 65.6
    pyautogui.write(campos.get('conselho_idoso', '9')) 
    pyautogui.press("tab")

    # pergunta 65.7
    pyautogui.write(campos.get('delegacia_atendimento_idoso', '9')) 
    pyautogui.press("tab")

    # pergunta 65.8
    pyautogui.write(campos.get('centro_ref_direitos_humanos', '9')) 
    pyautogui.press("tab")

    # pergunta 65.9
    pyautogui.write(campos.get('ministerio_publico', '9')) 
    pyautogui.press("tab")

    # pergunta 65.10
    pyautogui.write(campos.get('delegacia_especializada_infancia', '9')) 
    pyautogui.press("tab")

    # pergunta 65.11
    pyautogui.write(campos.get('delegacia_atendimento_mulher', '9')) 
    pyautogui.press("tab")

    # pergunta 65.12
    pyautogui.write(campos.get('outras_delegacias', '9')) 
    pyautogui.press("tab")

    # pergunta 65.13
    pyautogui.write(campos.get('justica_infancia_juventude', '9')) 
    pyautogui.press("tab")

    # pergunta 65.14
    pyautogui.write(campos.get('defensoria_publica', '9')) 
    pyautogui.press("tab")
    
    if campos.get('relacao_trabalho'): # pergunta 66 #se não vier nada colocar 9
        pyautogui.write(campos['relacao_trabalho']) 
    pyautogui.press("tab", presses=2)
    
    if campos.get('relacao_trabalho') == "1": 
        pyautogui.write('9') # OUTROS (não retorna CAT)
        pyautogui.press("tab")
    
    #[old]Caso os registros do REDCap estejam em branco/sem informação, repetir a data de notificação (data_notificacao)
    #if campos.get('data_encerramento'):
    #    pyautogui.write(campos['data_encerramento'])
    #pyautogui.press("tab")
    
    # Versão blindada (evita erro se ambas as datas faltarem)
    #pyautogui.write(campos.get('data_encerramento') or campos.get('data_notificacao') or "")
    #pyautogui.press("tab")

    #data_encerramento = campos.get('data_encerramento', '').strip() or campos.get('data_notificacao', '') # pergunta 69
    #pyautogui.write(data_encerramento)
    #log_debug(f"[Pergunta 69] Campos (Data de Encerramento): {data_encerramento}")
    #pyautogui.press("tab")

    # Bloco anterior (P66) finalizado, agora a P69 (Data de Encerramento)   
    # --- INÍCIO DA CORREÇÃO NA PERGUNTA 69 (Data de Encerramento) ---
    
    data_encerramento = campos.get('data_encerramento', '').strip() or campos.get('data_notificacao', '') # pergunta 69
    # Botar nova regra de preenchimento:
    # Se 'data_encerramento' for maior ou igual que 'data_notificacao', usar 'data_notificacao'
    

    # 1. Digitar o valor
    pyautogui.write(data_encerramento)
    log_debug(f"[Pergunta 69] Campos (Data de Encerramento): {data_encerramento}")
    
    # 2. Sair do campo para que o SINAN faça a validação inicial do formato
    pyautogui.press("tab")

    # 3. VALIDAÇÃO DE ERRO
    erro_contexto = f"Pergunta 69 - Data de Encerramento: {data_encerramento}"
    
    # Chama a função de verificação. Se retornar True (erro detectado):
    if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}") # Loga o contexto
        # Forçamos a interrupção do preenchimento desta ficha.
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
        
    # --- FIM DA CORREÇÃO NA PERGUNTA 69 ---

    if campos.get('observacoes'): # pergunta 70 (Observações adicionais)
        pyautogui.write(campos['observacoes'])