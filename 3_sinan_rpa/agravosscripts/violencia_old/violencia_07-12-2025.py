import sys
import os
import pyautogui
import time

pyautogui.FailSafeException = True  # DeAtivar a exceção de segurança do PyAutoGUI

# Garante que o Python encontre os módulos da pasta raiz do projeto (3_sinan_rpa)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# ATUALIZAÇÃO: 'verificar_e_tratar_erro' adicionada.
from utils import wait_and_click, get_usuario_ativo, formatar_unidade_saude, calcular_idade_formatada, verificar_e_tratar_erro, monitorar_recursos

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
    time.sleep(8)

def login(usuario, senha):
    log_info(f"Realizando login com o usuário: {usuario}") # Loga apenas o usuário
    log_info(f"Senha do usuário: {senha}") # senha
    pyautogui.write(usuario)
    pyautogui.press("tab")
    pyautogui.write(senha)
    pyautogui.press("enter")
    time.sleep(6)

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
    pyautogui.press("tab")

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
    #     "2": "Pula para a pergunta 07",
    #     "3": "Pula para a pergunta 07",
    #     "4": "Pula para a pergunta 07",
    #     "5": "Pula para a pergunta 07",
    #     "6": "Pula para a pergunta 07",
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
    if nome_para_comparacao in UNIDADES_ESPECIAIS_CNES:
        
        # REGRA NOVA: Substituir o NOME pelo CÓDIGO CNES (6468918)
        log_info(f"UNIDADE ESPECIAL DETECTADA ({nome_para_comparacao}). SUBSTITUINDO NOME por CNES {CNES_ESPECIAL}.")
        pyautogui.press("tab") 
        pyautogui.write(CNES_ESPECIAL)
        
    else:
        # REGRA PADRÃO: Digitar o nome formatado (ou como %BUSCA%)
        log_debug(f"Preenchendo Nome da Unidade: {nome_campo_destino}")
        pyautogui.write(nome_campo_destino)

    # 4. NAVEGAÇÃO FINAL (MOVE PARA P9)
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
    
    pyautogui.write(campos['nome_paciente']) # Pergunta 10
    pyautogui.press("tab")
    
    idade = 0
    if campos.get('data_nascimento_completa'): # Pergunta 11
        log_debug(f"[P-11]Data de Nascimento: {data_nascimento_completa}")
        pyautogui.write(campos['data_nascimento_completa'])
        idade = calcular_idade_formatada(campos['data_nascimento_completa'])
        pyautogui.press("tab")
        log_debug(f"P-11]Idade calculada: {idade}")
        # NOVO: Mensagem de erro contextualizada
        erro_contexto = f"Pergunta 11 - Data de Nascimento: {campos['data_nascimento_completa']}"
        if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}") # Loga o contexto
            # Forçamos a interrupção do preenchimento desta ficha.
            raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
        # --- FIM DA VALIDAÇÃO DE ERRO ---
        

    else:
        pyautogui.press("tab") # Pula o campo Data de Nascimento
        log_debug(f"[P-12]Idade calculada/fornecida: {idade}")
        idade = int(campos.get('idade_calculada_notificador', 0))
        pyautogui.write(str(idade)) # Pergunta 12 (Idade)
        pyautogui.press("tab") # Avança do campo P12 (Idade)
        
        # --- INÍCIO DA VALIDAÇÃO DE ERRO (PERGUNTA 12) ---
        erro_contexto_12 = f"Pergunta 12 - Idade: {idade}"
        if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
            log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto_12}") # Loga o contexto
            # Forçamos a interrupção do preenchimento desta ficha.
            raise Exception(f"Erro de digitação em {erro_contexto_12}. Interrupção forçada.")
        # --- FIM DA VALIDAÇÃO DE ERRO (PERGUNTA 12) ---
        
        pyautogui.write("4") # Pergunta 12 (Tipo Idade: Anos)
        pyautogui.press("tab") # Avança do campo P12 (Tipo Idade)
    
    pyautogui.write(campos['sexo']) # Pergunta 13
    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.press("tab")
        pyautogui.write(campos['gestante']) # Pergunta 14
    pyautogui.press("tab")
    
    if campos.get('raca'):
        pyautogui.write(campos['raca']) # Pergunta 15
    pyautogui.press("tab")
    
    # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO (ESCOLARIDADE) ---
    # Vamos ter que Comparar a idade dele com a escolaridade...
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
        # Pega o valor do SINAN no dicionário. Se não encontrar, usa '9' (Ignorado) como padrão.
        valor_sinan = mapeamento_escolaridade.get(valor_redcap, '9')
        
        log_debug(f"Mapeando Escolaridade: RedCap='{valor_redcap}' -> SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
        
    # Sai do campo de escolaridade (tendo preenchido ou não)
    pyautogui.press("tab")

    # --- INCLUSÃO DA VALIDAÇÃO DE ERRO APÓS ESCOLARIDADE (PERGUNTA 16) ---
    # Definimos o contexto para saber exatamente onde o erro ocorreu no log
    valor_log_escolaridade = valor_sinan if (idade >= 7 and campos.get('escolaridade')) else "N/A (Idade < 7 ou Vazio)"
    erro_contexto = f"Pergunta 16 - Escolaridade: {valor_log_escolaridade}"

    if verificar_e_tratar_erro(num_notificacao, "%VIOLENC%"):
        log_erro(f"ERRO DE VALIDAÇÃO DETECTADO após {erro_contexto}") # Loga o contexto
        # Forçamos a interrupção do preenchimento desta ficha.
        raise Exception(f"Erro de digitação em {erro_contexto}. Interrupção forçada.")
    # --- FIM DA VALIDAÇÃO DE ERRO ---

    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO (ESCOLARIDADE) ---

    if campos.get('cartao_sus'):
        pyautogui.write(campos['cartao_sus']) #pergunta 17
        log_debug(f"Código do cartão SUS '{campos['cartao_sus']}'")
    pyautogui.press("tab")
    
    # --- INÍCIO DA CORREÇÃO ---
    if campos.get('nome_mae'):
        log_debug(f"Preenchendo Nome da Mãe: {campos['nome_mae']}")
        pyautogui.write(campos['nome_mae'])
    # Pressiona TAB de qualquer maneira para manter o fluxo da automação
    pyautogui.press("tab")
    # --- FIM DA CORREÇÃO ---
    time.sleep(3)

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
    log_debug(f"Campos (40) UF OCORRÊNCIA - (antes): {campos['uf_ocorrencia']}")
    if campos.get('uf_ocorrencia'):
        pyautogui.write(campos['uf_ocorrencia'])
    log_debug(f"Campos (40) UF OCORRÊNCIA - (depois): {campos.get('uf_ocorrencia')}")
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
    
    if campos.get('bairro_ocorrencia'):
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
    pyautogui.write(campos['fisica']) # pergunta 56.1
    pyautogui.press("tab")
    pyautogui.write(campos['moral_psicologica']) # pergunta 56.2
    pyautogui.press("tab")
    pyautogui.write(campos['tortura']) # pergunta 56.3
    pyautogui.press("tab")
    pyautogui.write('9') # SEXUAL CHUMBADA (não trata violência sexual) # pergunta 56.4 
    pyautogui.press("tab")
    pyautogui.write(campos['trafico_pessoas']) # pergunta 56.5
    pyautogui.press("tab")
    pyautogui.write(campos['financeiro']) # pergunta 56.6
    pyautogui.press("tab")
    pyautogui.write(campos['negligencia_abandono']) # pergunta 56.7
    pyautogui.press("tab")
    pyautogui.write(campos['trabalho_infantil']) # pergunta 56.8
    pyautogui.press("tab")
    pyautogui.write(campos['intervencao_legal']) # pergunta 56.9
    pyautogui.press("tab")
    pyautogui.write(campos['outro_tipo_violencia'])  # pergunta 56.10
    pyautogui.press("tab")
    if campos.get('outro_tipo_violencia') == "1": # pergunta 56.10.1
        pyautogui.write(campos['esp_outro_tipo_violencia'])
        pyautogui.press("tab")
    pyautogui.write(campos['forca_corporal_espancamento']) # pergunta 57.1
    pyautogui.press("tab")
    pyautogui.write(campos['enforcamento']) # pergunta 57.2
    pyautogui.press("tab")
    pyautogui.write(campos['objeto_contundente']) # pergunta 57.3
    pyautogui.press("tab")
    pyautogui.write(campos['objeto_perfurante']) # pergunta 57.4
    pyautogui.press("tab")
    pyautogui.write(campos['objeto_quente']) # pergunta 57.5
    pyautogui.press("tab")
    pyautogui.write(campos['envenenamento']) # pergunta 57.6
    pyautogui.press("tab")
    pyautogui.write(campos['arma_fogo']) # pergunta 57.7
    pyautogui.press("tab")
    pyautogui.write(campos['ameaca']) # pergunta 57.8
    pyautogui.press("tab")
   
   
    # pergunta 57.9 - (x=671, y=505) ou (x=671, y=359)
    # --- INÍCIO DA LÓGICA CORRIGIDA E ATUALIZADA ---
    
    # 1. Preenche o campo principal (Pergunta 57.9 - Meio de Agressão "NUMERO")
    pyautogui.write(campos['outro_meio_agressao']) # pergunta 57.9
    log_info(f"[Pergunta 57.9 Número] Preenchido com: {campos['outro_meio_agressao']}")
    pyautogui.press("tab")
    time.sleep(1.5)
    #valor_especificacao = campos.get('esp_outro_meio_agressao', '')

    # 2. Lógica de Ajuste Técnico (NOVA REGRA 2 applied)
    # --- Ajuste condicional para "Outro meio de agressão" ---
    # Regra:
    # 1️⃣ Só executa se for a primeira execução (primeira_execucao == True)
    # 2️⃣ E se o valor de 'outro_meio_agressao' for diferente de "1"
    # Caso contrário, pula o ajuste e segue direto para o próximo passo.
    valor_outro_meio = str(campos.get('outro_meio_agressao', '')).strip()

    if primeira_execucao and valor_outro_meio != "1":
        log_info("Primeira execução e valor != '1': realizando clique de ajuste em (671, 505).")
        log_debug(f"Condição atendida: primeira_execucao={primeira_execucao}, valor_outro_meio='{valor_outro_meio}'")
        pyautogui.press("tab")
        pyautogui.click(x=671, y=505)
        pyautogui.press("tab")
        time.sleep(1.0)
    else:
        log_debug(f"Condição Ignorada: primeira_execucao={primeira_execucao}, valor_outro_meio='{valor_outro_meio}'")

    # 3. Lógica da Especificação (Executa SEMPRE que o valor for "1")
    # Se caiu na regra acima (!= 1), esse if será falso.
    # Se foi pulado acima (== 1), ele entra aqui, dá o TAB e escreve.
    if campos.get('outro_meio_agressao') == "1":
        pyautogui.press("tab")  # Entra no campo de texto "Especificar"
        #pyautogui.press("esc")  # Fecha o combo box, se estiver aberto
        valor_especificacao = campos.get('esp_outro_meio_agressao', '') # out_agres_vio
        log_info(f"[Pergunta 57.10 - CAMPO ABERTO] Opção 'Outros' (1). Preenchendo especificação: {valor_especificacao}")
        pyautogui.write(valor_especificacao)

    #{EXCLUIR} 4. Sai do campo (seja do combo box ou do campo de texto) para ir à próxima pergunta
    #log_debug("Saindo do campo 'Outro meio de agressão' para a próxima pergunta.")
    #pyautogui.press("tab")

    
    # --- FIM DA LÓGICA CORRIGIDA ---
    
    time.sleep(2.0)
    #numero de envolvidos 60
    # pergunta 60  # out_agres_vio
    valor_numero_envolvidos = campos.get('numero_envolvidos', '') 
    log_info(f"[Pergunta 60 ] NUmero de envolvidos: {valor_numero_envolvidos}")
    pyautogui.write(campos['numero_envolvidos']) # pergunta 60
    pyautogui.press("tab")

    
    
    pyautogui.write(campos['pai']) # pergunta 61.1
    pyautogui.press("tab")
    pyautogui.write(campos['mae'])  # pergunta 61.2
    pyautogui.press("tab")
    pyautogui.write(campos['padrasto'])  # pergunta 61.3
    pyautogui.press("tab")
    pyautogui.write(campos['madrasta'])  # pergunta 61.4
    pyautogui.press("tab")
    pyautogui.write(campos['conjuge_parceiro'])  # pergunta 61.5
    pyautogui.press("tab")
    pyautogui.write(campos['ex_conjuge_parceiro'])  # pergunta 61.6
    pyautogui.press("tab")
    pyautogui.write(campos['namorado'])  # pergunta 61.7
    pyautogui.press("tab")
    pyautogui.write(campos['ex_namorado']) # pergunta 61.8
    pyautogui.press("tab")
    pyautogui.write(campos['filho']) # pergunta 61.9
    pyautogui.press("tab")
    pyautogui.write(campos['irmao']) # pergunta 61.10
    pyautogui.press("tab")
    pyautogui.write(campos['amigos_conhecidos']) # pergunta 61.11
    pyautogui.press("tab")
    pyautogui.write(campos['desconhecido']) # pergunta 61.12
    pyautogui.press("tab")
    pyautogui.write(campos['cuidador']) # pergunta 61.13
    pyautogui.press("tab")
    pyautogui.write(campos['patrao_chefe']) # pergunta 61.14
    pyautogui.press("tab")
    pyautogui.write(campos['pessoa_relacao_instituicao']) # pergunta 61.15
    pyautogui.press("tab")
    pyautogui.write(campos['policial_agente']) # pergunta 61.16
    pyautogui.press("tab")
    pyautogui.write(campos['propria_pessoa'])   # pergunta 61.17
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
    if campos.get('sexo_agressor'): # pergunta 62
        pyautogui.write(campos['sexo_agressor'])
    pyautogui.press("tab") # Vai para o campo 'Suspeita de uso de álcool'
    pyautogui.write(campos['suspeita_alcool']) # pergunta 63
    pyautogui.press("tab")
    pyautogui.write(campos['ciclo_vida_autor']) # pergunta 64
    pyautogui.press("tab")
    pyautogui.write(campos['rede_saude']) # pergunta 65.1
    pyautogui.press("tab")
    pyautogui.write(campos['rede_assistencia_social']) # pergunta 65.2
    pyautogui.press("tab")
    pyautogui.write(campos['rede_educacao']) # pergunta 65.3
    pyautogui.press("tab")
    pyautogui.write(campos['rede_atendimento_mulher']) # pergunta 65.4
    pyautogui.press("tab")
    pyautogui.write(campos['conselho_tutelar']) # pergunta 65.5
    pyautogui.press("tab")
    pyautogui.write(campos['conselho_idoso']) # pergunta 65.6
    pyautogui.press("tab")
    pyautogui.write(campos['delegacia_atendimento_idoso']) # pergunta 65.7
    pyautogui.press("tab")
    pyautogui.write(campos['centro_ref_direitos_humanos']) # pergunta 65.8
    pyautogui.press("tab")
    pyautogui.write(campos['ministerio_publico']) # pergunta 65.9
    pyautogui.press("tab")
    pyautogui.write(campos['delegacia_especializada_infancia']) # pergunta 65.10
    pyautogui.press("tab")
    pyautogui.write(campos['delegacia_atendimento_mulher']) # pergunta 65.11
    pyautogui.press("tab")
    pyautogui.write(campos['outras_delegacias']) # pergunta 65.12
    pyautogui.press("tab")
    pyautogui.write(campos['justica_infancia_juventude']) # pergunta 65.13
    pyautogui.press("tab")
    pyautogui.write(campos['defensoria_publica']) # pergunta 65.14
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

    data_encerramento = campos.get('data_encerramento', '').strip() or campos.get('data_notificacao', '') # pergunta 69
    pyautogui.write(data_encerramento)
    log_debug(f"[Pergunta 69] Campos (Data de Encerramento): {data_encerramento}")
    pyautogui.press("tab")

    if campos.get('observacoes'): # pergunta 70 (Observações adicionais)
        pyautogui.write(campos['observacoes'])