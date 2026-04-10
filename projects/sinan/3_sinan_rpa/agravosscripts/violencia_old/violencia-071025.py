import sys
import os
import pyautogui
import time

# Garante que o Python encontre os módulos da pasta raiz do projeto (3_sinan_rpa)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from utils import wait_and_click, get_usuario_ativo, formatar_unidade_saude, calcular_idade_formatada
from api_client import atualizar_status
from logger import log_info, log_debug, log_erro
from unidades.buscar_unidades import buscar_estabelecimento


# Caminho absoluto da pasta de imagens
IMAGENS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "imagens")
)
print("Pasta de imagens usada:", IMAGENS_DIR)


primeira_execucao = True
pyautogui.PAUSE = 0.3

def executar_violencia(item, reaproveitar_sessao=False, tem_proxima=False):
    try:
        if not reaproveitar_sessao:
            abrir_sinan()
            username, password = get_usuario_ativo()
            login(username, password)
            selecionar_agravo("%VIOLENC%") # Seleciona o agravo de violência - ELLEN PAZ DE SOUZA 

        log_info(f"Iniciando preenchimento da notificação: {item['num_notificacao']}")
        idade = preencher_bloco_notificacao(item["notificacao"], item["num_notificacao"])
        log_info("Notificação preenchida. Iniciando investigação.")
        preencher_bloco_investigacao(item["investigacao"], idade)

        log_info("Preenchimento completo. Tentando salvar formulário.")
        time.sleep(2)

        # Usa caminho absoluto para salvar.png
        if wait_and_click(os.path.join(IMAGENS_DIR, "salvar.png"), timeout=15):
            log_info("Clicado em salvar. Aguardando confirmação.")
        else:
            log_erro("Não conseguiu clicar em salvar.")
            raise Exception("Botão 'Salvar' não encontrado.")

        time.sleep(2)
        # Usa caminho absoluto para ok.png
        if wait_and_click(os.path.join(IMAGENS_DIR, "ok.png"), timeout=10):
            log_info("Primeiro 'ok' clicado com sucesso.")
        else:
            log_erro("Não encontrou a primeira janela 'ok'.")
            raise Exception("Não encontrou primeira janela 'ok'")

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
       # if wait_and_click(os.path.join(IMAGENS_DIR, "novo_ou_nao.png"), timeout=15):
        if wait_and_click(os.path.join(IMAGENS_DIR, "novo_ou_nao.png"), timeout=7):
            log_info("Encontrada tela 'Deseja incluir nova notificação deste agravo?'.")
            time.sleep(3)
            if tem_proxima:
                log_info("Clicando em 'Sim' para novo formulário.")
                #if not wait_and_click("imagens/sim.png"): # CORREÇÃO AQUI
                if wait_and_click(os.path.join(IMAGENS_DIR, "sim.png"), timeout=5):
                    raise Exception("Botão 'Sim' não encontrado.")
                #pyautogui.click(x=1002, y=594)  # Sim
                #pyautogui.click(x=999, y=591)  # Sim
            else:
                log_info("Clicando em 'Não' para fechar formulário.")
                #if not wait_and_click("imagens/nao.png"): # CORREÇÃO AQUI
                if wait_and_click(os.path.join(IMAGENS_DIR, "nao.png"), timeout=5):
                    raise Exception("Botão 'Não' não encontrado.")
                #pyautogui.click(x=1082, y=593)  # Não
                #pyautogui.click(x=1080, y=588)  # Não
        else:
            screenshot_nome = f"erro_nova_notificacao_{item['num_notificacao']}.png"
            pyautogui.screenshot(screenshot_nome)
            log_erro("Não encontrou a tela 'Deseja incluir nova notificação?'.")
            log_erro(f"Screenshot salvo como {screenshot_nome}")
            raise Exception("Não encontrou tela 'Deseja incluir nova notificação?'")

        global primeira_execucao
        primeira_execucao = False

        num_notificacao = item.get("num_notificacao")
        if num_notificacao:
            atualizar_status(num_notificacao)
            log_info(f"Status atualizado na API para a notificação {num_notificacao}.")
            
    except Exception as e:
        log_erro(f"Erro durante execução do script violência: {e}")
        raise e


def abrir_sinan():
    pyautogui.press("win")
    time.sleep(3)
    pyautogui.write("sinan")
    time.sleep(3)
    pyautogui.press("enter")
    time.sleep(8)

def login(usuario, senha):
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
    time.sleep(6)

def preencher_bloco_notificacao(campos, num_notificacao):
    log_debug(f"Campos notificação: {campos}")
    pyautogui.write(num_notificacao)
    pyautogui.press("tab")  
    pyautogui.write(campos['data_notificacao']) # Pergunta 03
    pyautogui.press("tab", presses=3)
    pyautogui.write(campos['unidade_notificadora']) # Pergunta 06
    pyautogui.press("tab")

  
    # --- INÍCIO DA NOVA LÓGICA ---
    if campos['unidade_notificadora'] == "7":
        # Formata e armazena o nome da unidade em uma variável
        #unidade_formatada = formatar_unidade_saude(campos['nome_unidade_notificadora']) # NAs analises de dados, nesse campo boa parte dos 50 dados vem assim ('SECRETARIA DE SAUDE' ) ou sem informação
        unidade_formatada = (campos['nome_unidade_notificadora'])
      
        # Concatena " DO RECIFE" ao nome da unidade
        nome_completo = f"{unidade_formatada} DO RECIFE"
        
        # Adiciona o log para registrar o que será digitado
        log_debug(f"Preenchendo Unidade Notificadora (código 7): {nome_completo}")
        pyautogui.write(nome_completo)
        pyautogui.press("tab")
    else:
         # Pega o valor do JSON, que pode ser um código (ex: "721") ou um nome
        valor_unidade_saude = campos.get('nome_unidade_saude', '')
        
        # Tenta converter o valor para um número inteiro (o código da unidade)
        try:
            codigo_unidade = int(valor_unidade_saude)
            # Se conseguiu, busca o nome correspondente no dicionário
            nome_da_unidade = buscar_estabelecimento(codigo_unidade)
            log_debug(f"Código da unidade '{codigo_unidade}' convertido para nome: '{nome_da_unidade}'")
        except (ValueError, TypeError):
            # Se não conseguiu converter, assume que o valor já é o nome
            nome_da_unidade = valor_unidade_saude
            log_debug(f"Valor da unidade '{nome_da_unidade}' já está em formato de nome.")

        # Formata o nome final para a busca
        unidade_formatada = formatar_unidade_saude(nome_da_unidade)
        
        log_debug(f"Preenchendo Unidade de Saúde: {unidade_formatada}")
        pyautogui.write(unidade_formatada)
        pyautogui.press("tab")
    # --- FIM DA NOVA LÓGICA ---

    time.sleep(2)
    pyautogui.write(campos['data_ocorrencia']) # Pergunta 09
    pyautogui.press("tab")
    pyautogui.write(campos['nome_paciente'])
    pyautogui.press("tab")
    idade = 0
    if campos.get('data_nascimento_completa'):
        pyautogui.write(campos['data_nascimento_completa'])
        idade = calcular_idade_formatada(campos['data_nascimento_completa'])
        pyautogui.press("tab")
    else:
        pyautogui.press("tab")
        idade = int(campos.get('idade_calculada_notificador', 0))
        pyautogui.write(str(idade)) # Pergunta 12
        pyautogui.press("tab")
        pyautogui.write("4")
        pyautogui.press("tab")
    pyautogui.write(campos['sexo']) # Pergunta 13
    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.press("tab")
        pyautogui.write(campos['gestante']) # Pergunta 14
        #pyautogui.press("tab") #CASO DÊ CAQUINHA APAGAR ESSA LINHA 
    pyautogui.press("tab")
    
    if campos.get('raca'):
        pyautogui.write(campos['raca']) # Pergunta 15
    pyautogui.press("tab")
    
   # if idade >= 7 and campos.get('escolaridade'): # pergunta 16
    #    pyautogui.write(campos['escolaridade'])
     #   pyautogui.press("tab")

   # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO (ESCOLARIDADE) ---
   # Vamos ter que Comparar a idade dele com a escolaridade, ver uma tabela que tenha essas informações. Exemplo: Idade = 9 então escolaridade tem que ser menor/igual a 03

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
    pyautogui.press("tab")
    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO ---

    if campos.get('cartao_sus'):
        pyautogui.write(campos['cartao_sus']) #pergunta 17
        log_debug(f"Código do cartão SUS '{cartao_sus}'")
    pyautogui.press("tab")
   
    #time.sleep(3)
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
    if campos.get('distrito_residencia'):
        pyautogui.write(f"%{campos['distrito_residencia']}%")
    pyautogui.press("tab")
    if campos.get('bairro_residencia'):
        pyautogui.write(campos['bairro_residencia']) # Pergunta 22
        #pyautogui.press("esc")
    pyautogui.press("tab")

    
    #if campos.get('endereco_residencia'): 
    #    pyautogui.write(campos['endereco_residencia']) # Pergunta 23.0
   # pyautogui.press("tab")
   # pyautogui.press("esc")
    #pyautogui.press("tab")

    # Ao inves de ser tab tem que ser um click(x=685, y=507) para ir para o campo Codigo
    log_info("Clicando para focar no campo 'Código' (x=685, y=507).")
    pyautogui.click(x=685, y=507)
    time.sleep(0.5)
    log_debug("Pulando campo Código (vazio).")
    pyautogui.press("tab")


    # Preenche o campo "Código" que estava faltando
    #if campos.get('codigo_residencia'):
     #   pyautogui.write(campos['codigo_residencia'])
    #pyautogui.press("tab")
    
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
    
    # Preenche os campos "Geocampo1" e "Geocampo2" que estavam faltando
    #if campos.get('geocampo1_residencia'):
    #    pyautogui.write(campos['geocampo1_residencia'])
    #pyautogui.press("tab")

    #if campos.get('geocampo2_residencia'):
    #    pyautogui.write(campos['geocampo2_residencia'])
    #pyautogui.press("tab")
    
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
        log_info(f"Preenchimento da ZONA: {item['zona']}")
        pyautogui.write(campos['zona']) # Pergunta 31
    pyautogui.press("tab")
    log_debug(f"Idade calculada/fornecida: {idade}")
    return idade

def preencher_bloco_investigacao(campos, idade):
    log_debug(f"Campos investigação: {campos}")
    log_debug(f"Idade recebida para investigação: {idade}")
    
    #Verificar se a dados em ocupacao., ele faz a pesquisa via %NOME_OCUPACAO%
    if campos.get('ocupacao'): # Pergunta 34
        pyautogui.write(f"%{campos['ocupacao']}%")
        pyautogui.press("tab")
        pyautogui.press("enter")    
    pyautogui.press("tab")
   
    #if idade > 11 and campos.get('estado_civil'): # pergunta 35
     #   pyautogui.write(campos['estado_civil'])
    #    pyautogui.press("tab")

     # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO (ESTADO CIVIL) ---
    if idade > 11 and campos.get('estado_civil'):
        mapeamento_estado_civil = {
            '1': '1',  # Solteiro
            '2': '2',  # Casado / união consensual
            '3': '3',  # Viúvo
            '4': '4',  # Separado
            '5': '8',  # Não se aplica
            '6': '9'   # Ignorado
        }
        valor_redcap = campos.get('estado_civil')
        valor_sinan = mapeamento_estado_civil.get(valor_redcap, '9') # Padrão para Ignorado
        
        log_debug(f"Mapeando Estado Civil: RedCap='{valor_redcap}' -> SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
    pyautogui.press("tab")
    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO ---
   
    #if idade > 11 and campos.get('orientacao_sexual'):
     #   pyautogui.write(campos['orientacao_sexual'])
      #  pyautogui.press("tab")

     # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO (ORIENTAÇÃO SEXUAL) ---
    if idade > 11 and campos.get('orientacao_sexual'): #pergunta 36 
        mapeamento_orientacao = {
            '1': '1',  # Heterossexual
            '2': '2',  # Homossexual (gay/lésbica)
            '3': '3',  # Bissexual
            '4': '8',  # Não se aplica
            '5': '9'   # Ignorado
        }
        valor_redcap = campos.get('orientacao_sexual')
        valor_sinan = mapeamento_orientacao.get(valor_redcap, '9') # Padrão para Ignorado

        log_debug(f"Mapeando Orientação Sexual: RedCap='{valor_redcap}' -> SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
    pyautogui.press("tab")
    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO ---

    # if idade > 11 and campos.get('identidade_genero'): #pergunta 37 ( Só aceita  esse numero 1- Travesti / 2- Mulher Transexual / 3- Homem Transexual / 8- Não se aplica / 9- Ignorado )
    #    pyautogui.write(campos['identidade_genero'])
    #    pyautogui.press("tab")

     # --- INÍCIO DA NOVA LÓGICA DE MAPEAMENTO ---
    if idade > 11 and campos.get('identidade_genero'):
        # Dicionário de mapeamento "De-Para"
        mapeamento_genero = {
            '1': '1',  # Travesti -> Travesti
            '2': '2',  # Mulher Transexual -> Mulher Transexual
            '3': '3',  # Homem Transexual -> Homem Transexual
            '4': '8',  # Não se aplica -> Não se aplica
            '5': '9'   # Ignorado -> Ignorado
        }
        valor_redcap = campos.get('identidade_genero')
        # Pega o valor do SINAN no dicionário. Se não encontrar, usa '9' (Ignorado) como padrão.
        valor_sinan = mapeamento_genero.get(valor_redcap, '9')
        
        log_debug(f"Mapeando Identidade de Gênero: RedCap='{valor_redcap}' -> SINAN='{valor_sinan}'")
        pyautogui.write(valor_sinan)
        pyautogui.press("tab")
    else:
        # Se a idade for <= 11 ou o campo não existir, pula
        pyautogui.press("tab")
    # --- FIM DA NOVA LÓGICA DE MAPEAMENTO ---

    pyautogui.write(campos['deficiencia'])
    if campos.get('deficiencia') == "1":
        pyautogui.press("tab")
        pyautogui.write(campos['deficiencia_fisica'])
        pyautogui.press("tab")
        pyautogui.write(campos['deficiencia_intelectual'])
        pyautogui.press("tab")
        pyautogui.write(campos['deficiencia_visual'])
        pyautogui.press("tab")
        pyautogui.write(campos['deficiencia_auditiva'])
        pyautogui.press("tab")
        pyautogui.write(campos['deficiencia_mental'])
        pyautogui.press("tab")
        pyautogui.write(campos['transtorno_comportamento'])
        pyautogui.press("tab")
        pyautogui.write(campos['outras_deficiencias'])
        if campos.get('outras_deficiencias') == "1":
            pyautogui.write(campos['outra_deficiencia'])
    pyautogui.press("tab")
    pyautogui.write(campos['uf_ocorrencia']) # pergunta 40
    pyautogui.press("tab")
    time.sleep(5) # AUMENTEI PARA 5 SEGUNDOS PARA TENTAR RESOLVER O PROBLEMA DE NÃO PREENCHER O MUNICIPIO
   # pyautogui.write(campos['municipio_ocorrencia'])# pergunta 41 (reafazer colocando %nome%)
    pyautogui.write(f"%{campos['municipio_ocorrencia']}%")
    pyautogui.press("tab", presses=2)
    if campos.get('distrito'):
        pyautogui.write(campos['distrito']) #No teste manual so foi com %2% ( Pergunta 42)
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
        log_info(f"Preenchimento da ZONA_02: {item['zona']}")
        pyautogui.write(campos['zona'])
    pyautogui.press("tab")
    if campos.get('horario_ocorrencia'):
        pyautogui.write(campos['horario_ocorrencia'])
    pyautogui.press("tab")
    
    # Se o campo 'local_ocorrencia' for "9" (outro), escreve o valor do campo 'outro_local'
    # Pergunta 52   
   # if campos.get('local_ocorrencia') == "9":
   #    pyautogui.write(campos['local_ocorrencia'])
    #    pyautogui.press("tab")
    #    pyautogui.write(campos['outro_local'])
   # else:
   #     pyautogui.write(campos['local_ocorrencia'])
   # pyautogui.press("tab")
    
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

    if campos.get('ocorreu_outras_vezes'):
        pyautogui.write(campos['ocorreu_outras_vezes'])
    pyautogui.press("tab")
    if campos.get('lesao_autoprovocada'):
        pyautogui.write(campos['lesao_autoprovocada'])
    pyautogui.press("tab")
    pyautogui.write(campos['motivo_violencia'])
    pyautogui.press("tab")
    pyautogui.write(campos['fisica'])
    pyautogui.press("tab")
    pyautogui.write(campos['moral_psicologica'])
    pyautogui.press("tab")
    pyautogui.write(campos['tortura'])
    pyautogui.press("tab")
    pyautogui.write('9') # SEXUAL CHUMBADA (não trata violência sexual)
    pyautogui.press("tab")
    pyautogui.write(campos['trafico_pessoas'])
    pyautogui.press("tab")
    pyautogui.write(campos['financeiro'])
    pyautogui.press("tab")
    pyautogui.write(campos['negligencia_abandono'])
    pyautogui.press("tab")
    pyautogui.write(campos['trabalho_infantil'])
    pyautogui.press("tab")
    pyautogui.write(campos['intervencao_legal'])
    pyautogui.press("tab")
    pyautogui.write(campos['outro_tipo_violencia']) 
    pyautogui.press("tab")
    if campos.get('outro_tipo_violencia') == "1":
        pyautogui.write(campos['esp_outro_tipo_violencia'])
        pyautogui.press("tab")
    pyautogui.write(campos['forca_corporal_espancamento']) # pergunta 57.1
    pyautogui.press("tab")
    pyautogui.write(campos['enforcamento'])
    pyautogui.press("tab")
    pyautogui.write(campos['objeto_contundente'])
    pyautogui.press("tab")
    pyautogui.write(campos['objeto_perfurante'])
    pyautogui.press("tab")
    pyautogui.write(campos['objeto_quente'])
    pyautogui.press("tab")
    pyautogui.write(campos['envenenamento'])
    pyautogui.press("tab")
    pyautogui.write(campos['arma_fogo'])
    pyautogui.press("tab")
    pyautogui.write(campos['ameaca']) # pergunta 57.8
    pyautogui.press("tab")
    # pergunta 57.9 - (x=671, y=505) ou (x=671, y=359)
     # --- INÍCIO DA LÓGICA CORRIGIDA ---
    pyautogui.write(campos['outro_meio_agressao'])
    time.sleep(2.5)
    if primeira_execucao:
        pyautogui.press("tab")
        pyautogui.click(x=671, y=505)
        time.sleep(2.5)

    if campos.get('outro_meio_agressao') == "1":
        pyautogui.press("tab")
        pyautogui.write(campos['esp_outro_meio_agressao'])
    pyautogui.press("tab")
    # --- FIM DA LÓGICA CORRIGIDA ---    
    time.sleep(2.0)
    pyautogui.write(campos['numero_envolvidos']) # pergunta 60
    pyautogui.press("tab")
    pyautogui.write(campos['pai']) # pergunta 60.1
    pyautogui.press("tab")
    pyautogui.write(campos['mae'])  # pergunta 60.2
    pyautogui.press("tab")
    pyautogui.write(campos['padrasto'])  # pergunta 60.3
    pyautogui.press("tab")
    pyautogui.write(campos['madrasta'])  # pergunta 60.4
    pyautogui.press("tab")
    pyautogui.write(campos['conjuge_parceiro'])  # pergunta 60.5
    pyautogui.press("tab")
    pyautogui.write(campos['ex_conjuge_parceiro'])  # pergunta 60.6
    pyautogui.press("tab")
    pyautogui.write(campos['namorado'])  # pergunta 60.7
    pyautogui.press("tab")
    pyautogui.write(campos['ex_namorado']) # pergunta 60.8
    pyautogui.press("tab")
    pyautogui.write(campos['filho']) # pergunta 60.9
    pyautogui.press("tab")
    pyautogui.write(campos['irmao']) # pergunta 60.10
    pyautogui.press("tab")
    pyautogui.write(campos['amigos_conhecidos']) # pergunta 60.11
    pyautogui.press("tab")
    pyautogui.write(campos['desconhecido']) # pergunta 60.12
    pyautogui.press("tab")
    pyautogui.write(campos['cuidador']) # pergunta 60.13
    pyautogui.press("tab")
    pyautogui.write(campos['patrao_chefe']) # pergunta 60.14
    pyautogui.press("tab")
    pyautogui.write(campos['pessoa_relacao_instituicao']) # pergunta 60.15
    pyautogui.press("tab")
    pyautogui.write(campos['policial_agente']) # pergunta 60.16
    pyautogui.press("tab")
    pyautogui.write(campos['propria_pessoa'])   # pergunta 60.17
    pyautogui.press("tab")
    # Vim um if aqui para verificar a condição e preencher o campo de descrição se necessário
    
    outros_envolvidos_valor = campos.get('outros_envolvidos', '2')
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
    if campos.get('sexo_agressor'):
        pyautogui.write(campos['sexo_agressor'])
    pyautogui.press("tab") # Vai para o campo 'Suspeita de uso de álcool'
    pyautogui.write(campos['suspeita_alcool'])
    pyautogui.press("tab")
    pyautogui.write(campos['ciclo_vida_autor'])
    pyautogui.press("tab")
    pyautogui.write(campos['rede_saude'])
    pyautogui.press("tab")
    pyautogui.write(campos['rede_assistencia_social'])
    pyautogui.press("tab")
    pyautogui.write(campos['rede_educacao'])
    pyautogui.press("tab")
    pyautogui.write(campos['rede_atendimento_mulher'])
    pyautogui.press("tab")
    pyautogui.write(campos['conselho_tutelar'])
    pyautogui.press("tab")
    pyautogui.write(campos['conselho_idoso'])
    pyautogui.press("tab")
    pyautogui.write(campos['delegacia_atendimento_idoso'])
    pyautogui.press("tab")
    pyautogui.write(campos['centro_ref_direitos_humanos'])
    pyautogui.press("tab")
    pyautogui.write(campos['ministerio_publico'])
    pyautogui.press("tab")
    pyautogui.write(campos['delegacia_especializada_infancia'])
    pyautogui.press("tab")
    pyautogui.write(campos['delegacia_atendimento_mulher'])
    pyautogui.press("tab")
    pyautogui.write(campos['outras_delegacias'])
    pyautogui.press("tab")
    pyautogui.write(campos['justica_infancia_juventude'])
    pyautogui.press("tab")
    pyautogui.write(campos['defensoria_publica'])
    pyautogui.press("tab")
    if campos.get('relacao_trabalho'): #se não vier nada colocar 9
        pyautogui.write(campos['relacao_trabalho']) 
    pyautogui.press("tab", presses=2)
    if campos.get('relacao_trabalho') == "1":
        pyautogui.write('9') # OUTROS (não retorna CAT)
        pyautogui.press("tab")
    if campos.get('data_encerramento'):
        pyautogui.write(campos['data_encerramento'])
    pyautogui.press("tab")
    if campos.get('observacoes'):
        pyautogui.write(campos['observacoes'])

