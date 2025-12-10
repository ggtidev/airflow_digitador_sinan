import sys
import os
import pyautogui
import time

# Garante que o Python encontre os módulos da pasta raiz do projeto (3_sinan_rpa)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils import (
    find_and_click,
    wait_for_image,
    get_usuario_ativo,
    formatar_unidade_saude,
    calcular_idade_formatada,
    load_json
)
from api_client import atualizar_status
from logger import log_info, log_debug, log_erro

# --- Configurações Iniciais ---
pyautogui.PAUSE = 0.3
try:
    # Constrói o caminho correto para o config.json na pasta pai (3_sinan_rpa)
    config_path = os.path.join(os.path.dirname(__file__), '..', 'config.json')
    CONFIG = load_json(config_path)
except FileNotFoundError:
    log_erro("Arquivo 'config.json' não encontrado! O robô não pode continuar.")
    sys.exit(1)

# --- Função Principal de Execução ---

def executar_violencia(item, reaproveitar_sessao=False, tem_proxima=False):
    """
    Função principal e robusta para executar a automação de um item de violência.
    """
    try:
        if not reaproveitar_sessao:
            if not (abrir_sinan() and login() and selecionar_agravo()):
                log_erro("Falha na etapa de abertura/login. Abortando processamento deste item.")
                return # Pula para o próximo item

        log_info(f"Iniciando preenchimento da notificação: {item['num_notificacao']}")
        idade = preencher_bloco_notificacao(item["notificacao"], item["num_notificacao"])
        log_info("Notificação preenchida. Iniciando investigação.")
        preencher_bloco_investigacao(item["investigacao"], idade)

        log_info("Preenchimento completo. Tentando salvar formulário.")
        time.sleep(2)
        if not find_and_click(CONFIG['botoes']['salvar'], timeout=15):
            raise Exception("Botão 'Salvar' não encontrado.")

        time.sleep(2)
        if not find_and_click(CONFIG['botoes']['ok'], timeout=10):
            raise Exception("Não encontrou o primeiro pop-up de confirmação 'ok'.")

        # Segundo 'ok' é opcional, pois nem sempre aparece.
        find_and_click(CONFIG['botoes']['ok'], timeout=5)

        time.sleep(2)
        log_info("Aguardando janela 'Deseja incluir nova notificação?'.")
        if wait_for_image(CONFIG['telas']['tela_novo_ou_nao'], timeout=15):
            log_info("Encontrada tela 'Deseja incluir nova notificação?'.")
            time.sleep(1)
            if tem_proxima:
                log_info("Clicando em 'Sim' para novo formulário.")
                pyautogui.press('s')  # Atalho de teclado para 'Sim'
            else:
                log_info("Clicando em 'Não' para fechar formulário.")
                pyautogui.press('n')  # Atalho de teclado para 'Não'
        else:
            raise Exception("Não encontrou tela 'Deseja incluir nova notificação?'.")

        num_notificacao = item.get("num_notificacao")
        if num_notificacao:
            atualizar_status(num_notificacao)
            log_info(f"Status atualizado na API para a notificação {num_notificacao}.")
            
    except Exception as e:
        log_erro(f"Erro durante execução do script violência para o item {item.get('num_notificacao')}: {e}")
        from utils import take_screenshot
        take_screenshot("erro_fatal_violencia")
        raise e


def abrir_sinan():
    log_info("Abrindo SINAN...")
    pyautogui.press("win")
    pyautogui.write("sinan")
    pyautogui.press("enter")
    # time.sleep(3) #pc_andre
    time.sleep(6) #pc_indra

def login(usuario, senha):
    #username, password = get_usuario_ativo()
    log_info(f"Realizando login com o usuário: {username}")
    pyautogui.write(usuario)
    pyautogui.press("tab")
    pyautogui.write(senha)
    pyautogui.press("enter")
    # time.sleep(2) #pc_andre
    time.sleep(4) #pc_indra

def selecionar_agravo(nome_agravo):
    log_info("Selecionando agravo de Violência...")
    pyautogui.click(x=72, y=59)
    pyautogui.write(nome_agravo)
    pyautogui.press("enter")
    time.sleep(1)
    pyautogui.press("enter")
    # time.sleep(3) #pc_andre
    time.sleep(6) #pc_indra

def preencher_bloco_notificacao(campos, num_notificacao):
    log_debug(f"Campos notificação: {campos}")
    pyautogui.write(str(num_notificacao))
    pyautogui.press("tab")  
    pyautogui.write(campos['data_notificacao'])
    pyautogui.press("tab", presses=3)
    pyautogui.write(campos['unidade_notificadora'])
    pyautogui.press("tab")
    if campos['unidade_notificadora'] == "7":
        pyautogui.write(formatar_unidade_saude(campos.get('nome_unidade_notificadora', '')))
    else:
        pyautogui.write(formatar_unidade_saude(campos.get('nome_unidade_saude', '')))
    pyautogui.press("tab")
    time.sleep(2)
    pyautogui.write(campos['data_ocorrencia'])
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
        pyautogui.write(str(idade))
        pyautogui.press("tab")
        pyautogui.write("4")
        pyautogui.press("tab")
    pyautogui.write(campos['sexo'])
    pyautogui.press("tab")
    if campos['sexo'].upper() == "F" and idade >= 11:
        pyautogui.write(campos.get('gestante', '9')) # Adicionado valor padrão '9'
    pyautogui.press("tab")
    if campos.get('raca'):
        pyautogui.write(campos['raca'])
    pyautogui.press("tab")
    if idade >= 7 and campos.get('escolaridade'):
        pyautogui.write(campos['escolaridade'])
    pyautogui.press("tab")
    if campos.get('cartao_sus'):
        pyautogui.write(campos['cartao_sus'])
    pyautogui.press("tab")
    if campos.get('nome_mae'):
        pyautogui.write(campos['nome_mae'])
    pyautogui.press("tab")
    pyautogui.write(campos['uf_residencia_vio'])
    pyautogui.press("tab")
    pyautogui.write(campos['municipio_residencia'])
    pyautogui.press("tab")
    if campos.get('distrito_residencia'):
        pyautogui.write(f"%{campos['distrito_residencia']}%")
    pyautogui.press("tab")
    if campos.get('bairro_residencia'):
        pyautogui.write(campos['bairro_residencia'])
    pyautogui.press("tab")
    if campos.get('endereco_residencia'):
        pyautogui.write(campos['endereco_residencia'])
    pyautogui.press("tab")
    if campos.get('numero_residencia'):
        pyautogui.write(campos['numero_residencia'])
    pyautogui.press("tab")
    if campos.get('complemento_residencia'):
        pyautogui.write(campos['complemento_residencia'])
    pyautogui.press("tab", presses=3)
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
    else:
        pyautogui.press("tab", presses=2)
    pyautogui.press("tab")
    if campos.get('zona'):
        pyautogui.write(campos['zona'])
    pyautogui.press("tab")
    log_debug(f"Idade calculada/fornecida: {idade}")
    return idade

def preencher_bloco_investigacao(campos, idade):
    log_debug(f"Campos investigação: {campos}")
    log_debug(f"Idade recebida para investigação: {idade}")
    if campos.get('ocupacao'):
        pyautogui.write(f"%{campos['ocupacao']}%")
        pyautogui.press("tab")
        pyautogui.press("enter")    
    pyautogui.press("tab")
    if idade > 11 and campos.get('estado_civil'):
        pyautogui.write(campos['estado_civil'])
    pyautogui.press("tab")
    if idade > 11 and campos.get('orientacao_sexual'):
        pyautogui.write(campos['orientacao_sexual'])
    pyautogui.press("tab")
    if idade > 11 and campos.get('identidade_genero'):
        pyautogui.write(campos['identidade_genero'])
    pyautogui.press("tab")
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
    pyautogui.write(campos['uf_ocorrencia'])
    pyautogui.press("tab")
    time.sleep(5)
    pyautogui.write(campos['municipio_ocorrencia'])
    pyautogui.press("tab", presses=2)
    if campos.get('distrito'):
        pyautogui.write(campos['distrito'])
    pyautogui.press("tab")
    if campos.get('bairro_ocorrencia'):
        pyautogui.write(campos['bairro_ocorrencia'])
        pyautogui.press("esc")
    pyautogui.press("tab")
    if campos.get('endereco_ocorrencia'):
        pyautogui.write(campos['endereco_ocorrencia'])
    pyautogui.press("tab")
    pyautogui.press("esc")
    pyautogui.press("tab")
    if campos.get('codigo'):
        pyautogui.write(campos['codigo'])
    pyautogui.press("tab")
    if campos.get('numero'):
        pyautogui.write(campos['numero'])
    pyautogui.press("tab")
    if campos.get('complemento'):
        pyautogui.write(campos['complemento'])
    pyautogui.press("tab", presses=3)
    if campos.get('ponto_referencia'):
        pyautogui.write(campos['ponto_referencia'])
    pyautogui.press("tab")
    if campos.get('zona'):
        pyautogui.write(campos['zona'])
    pyautogui.press("tab")
    if campos.get('horario_ocorrencia'):
        pyautogui.write(campos['horario_ocorrencia'])
    pyautogui.press("tab")
    if campos.get('local_ocorrencia') == "9":
        pyautogui.write(campos['local_ocorrencia'])
        pyautogui.press("tab")
        pyautogui.write(campos['outro_local'])
    else:
        pyautogui.write(campos['local_ocorrencia'])
    pyautogui.press("tab")
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
    pyautogui.write('9')
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
    pyautogui.write(campos['forca_corporal_espancamento'])
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
    pyautogui.write(campos['ameaca'])
    pyautogui.press("tab")
    pyautogui.write(campos['outro_meio_agressao'])
    pyautogui.press("tab")
    if campos.get('outro_meio_agressao') == "1":
        pyautogui.write(campos['esp_outro_meio_agressao'])
        pyautogui.press("tab")
    time.sleep(2.0)
    pyautogui.write(campos['numero_envolvidos'])
    pyautogui.press("tab")
    pyautogui.write(campos['pai'])
    pyautogui.press("tab")
    pyautogui.write(campos['mae'])
    pyautogui.press("tab")
    pyautogui.write(campos['padrasto'])
    pyautogui.press("tab")
    pyautogui.write(campos['madrasta'])
    pyautogui.press("tab")
    pyautogui.write(campos['conjuge_parceiro'])
    pyautogui.press("tab")
    pyautogui.write(campos['ex_conjuge_parceiro'])
    pyautogui.press("tab")
    pyautogui.write(campos['namorado'])
    pyautogui.press("tab")
    pyautogui.write(campos['ex_namorado'])
    pyautogui.press("tab")
    pyautogui.write(campos['filho'])
    pyautogui.press("tab")
    pyautogui.write(campos['irmao'])
    pyautogui.press("tab")
    pyautogui.write(campos['amigos_conhecidos'])
    pyautogui.press("tab")
    pyautogui.write(campos['desconhecido'])
    pyautogui.press("tab")
    pyautogui.write(campos['cuidador'])
    pyautogui.press("tab")
    pyautogui.write(campos['patrao_chefe'])
    pyautogui.press("tab")
    pyautogui.write(campos['pessoa_relacao_instituicao'])
    pyautogui.press("tab")
    pyautogui.write(campos['policial_agente'])
    pyautogui.press("tab")
    pyautogui.write(campos['propria_pessoa'])
    pyautogui.press("tab")
    pyautogui.write(campos['outros_envolvidos'])
    pyautogui.press("tab")
    if campos.get('outros_envolvidos') == "1":
        pyautogui.write(campos['esp_outros_envolvidos'])
        pyautogui.press("tab")
    if campos.get('sexo_agressor'):
        pyautogui.write(campos['sexo_agressor'])
    pyautogui.press("tab")
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
    if campos.get('relacao_trabalho'):
        pyautogui.write(campos['relacao_trabalho']) 
    pyautogui.press("tab", presses=2)
    if campos.get('relacao_trabalho') == "1":
        pyautogui.write('9')
        pyautogui.press("tab")
    if campos.get('data_encerramento'):
        pyautogui.write(campos['data_encerramento'])
    pyautogui.press("tab")
    if campos.get('observacoes'):
        pyautogui.write(campos['observacoes'])