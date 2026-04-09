import os
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from models.models import RpaNotificacao, RpaNotificacaoDetalhe, SistemaAlvo, Agravo

load_dotenv()

# ### NOVO PASSO 1: DEFINIÇÃO DOS CAMPOS OBRIGATÓRIOS ###
# Consulte o arquivo services/redcap_violencia.py para obter a lista correta.
# Esta lista é apenas um EXEMPLO.
CAMPOS_OBRIGATORIOS = [
    'agravo_de_notificacao_individual',
    'dt_notificacao',
    'uf_de_notificacao',
    'municipio_de_notificacao',
    'unidade_de_saude',
    'dt_do_atendimento',
    'pac_nome',
    'pac_dt_nasc',
    'pac_sexo',
    'pac_gestante',
    'pac_raca',
    'pac_escolaridade',
    'pac_uf',
    'pac_municipio_de_residencia',
    'pac_cep',
    'pac_logradouro',
    'pac_numero',
    'pac_bairro',
    'pac_fone',
    'ocor_uf',
    'ocor_municipio',
    'ocor_cep',
    'ocor_logradouro',
    'ocor_numero',
    'ocor_bairro',
    'ocor_fone',
    'ocor_zona',
    'ocor_pais',
    'ocor_lesao_autoprov',
    'dt_ocorrencia',
    'hr_ocorrencia',
    'ocor_ciclo_de_vida',
    'ocor_orientacao_sexual',
    'ocor_identidade_de_genero',
    'ocor_deficiencia',
    'ocor_deficiencia_fisica',
    'ocor_deficiencia_mental',
    'ocor_deficiencia_visual',
    'ocor_deficiencia_auditiva',
    'ocor_outra_deficiencia',
    'viol_local_ocor',
    'viol_outros_locais_ocor',
    'viol_ocorreu_outras_vezes',
    'viol_tipo_violencia_fisica',
    'viol_tipo_violencia_psicologica',
    'viol_tipo_violencia_sexual',
    'viol_numero_envolvidos',
    'viol_sexo_autor',
    'dt_investigacao',
    'notificante_municipio'
]

# --- 1. CONFIGURAÇÃO DAS CONEXÕES (sem alterações) ---
conector_url = (
    f"postgresql+psycopg2://{os.getenv('CONECTOR_DB_USER')}:{os.getenv('CONECTOR_DB_PASSWORD')}"
    f"@{os.getenv('CONECTOR_DB_HOST')}:{os.getenv('CONECTOR_DB_PORT')}/{os.getenv('CONECTOR_DB_NAME')}"
)
conector_engine = create_engine(conector_url)
conector_conn = conector_engine.connect()

rpa_url = (
    f"postgresql+psycopg2://{os.getenv('API_DB_USER')}:{os.getenv('API_DB_PASSWORD')}"
    f"@{os.getenv('API_DB_HOST')}:{os.getenv('API_DB_PORT')}/{os.getenv('API_DB_NAME')}"
)
rpa_engine = create_engine(rpa_url)
Session = sessionmaker(bind=rpa_engine)
session = Session()


# --- 2. GARANTIA DE DADOS ESSENCIAIS (SEED - sem alterações) ---
sistema_alvo = session.query(SistemaAlvo).filter_by(nome='SINAN NET').first()
if not sistema_alvo:
    sistema_alvo = SistemaAlvo(nome='SINAN NET', descricao='Sistema de Informação de Agravos de Notificação')
    session.add(sistema_alvo)
    session.flush()

agravo = session.query(Agravo).filter_by(nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA', sistema_alvo_id=sistema_alvo.id).first()
if not agravo:
    agravo = Agravo(sistema_alvo_id=sistema_alvo.id, nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA', descricao='Violência interpessoal autoprovocada')
    session.add(agravo)
    session.flush()


# --- 3. EXTRAÇÃO E TRANSFORMAÇÃO (sem alterações na lógica de busca e agrupamento) ---
print("Extraindo e transformando dados do Redcap...")
records_result = conector_conn.execute(text("""
    SELECT record, field_name, value
    FROM redcap_respostas
    WHERE record IN (
        SELECT record FROM redcap_respostas
        WHERE field_name ILIKE '%vio%'
    )
    ORDER BY record::int, field_name
""")).mappings().all()

dados_por_record = {}
for row in records_result:
    record = row["record"]
    if record not in dados_por_record:
        dados_por_record[record] = []
    dados_por_record[record].append({
        "field_name": row["field_name"],
        "value": row["value"]
    })


# --- 4. CARGA DOS DADOS NO BANCO DE DESTINO (com a nova lógica de validação) ---
print(f"Iniciando a carga e validação de {len(dados_por_record)} notificações...")
for record, dados in dados_por_record.items():
    
    # ### NOVO PASSO 2: LÓGICA DE VALIDAÇÃO ###
    
    # Cria um dicionário com os campos presentes na ficha para facilitar a busca de valores.
    campos_presentes = {dado['field_name']: dado['value'] for dado in dados}
    
    ficha_valida = True
    campos_faltando = []

    # Itera sobre a lista de campos obrigatórios para validar a ficha.
    for campo_obrigatorio in CAMPOS_OBRIGATORIOS:
        # 1. Verifica se o campo existe E se o seu valor não é nulo ou vazio.
        if campo_obrigatorio not in campos_presentes or not campos_presentes[campo_obrigatorio]:
            ficha_valida = False
            campos_faltando.append(campo_obrigatorio)
    
    # Define o status com base no resultado da validação.
    status_final = "pendente" if ficha_valida else "erro"

    # Se a ficha for inválida, imprime um aviso no console para facilitar a depuração.
    if not ficha_valida:
        print(f"[AVISO] Ficha (record: {record}) marcada como 'erro'. Campos obrigatórios ausentes/vazios: {campos_faltando}")
    
    # Gera um número de notificação aleatório.
    num_notificacao = str(random.randint(1000000, 9999999))

    # Cria a instância principal da notificação usando o status_final definido acima.
    notificacao = RpaNotificacao(
        record=record,
        num_notificacao=num_notificacao,
        status=status_final, # ### ALTERADO ### Usa o status que foi decidido na validação.
        agravo_id=agravo.id
    )
    session.add(notificacao)
    session.flush()

    # A lógica de salvar os detalhes permanece a mesma.
    for dado in dados:
        detalhe = RpaNotificacaoDetalhe(
            rpa_notificacao_id=notificacao.id,
            field_name=dado["field_name"],
            value=dado["value"]
        )
        session.add(detalhe)

# --- 5. FINALIZAÇÃO ---
session.commit()
print("Carga finalizada com sucesso!")

conector_conn.close()
session.close()