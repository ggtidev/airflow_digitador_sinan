# -*- coding: utf-8 -*-

# --- 1. IMPORTAÇÕES ---
# Módulos necessários para o script.
import os
import random
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Importa os modelos ORM que representam as tabelas do banco de dados de destino.
# Certifique-se de que o caminho para 'models.models' está correto em seu projeto.
from models.models import RpaNotificacao, RpaNotificacaoDetalhe, SistemaAlvo, Agravo

# --- 2. CONFIGURAÇÃO INICIAL ---

# Carrega as variáveis de ambiente de um arquivo .env localizado na raiz do projeto.
# Este arquivo deve conter as credenciais de acesso aos bancos de dados para segurança.
load_dotenv()

# --- 3. DEFINIÇÃO DOS CAMPOS OBRIGATÓRIOS ---
# Esta lista foi gerada a partir da análise do arquivo "Dicionário de Dados" fornecido.
# Todos os campos marcados com 'y' (sim) na coluna "Required Field?" foram incluídos aqui.
# O script usará esta lista para validar cada ficha antes de inseri-la no banco.
CAMPOS_OBRIGATORIOS = [
    #'agravo_de_notificacao_individual', #nao achei
    'data_notificacao', #dt_not_vio
    'uf_notificacao_vio', #uf_notif_vio
    'mun_notificacao_vio', #mun_notif_vio
    #REGRA 01 Todas as vezes que for diferente de recife, não digitar Ex: "mun_notificacao_vio": "OLINDA",
    #REGRA 02 - Se "unidade_notificadora(un_not_vio)": "1", For diferente 01 e 7 da ERRO
    'unidade_notificadora', #un_not_vio
    #"us_vio", # pergnta 08 Obrigatorio
    
]

# --- 4. CONEXÃO COM OS BANCOS DE DADOS ---

# 4.1. Conexão com o Banco de Dados de Origem (Ex: Redcap)
conector_url = (
    f"postgresql+psycopg2://{os.getenv('CONECTOR_DB_USER')}:{os.getenv('CONECTOR_DB_PASSWORD')}"
    f"@{os.getenv('CONECTOR_DB_HOST')}:{os.getenv('CONECTOR_DB_PORT')}/{os.getenv('CONECTOR_DB_NAME')}"
)
conector_engine = create_engine(conector_url)
conector_conn = conector_engine.connect()

# 4.2. Conexão com o Banco de Dados de Destino (API/RPA)
rpa_url = (
    f"postgresql+psycopg2://{os.getenv('API_DB_USER')}:{os.getenv('API_DB_PASSWORD')}"
    f"@{os.getenv('API_DB_HOST')}:{os.getenv('API_DB_PORT')}/{os.getenv('API_DB_NAME')}"
)
rpa_engine = create_engine(rpa_url)
# Cria uma fábrica de sessões (Sessionmaker) para interagir com o banco de destino usando ORM.
Session = sessionmaker(bind=rpa_engine)
session = Session()

# --- 5. SETUP DE DADOS ESSENCIAIS (SEEDING) ---
# Garante que os registros básicos para 'SistemaAlvo' e 'Agravo' existam antes de inserir as notificações.
# Isso é crucial para manter a integridade referencial (chaves estrangeiras).

print("Verificando e criando dados essenciais (Sistema Alvo e Agravo)...")
# Verifica se o 'Sistema Alvo' chamado 'SINAN NET' já existe.
sistema_alvo = session.query(SistemaAlvo).filter_by(nome='SINAN NET').first()
if not sistema_alvo:
    print("Criando SistemaAlvo 'SINAN NET'...")
    sistema_alvo = SistemaAlvo(nome='SINAN NET', descricao='Sistema de Informação de Agravos de Notificação')
    session.add(sistema_alvo)
    session.flush() # Envia o comando ao banco para obter o ID do novo registro.

# Verifica se o 'Agravo' de violência já existe.
agravo = session.query(Agravo).filter_by(nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA', sistema_alvo_id=sistema_alvo.id).first()
if not agravo:
    print("Criando Agravo 'VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA'...")
    agravo = Agravo(sistema_alvo_id=sistema_alvo.id, nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA', descricao='Violência interpessoal autoprovocada')
    session.add(agravo)
    session.flush()

# --- 6. EXTRAÇÃO E TRANSFORMAÇÃO DOS DADOS ---

print("Extraindo dados do banco de origem (Redcap)...")
# Executa a consulta SQL para buscar os dados de violência no banco de origem.
records_result = conector_conn.execute(text("""
    SELECT record, field_name, value
    FROM redcap_respostas
    WHERE record IN (
        SELECT record FROM redcap_respostas
        WHERE field_name ILIKE '%vio%'
    )
    ORDER BY record::int, field_name
""")).mappings().all()

print("Transformando e agrupando os dados por ficha...")
# Agrupa os resultados por 'record' para que cada ficha seja um item único no dicionário.
dados_por_record = {}
for row in records_result:
    record = row["record"]
    if record not in dados_por_record:
        dados_por_record[record] = []
    dados_por_record[record].append({
        "field_name": row["field_name"],
        "value": row["value"]
    })

# --- 7. CARGA E VALIDAÇÃO DOS DADOS NO BANCO DE DESTINO ---

print(f"Iniciando a carga e validação de {len(dados_por_record)} notificações...")
for record, dados in dados_por_record.items():
    
    # 7.1. Lógica de Validação
    # Para cada ficha, verifica se todos os campos obrigatórios estão presentes e preenchidos.
    campos_presentes = {dado['field_name']: dado['value'] for dado in dados}
    ficha_valida = True
    campos_faltando = []

    for campo_obrigatorio in CAMPOS_OBRIGATORIOS:
        # A condição verifica se a chave não existe OU se o valor associado é nulo ou uma string vazia.
        if campo_obrigatorio not in campos_presentes or not campos_presentes[campo_obrigatorio]:
            ficha_valida = False
            campos_faltando.append(campo_obrigatorio)
    
    # Define o status da notificação com base no resultado da validação.
    status_final = "pendente" if ficha_valida else "erro"

    if not ficha_valida:
        # Imprime um aviso no console para depuração, informando qual ficha falhou e por quê.
        print(f"[AVISO] Ficha (record: {record}) marcada como 'erro'. Campos ausentes/vazios: {campos_faltando}")
    
    # 7.2. Inserção no Banco de Dados
    # Gera um número de notificação fictício para o registro.
    num_notificacao = str(random.randint(1000000, 9999999))

    # Cria o objeto RpaNotificacao com o status correto.
    notificacao = RpaNotificacao(
        record=record,
        num_notificacao=num_notificacao,
        status=status_final,
        agravo_id=agravo.id
    )
    session.add(notificacao)
    session.flush() # Sincroniza para obter o ID da notificação para a tabela de detalhes.

    # Insere todos os campos (detalhes), independentemente de serem válidos ou não.
    # Isso garante que mesmo as fichas com erro tenham seus dados guardados para análise.
    for dado in dados:
        detalhe = RpaNotificacaoDetalhe(
            rpa_notificacao_id=notificacao.id,
            field_name=dado["field_name"],
            value=dado["value"]
        )
        session.add(detalhe)

# --- 8. FINALIZAÇÃO ---

try:
    # Efetiva a transação, salvando todas as novas notificações e seus detalhes no banco.
    session.commit()
    print("Carga de dados finalizada com sucesso!")
except Exception as e:
    # Em caso de erro durante o commit, desfaz a transação para não deixar o banco em estado inconsistente.
    print(f"Ocorreu um erro ao commitar as alterações: {e}")
    session.rollback()
finally:
    # Garante que as conexões com os bancos de dados sejam sempre fechadas ao final da execução.
    print("Fechando conexões com os bancos de dados.")
    conector_conn.close()
    session.close()