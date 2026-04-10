import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.models import RpaNotificacao, RpaNotificacaoDetalhe, SistemaAlvo, Agravo
import random

load_dotenv()

# Conector (Redcap)
conector_url = (
    f"postgresql+psycopg2://{os.getenv('CONECTOR_DB_USER')}:{os.getenv('CONECTOR_DB_PASSWORD')}"
    f"@{os.getenv('CONECTOR_DB_HOST')}:{os.getenv('CONECTOR_DB_PORT')}/{os.getenv('CONECTOR_DB_NAME')}"
)
conector_engine = create_engine(conector_url)
conector_conn = conector_engine.connect()

# API (sinan_api)
rpa_url = (
    f"postgresql+psycopg2://{os.getenv('API_DB_USER')}:{os.getenv('API_DB_PASSWORD')}"
    f"@{os.getenv('API_DB_HOST')}:{os.getenv('API_DB_PORT')}/{os.getenv('API_DB_NAME')}"
)
rpa_engine = create_engine(rpa_url)
Session = sessionmaker(bind=rpa_engine)
session = Session()

# Verificar se o Sistema Alvo já existe
sistema_alvo = session.query(SistemaAlvo).filter_by(nome='SINAN NET').first()
if not sistema_alvo:
    sistema_alvo = SistemaAlvo(nome='SINAN NET', descricao='Sistema de Informação de Agravos de Notificação')
    session.add(sistema_alvo)
    session.flush()

# Verificar se o Agravo já existe
agravo = session.query(Agravo).filter_by(nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA', sistema_alvo_id=sistema_alvo.id).first()
if not agravo:
    agravo = Agravo(sistema_alvo_id=sistema_alvo.id, nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA', descricao='Violência interpessoal autoprovocada')
    session.add(agravo)
    session.flush()

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

for record, dados in dados_por_record.items():
    num_notificacao = str(random.randint(1000000, 9999999))

    notificacao = RpaNotificacao(
        record=record,
        num_notificacao=num_notificacao,
        status="pendente",
        agravo_id=agravo.id
    )
    session.add(notificacao)
    session.flush()

    for dado in dados:
        detalhe = RpaNotificacaoDetalhe(
            rpa_notificacao_id=notificacao.id,
            field_name=dado["field_name"],
            value=dado["value"]
        )
        session.add(detalhe)

session.commit()
print("Carga realizada com sucesso!")
