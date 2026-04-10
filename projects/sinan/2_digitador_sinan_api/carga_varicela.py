# -*- coding: utf-8 -*-
"""
Script: carga_varicela.py
Descrição:
    Este script executa a extração, validação e carga de dados de notificações de
    Varicela do banco de origem (ex: RedCap) para o banco de destino (RPA/API),
    aplicando regras de negócio específicas e gerando logs detalhados da execução.

Baseado em: carga_toxoplasmose.py (v1.0)
"""

# --- 1. IMPORTAÇÕES ---
import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models.models import RpaNotificacao, RpaNotificacaoDetalhe, SistemaAlvo, Agravo
from utils.utils import get_numero_sinan


# --- 2. CONFIGURAÇÃO INICIAL ---
load_dotenv()


# --- 3. DEFINIÇÃO DOS CAMPOS OBRIGATÓRIOS ---
# TODO: Atualizar os field_names abaixo com os sufixos reais do agravo VARICELA no RedCap.
# Os field_names atuais usam os sufixos de Toxoplasmose Congênita como placeholder.
CAMPOS_OBRIGATORIOS = [
    'dt_not_toxcong',       # data_notificacao
    'uf_toxo_cong',         # uf_notificacao
    'mun_toxo_cong',        # mun_notificacao
    'nm_toxcong2',          # unidade_notificadora (CNES)
    'us_toxo_cong'          # nome_unidade_saude
]


# --- 4. CONEXÃO COM OS BANCOS DE DADOS ---

# 4.1. Conexão com o banco de origem (ex: RedCap)
conector_url = (
    f"postgresql+psycopg2://{os.getenv('CONECTOR_DB_USER')}:{os.getenv('CONECTOR_DB_PASSWORD')}"
    f"@{os.getenv('CONECTOR_DB_HOST')}:{os.getenv('CONECTOR_DB_PORT')}/{os.getenv('CONECTOR_DB_NAME')}"
)
conector_engine = create_engine(conector_url)
conector_conn = conector_engine.connect()

# 4.2. Conexão com o banco de destino (RPA/API)
rpa_url = (
    f"postgresql+psycopg2://{os.getenv('API_DB_USER')}:{os.getenv('API_DB_PASSWORD')}"
    f"@{os.getenv('API_DB_HOST')}:{os.getenv('API_DB_PORT')}/{os.getenv('API_DB_NAME')}"
)
rpa_engine = create_engine(rpa_url)
Session = sessionmaker(bind=rpa_engine)
session = Session()


# --- 5. SETUP DE DADOS ESSENCIAIS (SEEDING) ---
print("Verificando e criando dados essenciais (Sistema Alvo e Agravo)...")

# 5.1. Verifica se o SistemaAlvo "SINAN NET" existe; se não, cria.
sistema_alvo = session.query(SistemaAlvo).filter_by(nome='SINAN NET').first()
if not sistema_alvo:
    print("Criando SistemaAlvo 'SINAN NET'...")
    sistema_alvo = SistemaAlvo(
        nome='SINAN NET',
        descricao='Sistema de Informação de Agravos de Notificação'
    )
    session.add(sistema_alvo)
    session.flush()

# 5.2. Verifica se o Agravo "VARICELA" existe; se não, cria.
agravo = session.query(Agravo).filter_by(
    nome='VARICELA',
    sistema_alvo_id=sistema_alvo.id
).first()
if not agravo:
    print("Criando Agravo 'VARICELA'...")
    agravo = Agravo(
        sistema_alvo_id=sistema_alvo.id,
        nome='VARICELA',
        descricao='Varicela'
    )
    session.add(agravo)
    session.flush()


# --- 6. EXTRAÇÃO E TRANSFORMAÇÃO DOS DADOS ---
print("Extraindo dados do banco de origem (RedCap) - Varicela...")

# TODO: Atualizar o filtro da query abaixo com o sufixo real do agravo VARICELA no RedCap.
# O filtro atual usa '%toxo%' como placeholder.
records_result = conector_conn.execute(text("""
    SELECT record, field_name, value
    FROM redcap_respostas
    WHERE record IN (
        SELECT record FROM redcap_respostas
        WHERE field_name ILIKE '%toxo%'
    )
    ORDER BY record::int, field_name
""")).mappings().all()

# Agrupamento dos resultados por "record"
print("Transformando e agrupando os dados por ficha...")
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
print(f"Iniciando a carga e validação de {len(dados_por_record)} notificações de Varicela...")

log_path = "log_validacao_varicela.txt"
data_execucao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
total_erros = 0
total_pendentes = 0

def obter_valor(campos_dict, *aliases):
    """
    Retorna o valor do primeiro alias encontrado e não vazio dentro de campos_dict.
    """
    for alias in aliases:
        val = campos_dict.get(alias)
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return None


with open(log_path, "w", encoding="utf-8") as log_file:
    log_file.write("==== LOG DE VALIDAÇÃO - VARICELA ====\n")
    log_file.write(f"Data e hora da execução: {data_execucao}\n")
    log_file.write("------------------------------------------------------------\n\n")

    for record, dados in dados_por_record.items():
        # --- VERIFICAÇÃO DE DUPLICIDADE ---
        exists = session.query(RpaNotificacao).filter_by(record=record).first()
        if exists:
            print(f"[SKIP] Record {record} já existe.")
            log_file.write(f"[SKIP] Record {record}: Já existe na base.\n")
            continue

        # --- Normalização das chaves e valores ---
        campos_presentes = {}
        for dado in dados:
            chave = str(dado["field_name"]).strip().lower()
            valor = "" if dado["value"] is None else str(dado["value"]).strip()
            campos_presentes[chave] = valor

        campos_faltando = []

        # ==========================================================
        # REGRA: Consistência entre Data de Encerramento e Notificação
        # ==========================================================
        # TODO: Atualizar os field_names abaixo com os sufixos reais do agravo VARICELA
        dt_not_str = obter_valor(campos_presentes, 'dt_not_toxcong', 'data_notificacao')
        dt_enc_str = obter_valor(campos_presentes, 'dt_encerra_tox_cong', 'data_encerramento')

        if dt_not_str and dt_enc_str:
            try:
                fmt = "%Y-%m-%d"
                dt_not_obj = datetime.strptime(dt_not_str, fmt)
                dt_enc_obj = datetime.strptime(dt_enc_str, fmt)

                if dt_enc_obj < dt_not_obj:
                    print(f"[REGRAS] Record {record} -> Encerramento ({dt_enc_str}) < Notificação ({dt_not_str}). Ajustando...")
                    
                    for item in dados:
                        if item["field_name"].strip().lower() in ['dt_encerra_tox_cong', 'data_encerramento']:
                            item["value"] = dt_not_str
                            campos_presentes[item["field_name"].strip().lower()] = dt_not_str
            except ValueError:
                pass

        # --- Verificação dos campos obrigatórios ---
        # TODO: Atualizar os field_names abaixo com os sufixos reais do agravo VARICELA
        campos_principais = [
            ('dt_not_toxcong', 'data_notificacao'),
            ('uf_toxo_cong', 'uf_notificacao'),
            ('mun_toxo_cong', 'mun_notificacao'),
        ]
        
        for aliases in campos_principais:
            valor = obter_valor(campos_presentes, *aliases)
            if not valor:
                campos_faltando.append(aliases[0])

        # --- REGRA: Município deve ser "RECIFE" ---
        # TODO: Atualizar o field_name abaixo com o sufixo real do agravo VARICELA
        mun_val = obter_valor(campos_presentes, 'mun_toxo_cong', 'mun_notificacao')
        if mun_val and mun_val.strip().upper() != "RECIFE":
            campos_faltando.append("mun_notif (deve ser 'RECIFE')")

        # --- REGRA: Validação de unidade notificadora ---
        # TODO: Atualizar os field_names abaixo com os sufixos reais do agravo VARICELA
        un_val = obter_valor(campos_presentes, 'nm_toxcong2', 'unidade_notificadora')
        us_val_us = obter_valor(campos_presentes, 'us_toxo_cong', 'nome_unidade_saude')

        if not un_val and not us_val_us:
            campos_faltando.append("unidade_notificadora, nome_unidade_saude (unidade ausente)")

        # --- DEFINIÇÃO DO STATUS FINAL ---
        status_final = "erro" if campos_faltando else "pendente"

        # --- REGISTRO NO LOG ---
        if status_final == "erro":
            total_erros += 1
            log_file.write(f"[ERRO] Record {record}: Campos ausentes ou inválidos -> {', '.join(campos_faltando)}\n")
        else:
            total_pendentes += 1
            log_file.write(f"[OK] Record {record}: Validado com sucesso.\n")

        # --- INSERÇÃO NO BANCO DE DESTINO (PostgreSQL) ---
        num_notificacao = str(get_numero_sinan())
        notificacao = RpaNotificacao(
            record=record,
            num_notificacao=num_notificacao,
            status=status_final,
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

    # --- RESUMO FINAL NO LOG ---
    log_file.write("\n------------------------------------------------------------\n")
    log_file.write("RESUMO FINAL DA EXECUÇÃO - VARICELA\n")
    log_file.write(f"Data/hora: {data_execucao}\n")
    log_file.write(f"Total de notificações processadas: {len(dados_por_record)}\n")
    log_file.write(f"Total com status 'pendente': {total_pendentes}\n")
    log_file.write(f"Total com status 'erro': {total_erros}\n")
    log_file.write("------------------------------------------------------------\n")

print(f"Validação concluída. Log salvo em: {log_path}")


# --- 8. FINALIZAÇÃO ---
try:
    session.commit()
    print("Carga de Varicela finalizada com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro ao commitar as alterações: {e}")
    session.rollback()
finally:
    print("Fechando conexões com os bancos de dados.")
    conector_conn.close()
    session.close()
