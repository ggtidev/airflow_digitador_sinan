# -*- coding: utf-8 -*-
"""
Script: carga_violencia.py
Descrição:
    Este script executa a extração, validação e carga de dados de notificações de
    violência do banco de origem (ex: RedCap) para o banco de destino (RPA/API),
    aplicando regras de negócio específicas e gerando logs detalhados da execução.

    Funcionalidades principais:
      - Validação de campos obrigatórios e consistência de dados.
      - Criação automática de registros essenciais (SistemaAlvo e Agravo).
      - Inserção das notificações e seus detalhes no banco de destino.
      - Geração de um arquivo de log com data/hora, erros e resumo final.

Autor: André ROdovalho / Minsait - Saúde Digital
Última atualização: 18/12/2025
Versão: 2.4
"""

# --- 1. IMPORTAÇÕES ---
# Importa os módulos essenciais utilizados pelo script.
import os
import random
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Importa os modelos ORM que representam as tabelas do banco de destino (API/RPA).
from models.models import RpaNotificacao, RpaNotificacaoDetalhe, SistemaAlvo, Agravo
from utils.utils import get_numero_sinan


# --- 2. CONFIGURAÇÃO INICIAL ---
# Carrega variáveis de ambiente do arquivo .env localizado na raiz do projeto.
# Esse arquivo deve conter as credenciais de conexão dos bancos de dados.
load_dotenv()


# --- 3. DEFINIÇÃO DOS CAMPOS OBRIGATÓRIOS ---
# A lista a seguir contém os campos essenciais de cada notificação.
# Eles são verificados durante a validação e não podem estar vazios.
CAMPOS_OBRIGATÓRIOS = [
    'dt_not_vio',      # data_notificacao
    'uf_notif_vio',    # uf_notificacao_vio
    'mun_notif_vio',   # mun_notificacao_vio
    'un_not_vio',      # unidade_notificadora
    'us_vio'           # nome_unidade_saude
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
# Garante que as tabelas básicas “SistemaAlvo” e “Agravo” existam no destino.
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

# 5.2. Verifica se o Agravo “VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA” existe; se não, cria.
agravo = session.query(Agravo).filter_by(
    nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA',
    sistema_alvo_id=sistema_alvo.id
).first()
if not agravo:
    print("Criando Agravo 'VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA'...")
    agravo = Agravo(
        sistema_alvo_id=sistema_alvo.id,
        nome='VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA',
        descricao='Violência interpessoal autoprovocada'
    )
    session.add(agravo)
    session.flush()


# --- 6. EXTRAÇÃO E TRANSFORMAÇÃO DOS DADOS ---
print("Extraindo dados do banco de origem (RedCap)...")

# Consulta SQL que busca todos os registros relacionados a “vio” (violência)
records_result = conector_conn.execute(text("""
    SELECT record, field_name, value
    FROM redcap_respostas
    WHERE record IN (
        SELECT record FROM redcap_respostas
        WHERE field_name ILIKE '%vio%'
    )
    ORDER BY record::int, field_name
""")).mappings().all()

# Agrupamento dos resultados por “record” (cada ficha de notificação)
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
print(f"Iniciando a carga e validação de {len(dados_por_record)} notificações...")

# Caminho do arquivo de log gerado na execução
log_path = "log_validacao_notificacoes.txt"

# Data e hora de início da execução
data_execucao = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

# Contadores de status para resumo final
total_erros = 0
total_pendentes = 0

# Função auxiliar para buscar valor por possíveis aliases
def obter_valor(campos_dict, *aliases):
    """
    Retorna o valor do primeiro alias encontrado e não vazio dentro de campos_dict.
    Aplica strip() e converte valores numéricos para string.
    """
    for alias in aliases:
        val = campos_dict.get(alias)
        if val is not None and str(val).strip() != "":
            return str(val).strip()
    return None


# Abertura do arquivo de log
with open(log_path, "w", encoding="utf-8") as log_file:
    log_file.write("==== LOG DE VALIDAÇÃO DAS NOTIFICAÇÕES ====\n")
    log_file.write(f"Data e hora da execução: {data_execucao}\n")
    log_file.write("------------------------------------------------------------\n\n")

    for record, dados in dados_por_record.items():
        # --- [NOVO] VERIFICAÇÃO DE DUPLICIDADE ---
        exists = session.query(RpaNotificacao).filter_by(record=record).first()
        if exists:
            # log_file.write(f"[IGNORE] Record {record}: Já existe no banco. Ignorado.\n")
            # print(f"Record {record} já processado. Pulando...")
            # continue
            # Opção: Se quiser logar que já existe, descomente acima.
            # Mas vamos manter log limpo se o objetivo é só não duplicar.
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
        # NOVA REGRA: Consistência entre Data de Encerramento e Notificação
        # ==========================================================
        # Captura as strings das datas - CORREÇÃO: dt_encerra_viole
        dt_not_str = obter_valor(campos_presentes, 'dt_not_vio', 'data_notificacao')
        dt_enc_str = obter_valor(campos_presentes, 'dt_encerra_viole', 'data_encerramento')

        # Comparação para evitar erro de validação do SINAN
        if dt_not_str and dt_enc_str:
            try:
                fmt = "%Y-%m-%d"
                dt_not_obj = datetime.strptime(dt_not_str, fmt)
                dt_enc_obj = datetime.strptime(dt_enc_str, fmt)

                # Se 'data_encerramento' for menor que 'data_notificacao', usar 'data_notificacao'
                if dt_enc_obj < dt_not_obj:
                    print(f"[REGRAS] Record {record} -> Encerramento ({dt_enc_str}) < Notificação ({dt_not_str}). Ajustando...")
                    
                    # Atualiza o valor na lista original 'dados' para persistência correta
                    for item in dados:
                        # CORREÇÃO: Verificando o nome correto dt_encerra_viole
                        if item["field_name"].strip().lower() in ['dt_encerra_viole', 'data_encerramento']:
                            item["value"] = dt_not_str
                            # Atualiza também o dicionário local de validação
                            campos_presentes[item["field_name"].strip().lower()] = dt_not_str
            except ValueError:
                # Caso as datas não estejam no formato esperado para conversão
                pass

        # --- REGRA 07: Verificação dos 5 campos principais ---
        campos_principais = [
            ('dt_not_vio', 'data_notificacao'),
            ('uf_notif_vio', 'uf_notificacao_vio'),
            ('mun_notif_vio', 'mun_notificacao_vio'),
            # Os campos de unidade (un_not_vio, us_vio) são validados abaixo na REGRA 1 (MODIFICADA)
        ]
        
        # Validação dos campos principais (exceto os de unidade)
        for aliases in campos_principais:
            valor = obter_valor(campos_presentes, *aliases)
            if not valor:
                campos_faltando.append(aliases[0])

        # --- REGRAS EXISTENTES DE NEGÓCIO ---

        # REGRA 01: Município deve ser "RECIFE"
        mun_val = obter_valor(campos_presentes, 'mun_notif_vio', 'mun_notificacao_vio')
        if mun_val and mun_val.strip().upper() != "RECIFE":
            campos_faltando.append("mun_notif_vio (deve ser 'RECIFE')")

        # --- REGRA 1 (NOVA): Validação unificada de unidade notificadora ---
        # O erro só ocorre se TODOS os 3 campos (ou seus aliases) estiverem vazios.
        un_val = obter_valor(campos_presentes, 'un_not_vio', 'unidade_notificadora')
        us_val_us = obter_valor(campos_presentes, 'us_vio', 'nome_unidade_saude')
        us_val_nm = obter_valor(campos_presentes, 'nm_un_vio')

        if not un_val and not us_val_us and not us_val_nm:
             campos_faltando.append("un_not_vio, us_vio, nm_un_vio (unidade ausente)")

        # --- REGRA 2.1: Ajuste do código de unidade notificadora (Transformação) ---
        un_val_final = None
        if un_val is not None:
            un_val_strip = str(un_val).strip()
            # Se for diferente de 1, força o valor a 7
            if un_val_strip == "1":
                un_val_final = "1"
            else:
                un_val_final = "7"
        
        # --- REGRA 2.2: Determina a origem do nome da unidade de saúde ---
        us_val = None
        origem_nome = ""

        if un_val_final in ["2", "3", "4", "6", "7"]:
            us_val = us_val_nm
            origem_nome = "nm_un_vio"
        elif un_val_final in ["1", "5"]:
            us_val = us_val_us
            origem_nome = "us_vio / nome_unidade_saude"
        else:
            if us_val_us:
                origem_nome = "us_vio / nome_unidade_saude (un_not_vio ausente)"
                us_val = us_val_us
            elif us_val_nm:
                origem_nome = "nm_un_vio (un_not_vio ausente)"
                us_val = us_val_nm

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
        # Gera um número de notificação aleatório para o RPA processar
        num_notificacao = str(get_numero_sinan())
        notificacao = RpaNotificacao(
            record=record,
            num_notificacao=num_notificacao,
            status=status_final,
            agravo_id=agravo.id
        )
        session.add(notificacao)
        session.flush() # Obtém o ID da notificação para os detalhes

        # Insere os detalhes da notificação (chave/valor) para o Robô digitar
        for dado in dados:
            detalhe = RpaNotificacaoDetalhe(
                rpa_notificacao_id=notificacao.id,
                field_name=dado["field_name"],
                value=dado["value"]
            )
            session.add(detalhe)

    # --- RESUMO FINAL NO LOG ---
    log_file.write("\n------------------------------------------------------------\n")
    log_file.write("RESUMO FINAL DA EXECUÇÃO\n")
    log_file.write(f"Data/hora: {data_execucao}\n")
    log_file.write(f"Total de notificações processadas: {len(dados_por_record)}\n")
    log_file.write(f"Total com status 'pendente': {total_pendentes}\n")
    log_file.write(f"Total com status 'erro': {total_erros}\n")
    log_file.write("------------------------------------------------------------\n")

print(f"Validação concluída. Log salvo em: {log_path}")


# --- 8. FINALIZAÇÃO ---
try:
    # Confirma todas as transações no banco de destino
    session.commit()
    print("Carga de dados finalizada com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro ao commitar as alterações: {e}")
    session.rollback()
finally:
    # Fecha as conexões de forma segura
    print("Fechando conexões com os bancos de dados.")
    conector_conn.close()
    session.close()