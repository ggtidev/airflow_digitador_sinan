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
Última atualização: 24/10/2025
Versão: 2.1
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


# --- 2. CONFIGURAÇÃO INICIAL ---
# Carrega variáveis de ambiente do arquivo .env localizado na raiz do projeto.
# Esse arquivo deve conter as credenciais de conexão dos bancos de dados.
load_dotenv()


# --- 3. DEFINIÇÃO DOS CAMPOS OBRIGATÓRIOS ---
# A lista a seguir contém os campos essenciais de cada notificação.
# Eles são verificados durante a validação e não podem estar vazios.
CAMPOS_OBRIGATORIOS = [
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
        # --- Normalização das chaves e valores ---
        campos_presentes = {}
        for dado in dados:
            chave = str(dado["field_name"]).strip().lower()
            valor = "" if dado["value"] is None else str(dado["value"]).strip()
            campos_presentes[chave] = valor

        campos_faltando = []

        # --- REGRA 07: Verificação dos 5 campos principais ---
        campos_principais = [
            ('dt_not_vio', 'data_notificacao'),
            ('uf_notif_vio', 'uf_notificacao_vio'),
            ('mun_notif_vio', 'mun_notificacao_vio'),
            ('un_not_vio', 'unidade_notificadora'),
            ('us_vio', 'nome_unidade_saude')
        ]

        for aliases in campos_principais:
            valor = obter_valor(campos_presentes, *aliases)
            if not valor:
                campos_faltando.append(aliases[0]) # COLOCAR UM TEXTO

        # --- REGRAS EXISTENTES DE NEGÓCIO ---

        # REGRA 01: Município deve ser "RECIFE"
        mun_val = obter_valor(campos_presentes, 'mun_notif_vio', 'mun_notificacao_vio')
        if mun_val and mun_val.strip().upper() != "RECIFE":
            campos_faltando.append("mun_notif_vio (deve ser 'RECIFE')")

        # REGRA 02: Unidade notificadora deve ser "01" ou "7" 
        # --------------  
        # REGRA 02: Unidade notificadora e nome da unidade (REGRAS 2.1 e 2.2)

        # ==========================================================
        # REGRA 02 e 03 — Unidade notificadora e nome da unidade
        # ==========================================================

        # --- REGRA 2.1: Ajuste do código de unidade notificadora ---
        un_val = obter_valor(campos_presentes, 'un_not_vio', 'unidade_notificadora')
        if un_val is not None:
            un_val = str(un_val).strip()
            print(f"[DEBUG] Record {record} → un_not_vio recebido do REDCAP: '{un_val}'")

            # Se for diferente de 1, força o valor a 7
            if un_val == "1":
                un_val_final = "1"
            else:
                un_val_final = "7"
                print(f"[INFO] Record {record} → un_not_vio ajustado automaticamente de '{un_val}' para '7'")

        else:
            un_val_final = None
            print(f"[DEBUG] Record {record} → un_not_vio ausente ou nulo")
            campos_faltando.append("un_not_vio (campo obrigatório)")

        # --- REGRA 2.2: Determina a origem do nome da unidade de saúde ---
        us_val = None
        origem_nome = ""

        if un_val_final in ["2", "3", "4", "6", "7"]:
            # Quando for tipo 2,3,4,6,7 — buscar em nm_un_vio
            us_val = obter_valor(campos_presentes, 'nm_un_vio')
            origem_nome = "nm_un_vio"
        elif un_val_final in ["1", "5"]:
            # Quando for tipo 1 ou 5 — buscar em us_vio ou nome_unidade_saude
            us_val = obter_valor(campos_presentes, 'us_vio', 'nome_unidade_saude')
            origem_nome = "us_vio / nome_unidade_saude"
        else:
            origem_nome = "desconhecida"

        print(f"[DEBUG] Record {record} → Origem da unidade: {origem_nome} | Valor: '{us_val}'")

        # --- REGRA 03: Validação do nome da unidade ---
        # Caso o campo não exista em nenhum dos três lugares
        if (
            'us_vio' not in campos_presentes and
            'nome_unidade_saude' not in campos_presentes and
            'nm_un_vio' not in campos_presentes
        ):
            campos_faltando.append(
                "[us_vio] = nome_unidade_saude/nm_un_vio -> (esse campo está AUSENTE no banco, não veio essa informação do REDCAP)"
            )

        else:
            us_val_str = str(us_val).strip() if us_val is not None else ""

            # Mostra no log o valor recebido
            print(f"[DEBUG] Record {record} → Valor final de unidade de saúde: '{us_val_str}'")

            # Verifica duplicidade antes de adicionar ao log
            campo_ja_adicionado = any("us_vio" in c or "nm_un_vio" in c for c in campos_faltando)

            if not campo_ja_adicionado:
                if not us_val_str:
                    campos_faltando.append(
                        f"{origem_nome} (campo obrigatório — valor recebido: vazio)"
                    )
                elif not us_val_str.replace(" ", "").isalnum():
                    campos_faltando.append(
                        f"{origem_nome} (valor inválido — valor recebido: '{us_val_str}')"
                    )





        # --- DEFINIÇÃO DO STATUS FINAL ---
        status_final = "erro" if campos_faltando else "pendente"

        # --- REGISTRO NO LOG ---
        if status_final == "erro":
            total_erros += 1
            log_file.write(f"[ERRO] Record {record}: Campos ausentes ou inválidos -> {', '.join(campos_faltando)}\n")
        else:
            total_pendentes += 1
            log_file.write(f"[OK] Record {record}: Todos os campos principais preenchidos corretamente.\n")

        # --- INSERÇÃO NO BANCO DE DESTINO ---
        num_notificacao = str(random.randint(1000000, 9999999))
        notificacao = RpaNotificacao(
            record=record,
            num_notificacao=num_notificacao,
            status=status_final,
            agravo_id=agravo.id
        )
        session.add(notificacao)
        session.flush()

        # Insere detalhes
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
    session.commit()
    print("Carga de dados finalizada com sucesso!")
except Exception as e:
    print(f"Ocorreu um erro ao commitar as alterações: {e}")
    session.rollback()
finally:
    print("Fechando conexões com os bancos de dados.")
    conector_conn.close()
    session.close()
