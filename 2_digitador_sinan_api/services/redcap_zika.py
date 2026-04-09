# -*- coding: utf-8 -*-

"""
Este módulo é responsável por buscar dados de notificações de Zika
no banco de dados, formatá-los e prepará-los para um formato específico,
para integração com o SINAN NET via automação RPA.

Baseado em: redcap_toxoplasmose.py
"""

# --- Importações de Módulos ---
from database import get_connection
from collections import OrderedDict
from utils.utils import (
    formatar_data,
    formatar_sexo,
    get_labels_map,
    remover_acentos_recursivo,
)

# --- Constantes de Configuração ---
REDCAP_TOKEN = '15A28A0BA2CFF32380E87F2003FDA610'
REDCAP_URL = 'https://redcap.recife.pe.gov.br/api/'

# --- Inicialização de Variáveis Globais ---
labels_map = get_labels_map()


def map_to_rpa_format(raw_data):
    """
    Mapeia e transforma os dados brutos de uma notificação de Zika
    para um formato estruturado, dividido em 'notificacao' e 'investigacao'.
    """
    # --- Mapeamento de Campos ---
    # TODO: Atualizar os field_names abaixo com os sufixos reais do agravo ZIKA no RedCap.
    # Os field_names atuais usam os sufixos de Toxoplasmose Congênita como placeholder.
    notificacao_fields = {
        "dt_not_toxcong": "data_notificacao",                # 3 - Data da Notificação
        "nm_toxo_cong": "nome_paciente",                     # 8 - Nome do Paciente
        "dt_nasc_toxcong": "data_nascimento",                # 9 - Data de Nascimento
        "id_tox_cong": "idade",                              # 10 - Idade
        "sexo_tox_cong": "sexo",                             # 11 - Sexo
        "gest_tox_cong": "gestante",                         # 12 - Gestante
        "raca_cor_toxcong": "raca",                          # 13 - Raça/Cor
        "escol_tox_cong": "escolaridade",                    # 14 - Escolaridade
        "sus_tox_cong": "cartao_sus",                        # 15 - Número do Cartão SUS
        "nm_mae_toxcong": "nome_mae",                        # 16 - Nome da mãe
        "uf_resid_toxcong": "uf_residencia",                 # 17 - UF Residência
        "mun_resid_toxcong2": "municipio_residencia",        # 18 - Município de Residência
        "ds_resid_toxcong": "distrito_residencia",           # 19 - Distrito
        "bairro_tox_cong": "bairro_residencia",              # 20 - Bairro
        "log_tox_cong2": "endereco_residencia",              # 21 - Logradouro
        "num_resid_tox_cong": "numero_residencia",           # 22 - Número
        "comp_tox_cong": "complemento_residencia",           # 23 - Complemento
        "geo1_tox_cong": "geocampo1_residencia",             # 24 - Geocampo1
        "geo2_tox_cong": "geocampo2_residencia",             # 25 - Geocampo2
        "ref_tox_cong": "ponto_referencia",                  # 26 - Ponto de Referência
        "cep_tox_cong": "cep_residencia",                    # 27 - CEP
        "tel_tox_cong": "telefone",                          # 28 - (DDD) Telefone
        "zona_tox_cong": "zona",                             # 29 - Zona
        "pais_tox_cong": "pais_residencia",                  # 30 - País
        "uf_toxo_cong": "uf_notificacao",                    # 4 - UF
        "mun_toxo_cong": "municipio_notificacao",            # 5 - Município de Notificação
        "nm_toxcong2": "unidade_notificadora",               # 6 - Unidade de Saúde (CNES)
        "us_toxo_cong": "nome_unidade_saude",                # 6 - Nome da Unidade de Saúde
        "dt_pri_toxcong": "data_primeiros_sintomas",         # 7 - Data de Primeiros Sintomas
        "nm_not_toxcong": "nome_notificador",                # Nome do notificador
        "fun_not_toxcong": "funcao_notificador",             # Função do notificador
        "tel_notif_toxcong": "telefone_uni_notif",           # Telefone da unidade notificadora
        "idade_calc_toxcong": "idade_calculada_notificador", # Idade calculada
    }

    # TODO: Atualizar os field_names abaixo com os sufixos reais do agravo ZIKA no RedCap.
    investigacao_fields = {
        "dt_invest_tox_cong": "data_investigacao",           # 31 - Data da Investigação
        "class_fin_tox_cong": "classificacao_final",         # 32 - Classificação Final
        "crit_conf_tox_cong": "criterio_confirmacao",        # 33 - Critério de Confirmação/Descarte
        "autoc_tox_cong": "caso_autoctone",                  # 34 - Caso autóctone
        "uf_autoc_tox_cong": "uf_autoctone",                 # 35 - UF (Local Provável)
        "pais_autoc_tox_cong": "pais_autoctone",             # 36 - País
        "mun_autoc_tox_cong": "municipio_autoctone",         # 37 - Município
        "ds_autoc_tox_cong": "distrito_autoctone",           # 38 - Distrito
        "bairro_autoc_tox_cong": "bairro_autoctone",         # 39 - Bairro
        "doenc_trab_tox_cong": "doenca_trabalho",            # 40 - Doença Relacionada ao Trabalho
        "evol_tox_cong": "evolucao_caso",                    # 41 - Evolução do Caso
        "dt_obito_tox_cong": "data_obito",                   # 42 - Data do Óbito
        "dt_encerra_tox_cong": "data_encerramento",          # 43 - Data do Encerramento
        "obs_tox_cong": "observacoes",                       # Observações adicionais
    }

    notificacao, investigacao, outros = {}, {}, {}

    for k, v in raw_data.items():
        if k in notificacao_fields:
            # TODO: Atualizar campo de sexo com o sufixo real do agravo ZIKA
            campos_sexo = ["sexo_tox_cong"]
            if k in campos_sexo:
                valor = formatar_sexo(v)
            elif "dt_" in k:
                valor = formatar_data(v)
            elif k in labels_map:
                valor = labels_map[k].get(v, v)
            else:
                valor = v
            notificacao[notificacao_fields[k]] = valor

        elif k in investigacao_fields:
            if "dt_" in k:
                valor = formatar_data(v)
            elif k in labels_map:
                valor = labels_map[k].get(v, v)
            else:
                valor = v
            investigacao[investigacao_fields[k]] = valor

        else:
            outros[k] = v

    resultado = {
        "agravo": "ZIKA",
        "notificacao": notificacao,
        "investigacao": investigacao,
        "outros": outros,
    }

    return remover_acentos_recursivo(resultado)


def get_redcap_filas():
    """
    Busca no banco de dados todas as notificações de Zika que estão com
    status 'pendente', formata-as e as retorna em uma lista.
    """
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT v.record, v.num_notificacao, v.status, d.field_name, d.value
        FROM rpa_notificacoes v
        JOIN rpa_notificacao_detalhes d ON v.id = d.rpa_notificacao_id
        WHERE v.status = 'pendente'
        ORDER BY v.record::int, d.field_name;
    """)

    rows = cur.fetchall()
    cur.close()
    conn.close()

    filas = {}
    for record, num_notificacao, status, field, value in rows:
        if record not in filas:
            filas[record] = {
                "num_notificacao": num_notificacao,
                "status": status,
                "dados": {},
            }
        filas[record]["dados"][field] = value

    filas_formatadas = []
    for fila in filas.values():
        dado_formatado = map_to_rpa_format(fila["dados"])

        ordenado = OrderedDict()
        ordenado["agravo"] = dado_formatado["agravo"]
        ordenado["num_notificacao"] = fila["num_notificacao"]
        ordenado["status"] = fila["status"]
        ordenado["notificacao"] = dado_formatado["notificacao"]
        ordenado["investigacao"] = dado_formatado["investigacao"]
        ordenado["outros"] = dado_formatado["outros"]

        filas_formatadas.append(ordenado)

    return filas_formatadas
