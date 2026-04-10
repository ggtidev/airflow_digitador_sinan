# -*- coding: utf-8 -*-

"""
Este módulo é responsável por buscar dados de notificações de Toxoplasmose Congênita
no banco de dados, formatá-los e prepará-los para um formato específico,
provavelmente para integração com outro sistema ou para automação de processos
(RPA - Robotic Process Automation).

O fluxo principal é:
1. Conectar-se a um banco de dados.
2. Buscar notificações com status 'pendente'.
3. Mapear e transformar os dados brutos em um formato estruturado.
4. Aplicar formatações específicas para campos como data, sexo, etc.
5. Retornar uma lista de notificações prontas para serem processadas.
"""

# --- Importações de Módulos ---
# get_connection: Função para obter uma conexão com o banco de dados.
from database import get_connection
# OrderedDict: Um dicionário que mantém a ordem em que os itens foram inseridos.
from collections import OrderedDict
# Funções utilitárias para formatação e processamento de dados.
from utils.utils import (
    formatar_data,
    formatar_sexo,
    get_labels_map,
    remover_acentos_recursivo,
)

# --- Constantes de Configuração ---
# Token de API para autenticação no REDCap (Toxoplasmose Congênita).
REDCAP_TOKEN = '15A28A0BA2CFF32380E87F2003FDA610'
# URL da API do REDCap.
REDCAP_URL = 'https://redcap.recife.pe.gov.br/api/'

# --- Inicialização de Variáveis Globais ---
# Carrega um mapa de "labels" (rótulos) que traduz códigos em valores legíveis.
labels_map = get_labels_map()


def map_to_rpa_format(raw_data):
    """
    Mapeia e transforma os dados brutos de uma notificação de Toxoplasmose Congênita
    para um formato estruturado, dividido em 'notificacao' e 'investigacao'.

    Args:
        raw_data (dict): Um dicionário contendo os dados brutos da notificação,
                         onde as chaves são os nomes dos campos do REDCap/banco de dados.

    Returns:
        dict: Um dicionário contendo os dados formatados e estruturados,
              com remoção de acentos.
    """
    # --- Mapeamento de Campos ---
    # Dicionário que mapeia os campos originais (chaves) para os novos nomes
    # de campos na seção 'notificacao'.
    # Campos da aba "Notificação" (Dados Gerais, Notificação Individual, Dados de Residência)
    # OBS: field_names extraídos diretamente do banco RedCap - sufixos _tox_cong, _toxcong, _toxo_cong
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

    # Dicionário que mapeia os campos originais (chaves) para os novos nomes
    # de campos na seção 'investigacao'.
    # Campos da aba "Investigação" (Conclusão)
    investigacao_fields = {
        "dt_invest_tox_cong": "data_investigacao",           # 31 - Data da Investigação
        "class_fin_tox_cong": "classificacao_final",         # 32 - Classificação Final
        "crit_conf_tox_cong": "criterio_confirmacao",        # 33 - Critério de Confirmação/Descarte
        "autoc_tox_cong": "caso_autoctone",                  # 34 - O caso é autóctone do município de residência?
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

    # Dicionários para armazenar os dados após a separação e formatação.
    notificacao, investigacao, outros = {}, {}, {}

    # Itera sobre cada item (chave, valor) nos dados brutos.
    for k, v in raw_data.items():
        # Verifica se a chave pertence aos campos de 'notificacao'.
        if k in notificacao_fields:
            campos_sexo = ["sexo_tox_cong"]
            # Aplica formatação específica para campos de sexo.
            if k in campos_sexo:
                valor = formatar_sexo(v)
            # Aplica formatação específica para campos de data.
            elif "dt_" in k:
                valor = formatar_data(v)
            # Se o campo existe no mapa de rótulos, traduz o valor.
            elif k in labels_map:
                valor = labels_map[k].get(v, v)
            # Caso contrário, mantém o valor original.
            else:
                valor = v
            # Adiciona o valor formatado ao dicionário 'notificacao' com a nova chave.
            notificacao[notificacao_fields[k]] = valor

        # Verifica se a chave pertence aos campos de 'investigacao'.
        elif k in investigacao_fields:
            # Aplica formatação para campos de data.
            if "dt_" in k:
                valor = formatar_data(v)
            # Se o campo existe no mapa de rótulos, traduz o valor.
            elif k in labels_map:
                valor = labels_map[k].get(v, v)
            # Caso contrário, mantém o valor original.
            else:
                valor = v
            # Adiciona o valor formatado ao dicionário 'investigacao' com a nova chave.
            investigacao[investigacao_fields[k]] = valor

        # Se a chave não pertence a nenhum dos mapeamentos, armazena em 'outros'.
        else:
            outros[k] = v

    # Monta o dicionário final com a estrutura desejada.
    resultado = {
        "agravo": "TOXOPLASMOSE_CONGENITA",  # Valor fixo para o tipo de agravo.
        "notificacao": notificacao,
        "investigacao": investigacao,
        "outros": outros,
    }

    # Remove acentos de todos os valores de texto no dicionário final de forma recursiva.
    return remover_acentos_recursivo(resultado)


def get_redcap_filas():
    """
    Busca no banco de dados todas as notificações de Toxoplasmose Congênita que estão com
    status 'pendente', formata-as e as retorna em uma lista.

    Returns:
        list: Uma lista de dicionários (OrderedDict), onde cada dicionário
              representa uma notificação formatada e pronta para processamento.
    """
    conn = get_connection()
    cur = conn.cursor()

    # Query SQL para selecionar os detalhes das notificações pendentes.
    # Junta as tabelas 'rpa_notificacoes' e 'rpa_notificacao_detalhes'
    # para obter todos os campos e valores de cada notificação.
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

    # Dicionário para agrupar os dados por 'record' (ID da notificação no REDCap).
    filas = {}
    for record, num_notificacao, status, field, value in rows:
        # Se o 'record' ainda não está no dicionário, inicializa sua estrutura.
        if record not in filas:
            filas[record] = {
                "num_notificacao": num_notificacao,
                "status": status,
                "dados": {},
            }
        # Adiciona o campo e o valor aos 'dados' do respectivo 'record'.
        filas[record]["dados"][field] = value

    # Lista para armazenar as notificações formatadas.
    filas_formatadas = []
    # Itera sobre cada notificação agrupada.
    for fila in filas.values():
        # Chama a função de mapeamento para transformar os dados brutos.
        dado_formatado = map_to_rpa_format(fila["dados"])

        # Cria um OrderedDict para garantir a ordem das chaves no resultado final.
        ordenado = OrderedDict()
        ordenado["agravo"] = dado_formatado["agravo"]
        ordenado["num_notificacao"] = fila["num_notificacao"]
        ordenado["status"] = fila["status"]
        ordenado["notificacao"] = dado_formatado["notificacao"]
        ordenado["investigacao"] = dado_formatado["investigacao"]
        ordenado["outros"] = dado_formatado["outros"]

        # Adiciona a notificação formatada e ordenada à lista final.
        filas_formatadas.append(ordenado)

    return filas_formatadas
