import requests
import unicodedata
import sys
from datetime import datetime

def formatar_data(valor):
    try:
        if isinstance(valor, str):
            dt = datetime.strptime(valor[:10], "%Y-%m-%d")
            return dt.strftime("%d%m%Y")
        return valor
    except:
        return valor
    
def formatar_sexo(valor):
    if valor == "1":
        return "M"
    elif valor == "2":
        return "F"
    return "I"

def formatar_deficiencia(valor):
    return valor if valor in ["1", "2"] else "9"

def formatar_vio_trabalho(valor):
    return valor if valor in ["1", "2"] else "9"

def parse_select_choices(choices_str):
    mapa = {}
    pares = choices_str.split('|')
    for par in pares:
        if ',' in par:
            codigo, label = par.split(',', 1)
            mapa[codigo.strip()] = label.strip()
    return mapa

def get_labels_map():
    campos = [
        "uf_notif_vio",
        "bairro_vio",
        "bairro_ocor_vio",
        "mun_resid_vio_lista",
        "uf_resid_vio", 
        "uf_ocor_vio"
    ]

    labels_map = {}
    for campo in campos:
        data = {
            'token': '15A28A0BA2CFF32380E87F2003FDA610',
            'content': 'metadata',
            'format': 'json',
            'returnFormat': 'json',
            'fields[0]': campo
        }
        r = requests.post('https://redcap.recife.pe.gov.br/api/', data=data)
        if r.status_code == 200:
            metadata = r.json()
            if metadata and 'select_choices_or_calculations' in metadata[0]:
                labels_map[campo] = parse_select_choices(metadata[0]['select_choices_or_calculations'])
    return labels_map

import unicodedata

def remover_acentos(texto):
    if not isinstance(texto, str):
        return texto
    return ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')

def remover_acentos_recursivo(data):
    if isinstance(data, dict):
        return {k: remover_acentos_recursivo(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [remover_acentos_recursivo(item) for item in data]
    elif isinstance(data, str):
        return remover_acentos(data)
    else:
        return data

def get_numero_sinan():
    """
    Faz uma requisição GET para a API de margem Sinan e retorna o valor de 'numero_sinan'.
    """
    url = 'https://vigilanciaemsaude.recife.pe.gov.br/margem-sinan/numero_sinan'
    headers = {
        'accept': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        data = response.json()
        
        # Retorna apenas o valor de numero_sinan
        return data.get('numero_sinan')
        
    except requests.exceptions.RequestException as e:
        print(f"Erro na requisição: {e}", file=sys.stderr)
        return None
    except ValueError as e:
        print(f"Erro ao decodificar JSON: {e}", file=sys.stderr)
        return None
