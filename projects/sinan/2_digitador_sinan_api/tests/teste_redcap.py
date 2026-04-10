import requests
import json

campos = ['uf_notif_vio', 'bairro_vio', 'motiv_vio']

def get_metadata_por_campo(campo):
    data = {
        'token': '15A28A0BA2CFF32380E87F2003FDA610',
        'content': 'metadata',
        'format': 'json',
        'returnFormat': 'json',
        'fields[0]': campo
    }

    r = requests.post('https://redcap.recife.pe.gov.br/api/', data=data)
    if r.status_code == 200:
        return r.json()
    else:
        print(f"Erro ao buscar campo {campo}: {r.status_code}")
        return []

todos_campos = []
for c in campos:
    resultado = get_metadata_por_campo(c)
    todos_campos.extend(resultado)

for campo in todos_campos:
    print(json.dumps(campo, indent=2, ensure_ascii=False))
