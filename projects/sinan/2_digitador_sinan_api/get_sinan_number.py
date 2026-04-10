import requests
import sys

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

if __name__ == "__main__":
    result = get_numero_sinan()
    if result is not None:
        print(str(result))
