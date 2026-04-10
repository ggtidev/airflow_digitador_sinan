import requests
import csv
import json
import time

# # Autenticação da API
# API_USERNAME=sevs_user
# API_PASSWORD=YrQKvg82DKfLooYT

# Marca o início do processo
inicio = time.time()

# URL e token de autorização
url = "https://vigilanciaemsaude.recife.pe.gov.br/api-sinan/unidade?descricao=imip"
auth = ("sevs_user", "YrQKvg82DKfLooYT")
headers = {}

print("⏳ Iniciando requisição ao endpoint...")

# Fazendo a requisição
response = requests.get(url, auth=auth, headers=headers)

# Verificando o status da resposta
if response.status_code == 200:
    print("✅ Requisição bem-sucedida! Processando dados...")

    data = response.json()

    # Verifica se o retorno é uma lista de dicionários
    if isinstance(data, list):
        keys = data[0].keys()
        with open("saida.csv", mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)
        print(f"📁 {len(data)} registros salvos em 'saida.csv'.")

    # Caso o retorno seja um dicionário com outro nível interno
    elif isinstance(data, dict):
        for value in data.values():
            if isinstance(value, list) and all(isinstance(item, dict) for item in value):
                keys = value[0].keys()
                with open("saida.csv", mode="w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=keys)
                    writer.writeheader()
                    writer.writerows(value)
                print(f"📁 {len(value)} registros salvos em 'saida.csv'.")
                break
        else:
            with open("saida.json", "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print("⚠️ Retorno não estruturado em lista. Salvo em 'saida.json'.")
    else:
        print("⚠️ Estrutura de dados inesperada.")
else:
    print(f"❌ Erro {response.status_code}: {response.text}")

# Calcula o tempo total
fim = time.time()
duracao = fim - inicio
minutos = int(duracao // 60)
segundos = duracao % 60

print(f"🕒 Processo concluído em {minutos} minuto(s) e {segundos:.2f} segundo(s).")
