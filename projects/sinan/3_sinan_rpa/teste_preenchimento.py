import os
from utils import load_json
from agravosscripts.violencia import executar_violencia

def main():
    dados_json = load_json("data.json")
    fila = dados_json.get("fila", [])

    os.environ["TOTAL_ITENS_FILA"] = str(len(fila))

    for idx, item in enumerate(fila, start=1):
        print(f"Iniciando teste do preenchimento - item {idx}")
        executar_violencia(item)
        print(f"Preenchimento do item {idx} concluído com sucesso.\n")

if __name__ == "__main__":
    main()
