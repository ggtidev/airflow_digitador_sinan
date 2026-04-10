from unidades.dicionario_unidades import lista_original

def criar_dicionario():
    itens = lista_original.split(" | ")
    dicionario = {}
    for item in itens:
        partes = item.split(", ", 1)
        id_num = int(partes[0])
        nome = partes[1]
        dicionario[id_num] = nome
    return dicionario

# Criar o dicionário
unidades = criar_dicionario()

def buscar_estabelecimento(id_chave):
    if id_chave in unidades:
        return unidades[id_chave]
    else:
        return f"ID {id_chave} não encontrado. IDs disponíveis: {list(unidades.keys())}"

def listar_todas_unidades():
    return unidades

# Exemplo de uso (pode ser removido se não precisar)
if __name__ == "__main__":
    # print("Testando o sistema de busca de unidades:")
    # print("="*50)
    
    # Testes
    print(buscar_estabelecimento(314))
    # print(f"ID 3: {buscar_estabelecimento(3)}")
    # print(f"ID 10: {buscar_estabelecimento(10)}")
    
    # print("\nTodas as unidades:")
    # for id_num, nome in listar_todas_unidades().items():
    #     print(f"ID {id_num}: {nome}")