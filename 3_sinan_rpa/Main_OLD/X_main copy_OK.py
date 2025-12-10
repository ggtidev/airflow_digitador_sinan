import time
from api_client import buscar_filas
from agravosscripts.violencia import executar_violencia
from logger import log_info, log_erro

AGRAVOS_DISPONIVEIS = {
    "VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA": executar_violencia
    # Adicione outros agravos e suas funções correspondentes aqui
}

def processar_fila(filas):
    total = len(filas)
    log_info(f"Iniciando processamento da fila com {total} itens.")

    for idx, item in enumerate(filas, start=1):
        nome_agravo = item.get("agravo")
        log_info(f"Iniciando item {idx}/{total} - Agravo: {nome_agravo}")

        executar_script = AGRAVOS_DISPONIVEIS.get(nome_agravo)

        if executar_script:
            try:
                reaproveitar = idx > 1
                tem_proxima = idx < total    
                executar_script(item, reaproveitar_sessao=reaproveitar, tem_proxima=tem_proxima)
                log_info(f"Item {idx} processado com sucesso!")
            except Exception as e:
                log_erro(f"Erro ao processar item {idx}: {e}")
        else:
            log_erro(f"Agravo '{nome_agravo}' não implementado.")

        time.sleep(5)

def main():
    filas = buscar_filas()

    if filas:
        processar_fila(filas)
    else:
        log_erro("404 - Nenhum dado encontrado na fila.")

if __name__ == "__main__":
    main()
