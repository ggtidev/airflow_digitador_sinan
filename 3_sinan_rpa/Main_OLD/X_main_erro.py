import time
from api_client import buscar_filas
from agravosscripts.violencia import executar_violencia
from logger import log_info, log_erro
from utils import start_stop_listener, stop_requested # Importações atualizadas
from dotenv import load_dotenv # <--- ADICIONE ESTA LINHA   

load_dotenv() # <--- ADICIONE ESTA LINHA PARA CARREGAR O .env

AGRAVOS_DISPONIVEIS = {
    "VIOLENCIA_INTERPESSOAL_AUTOPROVOCADA": executar_violencia
    # Adicione outros agravos e suas funções correspondentes aqui
}

def processar_fila(filas):
    total = len(filas)
    log_info(f"Iniciando processamento da fila com {total} itens.")

    for idx, item in enumerate(filas, start=1):
        # Verifica se o usuário pediu para parar a automação
        if stop_requested():
            log_info("Processamento interrompido pelo usuário.")
            break

        nome_agravo = item.get("agravo")
        log_info(f"Iniciando item {idx}/{total} - Agravo: {nome_agravo}")

        executar_script = AGRAVOS_DISPONIVEIS.get(nome_agravo)

        if executar_script:
            try:
                # Determina se a sessão do SINAN deve ser reaproveitada
                reaproveitar = idx > 1
                tem_proxima = idx < total
                
                # Executa o script do agravo
                executar_script(item, reaproveitar_sessao=reaproveitar, tem_proxima=tem_proxima)
                
                log_info(f"Item {idx} processado com sucesso!")
            except Exception as e:
                log_erro(f"Erro crítico ao processar item {idx}: {e}")
        else:
            log_erro(f"Agravo '{nome_agravo}' não implementado.")

        time.sleep(5)

def main():
    # Inicia o listener do atalho de parada (Ctrl+Esc)
    start_stop_listener()

    filas = buscar_filas()
    if filas:
        processar_fila(filas)
    else:
        log_info("Nenhum item encontrado na fila para processamento.")
    
    log_info("Processamento da fila finalizado.")

if __name__ == "__main__":
    main()