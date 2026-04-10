import logging
import os
import datetime

# --- Lógica para Adicionar Divisor de Data ---

# Define o caminho para o arquivo de log
log_path = os.path.join(os.path.dirname(__file__), 'rpa_log.txt')

# Pega a data de hoje no formato YYYY-MM-DD
hoje = datetime.date.today().strftime('%Y-%m-%d')

# Variável para decidir se o divisor é necessário
precisa_de_divisor = False

# Verifica se o arquivo de log não existe
if not os.path.exists(log_path):
    precisa_de_divisor = True
else:
    # Se o arquivo existe, lê a última linha para checar a data
    with open(log_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        # Se o arquivo estiver vazio, precisa do divisor
        if not lines:
            precisa_de_divisor = True
        else:
            # Pega a última linha que não esteja em branco
            last_line = lines[-1].strip()
            if last_line:
                # Extrai a data (os 10 primeiros caracteres: YYYY-MM-DD)
                ultimo_log_data = last_line[:10]
                # Compara com a data de hoje
                if ultimo_log_data != hoje:
                    precisa_de_divisor = True

# Se for um novo dia ou um novo arquivo, escreve o cabeçalho
if precisa_de_divisor:
    with open(log_path, 'a', encoding='utf-8') as f:
        divisor = f"\n{'-'*25} LOGS DO DIA {hoje} {'-'*25}\n\n"
        f.write(divisor)


# --- Configuração Padrão do Logger (como antes) ---

logging.basicConfig(
    filename=log_path,
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8'  # Boa prática para evitar problemas com acentos
)

# --- Funções para chamar o log (como antes) ---

def log_info(mensagem):
    """Registra uma mensagem de informação no log e no console."""
    print(mensagem)
    logging.info(mensagem)

def log_erro(mensagem):
    """Registra uma mensagem de erro no log e no console."""
    print(mensagem)
    logging.error(mensagem)

def log_debug(mensagem):
    """Registra uma mensagem de debug no log e no console."""
    print(mensagem)
    logging.debug(mensagem)