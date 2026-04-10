# Importa a função necessária para obter uma conexão com o banco de dados.
# Este módulo 'database' deve conter a lógica para se conectar ao seu banco de dados (ex: PostgreSQL, MySQL, etc.).
from database import get_connection

def atualizar_status(num_notificacao: str, novo_status: str, erro_pergunta: str = None):
    """
    Atualiza o status de uma notificação específica no banco de dados.
    Opcionalmente, grava o campo/pergunta onde ocorreu o erro.
    """
    conn = get_connection()
    cur = conn.cursor()

    try:
        if erro_pergunta:
            # Atualiza status E erro_pergunta
            cur.execute(
                "UPDATE rpa_notificacoes SET status = %s, erro_pergunta = %s WHERE num_notificacao = %s",
                (novo_status, erro_pergunta, num_notificacao)
            )
        else:
            # Atualiza apenas o status
            cur.execute(
                "UPDATE rpa_notificacoes SET status = %s WHERE num_notificacao = %s",
                (novo_status, num_notificacao)
            )

        if cur.rowcount > 0:
            conn.commit()
            return {"mensagem": f"Status da notificação {num_notificacao} atualizado para '{novo_status}'."}
        else:
            conn.rollback()
            return {"erro": f"Notificação {num_notificacao} não encontrada."}
            
    except Exception as e:
        conn.rollback()
        return {"erro": f"Erro ao atualizar status da notificação {num_notificacao}: {e}"}
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

# A função obter_status permanece inalterada, pois não é a causa do problema de atualização.

def obter_status(num_notificacao: str):
    """
    Obtém o status atual de uma notificação específica do banco de dados.

    Args:
        num_notificacao (str): O número identificador da notificação a ser consultada.

    Returns:
        dict: Um dicionário com o número da notificação e seu status, ou uma mensagem de erro.
    """
    # Obtém uma nova conexão com o banco de dados.
    conn = get_connection()
    # Cria um cursor para executar comandos SQL.
    cur = conn.cursor()

    try:
        # Executa o comando SQL SELECT para buscar o status da notificação.
        # A cláusula WHERE especifica qual notificação buscar.
        cur.execute(
            "SELECT status FROM rpa_notificacoes WHERE num_notificacao = %s",
            (num_notificacao,)  # A vírgula é importante para que o Python entenda como uma tupla.
        )

        # 'fetchone()' recupera a próxima linha do resultado da consulta.
        # Se nenhuma linha for encontrada, retorna None.
        resultado = cur.fetchone()

        # Verifica se um resultado foi encontrado.
        if resultado:
            # Se encontrou, retorna um dicionário com os dados da notificação.
            # resultado[0] contém o valor da primeira coluna selecionada (neste caso, 'status').
            return {"num_notificacao": num_notificacao, "status": resultado[0]}
        
        # Se 'resultado' for None, a notificação não existe.
        return {"erro": f"Notificação {num_notificacao} não encontrada."}

    finally:
        # Garante que o cursor e a conexão sejam sempre fechados após o uso.
        if cur:
            cur.close()
        if conn:
            conn.close()