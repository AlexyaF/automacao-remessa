from datetime import datetime, timedelta
import sql.queries as sql
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

#Identifica o ciclo da remessa de acordo com a data que o executavel for acionado
def identificarCiclo():
    hoje = datetime.today()
    ano_atual = datetime.now().year
    mes_atual = datetime.now().month
    data_fim_ciclo1 = datetime(ano_atual,mes_atual,6)
    data_fim_ciclo2= datetime(ano_atual,mes_atual,21)
    if data_fim_ciclo1 <= hoje <= (data_fim_ciclo1 + timedelta(days=5)):
        ciclo = 1
    elif data_fim_ciclo2 <= hoje <= (data_fim_ciclo2 + timedelta(days=5)):
        ciclo = 2
    else:
        ciclo = int(input("Ciclo nao identificado, qual ciclo deseja processar: "))
    return ciclo


def conexao_banco_op():
    config = {
        'host':os.getenv('HOST_OP'),
        'user':os.getenv('USER_OP'),
        'password':os.getenv('PASS_OP'),
        'database':os.getenv('DB_OP')   
    }
    try:
        conexao = mysql.connector.connect(**config)
        return conexao
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco de dados: {err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


def conexao_banco_rbm():
    config = {
        'host':os.getenv('HOST_RBM'),
        'user':os.getenv('USER_RBM'),
        'password':os.getenv('PASS_RBM'),
        'database':os.getenv('DB_RBM'),
        'port':os.getenv('PORT_RBM')
    }

    try:
        conexao = mysql.connector.connect(**config)
        print('conexão estabelecida')
        return conexao
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco de dados: {err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")



def consulta_titularidade_ciclo2():
    #1° conexao
    conexao_op = conexao_banco_op()
    cursor_op = conexao_op.cursor()
    cursor_op.execute(sql.consulta_titularidade)
    uc = cursor_op.fetchall()

    #Formatando para lista
    uc_valores = [str(item[0]) for item in uc]
    print(uc_valores)
    # Criando os placeholders dinamicamente
    placeholders = ', '.join(['%s'] * len(uc_valores)) # Exemplo: "%s, %s, %s"
    # Substituindo no SQL
    query = sql.consulta_op.replace("IN (%s)", f"IN ({placeholders})")


    #2° conexao
    conexao_rbm = conexao_banco_rbm()
    cursor_rbm = conexao_rbm.cursor()
    cursor_rbm.execute(query, tuple(uc_valores))
    op = cursor_rbm.fetchall()
    print(op)
   