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
        'host':os.getenv('DB_exemplo'),
        'user':os.getenv('DB_exemplo'),
        'password':os.getenv('DB_exemplo'),
        'database':os.getenv('DB_exemplo')   
    }
    
    conexao = mysql.connector.connect(**config)
    return conexao


def conexao_exemplo():
    config = {
        'host':os.getenv('DB_exemplo'),
        'user':os.getenv('DB_exemplo'),
        'password':os.getenv('DB_exemplo'),
        'database':os.getenv('DB_exemplo')   
    }

    conexao = mysql.connector.connect(**config)
    return conexao



def consulta_titularidade_ciclo2():
    #1° conexao
    conexao_op = conexao_banco_op()
    cursor_op = conexao_op.cursor()
    cursor_op.execute(sql.consulta_titularidade)
    uc = cursor_op.fetchall()

    #Formatando para lista
    uc_valores = [str(item[0]) for item in uc]
    ucs_final = ", ".join(uc_valores)

    #2° conexao
    conexao_exemplo = conexao_exemplo()
    cursor_exemplo = conexao_exemplo.cursor()
    cursor_exemplo.execute(sql.consulta_op, (ucs_final,))
    op = cursor_exemplo.fetchall()
    print(op)