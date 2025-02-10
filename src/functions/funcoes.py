from datetime import datetime, timedelta
import src.sql.queries as sql
import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

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

def conexao_banco():
    config = mysql.connector.connect(
        host=os.getenv('HOST_OP'),
        user=os.getenv('USER_OP'),
        password=os.getenv('PASS_OP'),
        database=os.getenv('DB_OP')   
    )
    
    cursor = config.cursor()
    return cursor


def consulta_titularidade_ciclo2():
    cursor = conexao_banco()
    cursor.execute(sql.consulta_titularidade_elektro)
    resultado = cursor.fetchall()
    print(resultado)