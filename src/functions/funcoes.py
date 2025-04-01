from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
import requests
import json
from sql.queries import * 

# Carrega as variáveis de ambiente do arquivo .env
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
        if conexao.is_connected():
            print("Conexão bem-sucedida!")
            return conexao
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco de dados: {err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


def conexao_banco_rbm():
    config = {
        'host':os.getenv('EX_HOST'),
        'user':os.getenv('EX_USER'),
        'password':os.getenv('EX_PASS'),
        'database':os.getenv('EX_DB'),
        'port':os.getenv('EX_PORT')
    }

    try:
        conexao = mysql.connector.connect(**config)
        if conexao.is_connected():
            print("Conexão bem-sucedida!")
            return conexao
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco de dados: {err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


# O ciclo dois conta com uma validação a mais por isso consulta casos especificos em uma query anterior a query principal
def consulta_titularidade_ciclo2():
    data_atual = datetime.now().strftime("%d-%m-%y")
    #1° conexao
    conexao_op = conexao_banco_op()
    cursor_op = conexao_op.cursor()
    cursor_op.execute(consulta_titularidade) #conusltando apenas casos especificos 
    uc = cursor_op.fetchall()

    #Formatando para lista
    uc_valores = [str(item[0]) for item in uc]
    # Criando os placeholders dinamicamente
    placeholders = ', '.join(['%s'] * len(uc_valores)) # Exemplo: "%s, %s, %s"
    # Substituindo no SQL
    query = consulta_op.replace("IN (%s)", f"IN ({placeholders})")

    #2° conexao
    conexao_rbm = conexao_banco_rbm()
    cursor_rbm = conexao_rbm.cursor()
    cursor_rbm.execute(query, tuple(uc_valores))
    bloquear75 = cursor_rbm.fetchall()
    colunas = ['Unidade Consumidora', 'Operacao', 'Cia','Motivo' ]
    df = pd.DataFrame(bloquear75, columns= colunas)
    path = r'\\10.44.250.4\M-Energia\Colaboradores\Alexya Silva\Scripts\RemessaNeo\arquivoBloqueio'
    nomeArquivo = ("CodMotivo75." + data_atual + ".csv") 
    allpath = os.path.join(path, nomeArquivo)
    df.to_csv(allpath, sep=";", index=False)


def get_token_api_cancelamento():
    url='https://api2-homolog.crefaz.com.br/tokenSistemaTerceiros'
    body = {
        'usuario':'T.Sistema',
        'senha':'teste'
    }

    body_str = json.dumps(body)
    print(body_str)

    response = requests.post(url=url, data=body_str)
    print(response)
    if response.status_code == 200:
        response = response.json()
        token = response.get('jwt')
        return token
    else:
        print('Erro')