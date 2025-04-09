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
    if data_fim_ciclo1 <= hoje <= (data_fim_ciclo1 + timedelta(days=5)): #Data de hoje entre dia 6 e 11
        ciclo = 1
    elif data_fim_ciclo2 <= hoje <= (data_fim_ciclo2 + timedelta(days=5)): #Data de hoje entre dia 21 e 26
        ciclo = 2
    else:
        ciclo = int(input("Você esta executando o script fora da data de envio!!!\nCiclo não identificado, qual ciclo deseja processar: "))
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
            return conexao
    except mysql.connector.Error as err:
        print(f"Erro ao conectar ao banco de dados: {err}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


# O ciclo dois conta com uma validação a mais por isso consulta casos especificos em uma query anterior a query principal
def consulta_titularidade_ciclo2():
    #1° conexao banco energia
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

    #2° conexao rbm
    conexao_rbm = conexao_banco_rbm()
    cursor_rbm = conexao_rbm.cursor()
    cursor_rbm.execute(query, tuple(uc_valores))
    bloquear75 = cursor_rbm.fetchall()
    #criando lista para salvar casos
    motivo75 = []
    for op in bloquear75:
        body = {
            'uc':f'{op[0]}',
            'operacao':f'{op[1]}',
            'ciaEletrica':op[2],
            'ciaEletricaComboBox':op[2],
            'motivo':'75'
        }
        motivo75.append(body)

    return motivo75


def get_token_header():
    url='https://api2.crefaz.com.br/tokenSistemaTerceiros'
    body = {
        'usuario':os.getenv('user'),
        'senha':os.getenv('pass')
    }

    body_str = json.dumps(body)
    try:
        response = requests.post(url=url, data=body_str)
        print(response)
        if response.status_code == 200:
            response = response.json()
            token = response.get('jwt')
            headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json" 
            }
            return headers
        else:
            print('Erro. Status code:', response.status_code)
    except requests.ConnectionError:
        print("Erro de conexão: Não foi possível conectar à API.")
    except requests.Timeout:
        print("Erro de timeout: A requisição demorou muito.")
    except requests.HTTPError as http_err:
        print(f"Erro HTTP {http_err.response.status_code}: {http_err.response.text}")
    except requests.RequestException as err:
        print(f"Erro na requisição: {err}")
    except Exception as err:
        print(f"Erro inesperado: {err}")


def envio_cancelamento(motivo):
    body = json.dumps(motivo)
    print(body)
    url = 'https://api2.crefaz.com.br/sistema/cobranca/bloquearUcLote'
    header = get_token_header()
    response = requests.post(url=url, headers=header, data=body)
    print(response)