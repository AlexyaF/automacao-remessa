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

def data_hora_atual():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
    solicitacao=[]
    for op in bloquear75:
        body = {
            'uc':f'{op[0]}',
            'operacao':f'{op[1]}',
            'ciaEletrica':op[2],
            'ciaEletricaComboBox':op[2],
            'motivo':'75'
        }
        motivo75.append(body)
        #dados inseridos na tabela de solicitação
        dados = {
            'uc':f'{op[0]}',
            'operacao':f'{op[1]}',
            'ciaEletrica':op[2],
            'motivo':'Rotina conuslta titularidade ELEKTRO'
        }
        solicitacao.append(dados)
    #inserir_banco(dados, "S")

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
    op_solicitada = [] #lista somente de operacoes enviadas
    op_com_erro=[] #lista casos com erros
    solicitado = [] #lista cosos enviados que vao direto para sucesso

    for op in motivo:
        #Dados de comparação - oq foi enviado X oq deu erro
        op_solicitada.append(op['operacao']) #apenas op
        body = json.dumps(op)
        op_geral = { #op, uc, e retorno ja que quando da sucesso nao retorna por operacao
            'operacao' : op.get("operacao"),
            'uc' : op.get("uc"),
            'request':body,
            'historico' : 'UC bloqueada com sucesso'
            }
        solicitado.append(op_geral)

    sucesso = []
    erro = []

    #Configurações de request e request
    body = json.dumps(motivo)
    url = 'https://api2.crefaz.com.br/sistema/cobranca/bloquearUcLote'
    header = get_token_header()

    def processar_retorno(response):
        if 'mensagemErro' in response: #se mensagem de erro pegar a operação com erro
            for item in response.get("tabelaLote", []):
                #buscar request em solicitados 
                body_correspondente = next(
                (s['request'] for s in solicitado if s['operacao'] == item.get("operacao")),
                None  # valor padrão se não encontrar
                )

                resposta = {
                'operacao' : item.get("operacao"),
                'uc' : item.get("uc"),
                'request': body_correspondente,
                'historico' : item.get("historico")
                }

                erro.append(resposta)
                #pegar diferença de casos cancelados e casos enviados
                op_com_erro.append(item.get("operacao"))
            op_sucesso = list(set(op_solicitada) - set(op_com_erro))
            dados_com_sucesso = [item for item in solicitado if item['operacao'] in op_sucesso] #puxar dados de solicitado só se ele estiver nas operacoes que deram erro
            sucesso.append(dados_com_sucesso)
        else:
            retorno = response.get('retorno') 
            if retorno == 'sucesso': # se retorno sucesso
                sucesso.append(solicitado) #pega os casos listados, como sucesso nao retorna caso a caso, pegar todos
            else:
                retorno = response.get('retorno')
                print('Retorno:', retorno, 'verificar casos enviados')

    try:
        response = requests.post(url=url, headers=header, data=body)
        #validando retorno
        if response.status_code == 200:
            response = response.json()
            processar_retorno(response)
        elif response.status_code == 400:
            header = get_token_header() #buscar novo token 
            response = requests.post(url=url, headers=header, data=body)
            if response.status_code == 200:
                response = response.json()
                processar_retorno(response)
        else:
            print('ERRO ENVIO CASOS:', response.status_code)
    except requests.exceptions.RequestException as e:
        print(f"Erro ao enviar requisição: {e}")


    print('SUCESSO:', sucesso)
    print('ERRO:', erro)

def inserir_banco(dados, fonte):
    conexao = conexao_banco_op()
    cursor = conexao.cursor()
    data_atual = data_hora_atual()
    if fonte == 'S':
        for op in dados:
            op = op[0]
            insert = 'INSERT INTO solicitacao_bloqueio (uc, operacao, motivo, cia, dt_solicitação) VALUES (%s, %s, %s, %s, %s)'
            valores = (op['uc'], op['operacao'], op['ciaEletrica'], op['motivo'], data_atual)
            cursor.execute(insert, valores)
    elif fonte == 'L':
        for op in dados:
            op = op[0]
            insert = 'INSERT INTO envio_log_bloqueio (uc, operacao, request, response, dt_envio) VALUES (%s, %s, %s, %s, %s)'
            valores = (op['uc'], op['operacao'], op['request'], op['historico'], data_atual)