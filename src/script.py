import functions.funcoes as f

def main():
    ciclo = 2
    print('CICLO:', ciclo)
    
    if ciclo == 2:
        motivo75 = f.consulta_titularidade_ciclo2()
        f.envio_cancelamento(motivo75)

if __name__ == "__main__":
    main()