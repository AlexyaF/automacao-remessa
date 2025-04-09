import functions.funcoes as f

ciclo = f.identificarCiclo()
print(ciclo)
if ciclo == 2:
    motivo75 = f.consulta_titularidade_ciclo2()
    f.envio_cancelamento(motivo75)


 