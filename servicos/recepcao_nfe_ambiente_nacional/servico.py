from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import random
import json

PROCESSO = "recepcao_nfe_ambiente_nacional"
PROCESSO_DE_RECEPCAO_NFE = "verificar_cnpj_emitente"

def iniciar():
    global offset
    offset = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

def validar_recepcao_nfe_ambiente_nacional(informacoes_da_nfe):
    valido, mensagem = (informacoes_da_nfe["sucesso"] == 1), ""

    if valido:
        valido = (random.randint(1, 5) > 2)
        if valido:
            mensagem = "NFE recebida no ambiente nacional com sucesso, chave de acesso: " + informacoes_da_nfe["chave_de_acesso"]
        else:
            mensagem = "Erro ao receber NFE no ambiente nacional!"
    else:
        mensagem = "Emissão da NFE impossibilitada pelo CNPJ do emitente!"

    return valido, mensagem


def executar():
    global offset
    resultado = "ok"

    consumidor_de_verificacao_de_cnpj = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1),
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000)
    particao = TopicPartition(PROCESSO_DE_RECEPCAO_NFE, 0)
    consumidor_de_verificacao_de_cnpj.assign([particao])
    consumidor_de_verificacao_de_cnpj.seek(particao, offset)

    for cnpj_verificado in consumidor_de_verificacao_de_cnpj:
        offset = cnpj_verificado.offset + 1

        informacoes_da_nfe = cnpj_verificado.value
        informacoes_da_nfe = json.loads(informacoes_da_nfe)

        valido, mensagem = validar_recepcao_nfe_ambiente_nacional(informacoes_da_nfe)
        if valido:
            sleep(4)

            if valido:
                informacoes_da_nfe["sucesso"] = 1
            else:
                informacoes_da_nfe["sucesso"] = 0
            informacoes_da_nfe["mensagem"] = mensagem

            try:
                produtor = KafkaProducer(
                    bootstrap_servers=["kafka:29092"],
                    api_version=(0, 10, 1)
                )
                produtor.send(topic=PROCESSO, value=json.dumps(
                    informacoes_da_nfe).encode("utf-8"))
            except KafkaError as erro:
                resultado = f"erro: {erro}"


    # registrar em um log de operacoes
    print(resultado)


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,
                      trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
