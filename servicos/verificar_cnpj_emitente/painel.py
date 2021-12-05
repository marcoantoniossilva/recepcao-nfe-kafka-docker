from kafka import KafkaClient, TopicPartition, consumer
from time import sleep
import json

from kafka.consumer.group import KafkaConsumer

painel_de_verificar_cnpj_emitente = KafkaConsumer(
    bootstrap_servers = ["kafka:29092"],
    api_version = (0, 10, 1),

    auto_offset_reset = "earliest",
    consumer_timeout_ms=1000)

particao = TopicPartition("verificar_cnpj_emitente", 0)
painel_de_verificar_cnpj_emitente.assign([particao])

painel_de_verificar_cnpj_emitente.seek_to_beginning(particao)
offset = 0
while True:
    print("esperando CNPJs para conferir...")

    for nfe in painel_de_verificar_cnpj_emitente:
        offset = nfe.offset + 1

        dados_da_nfe = json.loads(nfe.value)
        print("dados da NFE consultada: ", dados_da_nfe)

    painel_de_verificar_cnpj_emitente.seek(particao, offset)

    sleep(5)

# painel_de_verificar_cnpj_emitente.close()