from kafka import KafkaClient, TopicPartition, consumer
from time import sleep
import json

from kafka.consumer.group import KafkaConsumer

painel_de_recepcao_de_nfe = KafkaConsumer(
    bootstrap_servers = ["kafka:29092"],
    api_version = (0, 10, 1),

    auto_offset_reset = "earliest",
    consumer_timeout_ms=1000)

particao = TopicPartition("recepcao_de_nfe", 0)
painel_de_recepcao_de_nfe.assign([particao])

painel_de_recepcao_de_nfe.seek_to_beginning(particao)
offset = 0
while True:
    print("esperando notas fiscais...")

    for nfe in painel_de_recepcao_de_nfe:
        offset = nfe.offset + 1

        dados_da_nfe = json.loads(nfe.value)
        print("dados da nfe a ser autorizada: ", dados_da_nfe)

    painel_de_recepcao_de_nfe.seek(particao, offset)

    sleep(5)

# painel_de_recepcao_de_nfe.close()