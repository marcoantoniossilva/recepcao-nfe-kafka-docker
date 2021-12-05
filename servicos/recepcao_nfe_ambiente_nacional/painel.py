from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_recepcao_nfe_ambiente_nacional = KafkaConsumer(
    bootstrap_servers = ["kafka:29092"],
    api_version = (0, 10, 1),

    auto_offset_reset = "earliest",
    consumer_timeout_ms=1000)

particao = TopicPartition("recepcao_nfe_ambiente_nacional", 0)
painel_de_recepcao_nfe_ambiente_nacional.assign([particao])

painel_de_recepcao_nfe_ambiente_nacional.seek_to_beginning(particao)
offset = 0
while True:
    print("esperando NFEs no ambiente nacional...")

    for recepcao_nfe_ambiente_nacional in painel_de_recepcao_nfe_ambiente_nacional:
        offset = recepcao_nfe_ambiente_nacional.offset + 1

        dados_do_recepcao_nfe_ambiente_nacional = json.loads(recepcao_nfe_ambiente_nacional.value)
        print("dados da NFE no ambiente nacional: ", dados_do_recepcao_nfe_ambiente_nacional)

    painel_de_recepcao_nfe_ambiente_nacional.seek(particao, offset)

    sleep(5)

# painel_de_recepcao_nfe_ambiente_nacional.close()