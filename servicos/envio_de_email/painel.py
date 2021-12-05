from kafka import KafkaClient, TopicPartition, KafkaConsumer
from time import sleep
import json

painel_de_envio_de_email = KafkaConsumer(
    bootstrap_servers = ["kafka:29092"],
    api_version = (0, 10, 1),

    auto_offset_reset = "earliest",
    consumer_timeout_ms=1000)

particao = TopicPartition("envio_de_email", 0)
painel_de_envio_de_email.assign([particao])

painel_de_envio_de_email.seek_to_beginning(particao)
offset = 0
while True:
    print("esperando emails para enviar...")

    for envio_de_email in painel_de_envio_de_email:
        offset = envio_de_email.offset + 1

        dados_do_envio_de_email = json.loads(envio_de_email.value)
        print("dados do email: ", dados_do_envio_de_email)

    painel_de_envio_de_email.seek(particao, offset)

    sleep(5)

# painel_de_envio_de_email.close()