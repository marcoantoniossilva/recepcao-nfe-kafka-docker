from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import json
import random

PROCESSO = "verificar_cnpj_emitente"
PROCESSO_DE_RECEPCAO_DE_NFE = "recepcao_de_nfe"

def iniciar():
    global offset
    offset = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))
    cliente.add_topic(PROCESSO)
    cliente.close()


BANCO_RECEITA_FEDERAL = "/workdir/banco_receita_federal.json"
def validar_cnpj_emitente(recepcao_de_nfe):
    sucesso, razao_social,chave_de_acesso, mensagem = (recepcao_de_nfe["sucesso"] == 1), "","", ""
    if sucesso:
        with open(BANCO_RECEITA_FEDERAL, "r") as arquivo_bd_receita:
            banco = json.load(arquivo_bd_receita)
            inscritos = banco["inscritos"]
            for inscrito in inscritos:
                if str(inscrito["cnpj"]) == str(recepcao_de_nfe["cnpj_fornecedor"]):
                    razao_social = inscrito["razao_social"]
                    sucesso = inscrito["ativo"]
                    if sucesso:
                        COMPONENTES_CHAVE = (
                            recepcao_de_nfe["cnpj_fornecedor"], 
                            recepcao_de_nfe["cpf_cnpj_cliente"],
                            str("".join([str(random.randint(0, 9)) for _ in range(12)])))

                        chave_de_acesso = " ".join(COMPONENTES_CHAVE)
                        mensagem = "NFE validada com sucesso!"
                    else:
                        mensagem = "cnpj do emitente '"+razao_social+"' não está ativo"
                        sucesso  = False
                    break

            arquivo_bd_receita.close()
    else:
        mensagem = "NFE não emitida!"

    return sucesso, razao_social, mensagem, chave_de_acesso

def executar():
    global offset
    resultado = "ok"
    
    # recupera resultados do processo anterior
    consumidor_de_recepcao_de_pedidos = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        api_version=(0, 10, 1))
    particao = TopicPartition(PROCESSO_DE_RECEPCAO_DE_NFE, 0)
    consumidor_de_recepcao_de_pedidos.assign([particao])
    consumidor_de_recepcao_de_pedidos.seek(particao, offset)

    for nfe in consumidor_de_recepcao_de_pedidos:
        offset = nfe.offset + 1

        informacoes_da_nfe = json.loads(nfe.value)

        valida, razao_social, mensagem, chave_de_acesso = validar_cnpj_emitente(informacoes_da_nfe)
        if valida:
            # simula algum processamento atraves de espera ocupada
            sleep(4)

            informacoes_da_nfe["sucesso"] = 1
        else:
            informacoes_da_nfe["sucesso"] = 0

        informacoes_da_nfe["razao_social"] = razao_social
        informacoes_da_nfe["mensagem"] = mensagem
        informacoes_da_nfe["chave_de_acesso"] = chave_de_acesso

        try:
            produtor = KafkaProducer(
                bootstrap_servers=["kafka:29092"], api_version=(0, 10, 1))
            produtor.send(topic=PROCESSO, value=json.dumps(
                informacoes_da_nfe).encode("utf-8"))
        except KafkaError as erro:
            resultado = f"erro: {erro}"

    # o certo eh imprimir em (ou enviar para) um log de resultados
    print(resultado)


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,
                      trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)