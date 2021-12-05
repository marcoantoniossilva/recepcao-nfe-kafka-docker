from sys import api_version
from flask import Flask, jsonify

from kafka import KafkaClient, KafkaProducer
from kafka.errors import KafkaError

from time import sleep

import random
import json

servico = Flask(__name__)

PROCESSO = "recepcao_de_nfe"
DESCRICAO = "serviço para recepção de notas fiscais eletrônicas"
VERSAO = "0.0.1"


def iniciar():
    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1))
    cliente.add_topic(PROCESSO)
    cliente.close()


@servico.route("/info", methods=["GET"])
def get_info():
    return jsonify(descricao=DESCRICAO, versao=VERSAO)


@servico.route("/executar/<string:cnpj_fornecedor>/<string:cpf_cnpj_cliente>/<int:id_produto>/<int:quantidade>", methods=["POST", "GET"])
def executar(cnpj_fornecedor, cpf_cnpj_cliente,id_produto, quantidade):
    resultado = {
        "resultado": "sucesso",
        "numero": ""
    }

    # simula algum processamento atraves de espera ocupada
    sleep(4)

    NUMERO_NFE = random.randint(2000, 5000)

    try:
        produtor = KafkaProducer(
            bootstrap_servers=["kafka:29092"],
            api_version=(0, 10, 1))

        recepcao_de_nfe = {
            "numero": NUMERO_NFE,
            "sucesso": 1,
            "mensagem": "NFE recebida na SEFAZ",
            "cnpj_fornecedor": cnpj_fornecedor,
            "cpf_cnpj_cliente": cpf_cnpj_cliente,
            "id_produto": id_produto,
            "quantidade": quantidade
        }
        produtor.send(topic=PROCESSO, value=json.dumps(
            recepcao_de_nfe).encode("utf-8"))

        resultado["numero"] = NUMERO_NFE
    except KafkaError as erro:
        resultado["resultado"] = f"erro ocorrido durante a recepção da NFE: {erro}"

    return json.dumps(resultado).encode("utf-8")


if __name__ == "__main__":
    iniciar()

    servico.run(
        host="0.0.0.0",
        debug=True
    )
