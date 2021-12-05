from flask_apscheduler import APScheduler

from kafka import KafkaClient, KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from time import sleep
import smtplib
import ssl
from email.message import EmailMessage
import faker
import json

PROCESSO = "envio_de_email"
PROCESSO_DE_RECEPCAO_NFE_AMBIENTE_NACIONAL = "recepcao_nfe_ambiente_nacional"
EMAIL = "XXXXX"
PASSWORD = "XXXXX"
MAIL_SERVER = "smtp.gmail.com"
PORT = 465


def iniciar():
    global offset
    offset = 0

    cliente = KafkaClient(
        bootstrap_servers=["kafka:29092"],
        api_version=(0, 10, 1)
    )
    cliente.add_topic(PROCESSO)
    cliente.close()

BANCO_RECEITA_FEDERAL = "/workdir/banco_receita_federal.json"

def pegar_email_cliente(cpf_cnpj_cliente):
    email_cliente,nome = "",""
    with open(BANCO_RECEITA_FEDERAL, "r") as arquivo_bd_receita:
        banco = json.load(arquivo_bd_receita)
        inscritos = banco["inscritos"]
        for inscrito in inscritos:
            if str(inscrito["cnpj"]) == str(cpf_cnpj_cliente):
                email_cliente = inscrito["email"]
                nome = inscrito["razao_social"]
                break
        
        pessoas_fisicas = banco["pessoas_fisicas"]
        for pessoa in pessoas_fisicas:
            if str(pessoa["cpf"]) == str(cpf_cnpj_cliente):
                email_cliente = pessoa["email"]
                nome = pessoa["nome"]
                break

        arquivo_bd_receita.close()
    

    return email_cliente, nome


def validar_envio_de_email(nfe):
    valida, sucesso, email, nome,mensagem, mensagem_de_entrega = (nfe["sucesso"] == 1), False,"", "", "", ""

    if valida:
        mensagem = "Envio de email autorizado pelo ambiente nacional para a nota com chave de acesso: " + \
            nfe["chave_de_acesso"]
        email,nome = pegar_email_cliente(nfe["cpf_cnpj_cliente"])

        gerador_de_dados_falsos = faker.Faker("pt-BR")
        mensagem_de_entrega = "Prezado(a), " + nome + ", foi emitida uma NFE em seu nome pela empresa "+nfe["razao_social"]+", chave de acesso: "+nfe["chave_de_acesso"]+". A nota está disponível no link abaixo:\\" + gerador_de_dados_falsos.url()
        sucesso = enviar_email(email,mensagem_de_entrega)
        if sucesso:
            print("Email enviado com sucesso para: " + email)
        else:
            print("Não foi possível enviar o email para: " + email)
    else:
        mensagem = "Envio de email não autorizada pelo ambiente nacional!"

    return valida, mensagem, mensagem_de_entrega

def enviar_email(endereco_email,mensagem):
    sucesso = True

    email = EmailMessage()
    email.set_content(mensagem)
    email["Subject"] = "NFE emitida em seu nome"
    email["From"] = EMAIL
    email["To"] = endereco_email

    try:
        server = smtplib.SMTP_SSL(MAIL_SERVER, PORT)

        server.login(EMAIL, PASSWORD)
        server.sendmail(EMAIL, endereco_email, email.as_string())
        print(email)

        server.quit()
    except Exception as erro:
        sucesso, mensagem = False, "não foi possível enviar o e-mail, erro: " + str(erro)
        # print(e)

    return sucesso, mensagem

def executar():
    global offset
    resultado = "ok"

    consumidor_ambiente_nacional = KafkaConsumer(
        bootstrap_servers=["kafka:29092"],
        auto_offset_reset='earliest',
        consumer_timeout_ms=1000,
        api_version=(0, 10, 1))
    particao = TopicPartition(PROCESSO_DE_RECEPCAO_NFE_AMBIENTE_NACIONAL, 0)
    consumidor_ambiente_nacional.assign([particao])
    consumidor_ambiente_nacional.seek(particao, offset)

    for recepcao_nfe_ambiente_nacional in consumidor_ambiente_nacional:
        offset = recepcao_nfe_ambiente_nacional.offset + 1

        nfe_ambiente_nacional = recepcao_nfe_ambiente_nacional.value
        nfe_ambiente_nacional = json.loads(nfe_ambiente_nacional)

        valida, mensagem, mensagem_de_entrega = validar_envio_de_email(nfe_ambiente_nacional)
        sleep(4)

        if valida:
            nfe_ambiente_nacional["sucesso"] = 1
        else:
            nfe_ambiente_nacional["sucesso"] = 0
        nfe_ambiente_nacional["mensagem"] = mensagem
        nfe_ambiente_nacional["mensagem_de_entrega"] = mensagem_de_entrega

        try:
            produtor = KafkaProducer(
                bootstrap_servers=["kafka:29092"],
                api_version=(0, 10, 1)
            )
            produtor.send(topic=PROCESSO, value=json.dumps(
                nfe_ambiente_nacional
            ).encode("utf-8"))
        except KafkaError as erro:
            resultado = f"erro: {erro}"

    # deveria enviar para um log
    print(resultado)


if __name__ == "__main__":
    iniciar()

    agendador = APScheduler()
    agendador.add_job(id=PROCESSO, func=executar,
                      trigger="interval", seconds=3)
    agendador.start()

    while True:
        sleep(60)
