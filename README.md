# recepcao-nfe-kafka-docker

Serviço de recepção de NFE em [Python](https://www.python.org/) conteinerizado em microsserviços [Docker](https://github.com/docker) para demostração da utilização de [Apache Kafka](https://kafka.apache.org/).

Fluxo de execução:

1 - Serviço de recepção de NFEs recebe um pedido de emissão de NFE;<br>
2 - Serviço de validação de CNPJ valida ou não o CNPJ do emitente;<br>
3 - Serviço de recepção do ambiente nacional identifica e salva a NFE quando o CNPJ é válido;<br>
4 - Serviço de envio de e-mails envia um e-mail para o destinatário informando da emissão de uma NFE em seu nome.<br>
<br/>

## Vídeo explicativo

Para acessar o vídeo explicativo da ferramenta no Youtube, [clique aqui](https://youtu.be/9WCN0O71KHQ).

<br/>

## Requisitos

- [Docker](https://docs.docker.com/)

<br/>

## 1 - Crie os contêineres com o docker-compose e os execute:

Na pasta do projeto execute:

```bash
docker-compose up --build
```

<br/>

## 2 - Faça uma chamada HTTP GET para o serviço de recepção:

Para iniciar a recepção da NFE, execute uma chamada GET para o endereço localhost:5001 com a seguinte estrutura: localhost:5001/executar/<cnpj_do_emitente>/<cpf_cnpj_do_destinatario>/<id_do_produto>/<quantidade_produto>.

Exemplo de execução: localhost:5001/executar/45698253000140/12345678901/1/1

<br/>

## OBS

As execuções de cada serviço podem ser acompanhados dentro de cada conteiner executando o script python "painel" com o código:

```bash
python3 painel.py
```
