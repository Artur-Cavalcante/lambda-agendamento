import os
import uuid
import boto3
import json
import pickle

from aws_lambda_powertools import Logger

class ScheduleService():
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.url_fila_agendamento = os.environ["url_fila_agendamento"]
        self.bucket_name = "bucket-agendamentos-fiap"
        self.sqs_client = boto3.client("sqs")
        self.s3_client = boto3.client('s3')
    
    def solicitar_agendamento(self, agendamento) -> str:
        self.logger.info(f'Iniciando agendamento {agendamento}')
        id = str(uuid.uuid4())

        json_agendamento = {
            'id': id,
            'horario': agendamento["horario"],
            'crm_medico': agendamento["crm_medico"],
            'cpf_paciente': agendamento["cpf_paciente"],
            'status_agendamento': "EmAnalise",
            'nome_paciente': agendamento["nome_paciente"],
            'nome_medico': agendamento["nome_medico"],
            'email_paciente': agendamento["email_paciente"],
            'email_medico': agendamento["email_medico"]
        }

        self.logger.info(f'JSON Agendamento {json_agendamento}')

        pickled_obj = pickle.dumps(json_agendamento)

        self.logger.info(f'Iniciando put object s3')
        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"{id}.pkl",
            Body=pickled_obj
        )
        self.logger.info(f'Finalizado put object s3')
        
        self.logger.info(f"Enviando solicitação agendamento para fila de agendamento.")
        self.sqs_client.send_message(QueueUrl=self.url_fila_agendamento, MessageBody=json.dumps(json_agendamento))
        self.logger.info(f"Finalizado envio solicitação agendamento para fila de agendamento.")

        return id


    def verificar_status_agendamento(self, agendamento: dict) -> str:
        try:
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=f"{agendamento["id"]}.pkl"
            )
            conteudo = response['Body'].read().decode('utf-8')
            result = json.loads(conteudo)
            self.logger.info(f'RESULT arquivo {result}')
            return result["status_agendamento"]
        except Exception as e:
            self.logger.error('Ocorreu error ao verificar status agendamento')
            if(e.response["ResponseMetadata"]["HTTPStatusCode"] == 404):
                self.logger.info(f"Não foi encontrado agendamento com id {agendamento['id']}")
                return f"Não foi encontrado agendamento com id {agendamento['id']}"
            else:
                self.logger.error(f'Erro ao ler o arquivo {agendamento["id"]} do S3: {str(e)}')
                raise