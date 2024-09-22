import os
import uuid
import boto3
import json
import pickle

from aws_lambda_powertools import Logger

from src.agendamento_status_enum import AgendamentoStatus

class ScheduleService():
    def __init__(self, logger: Logger) -> None:
        self.logger = logger
        self.url_fila_agendamento = os.environ["url_fila_agendamento"]
        self.bucket_name = "bucket-agendamentos-fiap"
        self.sqs_client = boto3.client("sqs")
        self.s3_client = boto3.client('s3')
    
    def solicitar_agendamento(self, agendamento: dict) -> str:
        id = str(uuid.uuid4())  
        horario: str = agendamento["horario"]   
        crm_medico: str = agendamento["crm_medico"]
        cpf_paciente: str = agendamento["cpf_paciente"]
        status_agendamento = AgendamentoStatus.EmAnalise
        nome_paciente: str = agendamento["nome_paciente"],
        email_paciente: str = agendamento["email_paciente"],
        email_medico: str = agendamento["email_medico"]

        json_agendamento = {
            id: id,
            horario: horario,
            crm_medico: crm_medico,
            cpf_paciente: cpf_paciente,
            status_agendamento: status_agendamento,
            nome_paciente: nome_paciente,
            email_paciente: email_paciente,
            email_medico: email_medico
        }

        pickled_obj = pickle.dumps(json_agendamento)

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=f"{id}.pkl",
            Body=pickled_obj
        )
        
        self.logger.info(f"Enviando solicitação agendamento para fila de agendamento.")
        self.sqs_client.send_message(QueueUrl=self.url_fila_agendamento, MessageBody=json.dumps(json_agendamento))
        self.logger.info(f"Finalizado envio solicitação agendamento para fila de agendamento.")

        return id


    def verificar_status_agendamento(self, agendamento: dict) -> int:
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
            self.logger.info(e.response)
            self.logger.info(e)
            if e.response['Error']['Code'] == '404':
                return "Não encontrado"
            else:
                self.logger.error(f'Erro ao ler o arquivo {agendamento["id"]} do S3: {str(e)}')
                raise