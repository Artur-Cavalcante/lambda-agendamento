import json
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.data_classes import event_source, APIGatewayProxyEvent

from src.schedule_service import ScheduleService

logger = Logger(service="agendamento") 
schedule_service = ScheduleService(logger)

handlers = {
    ("POST", "/solicitar_agendamento"): schedule_service.solicitar_agendamento,
    ("GET", "/verificar_status_agendamento"): schedule_service.verificar_status_agendamento
}

@event_source(data_class=APIGatewayProxyEvent)
def lambda_handler(event: APIGatewayProxyEvent, context) -> dict:
    try:
        print(event)
        request = (event.http_method, event.path)
        if request in handlers:
            logger.info(f"Event: {event.body}")
            method = handlers[request]
            response = method(json.loads(event.body))
            return response
        else:
            return {
                "status_code": 404,
                "body": "Método/Rota não suportado"
            }
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        return {
            "status_code": 500,
            "body": f"An error occurred: {str(e)}"
        }


# event = {
#     "httpMethod": "POST",
#     "path": "/deletar_dados_cliente",
#     "body": json.dumps({
#         "crm_medico": "12345",
#         "cpf_paciente": "12345678955",
#         "nome_paciente": "Agamenon Silva"
#         "email_paciente": "gestaopedidos15@gmail.com",
#         "email_medico": "arturcavalcante2014.pp@gmail.com"
#         "horario": "2024-04-04T23:23:23"
#     })
# }