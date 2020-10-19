import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from datetime import datetime

def get_info_by_date(fecha):

    TABLA = os.getenv("TABLE_NAME")
    INDEX = os.getenv("INDEX_NAME")

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLA)

    nadadores, largos, segundos, metros, medidas, estilos, registros = set(), 0, 0, 0, 0, {}, []

    res = table.query(
        IndexName=INDEX,
        KeyConditionExpression=Key('fecha').eq(fecha)
    )

    for item in res['Items']:

        largos += 1

        nadadores.add(item["TagID"])
        segundos += item["segundos"]
        metros += item["metros"]
        medidas += item["medidas"]

        if item["estilo"] in estilos:
            estilos[item["estilo"]] += 1
        else:
            estilos[item["estilo"]] = 1

        registros.append(item["TagID"])

    if largos == 0:
        return None
    else:
        return {
            "fecha": fecha,
            "nadadores": len(nadadores),
            "largos": int(largos),
            "segundos": int(segundos),
            "metros": int(metros),
            "medidas": int(medidas),
            "estilos": estilos,
            "tags": list(set(registros))
        }


def lambda_handler(event, context):
    
    body = json.loads(event["body"])
    
    if not "fecha" in body:
        return {
            "statusCode": 400,
            "body": "Se debe indicar la fecha en la consulta"
        }
    else:
        fecha = body["fecha"]
        
    respuesta = get_info_by_date(fecha)

    if respuesta == None:
        return {
            "statusCode": 400,
            "body": "No hay ningún registro en la fecha indicada"
        }
    else:
        return {
            "statusCode": 200,
            "body": json.dumps(respuesta)
        }