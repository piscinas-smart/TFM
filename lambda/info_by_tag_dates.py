import os
import boto3
import json
from boto3.dynamodb.conditions import Key
from decimal import Decimal

def get_info_by_tag_dates(TagID, f_ini, f_fin, all_records=False):

    TABLA = os.getenv("TABLE_NAME")
    INDEX = os.getenv("INDEX_NAME")

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLA)

    fecha, registros = "", []

    if f_ini == "" and f_fin == "":
        res = table.query(
            IndexName=INDEX,
            KeyConditionExpression=Key("TagID").eq(TagID)
        )
    elif f_fin == "":
        res = table.query(
            IndexName=INDEX,
            KeyConditionExpression=Key("TagID").eq(TagID) & Key("fecha").gte(f_ini)
        )
    elif f_ini == "":
        res = table.query(
            IndexName=INDEX,
            KeyConditionExpression=Key("TagID").eq(TagID) & Key("fecha").lte(f_fin)
        )
    else:
        res = table.query(
            IndexName=INDEX,
            KeyConditionExpression=Key("TagID").eq(TagID) & Key("fecha").between(f_ini, f_fin)
        )

    for item in res['Items']:

        if fecha != item["fecha"]:

            if fecha != "": # Si tenemos fecha, guardamos el registro nuevo

                registro = {
                    "TagID": TagID,
                    "fecha": fecha,
                    "largos": int(largos),
                    "segundos": int(segundos),
                    "metros": int(metros),
                    "medidas": int(medidas),
                    "estilos": estilos
                }

                if all_records:
                    registro["registros"] = subregistros

                registros.append(registro)

            fecha = item["fecha"]
            largos = 1
            segundos = int(item["segundos"])
            metros = float(item["metros"])
            medidas = int(item["medidas"])

            estilos = {}
            estilos[item["estilo"]] = 1

            subregistros = [item]

        else:

            # Misma fecha. Incrementamos contadores

            largos += 1

            segundos += int(item["segundos"])
            metros += float(item["metros"])
            medidas += int(item["medidas"])

            if item["estilo"] in estilos:
                estilos[item["estilo"]] += 1
            else:
                estilos[item["estilo"]] = 1

            subregistros.append(item)

    if fecha != "": # Guardaos los datos del último registro

        registro = {
            "TagID": TagID,
            "fecha": fecha,
            "largos": int(largos),
            "segundos": int(segundos),
            "metros": int(metros),
            "medidas": int(medidas),
            "estilos": estilos
        }

        if all_records:
            registro["registros"] = subregistros

        registros.append(registro)

    if len(registros) == 0:
        return None
    else:
        return {
            "registros": registros
        }

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    
    print(type(event["body"]), event["body"])

    body = json.loads(event["body"])

    if not "f_ini" in body:
        f_ini = ""
    else:
        f_ini = body["f_ini"]

    if not "f_fin" in body:
        f_fin = ""
    else:
        f_fin = body["f_fin"]

    if not "TagID" in body:
        return {
            "statusCode": 400,
            "body": "Se debe indicar TagID en la consulta"
        }
    else:
        TagID = body["TagID"]

    if not "all_records" in body:
        all_records = False
    else:
        all_records = body["all_records"]

    respuesta = get_info_by_tag_dates(TagID, f_ini, f_fin, all_records)

    if respuesta == None:
        return {
            "statusCode": 400,
            "body": "No hay ningún registro para los parámetros proporcionados"
        }
    else:
        return {
            "statusCode": 200,
            "body": json.dumps(respuesta, default=decimal_default)
        }