import os
import sys
import boto3
import json
import time
import csv
import logging
from configparser import ConfigParser
from random import randint

from botocore.exceptions import ClientError

class Connectivity:
    """
        Clase para permitir enviar los datos on-premise al entorno Cloud.
    """

    _CONFIG_TEMPLATE = [
        {
            "section": "PARAMETERS",
            "name": "location_id",
            "type": "str"
        },
        {
            "section": "PARAMETERS",
            "name": "kinesis_stream",
            "type": "str"
        },
        {
            "section": "PARAMETERS",
            "name": "retries",
            "type": "int",
            "max": 5,
            "min": 0,
            "default": 5
        },
        {
            "section": "PARAMETERS",
            "name": "buffer",
            "type": "int",
            "max": 500,
            "min": 10,
            "default": 500
        },
        {
            "section": "PARAMETERS",
            "name": "region",
            "type": "str",
            "default": None
        },
        {
            "section": "LOGGER",
            "name": "level",
            "type": "str",
            "options": ["DEBUG", "INFO", "ERROR"],
            "default": "INFO"
        }
    ]

    _CONFIG = {}

    def _read_config(self):

        # Compruebo primero si el fichero existe

        if not os.path.exists("./config.ini"):
            raise BaseException("__init__: File 'config.ini' not found.")

        # Analizo los parámetros

        config_object = ConfigParser()
        config_object.read("./config.ini")

        for elemento in self._CONFIG_TEMPLATE:

            if elemento["name"] not in config_object[elemento["section"]]:

                # Si el valor no está en la configuración y no tenemos valor por defecto que asignar, lanzamos error

                if "default" not in elemento:
                    raise ValueError(f"{elemento['name']} no definido en fichero de configuración")
                else:
                    print(
                        f"{elemento['name']} no definido en fichero de configuración. Usando valor por defecto: {elemento['default']}")
                    if elemento["type"] == "str":
                        self._CONFIG[elemento["name"]] = elemento["default"]
                    else:
                        self._CONFIG[elemento["name"]] = int(elemento["default"])
            else:

                # Comprobamos el tipo de valor. Sólo dos opciones válidas: str e int

                valor = config_object[elemento["section"]][elemento["name"]]

                if valor.strip() == "":
                    raise ValueError(f"El elemento {elemento['name']} no puede estar vacío.")

                if elemento["type"] == "str":
                    if "options" not in elemento:
                        self._CONFIG[elemento["name"]] = valor
                    else:
                        if valor in elemento["options"]:
                            self._CONFIG[elemento["name"]] = valor
                        else:
                            print(type(elemento["options"]))
                            print(elemento["options"])
                            raise ValueError(f"Opción no válida para {elemento['name']}: {valor}")
                else:
                    if not valor.isnumeric():
                        raise ValueError(f"El parámetro {elemento['name']} debe ser numérico")
                    else:
                        if int(valor) >= elemento["min"] and int(valor) <= elemento["max"]:
                            self._CONFIG[elemento["name"]] = int(valor)
                        else:
                            raise ValueError(
                                f"Valor {elemento['name']} fuera de los límites [{elemento['min']}, {elemento['max']}]")

    @property
    def location_id(self):
        return self._CONFIG["location_id"]

    @property
    def kinesis_stream(self):
        return self._CONFIG["kinesis_stream"]

    @property
    def retries(self):
        return self._CONFIG["retries"]

    @property
    def buffer(self):
        return self._CONFIG["buffer"]

    @property
    def region(self):
        return self._CONFIG["region"]

    @property
    def logging_level(self):
        if self._CONFIG["level"] == "ERROR":
            return logging.ERROR
        elif self._CONFIG["level"] == "DEBUG":
            return logging.DEBUG
        else:
            return logging.INFO

    def __init__(self):

        # Leo la configuración

        self._read_config()

        # Si no existe la carpeta de logs, la creo

        try:
            os.mkdir('./logs')
        except FileExistsError:
            # La carpeta ya existe. Seguimos
            pass
        except OSError as e:
            print(f"__init__: Error creando carpeta de logs: {e.strerror}.")
            raise

        # Inicializo el logging

        log_filename = f'./logs/smart_{time.time()}_{randint(10000, 99999)}.log'

        try:

            logging.basicConfig(
                filename=log_filename,
                filemode='w',
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=self.logging_level
            )

        except:
            raise Exception(f"__init__: Error durante la creación del servicio de log.", sys.exc_info()[0])

        # Comprobamos que las credenciales de AWS funcionan

        logging.info("__init__: Comprobando conexión a AWS...")

        if self.region == None:
            sts = boto3.client('sts')
        else:
            sts = boto3.client('sts', region_name=self.region)

        try:
            sts.get_caller_identity()
        except ClientError as ce:
            logging.exception(
                f"__init__: Error inicializando conexión a AWS:\n\t{ce.response['Error']['Code']}: {ce.response['Error']['Message']}",
                exc_info=True
            )
            raise

        # Inicializamos el registro de fallos

        self._FailedRecords = []

    #########################################################################################################################################
    #########################################################################################################################################
    #########################################################################################################################################

    def sendOneRecordKinesis(self, record):
        """
        Esta función permite el envío de un registro al stream de Kinesis.

        Parámetros:

            - record: Registro de tipo diccionario a enviar
        """

        intentos = 0

        # Creo el cliente kinesis

        logging.info("sendOneRecordKinesis: Creando el cliente de Kinesis")

        if self.region == None:
            kinesis_client = boto3.client('kinesis')
        else:
            kinesis_client = boto3.client('kinesis', region_name = self.region)

        # Envío el registro

        while intentos < self.retries:

            try:

                intentos += 1

                logging.info(
                    f"sendOneRecordKinesis:\n\t*Intento: {intentos}\n\t* StreamName = {self.kinesis_stream}\n\t* PartitionKey = {self.location_id}\n\t* Data = {json.dumps(record)}"
                )

                put_response = kinesis_client.put_record(
                    StreamName=self.kinesis_stream,
                    Data=f"{json.dumps(record)}\n",  # Importante el salto para concatenar en fichero
                    PartitionKey=self.location_id
                )

                logging.info(
                    f"sendOneRecordKinesis: Registro enviado.\n\tResponse={put_response}"
                )

                return {
                    "status": True,
                    "message": ""
                }

            except ClientError as ce:

                logging.exception(
                    f"sendOneRecordKinesis: \n\t{ce.response['Error']['Code']}: {ce.response['Error']['Message']}",
                    exc_info=True
                )

        # Si después de varios reintentos no hemos conseguido enviar el mensaje, devolvemos False

        logging.error(
            f"sendOneRecordKinesis: No fue posible enviar el mensaje después de {self.retries} intentos."
        )

        # Guardamos el mensaje en la lista de mensajes fallados para intentar enviarlo más tarde

        self._FailedRecords.append(record)

        return {
            "status": False,
            "message": f"sendOneRecordKinesis: No fue posible enviar el mensaje después de {self.retries} intentos."
        }

    #########################################################################################################################################
    #########################################################################################################################################
    #########################################################################################################################################

    def createRecord(self, record):
        """
        Esta función crea un registro en el formato a enviar a Kinesis partiendo del diccionario a enviar e incorporando el PartitionKey

        Parámetros:

            - record: Diccionario a enviar como JSON
        """

#        logging.info(
#            f"createRecord: Creando registro usando diccionario {record}"
#        )

        try:

            kinesis_record = {
                'Data': f"{json.dumps(record)}\n",  # Importante el salto para concatenar en fichero
                'PartitionKey': self.location_id
            }

        except:
            logging.exception(
                f"createRecord: Error formateando diccionario.",
                exc_info=True
            )
            raise

#        logging.info(
#            f"createRecord: Conversión correcta: {kinesis_record}"
#        )

        return kinesis_record

    #########################################################################################################################################
    #########################################################################################################################################
    #########################################################################################################################################

    def sendRecordsKinesis(self, records):
        """
        Esta función permitirá el envío de más de un registro al stream de Kinesis.

        Parámetros:

            - records: Lista con diccionarios a enviar
        """

        # Creo el cliente Kinesis

        logging.info(
            f"sendRecordsKinesis: Creando el cliente de Kinesis"
        )

        if self.region == None:
            kinesis_client = boto3.client('kinesis')
        else:
            kinesis_client = boto3.client('kinesis', region_name = self.region)

        # Recorremos el bucle mientras queden elementos que enviar

        total_records = len(records)

        while len(records) > 0:

            # Cojo los primeros X elementos y reduzco el tamaño de los registros pendientes

            records_to_process = records[:self.buffer]
            records = records[self.buffer:]

            logging.info(
                f"sendRecordsKinesis: Enviando {len(records_to_process)} registros. {len(records)} pendientes."
            )

            # Reinicializamos el contador de intentos

            intentos = 0

            # Intentamos el envío durante una serie de intentos definidos al crear la clase

            while intentos < self.retries:

                intentos += 1

                logging.info(
                    f"sendRecordsKinesis:\n\t*Intento: {intentos}\n\t* StreamName = {self.kinesis_stream}"
                )

                try:

                    put_response = kinesis_client.put_records(
                        Records=[self.createRecord(r) for r in records_to_process],
                        StreamName=self.kinesis_stream
                    )

                    # Devolvemos el número de registros que fallaron en el envío

                    if put_response["FailedRecordCount"] == 0:

                        logging.info(
                            f"sendRecordsKinesis: Mensajes enviados con éxito"
                        )

                        records_to_process = []
                        intentos = self.retries # Para salir del bucle
                    else:
                        logging.error(
                            f"sendRecordsKinesis: Hubo {put_response['FailedRecordCount']} mensajes(s) que no se enviaron correctamente."
                        )

                        # Quito los que se mandaron de la lista, empezando desde el final para no cambiar los índices

                        for idx in range(len(put_response["Records"]) - 1, -1, -1):
                            if not "ErrorCode" in put_response["Records"][idx]:
                                records_to_process.pop(idx)

                        # Si quedan aún reintentos, intentamos enviar los que fallaron

                        if intentos < self.retries:

                            logging.error(
                                f"sendRecordsKinesis: Reintentando envío de los {len(records_to_process)} registros pendientes..."
                            )

                except ClientError as ce:

                    logging.exception(
                        f"sendRecordsKinesis: \n\t{ce.response['Error']['Code']}: {ce.response['Error']['Message']}",
                        exc_info=True
                    )

            # Si al acabar los reintentos aún nos quedan registros por enviar, mostramos mensaje y guardamos los registros
            # en el listado de fallados para reenviarlos más tarde.

            if len(records_to_process) > 0:

                logging.error(
                    f"sendRecordsKinesis: No fue posible enviar {len(records_to_process)} mensajes después de {self.retries} intentos."
                )

                # Guardamos los registros que fallaron en la lista de pendientes de enviar más adelante

                self._FailedRecords.extend(records_to_process)

        if len(self._FailedRecords) > 0:
            return {
                "status": False,
                "message": f"sendRecordsKinesis: No fue posible enviar {len(self._FailedRecords)} de un total de {total_records}."
            }
        else:
            return {
                "status": True,
                "message": ""
            }


    #########################################################################################################################################
    #########################################################################################################################################
    #########################################################################################################################################

    def sendCSVKinesis(self, filePath):
        """
        Esta función permitirá el envío del contenido de un CSV a un stream de Kinesis

        Parámetros:

            - filePath: Path donde se encuentra el CSV con la información
        """

        # Compruebo que el fichero existe

        if not os.path.exists(filePath):
            logging.error(
                f"sendCSVKinesis: El fichero {filePath} no existe"
            )

            return {
                "status": False,
                "message": f"sendCSVKinesis: El fichero {filePath} no existe"
            }

        # Convierto el CSV a lista de JSON

        logging.info(
            f"sendCSVKinesis: Leyendo el fichero {filePath}"
        )

        try:

            with open(filePath) as csvFile:

                csvReader = csv.DictReader(csvFile)

                records_json = [record for record in csvReader]

                logging.info(
                    f"sendCSVKinesis: Enviando {len(records_json)} registros"
                )

                return self.sendRecordsKinesis(records_json)

        except OSError:

            logging.exception(
                f"sendCSVKinesis: No se pudo leer del fichero {filePath}",
                exc_info=True
            )

            return {
                "status": False,
                "message": f"sendCSVKinesis: No se pudo leer del fichero {filePath}"
            }

        except ClientError as ce:

            logging.exception(
                f"sendCSVKinesis: \n\t{ce.response['Error']['Code']}: {ce.response['Error']['Message']}",
                exc_info=True
            )

            return {
                "status": False,
                "message": f"sendCSVKinesis: \n\t{ce.response['Error']['Code']}: {ce.response['Error']['Message']}"
            }

def mandaUno(c):

    registro = {
                   '_id': 'ObjectId("5e2a8cced388780364007ae4")',
                   'TimeStamp': 1579846862222,
                   'DrillID': 'ObjectId("5e2a8cced388780364007ae2")',
                   'TagID': 'a4da22e0a2dc',
                   'Position': '[9.19233768,-1.63553569,0.20000000000000004]',
                   'Velocity': '[0,0,0]',
                   'Acceleration': '[0,0,0]'
    }

    response = c.sendOneRecordKinesis(registro)

    if response["status"]:
        print("mandaUno: El envío fue correcto")
    else:
        print(f"mandaUno: Hubo algún error: {response['message']}")

def mandaVarios(c):

    registros  = [
        {
                   '_id': 'ObjectId("5e2a8cced388780364007ae4")',
                   'TimeStamp': 1579846862222,
                   'DrillID': 'ObjectId("5e2a8cced388780364007ae2")',
                   'TagID': 'a4da22e0a2dc',
                   'Position': '[9.19233768,-1.63553569,0.20000000000000004]',
                   'Velocity': '[0,0,0]',
                   'Acceleration': '[0,0,0]'
        },
        {
            '_id': 'ObjectId("5e2a8cced388780364007ae5")',
            'TimeStamp': 1579846862224,
            'DrillID': 'ObjectId("5e2a8cced388780364007ae3")',
            'TagID': 'a4da22e0a2dd',
            'Position': '[9.19233778,-1.63553579,0.20000000000000001]',
            'Velocity': '[0,0,0]',
            'Acceleration': '[0,0,0]'
        },
    ]

    response = c.sendRecordsKinesis(registros)

    if response["status"]:
        print("mandaVarios: El envío fue correcto")
    else:
        print(f"mandaVarios: Hubo algún error: {response['message']}")


def mandaCSV(c, fichero):

    response = c.sendCSVKinesis(fichero)

    if response["status"]:
        print("mandaCSV: El envío fue correcto")
    else:
        print(f"mandaCSV: Hubo algún error: {response['message']}")

if __name__ == '__main__':

    c = Connectivity()

    print(c.region)

    #mandaUno(c)

    #mandaVarios(c)

    #mandaCSV(c, "C:\\Mis cosas\\Master Big data\\__Proyecto\\Datasets\\DataTFM_5.csv")

    #mandaCSV(c, "C:\\Mis cosas\\Master Big data\\__Proyecto\\Datasets\\DataTFM_600.csv")

    #mandaCSV(c, "C:\\Mis cosas\\Master Big data\\__Proyecto\\Datasets\\DataTFM_10k.csv")

    #ficheros = ["20200124.csv"]#, "20200201.csv", "20200202.csv", "20200203.csv", "20200204.csv"]

#    for fichero in ficheros:

#        t_ini = time.time()

#        mandaCSV(c, "C:\\Mis cosas\\Master Big data\\__Proyecto\\Datasets\\" + fichero)

#        t_fin = time.time()

#        print(f"Hemos tardado {t_fin - t_ini} segundos en procesar el fichero '{fichero}'")