from os                     import listdir
from os.path                import isfile, join
from configparser           import ConfigParser
from botocore.exceptions    import ClientError
from boto3.exceptions       import S3UploadFailedError

import logging
import shutil
import boto3
import time
import sys
import os

class EnvioOffline():
    """Esta clase permite el envío de CSVs desde el centro directamente a un bucket S3 de históricos."""

    def __init__(self):

        # Establecemos el log

        try:

            logging.basicConfig(
                filename="envioOffline.log",
                filemode="a+",
                format='%(asctime)s - %(levelname)s - %(message)s',
                level=logging.INFO
            )

        except:
            raise Exception(f"Error durante la creación del servicio de log.", sys.exc_info()[0])

        # Leemos los parámetros de configuración

        try:
            self._read_config()
        except Exception as e:
            logging.exception(f"Leyendo fichero de configuración: {e}", exc_info=True)

        # Comprobamos si existe la carpeta de procesados. Si no, la creamos

        try:
            os.mkdir(join(self._CSV_PATH, "processed"))
        except FileExistsError:
            # La carpeta ya existe. Seguimos
            pass
        except OSError as e:
            logging.exception(f"Error creando carpeta de ficheros procesados: {e.strerror}", exc_info=True)

    def _read_config(self):
        """Lectura de los parámetros del fichero de configuración"""

        # Compruebo primero si el fichero existe

        if not os.path.exists("./config.ini"):
            raise BaseException("read_config: File 'config.ini' not found.")

        # Analizo los parámetros

        config_object = ConfigParser()
        config_object.read("./config.ini")

        # Confirmación de que todos los parámetros están

        if "csv_path" not in config_object["OFFLINE"]:
            raise ValueError("Elemento 'csv_path' no definido en fichero de configuración")

        if "bucket" not in config_object["OFFLINE"]:
            raise ValueError("Elemento 'bucket' no definido en fichero de configuración")

        if "key" not in config_object["OFFLINE"]:
            raise ValueError("Elemento 'key' no definido en fichero de configuración")

        # Guardamos los valores

        self._CSV_PATH = config_object["OFFLINE"]["csv_path"]
        self._S3_BUCKET = config_object["OFFLINE"]["bucket"]
        self._S3_KEY  = config_object["OFFLINE"]["key"]

    def upload_all(self):
        """
            Subir todos los CSVs que haya en el path especificado en el fichero de configuración

            Devolverá True si no hay ficheros a enviar o si todos los ficheros se enviaron y movieron correctamente. False en caso contrario.
        """

        hay_error = False

        # Obtenemos la lista de CSVs en la carpeta

        csv_files = [f for f in listdir(self._CSV_PATH) if isfile(join(self._CSV_PATH, f)) and f.endswith(".csv")]

        if len(csv_files) == 0:
            logging.info(f"No se encontraron ficheros a procesar en la carpeta {self._CSV_PATH}")
            return True

        # Enviamos los ficheros

        for file in csv_files:

            logging.info(f"Procesando fichero {file}")

            # Enviamos el fichero

            if self.upload_file(join(self._CSV_PATH, file), file):

                # Si se procesó correctamente, movemos el fichero a la carpeta de procesados

                try:
                    shutil.move(join(self._CSV_PATH, file), join(join(self._CSV_PATH, "processed"), file))
                except:
                    logging.error(f"Error moviendo fichero {file} a carpeta de procesados")
                    hay_error = True
                else:
                    logging.info(f"Fichero {file} movido a carpeta de procesados")

        # Si hubo algún error devolvemos false

        return not hay_error

    def upload_file(self, file, file_name):
        """
            Esta clase permite subier un fichero al bucket S3. Parámetros

                - file: Fichero a enviar con el path completo
                - file_name: Nombre del fichero en el bucket destino

            Devolverá True si el fichero se envió correctamente y False en caso contrario
        """

        """Upload a file to an S3 bucket

        :param file: Fichero a enviar con path completo
        :param file_name: Nombre del fichero
        :return: True si se subió el fichero, False en caso contrario
        """

        # Creamos el cliente S3

        s3_client = boto3.client('s3')

        # Subimos el fichero

        t_ini = time.time()

        try:
            response = s3_client.upload_file(file, self._S3_BUCKET, self._S3_KEY + "/" + file_name)
        except S3UploadFailedError as e:
            logging.error(f"Error subiendo fichero: {e}")
            return False
        except ClientError as e:
            logging.error(f"Error subiendo fichero: \n\t{e.response['Error']['Code']}: {e.response['Error']['Message']}")
            return False
        except Exception as e:
            logging.error(f"Excepción no reconocida subiendo fichero: {e}")
            return False

        t_fin = time.time()

        logging.info(f"Fichero {file_name} subido a S3 en {t_fin - t_ini} segundos")

        return True

if __name__ == "__main__":

    eo = EnvioOffline()

    eo.upload_all()