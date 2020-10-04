import socketserver
import threading
import sys
import json
import time
from http.server import BaseHTTPRequestHandler
from smart.conexion import Connectivity
import queue

class SmartHandler(BaseHTTPRequestHandler):

    my_queue = None

    def retrieve_info(self):

        post_body = ""

        # Parse the content length

        try:
            content_len = int(self.headers['content-length'])
        except KeyError:
            return {
                "status": False,
                "error": "No information in header for content-length"
            }

        if content_len <= 0:
            return {
                "status": False,
                "error": f"content-length invalid: {content_len}"
            }

        # Read the body

        try:
            post_body = self.rfile.read(content_len)
        except:
            return {
                "status": False,
                "error": f"Error reading body message: {str(sys.exc_info()[0])}"
            }

        # Check if it is json

        try:
            post_body = json.loads(post_body)
            is_json = True
        except ValueError:
            is_json = False

        return {
            "status": True,
            "error": "",
            "is_json": is_json,
            "message": post_body
        }

    def manage_response(self, response):

        self.send_response(200)  # OK
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        self.wfile.write(response.encode())

    def do_POST(self):

        if self.path == "/sendRecord":

            info_post = self.retrieve_info()

            if not info_post["status"]:
                result = info_post["error"]
            elif not info_post["is_json"]:
                result = "JSON was expected. String received."
            else:
                # Si todo estaba bien, metemos el registro en la cola

                self.my_queue.put(info_post["message"])
                result = "Element successfully put in queue"

            self.manage_response(result)

class SmartHttpServer:

    def __init__(self,
                 host="localhost",
                 port=59605
                 ):

        # Guardo los datos de host y puerto

        self._host = host
        self._port = port

        # Creo la cola de mensajes y la conectividad a AWS

        self._queue = queue.Queue()

        SmartHandler.my_queue = self._queue

        self._conn = Connectivity()

        # Creo el thread que se encargará de los envíos y lo inicio

        t = threading.Thread(target=self._queueDelivery, args=[self._queue, self._conn])

        t.start()

        # Abro el servidor  para escuchar las llamadas

        httpd = socketserver.TCPServer((host, port), SmartHandler)

        print(f"Server online listening on {host}:{port}")

        httpd.serve_forever()

    def _send_records(self, records, conn):

        print("send_records called")

        # Enviamos el registro

        try:
            send_response = conn.sendRecordsKinesis(records)

            if send_response["status"]:
                return {
                    "status": True,
                    "response": "Envío correcto :)"
                }
            else:
                return {
                    "status": True,
                    "response": f"Envío NO realizado correctamente: {send_response['message']}"
                }
        except:
            return {
                "status": True,
                "response": f"Envío NO realizado correctamente: {str(sys.exc_info()[0])}"
            }

    def _queueDelivery(self, outQueue, conn):

        while True:

            records = []

            # Vacío la cola y mando todos los mensajes que hubiera

            while not outQueue.empty():
                records.append(outQueue.get())

            if len(records) > 0:
                send_response = self._send_records(records, conn)

                if not send_response["status"]:
                    print(send_response["response"])

            time.sleep(5)

if __name__ == "__main__":

    shs = SmartHttpServer()