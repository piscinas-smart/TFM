import socketserver
import sys
import json
from http.server import BaseHTTPRequestHandler
from smart.conexion import connectivity


class SmartHandler(BaseHTTPRequestHandler):

    conn = None

    def send_csv(self, path):

        print("send_csv called")

        # Enviamos el registro

        try:
            send_response = self.conn.sendCSVKinesis(path)

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

    def send_records(self, records):

        print("send_records called")

        # Enviamos el registro

        try:
            send_response = self.conn.sendRecordsKinesis(records)

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

    def retrieve_info(self):

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
            post_body_json = json.loads(post_body)
            is_json = True
        except ValueError:
            is_json = False

        return {
            "status": True,
            "error": "",
            "is_json": is_json,
            "message": post_body_json if is_json else post_body
        }

    def manage_response(self, response):

        self.send_response(200)  # OK
        self.send_header('Content-type', 'text/html')
        self.end_headers()

        self.wfile.write(response.encode())

    def do_POST(self):

        if self.path in ("/sendRecords", "/sendCSV"):

            info_post = self.retrieve_info()

            if not info_post["status"]:
                result = info_post["error"]
            elif self.path == "/sendRecords" and not info_post["is_json"]:
                result = "JSON was expected. String received."
            elif self.path == "/sendCSV" and info_post["is_json"]:
                result = "Absolute path was expected. JSON received."
            elif self.path == "/sendRecords":
                info_delivery = self.send_records(info_post["message"])
                result = info_delivery["response"]
            else:
                info_delivery = self.send_csv(info_post["message"])
                result = info_delivery["response"]

            self.manage_response(result)

class SmartHttpServer:

    def __init__(self,
                 host="localhost",
                 port=59605
                 ):

        self._host = host
        self._port = port

    def start(self):

        # Creo el servidor y le paso el handler

        SmartHandler.conn = connectivity.Connectivity()

        httpd = socketserver.TCPServer(("localhost", 59605), SmartHandler)

        print(f"Server online listening on {self._host}:{self._port}")

        httpd.serve_forever()

if __name__ == "__main__":

    shs = SmartHttpServer()

    shs.start()