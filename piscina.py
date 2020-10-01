import random
import uuid
import sys
import hashlib
import threading
import nadador
import queue
import time
import connectivity
from botocore.exceptions import ClientError
from datetime import datetime

class Piscina():
    """
        Esta clase permite gestionar una piscina. Los parámetros de entrada son:

            - largo_piscina: Longitud de la piscina en metros
            - ancho_piscina: Anchura de la piscina en metros
            - ancho_calle: Ancho de cada calle en metros. Valor por defecto 2'5 m
            - refresco: Frecuencia de envío de mensajes en milisegundos. Valor por defecto 40 ms
            - output_mode: Modo de registro (online ó CSV). Valor por defecto CSV
    """

    ####################
    ##   CONSTANTES   ##
    ####################

    _REFRESCO = 40 # milisegundos
    _TIEMPO_VACIA = 5 # segundos

    ###################
    ##   INICIADOR   ##
    ###################

    def __init__(self, largo_piscina, ancho_piscina, ancho_calle=2.5, refresco=40, output_mode="CSV"):

        self._inicio = time.time()              # Tiempo inicial desde que la piscina está vacía
        self._vacia = True
        self._abierta = True
        self._REFRESCO = refresco
        self.ancho_piscina = int(ancho_piscina)
        self.largo_piscina = int(largo_piscina)
        self.ancho_calle = ancho_calle
        self.calles = int(ancho_piscina / ancho_calle)
        self._output_mode = output_mode

        # Habilitamos un 150% de tags sobre el número de nadadores simultáneos

        self._tags = [{"ID": self._getTagID(), "Available": True} for i in range(self.calles * 2)]

        self._nadadores = { (str(x) + y): {"PosicionInicial": x + (0.25 if y == "A" else 0.75), "Nadador": None } for x in range(self.calles) for y in ["A", "B"] }

        # Creamos la cola y el nombre de fichero

        self._queue = queue.Queue()
        self._logname = f"log_{time.time()}.csv"
        self._conn = None

        # Creamos el fichero y la cabecera si el modo es CSV

        if output_mode == "CSV":
            try:
                with open(self._logname, "w") as f:
                    f.write("_id,TimeStamp,DrillID,TagID,Position,Velocity,Acceleration\n")
            except (OSError, IOError) as e:
                print("Error escribiendo log: ", e)
                raise
            except:
                print("Error desconocido escribiendo log: ", sys.exc_info()[1])
                raise
        else:
            try:
                self._conn = connectivity.Connectivity()
            except ClientError as ce:
                print("Error conectando con AWS: ", ce.response['Error']['Code'])
                raise

        # Lanzamos el thread que gestiona la escritura del log piscina

        t = threading.Thread(target=self._printThread, args=[self._queue, self._logname, self._conn])
        t.start()

        if output_mode == "CSV":
            print(f"{datetime.now().time()} -- ¡Abrimos la piscina! Registrando toda la información en el fichero {self._logname}.")
        else:
            print(f"{datetime.now().time()} -- ¡Abrimos la piscina! Registrando toda la información online.")

    ###########################
    ##   PRESENTAR PISCINA   ##
    ###########################

    def __str__(self):

        texto =  "La piscina tiene las siguientes características:\n"\
                 + "\t* Dimensiones: " + str(self.largo_piscina) + " x " + str(self.ancho_piscina) + " metros\n" \
                 + "\t* Calles: " + str(self.calles) + " (con un ancho de " + str(self.ancho_calle) + " metros)\n" \
                 + "\t* Nadadores (" + str(len(self.nadadores)) + "):"

        for i, nad in enumerate(self.nadadores):
            #print(nad.tagID, nad.drillID, nad.estilo, nad.posicion, nad.velocidad)
            texto += "\n\t\t" + str(i+1) + ")\n" \
                     + "\t\t\tTagID: " + nad.tagID + "\n" \
                     + "\t\t\tDrillID: " + nad.drillID + "\n" \
                     + "\t\t\tEstilo: " + nad.estilo + "\n" \
                     + "\t\t\tPosición: " + nad.posicion + "\n"\
                     + "\t\t\tVelocidad: " + str(nad.velocidad) + " m/s"

        return texto

    ####################
    ##   PROPERTIES   ##
    ####################

    @property
    def nadadores(self):
        return [self._nadadores[x]["Nadador"] for x in self._nadadores if not self._nadadores[x]["Nadador"] == None]

    @property
    def abierta(self):
        return self._abierta

    ############################
    ##   FUNCIONES PRIVADAS   ##
    ############################

    def _checkSwimmers(self):

        for i, calle in enumerate(self._nadadores):

            nad = self._nadadores[calle]["Nadador"]

            if nad != None:

                if not nad.is_alive():

                    # Mostramos el mensaje de que el nadador ha salido

                    print(f"\nEl nadador de la calle {calle} ha terminado.\n")

                    # El Tag vuelve a estar disponible

                    for x in range(len(self._tags)):
                        if self._tags[x]["ID"] == nad.tagID:
                            self._tags[x]["Available"] = True

                    # Ya no hay nadador en esa calle

                    self._nadadores[calle]["Nadador"] = None

                    if len(self.nadadores) == 0:
                        self._vacia = True
                        self._inicio = time.time()

                    # Mostramos el estado de la piscina

                    #print(self.__str__())

    def _printThread(self, outQueue, log, conn):

        # Mientras la piscina esté abierta se registrarán los mensajes, bien online o en CSV

        while True:

            if self._output_mode == "CSV":

                with open(log, "a") as f:
                    while not outQueue.empty():

                        result = outQueue.get()

                        f.write(result)
            else:
                records = []

                while not outQueue.empty():
                    records.append(outQueue.get())

                if len(records) > 0:
                    conn.sendRecordsKinesis(records)

            self._checkSwimmers()

            if self._vacia:
                if time.time() - self._inicio >= self._TIEMPO_VACIA:
                    # Si está sin nadadores durante X segundos, salir

                    self._abierta = False
                    return ""

    def _getTagID(self):
        return hashlib.md5(str(uuid.uuid1()).encode()).hexdigest()[:12]

    def _getEmptyLane(self):

        try:
            return random.sample([x for x in self._nadadores if self._nadadores[x]["Nadador"] == None], 1)[0]
        except ValueError:
            return None

    def _getFreeTag(self):

        # Mezclamos los elementos de la lista

        random.shuffle(self._tags)

        # Ahora busco el primer disponible

        for i, tag in enumerate(self._tags):
            if tag["Available"]:
                self._tags[i]["Available"] = False
                return tag["ID"]

    ############################
    ##   FUNCIONES PÚBLICAS   ##
    ############################

    def nuevoNadador(self, tiempo=30*60):

        # Buscamos un hueco para el nuevo nadador

        lane = self._getEmptyLane()

        if lane == None:
            raise Exception("No hay calles disponibles")

        # Definimos si empieza por un lado de la piscina o por el otro
        # La posición Y estará definida por la calle en la que esté
        # La posición Z será fija

        posX = (self.largo_piscina / 2) * (1 if random.randint(0, 1) == 0 else -1)
        posY = self.ancho_calle * self._nadadores[lane]["PosicionInicial"]
        posZ = 0.20000000000000004

        self._nadadores[lane]["Nadador"] = nadador.Nadador(self._getFreeTag(),
                                                           self._REFRESCO,
                                                           self.largo_piscina,
                                                           posX,
                                                           posY,
                                                           posZ,
                                                           tiempo,
                                                           self._queue,
                                                           self._output_mode
                                                           )

        print(f"Entra un nuevo nadador en la calle {lane} y durante {tiempo} segundos")

        self._vacia = False

    def printStatus(self):

        status = "--" * (self.largo_piscina + 2) + "\n"

        for i in range(self.calles):

            nad = (self._nadadores[str(i) + "A"]["Nadador"], self._nadadores[str(i) + "B"]["Nadador"])
            pos = (0 if nad[0] == None else (int(self.largo_piscina / 2) + int(nad[0].X)), 0 if nad[1] == None else (int(self.largo_piscina / 2) + int(nad[1].X)))

            if nad[0] == None:
                status += "|" + ("  " * self.largo_piscina) + "| " + str(i) + "A\n"
            else:
                status += "|" + ("  " * pos[0]) + "##" + ("  " * (int(self.largo_piscina) - pos[0] - 1)) + "| " + str(i) + "A\n"

            if nad[1] == None:
                status += "|" + ("  " * self.largo_piscina) + "| " + str(i) + "B\n"
            else:
                status += "|" + ("  " * pos[1]) + "##" + ("  " * (int(self.largo_piscina) - pos[1] - 1)) + "| " + str(i) + "B\n"

            status += "--" * (self.largo_piscina + 2) + "\n"

        print(status)