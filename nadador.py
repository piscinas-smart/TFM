import random
import time
import uuid
import hashlib
import threading
import json

class Nadador():
    """
        Esta clase permite gestionar un nadador y hacer que vaya generando datos. Los parámetros de entrada son:

            - tagID: ID del tag utilizado por el nadador
            - tagFreq: Frecuencia de envío de la información (en milisegundos)
            - X_ini: Coordenada X para comenzar la sesión
            - Y_ini: Coordenada Y para comenzar la sesión
            - Z_ini: Coordenada Z para comenzar la sesión
            - tiempo: Tiempo en segundos de la duración de la sesión
            - cola: Cola donde poner los mensajes generados
            - output_mode: Tipo de registro: CSV u online
    """

    ####################
    ##   CONSTANTES   ##
    ####################

    _ESTILOS_POSIBILIDADES = [("mariposa", "braza", "crol"), (1, 3, 6)]         # Los estilos de natación y sus probabilidades
    _ESTILOS_VELOCIDADES = {"mariposa": 0.90, "braza": 0.80, "crol": 1.00}      # Sus velocidades medias en m/s (la mitad del record)

    ###################
    ##   INICIADOR   ##
    ###################

    @property
    def posicion(self):
        return "[" + str(self.X) + "," + str(self.Y) + "," + str(self.Z) + "]"

    @property
    def velocity(self):
        return "[0,0,0]"

    @property
    def aceleracion(self):
        return "[0,0,0]"

    ####################
    ##   PROPERTIES   ##
    ####################

    def __init__(self, tagID, tagFreq, largo, X_ini, Y_ini, Z_ini, tiempo, cola, output_mode):

        self._activo = True
        self.tagID = tagID
        self.tagFreq = tagFreq / 1000

        self.largo = largo

        self.X_ini = X_ini
        self.X = X_ini
        self.Y_ini = Y_ini
        self.Y = Y_ini
        self.Z_ini = Z_ini
        self.Z = Z_ini

        self.direccion = (1 if X_ini < 0 else -1)

        self._start = time.time()
        self.tiempo = tiempo

        self.drillID = self._getDrillID()

        self.estilo = random.choices(self._ESTILOS_POSIBILIDADES[0], weights=self._ESTILOS_POSIBILIDADES[1], k=1)[0]

        # Añadimos una pequeña variación aleatoria a la velocidad para que no todos vayan igual de rápido en el mismo estilo

        self.velocidad = self._ESTILOS_VELOCIDADES[self.estilo] + random.uniform(-0.5, 0.1)
        self._output_mode = output_mode

        # Iniciamos el thread de envío de valores

        t = threading.Thread(target=self._sendValue, args=[cola])

        t.start()

    def __str__(self):

        texto =  f"Los datos del nadador son:\n\t* TagID: {self.tagID}\n\t* Tag frequency: {self.tagFreq * 1000} ms\n\t" \
                 f"* Estilo: {self.estilo}\n\t* Velocidad: {self.velocidad} m/s\n\t* Posición inicial: [{self.X_ini}, {self.Y_ini}, {self.Z_ini}]" \
                 f"\n\t* Posición actual: {self.posicion}"

        return texto

    ############################
    ##   FUNCIONES PRIVADAS   ##
    ############################

    def _getDrillID(self):
        return "ObjectID(\"" + hashlib.md5(str(uuid.uuid1()).encode()).hexdigest()[:18] + "\")"

    def _sendValue(self, cola):

        while time.time() - self._start < self.tiempo:

            record = self._move()

            # Si estamos en modo online tendremos un JSON. Si no, un texto

            if self._output_mode == "online":
                cola.put(json.loads(record))
            else:
                cola.put(record)

            # Esperamos X ms al siguiente envío

            time.sleep(self.tagFreq)

        # Una vez que pase el tiempo de piscina, desactivamos al nadador

        self._activo = False

        return

    def _getID(self):
        return "ObjectID(\"" + hashlib.md5(str(uuid.uuid1()).encode()).hexdigest()[:18] + "\")"

    def _generateRecord(self):

        # _id, TimeStamp, DrillID, TagID, Position, Velocity, Acceleration

        if self._output_mode == "CSV":
            record = f"{self._getID()},{int(time.time() * 1000)},{self.drillID},{self.tagID},\"{self.posicion}\",\"{self.velocity}\",\"{self.aceleracion}\"\n"
        else:
            record = {
                "_id": self._getID(),
                "TimeStamp": int(time.time() * 1000),
                "DrillID": self.drillID,
                "TagID": self.tagID,
                "Position": f"\"{self.posicion}\"",
                "Velocity": f"\"{self.velocity}\"",
                "Acceleration": f"\"{self.aceleracion}\""
            }
            record = json.dumps(record)

        return record

    def _move(self):

        # La velocidad no será homogénea. Variará en un 5%, positiva o negativamente y de forma aleatoria

        new_vel = self.velocidad * (1 + 0.05 * random.randint(-1, 1))

        # La posición Y no será fija. Variará un 5% cada 40 ms con respecto a la posición inicial

        self.Y = self.Y_ini * (1 + 0.05 * random.randint(-1, 1))

        # La posición Z por ahora se mantiene fija

        self.Z = self.Z

        # La posición X

        new_x = self.X + \
                 new_vel * self.direccion * self.tagFreq

        if abs(new_x) > (self.largo / 2):
            new_x = (self.largo / 2) * (new_x / abs(new_x))

        self.X = new_x

        # Vemos si tiene que dar ya la vuelta (cuando hayamos llegado a la pared)

        record = self._generateRecord()

        if (self.largo / 2) - abs(self.X) <= 0:

            self.X = (self.largo / 2 + 0.0000001) * self.direccion      # Empezamos en la otra dirección
            self.direccion *= (-1)                                      # Cambiamos de dirección
            self.drillID = self._getDrillID()

        return(record)

    ############################
    ##   FUNCIONES PÚBLICAS   ##
    ############################

    def is_alive(self):
        return self._activo