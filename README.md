# TFM

Este repositorio contiene el código fuente de los componentes utilizados en nuestro trabajo fin de máster así como la memoria a presentar.

## S.Ma.R.T. (Swim Mapping, Reporting & Training)

Los ficheros disponibles son:

* config.ini --> Fichero de configuración para la conectividad

* memoria --> Carpeta con la documentación del proyecto

* smart --> Paquete del proyecto

  * conexion --> Paquete para gestionar conexiones
  
    * connectivity.py --> Fichero para realizar la comunicación con Kinesis
    
    * httpserver.py --> Servidor para realizar enviar registros a Kinesis utilizando llamadas directas POST.
    
    * serverqueue.py --> Servidor para realizar enviar registros a Kinesis utilizando llamadas POST mediante una cola.

  * simulador --> Paquete para simular la piscina
  
    * piscina.py --> Clase para gestionar un simulador de una piscina
    
    * nadador.py --> Clase para gestionar la simulación de un nadador en una piscina

* SMART_Gestión_Piscina.ipynb --> Notebook para la gestión de ejemplo de una piscina
