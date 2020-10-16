# TFM

Este repositorio contiene el código fuente de los componentes utilizados en nuestro trabajo fin de máster así como la memoria a presentar.

## S.Ma.R.T. (Swim Mapping, Reporting & Training)

Los ficheros disponibles son:

* config.ini --> Fichero de configuración para la conectividad

* lambda --> Código fuente de las lambdas utilizadas en las APIs

 * info_by_date.py --> Información sobre un día concreto en la piscina
 
 * info_by_tag_dates.py --> Información para un tag concreto en un rango de fechas

* requirements.txt --> Fichero con los paquetes necesarios para una correcta ejecución.

* smart --> Paquete del proyecto

  * conexion --> Paquete para gestionar conexiones
  
    * connectivity.py --> Clase para realizar la comunicación con Kinesis
    
    * enviooffline.py --> Clase para realizar el envío offline de ficheros CSV a la nube
    
    * httpserver.py --> Clase para crear un servidor para realizar enviar registros a Kinesis utilizando llamadas directas POST.
    
    * serverqueue.py --> Clase para crear un servidor para realizar enviar registros a Kinesis utilizando llamadas POST mediante una cola.

  * simulador --> Paquete para simular la piscina
  
    * piscina.py --> Clase para gestionar un simulador de una piscina
    
    * nadador.py --> Clase para gestionar la simulación de un nadador en una piscina

* SMART_Gestión_Piscina.ipynb --> Notebook para la gestión de ejemplo de una piscina
