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

## Machine Learning - Insight Detección Automática de Estilo de Natación

Los ficheros que se pueden encontrar en este repositorio son los siguientes:

* notebooks --> Jupyter Notebooks ejecutados y optimizados (sin markdowns) para su ejecución en Cloud.

  * smart_data_preparation.ipynb --> limpieza y preparación de archivos de datos (.csv).
  * smart_clustering_insight.ipynb --> toma como entrada un dataframe con datos procesados y detecta el estilo de nado de cada sesión.
  * smart_model_training.ipynb --> para reentrenar el modelo según se vayan generando datos nuevos.

* pdfs --> Jupyter Notebooks ejecutados en formato PDF para poder leerlo con mayor facilidad.

* scripts --> los tres mismos notebooks de preparación de datos, ejecución del algoritmo de clustering, y reentramiento del modelo pero en scripts .py de Python, para que se puedan llamar desde módulos de AWS.

* referencias.rar --> referencias bibliográficas utilizadas para la fase de Machine Learning.

* smart_insight_swimming_style.csv --> archivo CSV con los resultados obtenidos con los datos entregados por nuestro partner.

* smart_machine_learning.ipynb --> Jupyter Notebook con markdowns y ejecutado donde se ve el desarrollo completo de las fases de limpieza y preparación de datos, diseño de modelo de clustering, y aplicación sobre los datasets proporcionados por el partner.

* smart_model.pkl --> modelo de clustering obtenido y exportado en formato .pkl.
