# Trabajo Practico 1: Escalabilidad

## Integrantes:
* Harriet Eliana
* Godoy Dupont Mateo

## Entrega - Parte 1: Documentación

La documentación del proyecto se realizará utilizando la arquitectura de 4+1 views. Para ello, iremos explicando vista por vista los distintos diagramas con el fin de describir todos los aspectos del proyecto.  

### Vista de Escenarios
![Diagrama de casos de uso](<diagrams/Diagrama de casos de uso.png>)

El diagrama de casos de uso muestra las distintas interacciones que puede tener el usuario con nuestro sistema. En primer lugar, el usuario puede iniciar el sistema para que comience a hacer el análisis de los datos. Una vez que el sistema comienza a correr, el usuario puede ir constatando el archivo de resultados que se va generando en su computadora. (El sistema no se cerrará hasta completar el análisis).  

### Vista Lógica
![Diagrama de estados (nivel sistema)](<diagrams/Diagrama de estados.png>)

![Diagrama de estados (nivel data processors)](<diagrams/Diagrama de estados (data processors).png>)

Los diagramas de estados muestran el comportamiento del sistema según la instancia en la que se encuentre. En el primer diagrama se muestra el comportamiento del sistema en general, es decir, cómo se comporta el sistema en su totalidad. Por otro lado, el segundo diagrama muestra el comportamiento general de los nodos de procesamiento de datos.  
Puede verse que el sistema hace circular la información a los nodos que corresponda según el tipo y el estado de la query que se está realizando, mientras que recibe la información procesada de los nodos de procesamiento de datos. Esta información es redirigida nuevamente a los nodos de procesamiento de datos cuantas veces sea necesario hasta que se cumpla con cada query. Finalmente al obtener la información resultante se notifica al usuario y se vuelve a comenzar.  
Respecto a los nodos de procesamiento de datos, se puede observar que estos nodos reciben información hasta que se cierren. De esta forma se puede ver que los nodos de procesamiento de datos son independientes de la query que se esté realizando, es decir, no se cierran al finalizar una query, sino que pueden procesar información de forma continua.  

### Vista de Desarrollo
![Diagrama de componentes](<diagrams/Diagrama de componentes.png>)

El diagrama de componentes muestra los distintos componentes que conforman nuestro sistema. En primer lugar, se encuentra el *Amazon Books Analyzer* que correrá en la pc del usuario y será la *"cara"* de nuestro sistema. Por otro lado se encuentra el *query master* que dirigirá el flujo de la query a los distintos nodos de procesamiento de datos.  

### Vista de Procesos
![Diagrama de actividad](<diagrams/Diagrama de actividades.png>)

El diagrama de actividad se encuentra separado en cuatro columnas. Tanto la primera como la última de ellas se corresponden a dos módulos de la misma aplicación que corre en la computadora del usuario. (Potencialmente serán hilos, queda sujeto a decisiones de implementación).  
El *main service* se encarga de leer los archivos de datos y enviarlos mediante nuestro middleware a los nodos para su procesamiento. Por otro lado, el *reporting service* quedará a la espera de los resultados provenientes de nuestro middleware para su posterior guardado en un archivo en la computadora del usuario.  
Por su parte, el *query master* es un nodo encargado de gestionar el flujo de las query, es decir, es el componente que envía la información a los distintos tipos de nodos a los que les corresponderá hacer determinado procesamiento de data según el tipo de query y la instancia de la query en la que se encuentra.  
Por último, la columna *data processor* muestra las actividades en modo genérico de los distintos tipos de nodos de procesamiento. Estos nodos pueden ser de tipo Filter, Data Joiner, Counter y Sentiment analyzer.

### Vista Física
![Diagrama de despliegue](<diagrams/Diagrama de despliegue.png>)


Uno de los objetivos de nuestro sistema es lograr un procesamiento distribuido de la información, para ello planteamos que cada uno de los distintos componentes del sistema puede ser ejecutado de forma independiente. Una de las formas de lograr cumplir esta meta es tener a cada nodo en un container propio que se conecte con el resto del sistema mediante nuestro middleware.

![Diagrama de robustez](<diagrams/Diagrama de robustez.png>)

El diagrama de robustez nos muestra las interacciones entre los distintos componentes del sistema. El proceso inicia desde el usuario, donde por consola inicia la aplicación encargada de leer los datos a procesar, enviarlos y recibir los resultados. Este proceso se realiza mediante un middleware representado con una “caja” con el logo de RabbitMQ ya que dentro de nuestro desarrollo se utilizará dicha herramienta como soporte para el envio de paquetes mediante colas.  
Tal como se ve en el diagrama, el resto de componentes también utilizará dicho middleware para la comunicación entre ellos. Cabe aclarar que gráficamente podemos observar una query “genérica”, es decir, que el flujo que realizá un dato en cada una de las query posibles no está especificado. Esto con el fin de obtener un diagrama más sencillo y fácil de comprender, a continuación se especificará el flujo para cada query:

+ Título, autores y editoriales de los libros de categoría "Computers" entre 2000 y 2023 que contengan 'distributed' en su título:  
    1. **Query Master**
    2. filtro por **categoria**
    3. **Query Master**
    4. filtro por **año**
    5. **Query Master**
    6. filtro por **título**
    7. **Query Master**

+ Autores con títulos publicados en al menos 10 décadas distintas:  
    1. **Query Master**
    2. Filtro por **cantidad de decadas distintas**
    3. **Query Master**

+ Títulos y autores de libros publicados en los 90' con al menos 500 reseñas:
    1. **Query Master**
    2. Filtro por **año**
    5. **Query Master**
    6. Filtro por **cantidad de reseñas**
    7. **Query Master**

+ 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas:
    1. **Query Master**
    2. Filtro por **año**
    5. **Query Master**
    6. Filtro por **cantidad de reseñas**
    7. **Query Master**
    8. Anlisis de sentimiento
    9. **Query Master**
    10. **contador sentimiento**
    11. Filtro por **mejores 10 en rating**
    12. **Query Master**

+ Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté en el percentil 90 más alto (se asume que es el percentil 90 de los sentimientos de reseñas de libros del total):
    1. **Query Master**
    2. Anlisis de sentimiento
    3. **Query Master**
    4. **contador sentimiento**
    5. Filtro por **sentimiento promedio**
    6. **Query Master**
    7. Filtro por **categoria**
    8. **Query Master**