# Trabajo Practico 1: Escalabilidad

## Integrantes:
* Harriet Eliana
* Godoy Dupont Mateo

## Entrega - Parte 1: Documentación

La documentación del proyecto se realizara utilizando la arquitectura de 4+1 views. Para ello, iremos explicando vista por vista los distintos diagramas con el fin de describir todos los aspectos del proyecto.  

### Vista de Escenarios
#### Diagrama de casos de uso

### Vista Lógica
#### Diagrama de estados

### Vista de Desarrollo
#### Diagrama de componentes


### Vista de Procesos
#### Diagrama de actividad

El diagrama de actividad se encuentra separado en cuatro columnas. Tanto la primera como la última de ellas se corresponden a dos hilos de la misma aplicación que corre en la computadora del usuario.  
El main service se encarga de leer los archivos de datos y enviarlos mediante nuestro middleware a los nodos para su procesamiento. Por otro lado, el reporting service quedara a la espera de los resultados provenientes de nuestro middleware para su guardado en un archivo en la computadora del usuario.  
Por su parte el query master es un nodo encargado de gestionar el flujo de las query, es decir, es el componente que envía la información a los distintos tipos de nodos según el tipo de query y la instancia de la query en la que se encuentra el dato.  
Por último, la columna data processor muestra las actividades en modo genérico de los distintos tipos de nodos de procesamiento. Estos nodos pueden ser de tipo Filter, Data Joiner, Counter y Sentiment analyzer.

### Vista Física
#### Diagrama de despliege


Uno de los objetivos de nuestro sistema es lograr un procesamiento distribuido de la información, para ello planteamos que cada uno de los distintos que componen el sistema puede ser ejecutado de forma independiente. Una de las formas de lograr cumplir esta meta es tener a cada nodo en un container propio que se conecte con el resto del sistema mediante nuestro middleware.

#### Diagrama de robustez

El diagrama de robustez nos muestra las interacciones entre los distintos componentes del sistema. El proceso inicia desde el usuario, donde por consola inicia la aplicación encargada de leer los datos a procesar, enviarlos y recibir los resultados. Este proceso se realiza mediante un middleware representado con una “caja” con el logo de RabbitMQ ya que dentro de nuestro desarrollo se utilizara dicha herramienta como soporte para el envió de paquetes mediante colas.  
Tal como se ve en el diagrama, el resto de componentes también utilizará dicho middleware para la comunicación entre ellos. Cabe aclarar que gráficamente podemos observar una query “generica”, es decir, que el flujo que realiza un dato en cada una de las query posibles no está especificado. Esto con el fin de obtener un diagrama más sencillo y fácil de comprender, a continuación se especificará el flujo para cada query:

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
    3. **Query Master**
    4. **contador reseñas por titulo**
    5. **Query Master**
    6. Filtro por **cantidad de reseñas**
    7. **Query Master**

+ 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas:
    1. **Query Master**
    2. Filtro por **año**
    3. **Query Master**
    4. **contador reseñas por titulo**
    5. **Query Master**
    6. Filtro por **cantidad de reseñas**
    7. **Query Master**
    8. Anlisis de sentimiento
    9. **Query Master**
    10. **contador sentimiento**
    11. Filtro por **mejores 10 en rating**
    12. **Query Master**

+ Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté en el percentil 90 más alto:
    1. **Query Master**
    2. Anlisis de sentimiento
    3. **Query Master**
    4. **contador sentimiento**
    5. Filtro por **sentimiento promedio**
    6. **Query Master**
    7. Filtro por **categoria**
    8. **Query Master**