# Trabajo Practico 1: Escalabilidad

## Integrantes:
* Harriet Eliana
* Godoy Dupont Mateo

La documentación del proyecto se realizará utilizando la arquitectura de 4+1 views. Para ello, iremos explicando vista por vista los distintos diagramas con el fin de describir todos los aspectos del proyecto. 


Notas:
Q1 sola: tarda 2 min
Q2 sola: 
Q3 sola:
Q4 sola:
Q5 sola:

Todas juntas:
Los datos no se van de results -> confusión entre las dos r
Dsp de hacer una query tuve que reiniciar el cleaner


**Idea del sistema:**

![alt text](diagrams/robustez-general.png)

Cada query necesita que el flujo pase por distintos procesadores de datos. A continuación los mostramos:

Query 1:  
![alt text](diagrams/q1.png)

Query 2:  
![alt text](diagrams/q2.png)

Query 3:  
![alt text](diagrams/q3.png)

Query 4:  
![alt text](diagrams/q4.png)

Query 5:  
![alt text](diagrams/q5.png)

### Vista de Escenarios
![Diagrama de casos de uso](<diagrams/Diagrama de casos de uso.png>)

El diagrama de casos de uso muestra las distintas interacciones que puede tener el usuario con nuestro sistema. En primer lugar, el usuario puede iniciar el sistema para que comience a hacer el análisis de los datos. Una vez que el sistema comienza a correr, el usuario puede ir constatando el archivo de resultados que se va generando en su computadora. (El sistema no se cerrará hasta completar el análisis).  

<!-- ### Vista Lógica -->

### Vista de Desarrollo

![alt text](diagrams/paquetes.png)
Se utilizó un módulo utils el cual es utilizado por todos los nodos, ya que contiene funciones que son comunes a todos.
 
### Vista de Procesos

![alt text](<diagrams/diagrams/a.png>)

### Vista Física
![Diagrama de despliegue](<diagrams/Diagrama de despliegue.png>)

Uno de los objetivos de nuestro sistema es lograr un procesamiento distribuido de la información, para ello planteamos que cada uno de los distintos componentes del sistema puede ser ejecutado de forma independiente. Una de las formas de lograr cumplir esta meta es tener a cada nodo en un container propio que se conecte con el resto del sistema mediante nuestro middleware. Algunos de estos nodos pueden levantarse múltiples veces a fin de agilizar el proceso.