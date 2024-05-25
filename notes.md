**<span style="color:#C773FF"> Queda hacer: </span>**
- Actualizar en los diagramas que corresponda el atributo `client_id` en `DataFragment`.
- Agregar una forma de obtener el `client_id` en el Cleaner a fin de poder crear los DataFragments.
- Agregar manejos por clientes en los nodos joiner y datacleaner.
- Agregar limpieza de información en los nodos.
- Ver si es necesario agregar el manejo por clientes en la variable `self.books` en el nodo counter.

**<span style="color:#78FF73"> Hecho: </span>**
- Se agregó el atributo `client_id` al struct `DataFragment`, a fin de identificar a qué cliente pertenece cada fragmento de datos.
- Se agregó el manejo por clientes en el nodo counter para la variable `self.counted_data`.