**<span style="color:#C773FF"> Queda hacer: </span>**
- Agregar una forma de obtener el `client_id` en el Cleaner a fin de poder crear los DataFragments.
- Agregar manejos por clientes en el nodo datacleaner.
- Agregar limpieza de estado en el nodo cleaner.
- Ver si es necesario agregar el manejo por clientes en la variable `self.books` en los nodos counter y joiner.
- Actualizar en los diagramas que corresponda el atributo `client_id` en `DataFragment`.
- Actualizar en los diagramas que corresponda el manejo por clientes.
- Ver si hay que agregar algún diagrama con el manejo por clientes.

**<span style="color:#78FF73"> Hecho: </span>**
- Se agregó el atributo `client_id` al struct `DataFragment`, a fin de identificar a qué cliente pertenece cada fragmento de datos.
- Se agregó el manejo por clientes en el nodo counter para la variable `self.counted_data`.
- Se agregó el manejo por clientes en el nodo joiner para las variables `self.books_side_tables` y `self.side_tables_ended`.
- Se agregó la limpieza de la query al llegar el last_fragment en los nodos counter y joiner.