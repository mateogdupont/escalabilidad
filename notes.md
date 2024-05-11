**Sobre el informe 4+1:**  
- <span style="color:red"> 🔥 major:</span> Un sistema tan complejo como el que desarrollaron ustedes, donde se tienen Chunks, DataFragments, QueryInfo, mensajes con meta información para entender la ruta que van a tomar, etc.. merece tener una documentación a la altura para que cualquier persona lo pueda entender. Esto no lo explican en ningún lado, podrían haber explicado cómo se relacionan estas clases y como funcionan con un diagrama de clase en la vista lógica, o con una descripción textual.
- <span style="color:yellow"> ❕minor:</span> Sería bueno sumar sobre las flechas del DAG, los campos/atributos que se envían en cada mensaje, para entender qué cosas se filtran (por ejemplo).
- <span style="color:yellow"> ❕minor:</span> En la entrega no se especificó en ningún lado las instrucciones para ejecutar el sistema.

**Sobre el código / solución:**
- <span style="color:red"> 🔥 major:</span> La ejecución de la demo grabada demoró más de 1 hora. Esto es un tiempo bastante grande y habla de optimizaciones que no se hicieron pero que pudieron haberse hecho. 
- <span style="color:red"> ✨ ~~major:~~ </span> ~~La función update_data_fragment_step de query_updater se invoca múltiples veces entre los distintos tipos de nodo, para cada fragmento (cada review o libro, osea al menos 3 millones de veces), por ejemplo en los filtros tienen:~~

```py
    def filter_data_chunk(self,chunk: DataChunk):
        for fragment in chunk.get_fragments():
            if self.exit:
                return
            if self.filter_data_fragment(fragment):
                # -> código actualizado <-
```

~~La llamada a update_data_fragment_step parece inofensiva, pero se hace casi 2 veces por cada data fragment (2 * 3M), solamente en los filtros, y esa función por dentro tiene múltiples loops con evaluaciones del estilo _update_first_query() a las cuales se le pasa como parámetro data_fragment.clone()~~

~~¿Es necesario clonar la información? ¿Es necesario calcular varias veces en el mismo filtro, nodo, etc los "next steps"? Esto que parece inofensivo, es copiar memoria una y otra vez, millones de veces, y me parece que se puede evitar pasando una referencia al objeto en lugar de una copia, y en el filtro/nodo calculando una sóla vez los "next steps" para el fragmento. Vi uso de clone() en varios lugares, el uso de esto debe estar bien justificado, ya que implica copiar memoria y con los volúmenes de información que manejamos parece ser una mala idea. Este punto está relacionado con el primero, ya que les pega en la performance y puede contribuir a que el sistema les tarde tanto en ejecutar.~~

- <span style="color:red"> 🔥 major:</span> Otro punto relacionado al descarte temprano de datos para mejorar la eficiencia. La Query 3 pide "Títulos y autores de libros publicados en los 90' con al menos 500 reseñas.". Sin embargo, en todo los steps intermedios que tiene la Query 3, nunca descartan el atributo "review/text" (el que tiene varios bytes de texto libre), si descartaran esa información innecesaria lo antes posible, tendrán menos cantidad de datos viajando por la red y replicados en memoria, por lo tanto mejor rendimiento. El campo "review/text" se necesita únicamente para el cálculo del sentimient, cosa que podrían hacer en una etapa temprana sin pasar por varios steps (y sin hacer varios .clones() innecesarios como les marqué en el punto anterior).

- <span style="color:red"> ✨ ~~major:~~ </span> ~~Fragmento de código del Joiner~~

```py
    def save_book_in_table(self,book: Book, query_id: str):
        if query_id not in self.books_side_tables.keys():
            self.books_side_tables[query_id] = {}
        # -> código actualizado <-
```

~~¿Como funciona esta side table? Por que es necesaria una array de arrays en lugar de un mapa title -> book ? Acá pueden tener información redundante, otro punto que puede pegar en la performance. Además, vi que devuelven NACK en el joiner si falta info en la side table. Esto lo pueden evitar haciendo lo que les comenté en algún meet: primero ingestan libros, luego envían EOF, y ahi sus nodos saben que pueden empezar a leer reviews.~~

- <span style="color:red"> 🔥 major:</span> En el método read_chunk_with_columns de client.py se pueden perder mensajes si el archivo no tiene una cantidad de registros exactamente igual a un múltiplo de CHUNK_SIZE.

- <span style="color:red"> 🔥 major:</span> Uso de sleeps() en varios puntos del código, en forma injustificada. ¿A que se deben estos sleeps? Pueden evidenciar malas prácticas de sincronización.

- <span style="color:red"> 🔥 major:</span> Filtran datos en client-side. En la función "parse_data" de client.py se toman algunos atributos de data pero otros se setean en NoNe. Esto es un filtrado que se hace del lado del cliente. Esto debería hacerse del lado del servidor. Además, veo que no filtran todos los datos innecesarios, y esto les puede estar afectando a los tiempos de ejecución (ejemplo: id de review, helpfulness, summary, son datos que no se necesitan para nada). Los datos deben filtrarse del lado del servidor, el nodo data_cleaner es un buen candidato. Aquí mismo tienen que eliminar esas columnas que no se necesitan para que no les meta datos innecesarios en todas las etapas posteriores.

- <span style="color:yellow"> ❕minor:</span> Confuso algoritmo de formación de chunks y de conformación de data_fragments. En parse_data de client.py, desambiguan si el dato es un libro o una review por la cantidad de columnas del registro, si casualmente tuvieran la misma cantidad de "columnas" relevantes este algoritmo no serviría. Se confunde semánticamente los chunks que leen del disco, con los chunks de información que contienen data fragments. Toda esta sobre-comlpejidad se podría haber documentado en un diagrama de clases aprovechando la vista lógica.

- <span style="color:orange"> ⚠️ medium: </span>  El TP exige que siempre se contesten las 5 queries, esto es algo que pueden asumir y que va a ser siempre cierto. Hay 3 millones de reviews, entonces al menos van a tener 1 millón de data fragments, todos ellos con el valor de queries en "1,2,3,4,5"... Ese string tiene 9 bytes, multipliquen por 3 millones y eso son datos innecesarios que tienen en memoria, viajando por red, etc...


**<span style="color:#7DDA58"> Arreglos: </span>**
- Se eliminaron las llamadas repetidas a `update_data_fragment_step`. Ahora se hace una única vez por fragmento en cada nodo (y si es necesario, sino no).
- Se redujeorn la cantidad de clones a la mínima necesaria en `update_data_fragment_step`. Como esa función demultiplexa es imposible evitar los clones, pero se redujeron al mínimo.
- Respecto a los clones que se hacen en el nodo `counter`, fueron todos reemplazados por hacer el data fragment desde cero, ya que sólo contiene las queries que le corresponden.
- Para tener dos EOFs (uno para libros y otro para reviews), se verificó lo siguiente:
    - Los nodos counter, filter, y sentiment analyser no procesan el last data fragment, lo utilizan para enviar el chunk que corresponda.
    - El joiner usa el EOF de libros para saber que ya puede comenzar a procesar reviews, y el EOF de reviews para enviar el chunk que corresponda.
    - La función `update_data_fragment_step` actualiza el last data fragment según correponda, entendiendo que hay dos tipos (uno para books y otro para reviews).
- Respecto a la side table del joiner, no era un array de arrays, sino que un diccionario de diccionarios. Tenía la forma `{query_id1: {title1: book1, ..., titlen: bookn}, ..., query_idn: {title1: book1, ..., titlen: bookn}}`, en donde cada query guardaba los libros que le pertenecían. Es necesario diferenciar los libros por queries ya que el joiner a cierto modo funciona como filter de las reviews para las que su libro no pasó un filtro anterior. Para reducir el espacio con el que se trabaja, ahora se guarda el título del libro, y los libros completos son guardados una única vez en un diccionario auxiliar.
- Respecto al algoritmo para recibir books o reviews en el joiner, ahora se aceptan libros al principio, hasta completar la primer side table que se reciba. Luego de completar la side table se pasan a procesar las reviews. Si una review corresponde a una sidetable que todavía no se completó se manda el NACK para esa review, se aceptan libros hasta completar la side table correspondiente y luego se procesan las reviews. Se realia de esa forma debido a que se pueden necesitar más de una side table según las queries que se reciban.