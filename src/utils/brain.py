"""
Query 1

 Título, autores y editoriales de los libros de categoría "Computers" entre 2000 y 2023 que contengan 'distributed' en su título.

Paso | data | routing key | parameters
Book: category, year, title, authors, publisher
1    | book |  filter      | category = "Computers"
2    | book |  filter      | year >= 2000 and year <= 2023
3    | book |  filter      | title contains 'distributed'
Return: title, authors, publisher

Query 2

 Autores con títulos publicados en al menos 10 décadas distintas

Paso | data | routing key | parameters
Book: 
1    | book |  counter    | {group_by: author, count_distinct: decade}
2    | book |  filter     | count_distinct >= 10
Return: author

Query 3

 Títulos y autores de libros publicados en los 90' con al menos 500 reseñas.

Paso | data | routing key | parameters
Book: title, authors, year
Review: book_title
1b   | book |  filter     | year >= 1990 and year <= 1999
1r   |review| counter     | {group_by: book_title, count_distinct: review}
2r   |review| filter      | count >= 500
3    | both | join        |
Return: title, authors

Query 4

 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas.

Paso | data | routing key | parameters
Book: (all)
Review: book_title, score
1b   | book |  filter     | year >= 1990 and year <= 1999
1r   |review| counter     | {group_by: book_title, count_distinct: review, avg: rating}
2r   |review| filter      | count >= 500
3    | both | join        |
4    |      | filter      | {top: 10, order_by: avg_rating}
Return: book (all), count, avg_rating

Query 5

 Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté en el percentil 90 más alto.

Paso | data | routing key | parameters
Book: title, category
Review: book_title, text
1b   | book |  filter     | category = "Fiction"
1r   |review| sentiment   | 
2r   |review| counter     | {group_by: book_title, percentile: 90, on: sentiment}
3r   |review| filter      | sentiment >= percentile
4    | both | join        |
Return: title

"""