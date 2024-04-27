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
1r   |review| counter     | {group_by: book_title, count_distinct: review, avg: score} (avg for query 4)
2r   |review| filter      | count >= 500
3    | both | join        |
Return: title, authors

Query 4

 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas.

Paso | data | routing key | parameters
Book: (all)
Review: book_title, score
1b   | book |  filter     | year >= 1990 and year <= 1999
1r   |review| counter     | {group_by: book_title, count_distinct: review, avg: score}
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
2r   |review| counter     | {group_by: book_title, percentile: (90, sentiment)}
3r   |review| filter      | sentiment >= percentile
4    | both | join        |
Return: title

"""

from utils.structs.data_fragment import DataFragment
import logging as logger

def update_first_query(data_fragment: DataFragment) -> dict[DataFragment, str]:
    if data_fragment.get_book() is None:
        return {}
    logger.info("Updating first query DataFragment")
    querys = data_fragment.get_querys()
    query_info = data_fragment.get_query_info()
    step = querys[1]
    querys[1] += 1
    data_fragment.set_querys(querys)
    if step == 0:
        query_info.set_filter_params("CATEGORY", "Computers", None, None, None)
    elif step == 1:
        query_info.set_filter_params("YEAR", None, 2000, 2023, None)
    elif step == 2:
        query_info.set_filter_params("TITLE", "distributed", None, None, None)
    elif step == 3:
        # TODO: delete unwanted columns
        logger.info("Next step is to return the results")
        return {data_fragment: "results"}
    data_fragment.set_query_info(query_info)
    logger.info("Next step is to filter")
    return {data_fragment: "filter"}

def update_second_query(data_fragment: DataFragment) -> dict[DataFragment, str]:
    if data_fragment.get_book() is None:
        return {}
    logger.info("Updating second query DataFragment")
    querys = data_fragment.get_querys()
    query_info = data_fragment.get_query_info()
    step = querys[2]
    querys[2] += 1
    data_fragment.set_querys(querys)
    if step == 0:
        query_info.set_counter_params("AUTHOR", "DECADE", None, None)
        data_fragment.set_query_info(query_info)
        logger.info("Next step is to count")
        return {data_fragment: "counter"}
    elif step == 1:
        query_info.set_filter_params("COUNT_DISTINCT", None, 10, None, None)
        data_fragment.set_query_info(query_info)
        logger.info("Next step is to filter")
        return {data_fragment: "filter"}
    
def update_third_and_fourth_query(data_fragment: DataFragment) -> dict[DataFragment, str]:
    querys = data_fragment.get_querys()
    query_info = data_fragment.get_query_info()
    step = querys[3] if 3 in querys.keys() else querys[4]
    if 3 in querys.keys():
        logger.info("Updating third query DataFragment")
        querys[3] += 1
    if 4 in querys.keys():
        logger.info("Updating fourth query DataFragment")
        querys[4] += 1
    data_fragment.set_querys(querys)
    if step == 0:
        if data_fragment.get_book() is not None:
            query_info.set_filter_params("YEAR", None, 1990, 1999, None)
            data_fragment.set_query_info(query_info)
            # the next step for the book is to join with the review
            if 3 in querys.keys():
                querys[3] += 1
            if 4 in querys.keys():
                querys[4] += 1
            data_fragment.set_querys(querys)
            logger.info("Next step is to filter")
            return {data_fragment: "filter"}
        else:
            query_info.set_counter_params("BOOK_TITLE", "REVIEW", "SCORE", None)
            data_fragment.set_query_info(query_info)
            logger.info("Next step is to count")
            return {data_fragment: "counter"}
    elif step == 1:
        query_info.set_filter_params("COUNT_DISTINCT", None, 500, None, None)
        data_fragment.set_query_info(query_info)
        logger.info("Next step is to filter")
        return {data_fragment: "filter"}
    elif step == 2:
        if data_fragment.get_book() is not None:
            logger.info("Next step is to join (goes to books queue)")
            return {data_fragment: "joiner_books"}
        else:
            logger.info("Next step is to join (goes to reviews queue)")
            return {data_fragment: "joiner_reviews"}
    
    next_steps = {}
    if step == 3:
        if 3 in querys.keys() and 4 in querys.keys():
            logger.info("The data fragment will be splitted in two")
        if 3 in querys.keys():
            next_steps[data_fragment] = "results"
            logger.info("Next step is to return the results (query 3)")
        if 4 in querys.keys():
            new_data_fragment = data_fragment.clone()
            query_info = new_data_fragment.get_query_info()
            query_info.set_filter_params(None, None, None, None, (10, "AVERAGE"))
            new_data_fragment.set_query_info(query_info)
            next_steps[new_data_fragment] = "filter"
            logger.info("Next step is to filter (query 4)")
        return next_steps
    if step == 4:
        logger.info("Next step is to return the results")
        return {data_fragment: "results"}
    
def update_fifth_query(data_fragment: DataFragment) -> dict[DataFragment, str]:
    logger.info("Updating fifth query DataFragment")
    querys = data_fragment.get_querys()
    query_info = data_fragment.get_query_info()
    step = querys[5]
    querys[5] += 1
    data_fragment.set_querys(querys)
    if step == 0:
        if data_fragment.get_book() is not None:
            querys[5] += 2
            data_fragment.set_querys(querys)
            query_info.set_filter_params("CATEGORY", "Fiction", None, None, None)
            data_fragment.set_query_info(query_info)
            logger.info("Next step is to filter")
            return {data_fragment: "filter"}
        else:
            logger.info("Next step is to sentiment analysis")
            return {data_fragment: "sentiment_analysis"}
    if step == 1:
        query_info.set_counter_params("BOOK_TITLE", None, None, (90, "SENTIMENT"))
        data_fragment.set_query_info(query_info)
        logger.info("Next step is to count")
        return {data_fragment: "counter"}
    if step == 2:
        percentile_90 = query_info.get_percentile()
        logger.info(f"The percentile 90 is: {percentile_90}")
        query_info.set_filter_params("SENTIMENT", None,  percentile_90, None, None)
        data_fragment.set_query_info(query_info)
        logger.info("Next step is to filter")
        return {data_fragment: "filter"}
    if step == 3:
        if data_fragment.get_book() is not None:
            logger.info("Next step is to join (goes to books queue)")
            return {data_fragment: "joiner_books"}
        else:
            logger.info("Next step is to join (goes to reviews queue)")
            return {data_fragment: "joiner_reviews"}
    if step == 4:
        logger.info("Next step is to return the results")
        return {data_fragment: "results"}

def update_data_fragment_step(data_fragment: DataFragment) -> dict[DataFragment, str]:
    querys = data_fragment.get_querys()
    logger.info(f"Updating data fragment with querys: {querys}")

    next_steps = {}
    
    if 1 in querys.keys():
        for datafragment, key in update_first_query(data_fragment.clone()).items():
            next_steps[datafragment] = key
    
    if 2 in querys.keys():
        for datafragment, key in update_second_query(data_fragment.clone()).items():
            next_steps[datafragment] = key
        
    if 3 in querys.keys() or 4 in querys.keys():
        for datafragment, key in update_third_and_fourth_query(data_fragment.clone()).items():
            next_steps[datafragment] = key
    
    if 5 in querys.keys():
        for datafragment, key in update_fifth_query(data_fragment.clone()).items():
            next_steps[datafragment] = key
    
    return next_steps
                    