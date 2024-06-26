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
2    | both | join        |
3    |      | counter     | {group_by: book_title, count_distinct: review, avg: score} (avg for query 4)
4    |      | filter      | count >= 500
Return: title, authors

Query 4

 10 libros con mejor rating promedio entre aquellos publicados en los 90’ con al menos 500 reseñas.

Paso | data | routing key | parameters
Book: (all)
Review: book_title, score
1b   | book |  filter     | year >= 1990 and year <= 1999
2    | both | join        |
3    |      | counter     | {group_by: book_title, count_distinct: review, avg: score}
4    |      | filter      | count >= 500
5    |      | filter      | {top: 10, order_by: avg_rating}
Return: book (all), count, avg_rating

Query 5

 Títulos en categoría "Fiction" cuyo sentimiento de reseña promedio esté en el percentil 90 más alto.

Paso | data | routing key | parameters
Book: title, category
Review: book_title, text
1b   | book |  filter     | category = "Fiction"
2    | both | join        |
3    |      | sentiment   | 
4    |      | counter     | {group_by: book_title, percentile: (90, sentiment)}
5    |      | filter      | sentiment >= percentile
Return: title

"""

import time
from utils.structs.data_fragment import DataFragment
import logging as logger

Q1 = "Q1"
Q2 = "Q2"
Q34 = "Q34"
Q5 = "Q5"

def _update_first_query(data_fragment: DataFragment) -> 'dict[DataFragment, str]':
    if data_fragment.get_review():
        return {}
    #logger.info("Updating first query DataFragment")
    queries = data_fragment.get_queries()
    queries = {key: value for key, value in queries.items() if key == 1}
    query_info = data_fragment.get_query_info()
    step = queries[1]
    queries[1] += 1
    data_fragment.set_queries(queries)
    if step == 0:
        query_info.set_filter_params("CATEGORY", "Computers", None, None, None)
    elif step == 1:
        query_info.set_filter_params("YEAR", None, 2000, 2023, None)
    elif step == 2:
        query_info.set_filter_params("TITLE", "distributed", None, None, None)
    elif step == 3:
        #logger.info("Next step is to return the results")
        return {data_fragment: "results"}
    data_fragment.set_query_info(query_info)
    #logger.info("Next step is to filter")
    return {data_fragment: "filter"}

def _update_second_query(data_fragment: DataFragment) -> 'dict[DataFragment, str]':
    if data_fragment.get_review():
        return {}
    #logger.info("Updating second query DataFragment")
    queries = data_fragment.get_queries()
    queries = {key: value for key, value in queries.items() if key == 2}
    query_info = data_fragment.get_query_info()
    step = queries[2]
    queries[2] += 1
    data_fragment.set_queries(queries)
    if step == 0:
        query_info.set_counter_params("AUTHOR", "DECADE", None, None)
        data_fragment.set_query_info(query_info)
        #logger.info("Next step is to count")
        return {data_fragment: "counter"}
    elif step == 1:
        query_info.set_filter_params("COUNT_DISTINCT", None, 10, None, None)
        data_fragment.set_query_info(query_info)
        #logger.info("Next step is to filter")
        return {data_fragment: "filter"}
    elif step == 2:
        #logger.info("Next step is to return the results")
        return {data_fragment: "results"}
    else:
        logger.info("Invalid step in query 2")
    
def _update_third_and_fourth_query(data_fragment: DataFragment) -> 'dict[DataFragment, str]':
    # if data_fragment.is_last():
    #     logger.info("DataFragment is last")
    queries = data_fragment.get_queries()
    queries = {key: value for key, value in queries.items() if key == 3 or key == 4}
    query_info = data_fragment.get_query_info()
    step = queries[3] if 3 in queries.keys() else queries[4]
    if 3 in queries.keys():
        queries[3] += 1
    if 4 in queries.keys():
        queries[4] += 1
    data_fragment.set_queries(queries)

    next_steps = {}
    # logger.info(f"Step: {step}")

    if step == 0:
        if data_fragment.get_book() is not None:
            new_data_fragment = data_fragment.clone()
            new_query_info = new_data_fragment.get_query_info()
            new_query_info.set_filter_params("YEAR", None, 1990, 1999, None)
            new_data_fragment.set_query_info(new_query_info)
            next_steps[new_data_fragment] = "filter"
            # logger.info("queries 3-4 | step 0 | im not a review | going to filter")
        if data_fragment.get_review() is not None:
            if 3 in queries.keys():
                queries[3] += 1
            if 4 in queries.keys():
                queries[4] += 1
            data_fragment.set_queries(queries) # goes to 2 directly
            next_steps[data_fragment] = "joiner_reviews"
            # logger.info("queries 3-4 | step 0 | im not a book | going to joiner_reviews")

    if step == 1:
        next_steps[data_fragment] = "joiner_books"
        # logger.info("queries 3-4 | step 1 | going to joiner_books")

    if step == 2:
        query_info.set_counter_params("BOOK_TITLE", "REVIEW", "SCORE", None)
        data_fragment.set_query_info(query_info)
        next_steps[data_fragment] = "counter"
        # logger.info("queries 3-4 | step 2 | going to counter")

    if step == 3:
        query_info.set_filter_params("COUNT_DISTINCT", None, 500, None, None)
        data_fragment.set_query_info(query_info)
        next_steps[data_fragment] = "filter"
        # logger.info("queries 3-4 | step 3 | going to filter")

    if step == 4:
        if 4 in queries.keys():
            new_queries = {}
            new_queries[4] = queries[4]
            new_data_fragment = data_fragment.clone()
            new_data_fragment.set_queries(new_queries)
            new_query_info = new_data_fragment.get_query_info()
            new_query_info.set_filter_params(None, None, None, None, (10, "AVERAGE"))
            new_data_fragment.set_query_info(new_query_info)
            next_steps[new_data_fragment] = "counter"
        if 3 in queries.keys():
            new_queries = {}
            new_queries[3] = queries[3]
            data_fragment.set_queries(new_queries)
            next_steps[data_fragment] = "results"
            # logger.info("query 3 | step 4 | going to results")
            # logger.info("query 4 | step 4 | going to filter")
    
    if step == 5:
        next_steps[data_fragment] = "results"
        # logger.info("query 4 | step 5 | going to results")
    # logger.info("---------------")
    return next_steps
    
def _update_fifth_query(data_fragment: DataFragment) -> 'dict[DataFragment, str]':
    #logger.info("Updating fifth query DataFragment")
    queries = data_fragment.get_queries()
    queries = {key: value for key, value in queries.items() if key == 5}
    query_info = data_fragment.get_query_info()
    step = queries[5]
    queries[5] += 1
    data_fragment.set_queries(queries)

    next_steps = {}

    if step == 0:
        if data_fragment.get_book() is not None:
            new_data_fragment = data_fragment.clone()
            new_query_info = new_data_fragment.get_query_info()
            new_query_info.set_filter_params("CATEGORY", "Fiction", None, None, None)
            new_data_fragment.set_query_info(new_query_info)
            next_steps[new_data_fragment] = "filter"
        if data_fragment.get_review() is not None:
            if 5 in queries.keys():
                queries[5] += 1
            data_fragment.set_queries(queries) # goes to 2 directly
            next_steps[data_fragment] = "joiner_reviews"

    if step == 1:
        next_steps[data_fragment] = "joiner_books"

    if step == 2:
        next_steps[data_fragment] = "sentiment_analysis"

    if step == 3:
        query_info.set_counter_params("BOOK_TITLE", None, None, (90, "SENTIMENT"))
        data_fragment.set_query_info(query_info)
        next_steps[data_fragment] = "counter"

    if step == 4:
        if not data_fragment.is_last():
            percentile_90 = query_info.get_percentile()
            query_info.set_filter_params("SENTIMENT", None,  percentile_90, None, None)
            data_fragment.set_query_info(query_info)
        next_steps[data_fragment] = "filter"

    if step == 5:
        next_steps[data_fragment] = "results"

    return next_steps

def _paths(data_fragment: DataFragment, queries: dict) -> dict:
    """
    This function demultiplexes the data fragment to follow different paths
    Returns a data fragment to update in each query

    - If the path to follow is unique, the same data fragment is returned
    - If there are multiple paths, this will return a dictionary with a data fragment
      to update for each query

    This function is used to return n data fragments from one (now is up to 4 data fragments),
    to follow different paths, minimizing the amount of clones needed
    """
    paths = {}
    if 1 in queries.keys():
        paths[Q1] = data_fragment
    
    if 2 in queries.keys() and len(paths.keys()) == 0:
        paths[Q2] = data_fragment
    elif 2 in queries.keys():
        paths[Q2] = data_fragment.clone()
        
    if (3 in queries.keys() or 4 in queries.keys()) and len(paths.keys()) == 0:
        paths[Q34] = data_fragment
    elif 3 in queries.keys() or 4 in queries.keys():
        paths[Q34] = data_fragment.clone()

    if 5 in queries.keys() and len(paths.keys()) == 0:
        paths[Q5] = data_fragment
    elif 5 in queries.keys():
        paths[Q5] = data_fragment.clone()
    
    return paths

def update_data_fragment_step(data_fragment: DataFragment) -> 'dict[DataFragment, str]':
    # logger.info(f"Updating DataFragment\n{data_fragment.to_json()}")
    # if data_fragment.get_query_info().is_clean_flag():
    #     return {data_fragment: "info_all"}
    
    queries = data_fragment.get_queries()
    next_steps = {}
    paths = _paths(data_fragment, queries) # => we need to check if the datafragment path 
                                           # is unique or we need to split it to different
                                           # paths for each query
    # logger.info(f"Paths: {paths}")
    
    if 1 in queries.keys():
        # logger.info("Updating first query")
        df = paths[Q1]
        next_step = _update_first_query(df)
        if len(next_step) == 1: # the path of the datafragment in the query 1 is unique
            datafragment, key = next_step.popitem()
            next_steps[datafragment] = key
            
    if 2 in queries.keys():
        # logger.info("Updating second query")
        df = paths[Q2]
        next_step = _update_second_query(df)
        if len(next_step) == 1: # the path of the datafragment in the query 2 is unique
            datafragment, key = next_step.popitem()
            next_steps[datafragment] = key
        
    if 3 in queries.keys() or 4 in queries.keys():
        # logger.info("Updating third and fourth query")
        df = paths[Q34]
        next_step = _update_third_and_fourth_query(df)
        for datafragment, key in next_step.items(): # this for loop will have up to 2 iterations (because of the
            next_steps[datafragment] = key                                  # demux at the end of q3, when q3 and q4 takes different paths)
    
    if 5 in queries.keys():
        # logger.info("Updating fifth query")
        df = paths[Q5]
        next_step = _update_fifth_query(df)
        if len(next_step) == 1: # the path of the datafragment in the query 5 is unique
            datafragment, key = next_step.popitem()
            next_steps[datafragment] = key
    
    # if data_fragment.is_last():
    #     time.sleep(10) # dont delete this!
    #     logger.info("DataFragment is last - - - -")
    #     logger.info(next_steps)
    #     for datafragment, key in next_steps.items():
    #         logger.info(f"El fragment es {datafragment.to_json()}")
    #         logger.info(f"El destino es {key}")
    # logger.info(f"Next steps: {next_steps}")
    
    return next_steps
                    