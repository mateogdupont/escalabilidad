import signal
from utils.structs.book import *
from utils.structs.review import *
from utils.structs.data_fragment import *
from utils.mom.mom import MOM
from utils.query_updater import update_data_fragment_step

CATEGORY_FILTER = "CATEGORY"
YEAR_FILTER = "YEAR"
TITLE_FILTER = "TITLE"
DISTINCT_FILTER = "COUNT_DISTINCT"
SENTIMENT_FILTER = "SENTIMENT"


class Filter:
    def __init__(self):
        self._exit = False
        self.mom = MOM({'filter':None,'counter':None,'joiner_books':None,'joiner_reviews':None,'sentiment_analysis':None, 'results':None})
        signal.signal(signal.SIGTERM, self.sigterm_handler)
    
    def sigterm_handler(self):
        self._exit = True

    def filter_data_fragment(self, data_fragment: DataFragment) -> bool:
        query_info = data_fragment.get_query_info()
        filter_on, word, min_value, max_value = query_info.get_filter_params()
        book = data_fragment.get_book()
        query_info = data_fragment.get_query_info()

        if (filter_on == CATEGORY_FILTER) and (book is not None):
            return word in book.get_categories()
        elif (filter_on == YEAR_FILTER) and (book is not None):
            book_year = book.get_published_date().year
            return  min_value < book_year < max_value
        elif (filter_on == TITLE_FILTER) and (book is not None):
            return word in book.get_title()
        elif (filter_on == DISTINCT_FILTER) and (query_info is not None):
            return query_info.get_n_distinct() >= min_value
        elif (filter_on == SENTIMENT_FILTER) and (query_info is not None):
            return query_info.get_sentimentt() >= min_value
        else:
            print("Error processing data in filter")
        return False

    def run(self):
        while not self._exit:
            (data_fragment, tag) = self.mom.consume("filter")
            pass_filter = self.filter_data_fragment(data_fragment)
            if pass_filter:
                updated_dict = update_data_fragment_step(DataFragment)
                for data in updated_dict:
                    self.mom.publish(updated_dict[data],data)
            self.mom.ack(tag)

def main():
    filter = Filter()
    filter.run()
   
if __name__ == "__main__":
    main()