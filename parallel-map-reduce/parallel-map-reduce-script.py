import timeit
import re
import multiprocessing
import itertools
import collections
import string

import pandas as pd

from nltk.corpus import stopwords

class SimpleMapReduce(object):
    def __init__(self, map_func, reduce_func, num_workers=None):
        """
        """
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)

    def partition(self, mapped_values):
        """
        Function that is responsible for organizing the mapped values by their key
        """
        partitioned_data = collections.defaultdict(list)

        for key,value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        """
        """
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)

        return reduced_values

def file_to_words(filename, custom_stopwords):
    """
    (str, list) -> (list)

    Function that is responsible for reading the CSV
    """
    STOP_WORDS = stopwords.words("english")
    STOP_WORDS.extend(custom_stopwords)

    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

    print(multiprocessing.current_process().name, "reading data", filename)

    output = []

    #colnames = ['Tweet_Text', 'Type', 'Media_Type', 'Hashtags', 'Tweet_Id', 'Tweet_Url', 'twt_favourites_IS_THIS_LIKE_QUESTION_MARK','Retweets']
    raw_data = pd.read_csv(filename, names='Tweet_Text')
    print(raw_data)
    tweets = raw_data.Tweet_Text.tolist()
    #dataset = raw_data.loc[:, "Tweet_Text"]

    

    #return tweets

def count_occurances(data):
    """
    Function that is responsible for converting the partitioned data for a
    word into tuple containing the words and its number of occurances
    """
    word, occurances = data
    return (word, sum(occurances))

twitter_stopwords = ['realdonaldtrump','amp', 'rt']
print(file_to_words("Donald-Tweets!.csv", twitter_stopwords))

