import collections
import itertools
import multiprocessing
import time
import string
import pandas as pd
from nltk.corpus import stopwords


class SimpleMapReduce(object):

    def __init__(self, map_func, reduce_func, num_workers=None):
        self.map_func = map_func
        self.reduce_func = reduce_func
        self.pool = multiprocessing.Pool(num_workers)

    def partition(self, mapped_values):
        partitioned_data = collections.defaultdict(list)
        for key, value in mapped_values:
            partitioned_data[key].append(value)
        return partitioned_data.items()

    def __call__(self, inputs, chunksize=1):
        map_responses = self.pool.map(self.map_func, inputs, chunksize=chunksize)
        partitioned_data = self.partition(itertools.chain(*map_responses))
        reduced_values = self.pool.map(self.reduce_func, partitioned_data)
        return reduced_values

def file_to_words(filename):
    STOP_WORDS = stopwords.words('english')
    twitter_stopwords = ['realdonaldtrump','amp', 'rt', 'co', 'https', 'http', 'trump']
    STOP_WORDS.extend(twitter_stopwords)

    TR = str.maketrans(string.punctuation, ' ' * len(string.punctuation))

    output = []

    with open(filename, "r") as f:
        for line in f:
            if line.lstrip().startswith('..'): # Skip rst comment lines
                continue
            line = line.translate(TR) # Strip punctuation
            for word in line.split():
                word = word.lower()
                if word.isalpha() and word not in STOP_WORDS:
                    output.append( (word, 1) )
    return output


def count_words(item):
    word, occurances = item
    return (word, sum(occurances))


if __name__ == '__main__':
    import operator
    import glob

    input_files = glob.glob('*.txt')

    mapper = SimpleMapReduce(file_to_words, count_words)
    word_counts = mapper(input_files)
    word_counts.sort(key=operator.itemgetter(1))
    word_counts.reverse()

    print ('\nTop 10 Words Trump Tweets\n')
    top10 = word_counts[:10]
    longest = max(len(word) for word, count in top10)
    for word, count in top10:
        print ('%-*s: %5s' % (longest+1, word, count))

    print(time.process_time(), " Seconds")
