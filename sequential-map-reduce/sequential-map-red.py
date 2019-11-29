import timeit
import re
import pandas as pd
import nltk

from collections import Counter
from nltk.corpus import stopwords

# Define the set of stopwords
STOP_WORDS = set(stopwords.words("english"))

# Import the dataset
raw_data = pd.read_csv("Donald-Tweets!.csv")
dataset = raw_data.loc[:,"Tweet_Text"]

def clean_word(word):
    """
    (str) -> (str)

    Function that is responsible for cleaning the words from
    any special characters
    """
    return re.sub(r'[^\w\s]', '', word).lower()

def check_stopword(word):
    """
    (str) -> str

    Function that is responsible for checking whether or not the
    current word is in the set of stopwords
    """
    if word not in STOP_WORDS and word.isalpha():
        return word

def find_top_words(dataset):
    """
    (list) -> list

    Function that is responsible for finding the top words in the dataset
    """
    counter = Counter()

    # Iterate through the dataset and isolate the top recurring words
    for data in dataset:
        text_token = data.split()
        text_token = map(clean_word, text_token)
        text_token = filter(check_stopword, text_token)

        counter.update(text_token)
    
    # Return the top 10 common words
    return counter.most_common(10)

print(timeit.timeit(lambda: print(find_top_words(dataset)), number=1))