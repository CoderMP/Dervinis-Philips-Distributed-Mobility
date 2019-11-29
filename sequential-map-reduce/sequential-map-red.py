import timeit
import re
import pandas as pd
import nltk

from collections import Counter
from nltk.corpus import stopwords

# Define the set of stopwords
STOP_WORDS = stopwords.words("english")

# Adding some extra stopwords to filter out extraneous twitter data 
# (e.g. @ tags, 'rt', & symbol conversions)
twitter_stopwords = ['realdonaldtrump','amp', 'rt']
STOP_WORDS.extend(twitter_stopwords)

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
        # Split the text token
        text_token = data.split()

        # Map the split text token to the clean_word function
        text_token = map(clean_word, text_token)

        # filter the word through the list of stopwords
        text_token = filter(check_stopword, text_token)

        # Update the counter
        counter.update(text_token)
    
    # Return the top 10 common words
    return counter.most_common(10)

smr_time = timeit.timeit(lambda: find_top_words(dataset), number=1)
top_words = find_top_words(dataset)

# Output and format the result data
for word in top_words:
    print("{} : {}".format(word[0], word[1]))
    
print("Total Execution time: %.2f" %(smr_time))