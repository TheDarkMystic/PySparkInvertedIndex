## Import libraries

import string
from pprint import pprint

from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import *
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import string
from nltk.stem.porter import PorterStemmer
import itertools
from operator import add
from collections import Counter


import re, string, unicodedata
import nltk
import contractions
import inflect
from bs4 import BeautifulSoup
from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords
from nltk.stem import LancasterStemmer, WordNetLemmatizer
'''
1. read all files in 1 RDD:  (filename,Entire_text_from_file)
2. convert each (k,v) pair from unicode to String from RDD
3. convert the v part to lowercase
4. remove punctuation from the v
5. tokenize v into list of words
6. remove stop words
7. 

'''
## Constants
APP_NAME = " InvertedIndex"

def remove_non_ascii(words):
    """Remove non-ASCII characters from list of tokenized words"""
    new_words = []
    for word in words:
        new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')
        new_words.append(new_word)
    return new_words

def to_lowercase(words):
    """Convert all characters to lowercase from list of tokenized words"""
    new_words = []
    for word in words:
        new_word = word.lower()
        new_words.append(new_word)
    return new_words

def remove_punctuation(words):
    """Remove punctuation from list of tokenized words"""
    new_words = []
    for word in words:
        new_word = re.sub(r'[^\w\s]', '', word)
        if new_word != '':
            new_words.append(new_word)
    return new_words

def replace_numbers(words):
    """Replace all interger occurrences in list of tokenized words with textual representation"""
    p = inflect.engine()
    new_words = []
    for word in words:
        if word.isdigit():
            new_word = p.number_to_words(word)
            new_words.append(new_word)
        else:
            new_words.append(word)
    return new_words

def remove_stopwords(words):
    """Remove stop words from list of tokenized words"""
    new_words = []
    for word in words:
        if word not in stopwords.words('english'):
            new_words.append(word)
    return new_words

def stem_words(words):
    """Stem words in list of tokenized words"""
    stemmer = LancasterStemmer()
    stems = []
    for word in words:
        stem = stemmer.stem(word)
        stems.append(stem)
    return stems

def lemmatize_verbs(words):
    """Lemmatize verbs in list of tokenized words"""
    lemmatizer = WordNetLemmatizer()
    lemmas = []
    for word in words:
        lemma = lemmatizer.lemmatize(word, pos='v')
        lemmas.append(lemma)
    return lemmas

def normalize(words):
    words = remove_non_ascii(words)
    words = to_lowercase(words)
    words = remove_punctuation(words)
    words = replace_numbers(words)
    words = remove_stopwords(words)
    return words

#------------------------------------------

def uni_to_clean_str(text):
    converted_str = text.encode('utf-8')
    return converted_str.lower()


def tokenize_to_words(text):
    # split into words
    tokens = word_tokenize(text)
    # remove punctuation from each word
    #table = str.maketrans(",", string.punctuation)
    #stripped = [w.translate(table) for w in tokens]
    # remove all tokens that are not alphabetic
    words = [word for word in tokens if word.isalpha()]
    stemmed_words=stemmer(words)
    return remove_stopwords(stemmed_words)


def remove_stopwords(words):
    # filter out stop words
    stop_words = set(stopwords.words('english'))
    words = [w for w in words if not w in stop_words]
    return words

# stemming of words
def stemmer(tokens):
    porter = PorterStemmer()
    stemmed = [porter.stem(word).encode('utf-8') for word in tokens]
    return stemmed

def token_to_doc(token_list,doc_name):
    token_to_doc_map=[ (token,[doc_name])for token in token_list]
    return token_to_doc_map


# Start buildIndex Function
def buildInvertedIndex(sc, inputFiles, stop_words_list):
    inputFiles_rdd = sc.wholeTextFiles(inputFiles)
    #pprint(inputFiles_rdd.collect())

    #convert each fileName and its text from UNICODE to string
    unicode_to_str_rdd = inputFiles_rdd.map(lambda (x, y): (uni_to_clean_str(y),uni_to_clean_str(x).split('shakespeare/')[1]))
    #pprint(unicode_to_str_rdd.collect())

    cleanInputRDD=unicode_to_str_rdd.map(lambda (x, y):(tokenize_to_words(x),y))
    #pprint(cleanInputRDD.collect())

    line_enum_rdd= cleanInputRDD.flatMap(lambda (word_list,doc_name):token_to_doc(word_list,doc_name))\
                                .reduceByKey(add)

    term_freq_posting_rdd= line_enum_rdd.map(lambda (x,y): (x,len(y),y))
    pprint(term_freq_posting_rdd.collect())


    #cleanInputRDD=unicode_to_str_rdd.map(lambda (x, y):(x.split('shakespeare/')[1],tokenize_to_words(y)))
    #pprint(cleanInputRDD.collect())

    #line_enum_rdd=cleanInputRDD.map(lambda (x, y):(y,x))
    #pprint(line_enum_rdd.collect())

    #line_enum_rdd=cleanInputRDD.flatMap(lambda (x, y):map(lambda word: (word,[x]),y))
    #pprint(line_enum_rdd.collect())
    #pprint(line_enum_rdd.reduceByKey(add).collect())

    #term_freq_rdd = line_enum_rdd.reduceByKey(add).map(lambda (x,y):((x,len(y)),y))
    #pprint(term_freq_rdd.collect())

    #final_postings_rdd = term_freq_rdd.flatmap(lambda (x, y):(x, list(dict.fromkeys(y))))
    #pprint(final_postings_rdd.collect())
    #mapping_rdd=line_enum_rdd.flatMap(lambda ((lno, line), fname): map(lambda word: ((word, fname), lno), (line.split())))
    #pprint(mapping_rdd.collect())
'''
    filename_and_tokens_rdd = line_enum_rdd.map(lambda (x, y):(x,map(lambda line: tokenize_to_words(line),y)))
    #pprint(filename_and_tokens_rdd.collect())


    stopword_removed_rdd=filename_and_tokens_rdd.map(lambda (x,y): (x, map(lambda token_list:remove_stopwords(token_list),y)))
    #pprint(stopword_removed_rdd.collect())

    stemmed_rdd=stopword_removed_rdd.map(lambda (x,y): (x, map(lambda token_list:stemmer(token_list),y)))
    #pprint(stemmed_rdd.collect())

    file_to_words_flatmapped_rdd=stemmed_rdd.map(lambda(x,y):(x,list(itertools.chain.from_iterable(y))))
    #pprint(file_to_words_flatmapped_rdd.collect())

    enumerate_word_per_file_rdd=file_to_words_flatmapped_rdd.map(lambda(x,y):(x, Counter(y).items()))
    pprint(enumerate_word_per_file_rdd.collect())

    word_file_mapping_rdd=enumerate_word_per_file_rdd.map(lambda (x,y):(y,x))
    pprint(word_file_mapping_rdd.collect())

    each_word_file_rdd=word_file_mapping_rdd.map(lambda x,y: (tuple,y) for tuple in (x))

'''
'''

    
    #input_file_lineOffset = cleanRDD.flatMap(lambda (x, y): map(lambda line: (line, x.split('invertedindex', 1)[1]), enumerate(y.split('\n'), 1)))
    input_file_lineOffset = cleanRDD.flatMap(
        lambda (x, y): map(lambda line: (line, x.split('shakespeare', 1)[1]), enumerate(y.split('\n'), 1))) \
        .flatMap(lambda ((lno, line), fname): map(lambda word: ((word, fname), lno), (line.split()))).groupByKey() \
        .mapValues(set).mapValues(list)
    pprint(input_file_lineOffset.collect())

    # aggregate word offset List in the searchable format
    inverted_index_rdd = input_file_lineOffset.map(lambda ((wrd, fname), lst): ((wrd), ((fname, lst)))) \
        .groupByKey().mapValues(list)

    pprint(inverted_index_rdd.collect())'''



# Configuration file
if __name__ == "__main__":
    # Configuration for Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[8]")
    sc = SparkContext(conf=conf)
    inputFiles = "../../data/Shakespeare*"
    stop_words = set(stopwords.words('english'))
    '''
    >>> set(stopwords.words('english'))
    {'ourselves', 'hers', 'between', 'yourself', 'but', 'again', 'there', 'about', 'once', 'during', 'out', 'very', 
    'having', 'with', 'they', 'own', 'an', 'be', 'some', 'for', 'do', 'its', 'yours', 'such', 'into', 'of', 'most', 
    'itself', 'other', 'off', 'is', 's', 'am', 'or', 'who', 'as', 'from', 'him', 'each', 'the', 'themselves', 'until',
    'below', 'are', 'we', 'these', 'your', 'his', 'through', 'don', 'nor', 'me', 'were', 'her', 'more', 'himself', 
    'this', 'down', 'should', 'our', 'their', 'while', 'above', 'both', 'up', 'to', 'ours', 'had', 'she', 'all', 'no',
    'when', 'at', 'any', 'before', 'them', 'same', 'and', 'been', 'have', 'in', 'will', 'on', 'does', 'yourselves', 
    'then', 'that', 'because', 'what', 'over', 'why', 'so', 'can', 'did', 'not', 'now', 'under', 'he', 'you', 
    'herself', 'has', 'just', 'where', 'too', 'only', 'myself', 'which', 'those', 'i', 'after', 'few', 'whom', 
    't', 'being', 'if', 'theirs', 'my', 'against', 'a', 'by', 'doing', 'it', 'how', 'further', 'was', 'here', 
    'than'}
    '''
    # stop_words.add("be","me")
    # Start Building Inverted Index
    buildInvertedIndex(sc, inputFiles, stop_words)
