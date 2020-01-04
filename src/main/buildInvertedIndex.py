## Import libraries


import pprint
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from nltk.stem.porter import PorterStemmer
from operator import add
import re, string, unicodedata
import inflect
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


# ------------------------------------------

def uni_to_clean_str(text):
    converted_str = text.encode('utf-8')
    return converted_str.lower()


def tokenize_to_words(text):
    # split into words
    tokens = word_tokenize(text)
    # remove punctuation from each word
    # table = str.maketrans(",", string.punctuation)
    # stripped = [w.translate(table) for w in tokens]
    # remove all tokens that are not alphabetic
    words = [word for word in tokens if word.isalpha()]
    stemmed_words = stemmer(words)
    return remove_stopwords(stemmed_words)


def remove_stopwords(words):
    # filter out stop words
    stop_words_list = set(stopwords.words('english'))
    words = [w for w in words if w not in stop_words_list]
    return words


# stemming of words
def stemmer(tokens):
    porter = PorterStemmer()
    stemmed = [porter.stem(word).encode('utf-8') for word in tokens]
    return stemmed


def token_to_doc(token_list, doc_name):
    token_to_doc_map = [(token[1], [(doc_name, [token[0]])]) for token in token_list]
    return token_to_doc_map


# [('f1.txt', [0]), ('f2.txt', [3]), ('f2.txt', [4])]

# [('f1.txt', [0]), ('f2.txt', [3,4])]

def compress_positions(index_line):
    posting_list = index_line[1]
    word_pos_dict = dict()
    for doc_pos_tuple in posting_list:
        try:
            word_pos_dict[doc_pos_tuple[0]].append(doc_pos_tuple[1][0])
        except:
            word_pos_dict[doc_pos_tuple[0]] = doc_pos_tuple[1]
    return index_line[0], list(word_pos_dict.items())


def enumerate_tokens_pos(word_list, doc_name):
    return list(enumerate(word_list)), doc_name


# Start buildIndex Function
def buildInvertedIndex(sc, inputFiles):
    inputFiles_rdd = sc.wholeTextFiles(inputFiles)
    # pprint.pprint(inputFiles_rdd.collect())

    inputFiles_rdd_partitioned = inputFiles_rdd.repartition(4);
    # convert each fileName and its text from UNICODE to string
    unicode_to_str_rdd = inputFiles_rdd_partitioned.map(
        lambda (x, y): (uni_to_clean_str(y), uni_to_clean_str(x).split('shakespeare/')[1]))
    # pprint(unicode_to_str_rdd.collect())

    cleanInputRDD = unicode_to_str_rdd.map(lambda (x, y): (tokenize_to_words(x), y))
    # pprint(cleanInputRDD.collect())

    word_enum_rdd = cleanInputRDD.map(lambda (word_list, doc_name): enumerate_tokens_pos(word_list, doc_name))
    # pprint.pprint(word_enum_rdd.collect())

    line_enum_rdd = word_enum_rdd.flatMap(lambda (word_list, doc_name): token_to_doc(word_list, doc_name)) \
        .reduceByKey(add) \
        .sortByKey()

    compressed_pos_rdd = line_enum_rdd.map(lambda index_line: compress_positions(index_line))
    # There is a PairRDD function named "collectAsMap" that returns a dictionary from a RDD.
    pprint.pprint(compressed_pos_rdd.collectAsMap())


    # final Inverted Index
    compressed_pos_rdd.coalesce(1).saveAsTextFile("../../InvertedIndexFile/")


# Configuration file
if __name__ == "__main__":
    # Configuration for Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[8]")
    sc = SparkContext(conf=conf)
    inputFiles = "../../data/Shakespeare*"
    stop_words = set(stopwords.words('english'))
    # stop_words.add("be","me")
    # Start Building Inverted Index
    buildInvertedIndex(sc, inputFiles)
