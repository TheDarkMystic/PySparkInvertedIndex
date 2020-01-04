## Import statements

from pyspark import SparkConf, SparkContext

import ast
import itertools
import pprint
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from nltk.stem.porter import PorterStemmer

from nltk import word_tokenize, sent_tokenize
from nltk.corpus import stopwords


## Constants
APP_NAME = "Search Inverted Index"

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
    stop_words = set(stopwords.words('english'))
    words = [w for w in words if not w in stop_words]
    return words


# stemming of words
def stemmer(tokens):
    porter = PorterStemmer()
    stemmed = [porter.stem(word).encode('utf-8') for word in tokens]
    return stemmed


def token_to_doc(token_list, doc_name):
    token_to_doc_map = [(token[1], [(doc_name,[token[0]])]) for token in token_list]
    return token_to_doc_map
# Search Logic finding intersection of locations of multiple words in a phrase
def find_locations(s):
    finalList = [];
    tempList1 = [];
    counter = 0;
    for k in s:
        counter += 1
        for x in k[1:]:
            del tempList1[:]
            for y in x:
                for z in y[1]:
                    if (counter == 1):
                        tempList1.append(y[0] + " : " + str(z))
                        finalList = tempList1;
                    else:
                        tempList1.append(y[0] + " : " + str(z))
            finalList = set(tempList1) & set(finalList);
    return finalList


# clean query list & return set of words in each line of query file
def clean_query(query_line):
    search_keywords = set(list(query_line.lower().replace("\"", "").replace("?", "").replace("-", " ").replace("!", "") \
                               .replace("(", " ").replace(")", " ").replace(",", "").replace(".", "").replace("'",
                                                                                                              "").replace(
        ";", "").replace("#", " ").replace(":", "").split()))
    return search_keywords


# main Function
def searchQueries(sc, invertedIndex, list_of_queries):
    # read query input line-by-line from file
    query_list_rdd = sc.textFile(list_of_queries)

    # read Inverted index from file
    inverted_index_rdd = sc.textFile(invertedIndex).map(lambda x: ast.literal_eval(x))

    # Below commented code can be used for printing the search output in the form of textfiles
    # create output file
    # opfile = open('/usr/local/spark/Final_Output.txt', 'w+')

    for eachphrase in query_list_rdd.collect():
        filtered_index_rdd = inverted_index_rdd.filter(lambda (word, name_count_list): word in clean_query(eachphrase)) \
            .groupByKey().map(lambda x: (x[0], list(itertools.chain.from_iterable(x[1]))))
        print eachphrase
        # commented code can be used to print the output to the file
        # opfile.write("%s\n" % "--------------------------------------------------------------------------" )
        # opfile.write("%s %s\n" % ("Search Query ==>	", eachline))
        # opfile.write("%s\n" % "--------------------------------------------------------------------------" )
        # if len(sanitize(clean_query(eachline),stop_words)) == len(filtered_index_rdd.collect()):
        if len(filtered_index_rdd.collect()) == 0:
            print "Phrase NOT Found"
        # opfile.write("%s %s\n" % ("Location ==>"	,"Phrase NOT Found"))
        else:
            for line in list(find_locations(filtered_index_rdd.collect())):
                print line
    # opfile.write("%s %s \n" % ("Location ==>	",line))


if __name__ == "__main__":
    # Configure Spark
    conf = SparkConf().setAppName(APP_NAME)
    conf = conf.setMaster("local[8]")
    sc = SparkContext(conf=conf)

    # Path of Inverted Index
    invertedIndex = "../../InvertedIndexFile/part-00000"
    # Path of file containing SearchQueries
    list_of_queries = "../../searchQueryList/queryList.txt"
    # Call to searchQueries Function
    searchQueries(sc, invertedIndex, list_of_queries)
