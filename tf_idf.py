import sys
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql.types import StringType, BooleanType
import pyspark.sql.functions as F
from pyspark.sql.functions import UserDefinedFunction
import codecs

# NAMES OF MY DATA FILES: to be changed in case of other file names
LISTINGS_US = "listings_us.csv"
NEIGHBORHOODS_FILE = "listings_with_neighbourhoods.tsv"

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")
sqlContext = sql.SQLContext(sc)

# --------------------------------------------------------------------

print '\n###############\n'
print "TF-IDF Assignment\n"


def tf_idf(path_to_data_sets, category, test_example):

    # LOADING DATA
    listings = sc.textFile(path_to_data_sets + LISTINGS_US)  # load listings_us
    header = listings.first().split('\t')  # creates a list of the column names
    listings = listings.subtract(listings.filter(lambda line: "availability_30" in line))  # remove header from RDD

    # CLEANING THE DATA:
    df_list = sqlContext.createDataFrame(listings.map(lambda line: line.split('\t')), header)  # creating a dataframe
    header = ['id', 'description']  # I now remove all columns but those in the new header
    df_list = df_list.select([column for column in df_list.columns if column in header])

    # cleaning and normalizing the text in the description column
    udf = UserDefinedFunction(
        lambda value: str(value.encode('utf-8')).translate(None, """!"#$%&'()*+,.:;<=>?@[\]^_`{|}~""") \
            .lower(), StringType())  # I replace all string punctuation but "/" and "-" as they connect words
    df_list = df_list.withColumn('description', udf(df_list['description']))
    df_list.registerTempTable('df_list')  # Registering the dataframe as a table, allowing me to use sql commands on it
    df_list.cache()  # now I save the dataframe in the cache for faster operations on it


    # RUNNING THE PROGRAM BASED ON INPUT PARAMETERS
    if category == "-l":    # if the flag marks a search among the listings
        description = tf_idf_listing(df_list, test_example)
    elif category == "-n":    # if the flag marks a search for a neighbourhood
        description = tf_idf_neighb(path_to_data_sets, df_list, test_example)
    else:   # if wrong input
        print "Wrong parameters. Function did not run."
        return 0

    description = description.split(' ')  # split up string into list of words
    description = list(filter(lambda word: word != '', description))  # remove words (list elements) that are empty

    n_documents = float(df_list.select('id').distinct().count())
    n_terms = float(len(description))
    term_list = {}

    # I create a UDF to check if documents contain a term or not
    for term in set(description):  # I use a set to iterate only one time through each distinct word

        # I cannot manage to reference a changing variable in my UDF, therefore I need to have the udf inside the loop
        contains = UserDefinedFunction(lambda lst: True if term in lst.split(' ') else False, BooleanType())

        docs_with_term = df_list.where(contains(df_list.description)).select(df_list.id).count()
        tf = float(description.count(term)) / n_terms
        idf = n_documents / docs_with_term
        term_list[term] = float(tf * idf)

        if term == "choic":  # for testing purposes
            print "term:", term
            print "terms in this document:", float(n_terms)
            print "times in this document:", float(description.count(term))
            print "docs with this term:", docs_with_term

    # Creating the result file and its header
    output_file = codecs.open(path_to_data_sets + "tf_idf_results.tsv", 'w', encoding='utf8')
    output_file.write('term' + '\t' + 'TF-IDF' + '\n')

    counter = 0
    for key, value in sorted(term_list.items(), key=lambda x: x[1], reverse=True):
        output_file.write(key + '\t' + str(value).encode('utf8') + '\n')
        counter += 1
        if counter > 99:    # I only want the top 100 results
            break

    print "\n###\n"
    print "The analysis is complete.", counter, "terms and their TF-IDF weight are now saved in tf_idf_results.tsv.\n"


# Function for analyzing a listing
def tf_idf_listing(df_list, listing_id):
    print "\nAnalyzing your chosen listing......"
    description = df_list.where(df_list.id == listing_id).select('description').collect()[0][0]
    return description


# Function for analyzing a neighborhood
def tf_idf_neighb(path, df_list, neighb_name):
    print "\nAnalyzing your chosen neigbourhood......"

    # I load the data set with neighbourhoods and listing_ids I created in project 1
    neighborhoods = sc.textFile(path + NEIGHBORHOODS_FILE)
    header = ['id', 'neighbourhood']
    df_neighb = sqlContext.createDataFrame(neighborhoods.map(lambda line: line.split('\t')), header)

    # I join my to data frames on the listing_id, group by neighbourhoods and concatenate their descriptions
    description = df_list.join(df_neighb, df_list.id==df_neighb.id).groupBy(df_neighb.neighbourhood)\
        .agg(F.collect_list(df_list.description)).where(df_neighb.neighbourhood == neighb_name).collect()[0][1][0]
    return description


# MAIN-function
if __name__ == '__main__':
    tf_idf(sys.argv[1], sys.argv[2], sys.argv[3])
    # tf_idf("/home/eirik/Downloads/", "-l", "176129")     # for faster testing
    # tf_idf("/home/eirik/Downloads/", "-n", "Midtown")     # for faster testing

sc.stop()
