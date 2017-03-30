import sys
from pyspark import SparkContext
from pyspark import sql
from pyspark.sql.types import StringType, DoubleType, BooleanType
from pyspark.sql.functions import UserDefinedFunction
from math import sin, asin, cos, sqrt, radians
import codecs


# NAMES OF MY DATA FILES: to be changed in case of other file names
PATH = "/home/eirik/Downloads/"
LISTINGS_US = "listings_us.csv"
CALENDAR_US = "calendar_us.csv"

sc = SparkContext("local", "TF-IDF")
sc.setLogLevel("ERROR")
sqlContext = sql.SQLContext(sc)

# --------------------------------------------------------------------

# LOAD listings_us
listings = sc.textFile(PATH + LISTINGS_US)
header = listings.first().split('\t')  # creates a list of the column names
listings = listings.subtract(listings.filter(lambda line: "availability_30" in line))  # remove header from RDD

# LOAD calendar_us
calendar = sc.textFile(PATH + CALENDAR_US)
header_cal = ['listing_id', 'date', 'available']
# I create a dataframe of all the dates where listings are available
df_cal = sqlContext.createDataFrame(calendar.map(lambda line: line.split('\t')), header_cal)
df_cal = df_cal.filter(df_cal.available == 't')

# CLEANING THE DATA:
df_list = sqlContext.createDataFrame(listings.map(lambda line: line.split('\t')), header)  # creating a dataframe
header = ['id', 'amenities', 'room_type', 'latitude', 'longitude', 'price',
          'name', 'accommodates']  # I now remove unnecessary columns
df_list = df_list.select([column for column in df_list.columns if column in header])

# I define a UDF to strip the price column of the "$"-sign and cast the values to doubles
price_strip = UserDefinedFunction(lambda value: str(value).strip('$').replace(',', ''), StringType())
df_list = df_list.withColumn('price', price_strip(df_list['price']).cast(DoubleType()))

# cleaning and normalizing the text in the amenities column, for them to easily to be comparable for different rows
# I remove all string punctuation but "/" and "-", which are word-connectors, and ",", which separates the amenities
udf_clean = UserDefinedFunction(lambda value: str(value.encode('utf-8')) \
                                .translate(None, """!"#$%&'()*+.:;<=>?@[\]^_`{|}~""").lower(), StringType())
df_list = df_list.withColumn('amenities', udf_clean(df_list['amenities']))
df_list.cache()  # now I save the dataframe in the cache for faster operations on it

# --------------------------------------------------------------------


print '\n###############'
print "Alternative listings:\n"


def alternative_listings(listing_id, date, price_sensitivity, radius, n):

    # First I retrieve data from the chosen listing
    chosen_listing = df_list.select('latitude', 'longitude', 'price', 'room_type', 'amenities').where(df_list.id == listing_id).collect()[0]
    lat = chosen_listing[0]
    lon = chosen_listing[1]
    max_price = chosen_listing[2] * (1+float(price_sensitivity)/100)
    type_of_room = chosen_listing[3]
    amenities = str(chosen_listing[4]).split(',')

    # A UDF for using the haversine function for filtering on my dataframe
    distance_udf = UserDefinedFunction(lambda x, y: haversine(lat, lon, x, y), DoubleType())

    # I filter all the by the constraints specified in the task, and thus find valid alternative listings
    alternatives = df_list.join(df_cal, df_list.id == df_cal.listing_id) \
        .select('id', 'name', 'amenities', 'price', 'latitude', 'longitude', 'room_type',
                (distance_udf(df_list.latitude, df_list.longitude)).alias('distance')) \
        .where(df_cal.date == date) \
        .where(df_list.room_type == type_of_room) \
        .where(df_list.price <= max_price)

    # I split the filtering up simply to be able to access the renamed distance column
    alternatives = alternatives.where(alternatives.distance <= radius) \
        .select('id', 'name', 'amenities', 'distance', 'price').collect()

    # I create a dict with all my results and a counter of similar amenities as in the input listing
    result_list = {}
    for row in alternatives:
        counter = 0
        for word in row[2].split(','):
            if word in amenities:
                counter += 1
        result_list[row[0]] = [counter, row[1], row[3], row[4]]

    # Creating the result file and writing my n results with highest number of common amenities to it
    output_file = codecs.open(PATH + "alternatives.tsv", 'w', encoding='utf8')
    output_file.write('listing_id' + '\t' + 'listing_name' + '\t' + 'number_of_common_amenities' + '\t' +
                      'distance' + '\t' + 'price' + '\t' + '\n')
    counter = 0
    for key, value in sorted(result_list.items(), key=lambda x: x[1], reverse=True):    # sort by common amenities
        output_file.write(str(key).encode('utf8') + '\t' + value[1] + '\t' +
                          str(value[0]).encode('utf8') + '\t' + str(value[2]).encode('utf8') + '\t' +
                          str(value[3]).encode('utf8') + '\n')
        counter += 1
        if counter > int(n)-1:    # I only want the top n results
            break

    print "\n###\n"
    print "The analysis is complete.", counter, "alternative listings can be found in alternatives.tsv.\n"


# --------------------------------------------------------------------


def haversine(lat1, lon1, lat2, lon2):
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [float(lon1), float(lat1), float(lon2), float(lat2)])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


# MAIN-function
if __name__ == '__main__':
    alternative_listings(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])
    # alternative_listings("4717459", "2016-12-15", 30, 20, 50)     # for faster testing

sc.stop()
