from pyspark import SparkContext
from pyspark import sql
from pyspark.sql.types import DoubleType, IntegerType, StringType
import pyspark.sql.functions as F
from pyspark.sql.functions import UserDefinedFunction
import pygeoj
import polygon_finder


# FUNCTIONS:
def cast_df_columns(data_frame, df_fields, new_type, udf=None):
    if udf is None:     # if no UserDefinedFunction is given as parameter
        for field in df_fields:
            data_frame = data_frame.withColumn(field, data_frame[field].cast(new_type))
    else:
        for field in df_fields:
            data_frame = data_frame.withColumn(field, udf(data_frame[field]).cast(new_type))
    return data_frame


# EXTRACT DATA:
sc = SparkContext("local", "AirBnB")

listings = sc.textFile("/home/eirik/Downloads/listings_us.csv")     # load listings_us

header = listings.first().split('\t')       # creates a list of the column names
listings = listings.subtract(listings.filter(lambda line: "availability_30" in line))   # remove header from RDD
# listings = listings.sample(True, .01, 7)     # a sample for faster calculations while testing my code


# CLEAN DATA:
sqlContext = sql.SQLContext(sc)
df = sqlContext.createDataFrame(listings.map(lambda line: line.split('\t')), header)    # creating a dataframe

# cleaning the dataset for uneccessary columns by removing every columnt but those in the new header
header = ['amenities', 'city', 'country', 'host_id','host_listings_count', 'host_name', 'id', 'latitude', 'longitude',
             'number_of_reviews', 'price', 'reviews_per_month', 'room_type', 'state', 'street', 'zipcode']
df = df.select([column for column in df.columns if column in header])


#TRANSFORM DATA:
df = cast_df_columns(df, ['host_listings_count', 'latitude', 'longitude', 'number_of_reviews', 'reviews_per_month'], DoubleType())
df = cast_df_columns(df, ['host_id', 'id', 'zipcode'], IntegerType())

price_strip = UserDefinedFunction(lambda value: value.strip('$').replace(',',''), StringType())
df = cast_df_columns(df, ['price'], DoubleType(), price_strip)

# df = df.fillna({'reviews_per_month' : 0})   # I replace all null values where I don't think they represent 'Uknknown'

df.cache()  # now, after the ETL, the dataframe is ready, and can be saved in the cache for faster operations on it
df.registerTempTable('df')   # Registering the dataframe as a table, allowing me to use sql commands on it

# # ----------------------------------------------------------------------------------------------------

# # SOLVING THE TASKS:

# 2b) count the number of each value in one category
category_count = {}
for category in header:
    category_count[category] = str(sqlContext.sql("SELECT count(DISTINCT {}) FROM df".format(category)).collect()[0][0])
for key in sorted(category_count):
    print str(key + " has").rjust(25), str(category_count[key]).rjust(6), "distinct values"

# 2c) Number of distinct cities, and a list of them
df.select('city').groupBy('city').agg({'city' : 'count'}).show()

# 2d) Characteristics of five chosen fields
for field in ['number_of_reviews', 'accommodates', 'minimum_nights', 'price']:
    sqlContext.sql("SELECT MIN({}), MAX({}), AVG({}) FROM df".format(field, field, field)).show()

# ----------------------------------------------------------------------------------------------------

# 3a) Average booking price per night
df.where(df['city'].isNotNull()).groupBy('city').agg({'price' : 'avg'}).show()

# 3b) Average booking price per room type per night
df.where(df['city'].isNotNull()).orderBy('city').groupBy('city', 'room_type').agg({'price' : 'avg'}).show()

# 3c) Average number of reviews per month
df.where(df['city'].isNotNull()).groupBy('city').agg({'reviews_per_month' : 'avg'}).show()

# 3d) Estimated number of nights booked per year
df.select('city', (df['reviews_per_month'] * 12 * 1/0.7 * 3).alias('Number of nights booked per year')).groupBy('city')\
    .agg({'Number of nights booked per year' : 'sum'}).show()

# 3e) Estimated amount of money spent on AirBnB accommodation per year
df.select('city', (df['reviews_per_month'] * 12 * 1/0.7 * 3 * df['price']).alias('Amount of $ spent on AirBnB')).groupBy('city')\
    .agg({'Amount of $ spent on AirBnB' : 'sum'}).show()

# ----------------------------------------------------------------------------------------------------

# 4a) Global average number of listings per host:
df.select('host_listings_count', 'host_id').groupBy('host_id').agg({'host_listings_count' : 'sum'})\
    .agg({'sum(host_listings_count)' : 'avg'}).show()

# 4b) Percentage of hosts with more than 1 listings:
denominator = sqlContext.sql("SELECT count(DISTINCT host_id) FROM df").collect()[0][0]
numerator = df.select('host_id', 'host_listings_count').groupBy('host_id').agg({'host_listings_count' : 'sum'}).filter('sum(host_listings_count) > 1').count()
print "Percentage = ", numerator, "/",denominator, " = ", float(numerator)/float(denominator)

# 4c) Find top 3 hosts with the highest income
calendar = sc.textFile("/home/eirik/Downloads/calendar_us.csv")
header = ['listing_id', 'date', 'available']
df_cal = sqlContext.createDataFrame(calendar.map(lambda line: line.split('\t')), header)    # creating a dataframe

for city_name in ['New York', 'San Francisco', 'Seattle']:
    print "Top 3 hosts with highest income in", city_name, ":"
    print df_cal.filter(df_cal.available == 'f').join(df.filter(df.city == city_name), df_cal.listing_id == df.id)\
        .select('host_id', 'host_name', 'listing_id', 'price')\
        .groupBy('host_id', 'host_name', 'listing_id').agg({'price' : 'sum'})\
        .groupBy('host_id', 'host_name').agg({'sum(price)': 'sum'})\
        .orderBy('sum(sum(price))', ascending=False).show(3)

# ----------------------------------------------------------------------------------------------------

# 5a) Find top 3 guests (reviewers) ranked by their number of bookings
reviews = sc.textFile("/home/eirik/Downloads/reviews_us.csv")
header = ['listing_id', 'id', 'date', 'reviewer_id', 'reviewer_name', 'comments']
df_rev = sqlContext.createDataFrame(reviews.map(lambda line: line.split('\t')), header)    # creating a dataframe
df_rev.drop('date', 'id')     # I do not need this column
udf_count_bookings = UserDefinedFunction(lambda x: '1', StringType())
df_rev = df_rev.withColumn('comments', udf_count_bookings(df_rev['comments']).cast(IntegerType()))  # the column is not necessary, but useful as number of bookings

for city_name in ['New York', 'San Francisco', 'Seattle']:
    print "Top 3 reviewers with highest number of bookings in", city_name, ":"
    print df.filter(df.city == city_name)\
        .join(df_rev, df.id == df_rev.listing_id).select('reviewer_id', 'reviewer_name', 'listing_id', (df_rev['comments']).alias('bookings'))\
        .groupBy('reviewer_id', 'reviewer_name').agg({'bookings' : 'sum'})\
        .orderBy('sum(bookings)', ascending=False).show(3)

# 5b) The guest who spent the most money  accommodation
print df.join(df_rev, df.id == df_rev.listing_id)\
    .select('reviewer_id', 'reviewer_name', (df['price']*3/0.7).alias('money_spent'))\
    .groupBy('reviewer_id', 'reviewer_name').agg({'money_spent' : 'sum'})\
    .orderBy('sum(money_spent)', ascending=False).show(1)

# ----------------------------------------------------------------------------------------------------

# 6) Assign a neighborhood name to each listing
# I import the pygeoj library at the top of the file to easier load and parse geojson-files
geo_file = pygeoj.load("/home/eirik/Downloads/neighbourhoods.geojson")
neighborhoods = open("/home/eirik/Downloads/6a_neighbourhoods.tsv", 'w')
neighborhoods.write("id" + '\t' + 'neighbourhood' + '\t' + 'city' + '\n')

for line in df.select('id', 'longitude', 'latitude', 'city').collect():
    neighborhoods.write(str(line[0]) + '\t' +
        str(polygon_finder.find_neighbourhood(geo_file, line[1], line[2])) + '\t' + str(line[3]) + '\n')
print "Finished going through all the listings."


# 6a) My results compared to the provided testing set of listings
# I create two separate dataframes for my own neighbourhood results and those from the provided test set
seattle = sc.textFile("/home/eirik/Downloads/neighbourhoods_Seattle.tsv")
header = ['id', 'neighbourhood', 'city']
df_seattle = sqlContext.createDataFrame(seattle.map(lambda line: line.split('\t')), header)

test = sc.textFile("/home/eirik/Downloads/neighborhood_test.csv")
df_test = sqlContext.createDataFrame(test.map(lambda line: line.split('\t')), header)

if df_seattle.count() == df_test.count():
    total = float(df_seattle.count())
different = float(df_seattle.join(df_test, df_seattle.id == df_test.id)\
    .where(df_seattle.neighbourhood != df_test.neighbourhood)\
    .select(df_seattle.neighbourhood).count())
print "similar / total =", total-different, '/', total, "=", (total-different)/total

# Exploring why I get other results than in the testing set - here finding differences in neighbourhoods found
for i in df_test.select('neighbourhood').distinct().subtract(df_seattle.select('neighbourhood').distinct().intersect(df_test.select('neighbourhood').distinct())).collect():
    print i[0]


# 6b) For each neighborhood, find a distinct set of amenities belonging to the listings within the neighborhood.
neighborhoods = sc.textFile("/home/eirik/Downloads/6a_neighbourhoods.tsv")
header = ['id', 'neighbourhood', 'city']
df_neighb = sqlContext.createDataFrame(neighborhoods.map(lambda line: line.split('\t')), header)

amenities_all_cities = open("/home/eirik/Downloads/6b_amenities.tsv", 'w')
amenities_all_cities.write('neighbourhood' + '\t' + 'city' + '\t' + 'amenities' + '\n')

for neighbourh in df.join(df_neighb, df.id == df_neighb.id).select('neighbourhood', 'amenities', df.city)\
    .groupBy('neighbourhood', df.city).agg(F.collect_list('amenities')).orderBy('neighbourhood').collect():
    amenities_set = set()	# I use a set to only get unique values
    for row in neighbourh[2]:
        amenities_list = sc.parallelize([row]).map(lambda x: x.split(',')).collect()
        for record in amenities_list[0]:
            record = str(record).replace('"', "").replace("{", "").replace("}", "").replace(",", "")    # cleaning
            amenities_set.add(record)
    amenities_all_cities.write(str(neighbourh[0]) + '\t' + str(neighbourh[1]) + '\t' + ','.join(str(element) for element in amenities_set) + '\n')


# 7) Visualization in Carto.com
NY_coordinates_and_price = open("/home/eirik/Downloads/carto_NY_price.tsv", 'w')
NY_coordinates_and_price.write('id' + '\t' + 'lat' + '\t' 'lon' + '\t' + 'price' + '\n')

for row in df.filter(df.city == "New York").select('id', 'latitude', 'longitude', 'price').collect():
    NY_coordinates_and_price.write(str(row[0]) + '\t' + str(row[1]) + '\t' + str(row[2]) + '\t' + str(row[3]) + '\n')
