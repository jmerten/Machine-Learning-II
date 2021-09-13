import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import round

# Create a SparkSession (Note, the config section is only for Windows!)
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///C:/temp").appName("M6individual").getOrCreate()

def loadMovieData():
    movieNames = {}
    with open("./BUAD5132/M6/Individual/ml-100k/u.ITEM") as f:
        for line in f:
            fields = line.split('|')
            if fields[1] == 'unknown':
                continue
            else:
                movieNames[int(fields[0])] = (fields[1],fields[2],fields[5:len(fields)])
                movieNames[int(fields[0])][2][-1] = movieNames[int(fields[0])][2][-1].split('\n')[0]
    return movieNames

# Load base movie data
lines = spark.sparkContext.textFile("./BUAD5132/M6/Individual/ml-100k/u.data")
rating = lines.map(lambda x: Row(movieID = int(x.split()[1]), 
                                 userID = int(x.split()[0]), 
                                 rating = int(x.split()[2])))
# Infer the schema, and register the DataFrame as a table.
schemaMovies = spark.createDataFrame(rating)


topMovieIDs = schemaMovies.groupBy('movieID').count().orderBy('count',ascending=False).cache()

movieRatings = schemaMovies.groupBy('movieID').agg({'rating':'sum'}).cache()

avgRatings = topMovieIDs.where(topMovieIDs['count'] > 100).join(movieRatings,'movieID').cache()
avgRatings = avgRatings.withColumn('avgRating',round(avgRatings['sum(rating)']/avgRatings['count'],3)).orderBy('avgRating',ascending=False).cache()

avgRatings.show()

top10 = avgRatings.take(10)

# Load up our movie ID -> name dictionary
nameDict = loadMovieData()

# Print the results
print("\n")
for result in top10:
    # Each row has movieID, count as above.
    print("%s: Avg Rating: %.3f, Num Ratings: %d" % (nameDict[result[0]][0], result[3], result[1]))

# Stop the session
spark.stop()

#! spark-submit ./BUAD5132/M6/Individual/merten-m6.py > results.txt