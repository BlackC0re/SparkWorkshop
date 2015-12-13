__author__ = 'Andrei'

import sys
import itertools
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import pandas as pd

def loadMovies(moviesFile):
    """ Loads the movie data set in Spark """
    pdf = pd.read_csv(moviesFile, sep = '|')
    movies = sqlC.createDataFrame(pdf)
    movies.show()
    movies.printSchema()
    # Filter only comedy movies
    movies.filter(movies['Comedy'] == 1).show()
    # Count movies by Release date
    movies.groupBy("RelDate").count().show()

    return movies

def loadUsers(usersFile):
    """ Loads the users data set in Spark """
    pdf = pd.read_csv(usersFile, sep = '|')
    users = sqlC.createDataFrame(pdf)
    users.show()
    users.printSchema()

    #Register the DataFrame as a table
    users.registerTempTable("Users")
    # SQL can be run over DataFrames that have been registered as a table.
    teenagers = sqlC.sql("SELECT UserID, Age, Gender, Occupation FROM Users WHERE Age >= 13 AND Age <= 19")
    # The results of SQL queries are RDDs and support all the normal RDD operations.
    teens = teenagers.map(lambda p: "ID: " + str(p.UserID) + " Age:" + str(p.Age) + " Gender: " + p.Gender + " Occupation:" + p.Occupation)
    for teen in teens.collect():
        print(teen)

    return users

def loadRatings(ratingsFile):
    """ Loads the ratings data set in Spark """
    pdf = pd.read_csv(ratingsFile, sep = '\t')
    ratings = sqlC.createDataFrame(pdf)
    ratings.show()
    ratings.printSchema()

    return ratings

if __name__ == "__main__":
    if (len(sys.argv) != 1):
        print("Usage: /path/to/spark/bin/spark-submit --driver-memory 4g spark_workshop.py")
        sys.exit(1)

    # set up environment
    conf = SparkConf().setAppName("SparkWorkshop").set("spark.executor.memory", "4g")
    sc = SparkContext(conf=conf)
    sqlC = SQLContext(sc)

    movies = loadMovies("./input/movies")
    numMovies = movies.count()
    users = loadUsers("./input/users")
    numUsers = users.count()
    ratings = loadRatings("./input/ratings")
    numRatings = ratings.count()
    numUsers = ratings.map(lambda r: r[0]).distinct().count()
    numMovies = ratings.map(lambda r: r[1]).distinct().count()

    print("Got %d ratings from %d users on %d movies." % (numRatings, numUsers, numMovies))

    movies.registerTempTable("Movies")
    users.registerTempTable("Users")
    ratings.registerTempTable("Ratings")

    topRatings = sqlC.sql("SELECT Title, Sum(Rating) AS Rating FROM Movies m JOIN Ratings r ON m.MovieID = r.MovieID GROUP BY m.Title ORDER BY Rating DESC")
    topRatings = topRatings.map(lambda p: " Movie: " + p.Title + " Rating:" + str(p.Rating))
    for rating in topRatings.collect():
        print(rating)
