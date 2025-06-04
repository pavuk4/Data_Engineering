from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, count, date_format, desc, from_unixtime, dense_rank


def save_to_disc(path, data):
    data.write.csv(f"{path}", header=True, mode='overwrite')

def average_trip_duration(data):
    avg_trip_duration = data.withColumn("date", date_format("start_time", "yyyy-MM-dd")) \
        .groupBy("date") \
        .agg(date_format(from_unixtime(avg("tripduration") * 60), "HH:mm:ss").alias("avarage_trip_duration"))

    save_to_disc("out/avarage_trip_duration", avg_trip_duration)

def trips_count(data):
    trips_count = data.withColumn("date", date_format("start_time", "yyyy-MM-dd")) \
        .groupBy("date") \
        .agg(count("tripduration").alias("trips_count"))
    
    save_to_disc("out/trips_count", trips_count)

def most_popular_start_for_month(data):
    most_popular_start_for_month = data.withColumn("month", date_format("start_time", "yyyy-MM")) \
        .groupBy("month", "from_station_name") \
        .agg(count("from_station_name").alias("count")) \
        .withColumn("rank", dense_rank().over(Window.partitionBy("month").orderBy(desc("count")))) \
        .filter(col("rank") == 1) \
        .select("month", "from_station_name", "count")
    
    save_to_disc("out/most_popular_start_for_month", most_popular_start_for_month)

def top_3_to_station_for_2_weeks(data):
    top_3_to_station_for_2_weeks = data.withColumn("date", date_format("start_time", "yyyy-MM-dd")) \
        .groupBy("date", "to_station_name") \
        .agg(count("to_station_name").alias("count")) \
        .withColumn("rank", dense_rank().over(Window.partitionBy("date").orderBy(desc("count")))) \
        .filter(col("rank").isin(1, 2, 3)) \
        .orderBy(desc("date")) \
        .limit(14 * 3) \
        .select("date", "to_station_name", "count")
    
    save_to_disc("out/top_3_to_station_for_2_weeks", top_3_to_station_for_2_weeks)

def male_or_female_averate_trip_duration_bigger(data):
    male_or_female_averate_trip_duration_bigger = data.groupBy("gender") \
        .agg(date_format(from_unixtime(avg("tripduration") * 60), "HH:mm:ss").alias("avarage_trip_duration")) \
        .filter(col("gender").isin("Male", "Female")) \
        .orderBy(desc("avarage_trip_duration"))
    
    save_to_disc("out/male_or_female_averate_trip_duration_bigger", male_or_female_averate_trip_duration_bigger)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Trip Analysis").getOrCreate()
    data = spark.read.csv("Divvy_Trips_2019_Q4.csv", header=True, inferSchema=True)

    average_trip_duration(data)
    trips_count(data)
    most_popular_start_for_month(data)
    top_3_to_station_for_2_weeks(data)
    male_or_female_averate_trip_duration_bigger(data)
