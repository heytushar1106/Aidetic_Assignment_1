# Databricks notebook source
#Load the  dataset into a PySpark DataFrame.
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Earthquake_Analysis").getOrCreate()
file_path = "/FileStore/tables/database-2.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

df.show(truncate=False)

# COMMAND ----------

#Convert the Date and Time columns into a timestamp column named Timestamp
from pyspark.sql.functions import col, concat, to_timestamp
df = df.withColumn("Timestamp", to_timestamp(concat(col("Date"), col("Time")), "dd/MM/yyyy" "HH:mm:ss"))
df.show()

# COMMAND ----------

#Filter the dataset to include only earthquakes with a magnitude greater than 5.0.
filter_df = df.filter(col("Magnitude") >5.0)
filter_df.select("Magnitude").show()

# COMMAND ----------

#Calculate the average depth and magnitude of earthquakes for each earthquake type.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

result_df = filter_df.groupBy("Type").agg(
    avg("Depth").alias("Avg_Depth"),
    avg("Magnitude").alias("Avg_Magnitude")
)

result_df.show()

# COMMAND ----------

#Implement a UDF to categorize the earthquakes into levels (e.g., Low, Moderate, High) based on their magnitudes.

from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Define a UDF to catogorize earthquakes based on their magnitudes

def categorize_magnitude(magnitude):
    if magnitude<4.0:
        return"Low"
    elif 4.0 <= magnitude < 6.0:
        return"Moderate"
    else:
        return"High"
    

#Registerd UDF 
categorize_magnitude_udf = udf(categorize_magnitude,StringType())

#Apply UDF and Create a New Column 

df = df.withColumn("Magnitude_Level", categorize_magnitude_udf(col("Magnitude")))

df.select("Magnitude_Level").show(truncate=False)

# COMMAND ----------

#Calculate the distance of each earthquake from a reference location (e.g., (0, 0)).

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sqrt

#Reference Location 
reference_latitude =0
reference_longitude = 0

#For Calculate Distance We use Pythagores Theorem
df = df.withColumn(
    "Distance",
    sqrt((col("Latitude") - reference_latitude)**2 + (col("Longitude") - reference_longitude)**2)
)

df.select("Distance").show()


# COMMAND ----------

#Visualize the geographical distribution of earthquakes on a world map using appropriate libraries (e.g., Basemap or Folium).

from pyspark.sql.functions import col, concat, to_timestamp
import folium

# Initialize a Folium map centered at (0, 0)
map_center = [0, 0]
mymap = folium.Map(location=map_center, zoom_start=2)

# Define a function to add markers to the map
def add_marker(row):
    latitude = row['Latitude']
    longitude = row['Longitude']
    magnitude = row['Magnitude']
    popup_text = f"Magnitude: {magnitude}<br>Latitude: {latitude}<br>Longitude: {longitude}"
    folium.Marker([latitude, longitude], popup=popup_text).add_to(mymap)

# Use foreach to apply the add_marker function to each row
df.foreach(add_marker)

# Display the map in Databricks notebook using display
display(mymap)

# COMMAND ----------

#Final CSV format
final_csv_path = "final_earthquake_data.csv"
df.write.csv(final_csv_path, header=True, mode="overwrite")

# COMMAND ----------

final_csv_path = "/dbfs/FileStore/your_custom_path/final_earthquake_data.csv"
print(f"The file is stored at: {final_csv_path}")
