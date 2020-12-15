// Databricks notebook source
// DBTITLE 1,Airport data Rdd
//publish -> https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/812815359225522/3379798727724210/4471544797058022/latest.html

val airportrdd=sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

// DBTITLE 1,Task1- Find out the airport with country code "Iceland" or whose latitude is greater than 40
val icelandairport=airportrdd.filter(x=>{x.split(",")(3)=="\"Iceland\"" || x.split(",")(6).toFloat>40  })
icelandairport.take(10)

// COMMAND ----------

// DBTITLE 1,Task1 complete and save the file as "iceland.txt"
icelandairport.saveAsTextFile("countryiceland.text")

//for saving the rdd file -> rdd.saveAsTextFile("name.csv")

// COMMAND ----------

// DBTITLE 1,Task2- Find our the airport with Country_timestamp "Pacific/Port_Moresby" and whose altitude is even
val fil_airport=airportrdd.filter(x => x.split(",")(8).toFloat.toInt%2==0 )

// COMMAND ----------

val map_airport=fil_airport.map( x => (x.split(",")(11),1))

// COMMAND ----------

val reduce_airport=map_airport.reduceByKey(_+_)

// COMMAND ----------

// DBTITLE 1,Task2 output
reduce_airport.collect() foreach println

// COMMAND ----------


