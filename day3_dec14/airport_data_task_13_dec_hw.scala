// Databricks notebook source
// DBTITLE 1,Airport data Rdd
val airportrdd=sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

// DBTITLE 1,Task1- Find out the airport with country code "Iceland" or whose latitude is greater than 40
val icelandairport=airportrdd.filter(x=>{x.split(",")(3)=="\"Iceland\"" || x.split(",")(6).toFloat>40  })
icelandairport.take(10)

// COMMAND ----------

// DBTITLE 1,Task1 complete and save the file as "iceland.txt"
icelandairport.saveAsTextFile("iceland.text")

//for saving the rdd file -> rdd.saveAsTextFile("name.csv")

// COMMAND ----------

// DBTITLE 1,Task2- Find our the airport with Country_timestamp "Pacific/Port_Moresby" and whose altitude is even
val fil_airport=airportrdd.filter(x => { x.split(",")(11)=="\"Pacific/Port_Moresby\"" && x.split(",")(8).toInt%2==0 })

// COMMAND ----------

// DBTITLE 1,Task2 output
fil_airport.collect()foreach println

// COMMAND ----------


