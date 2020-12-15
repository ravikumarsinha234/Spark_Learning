// Databricks notebook source
val data=sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

data.take(2)

// COMMAND ----------

val removeHeader=data.filter(line => !line.contains("Price") )
removeHeader.take(10)

// COMMAND ----------

val roomRdd=removeHeader.map(x => (x.split(",")(3).toInt,(1,x.split(",")(2).toDouble)))
roomRdd.collect()

// COMMAND ----------

roomRdd.map(x=> x._1).take(10)

// COMMAND ----------

val reducedRdd=roomRdd.reduceByKey( (x,y) => (x._1 + y._1,x._2 +y._2))
reducedRdd.take(10)

// COMMAND ----------

val finalRdd=reducedRdd.mapValues(data => data._2 / data._1)
finalRdd.collect()

// COMMAND ----------

for((bedRoom,avg)<- finalRdd.collect()) println(bedRoom+" : "+avg)

// COMMAND ----------

finalRdd.saveAsTextFile("PropertyFinal.csv")

// COMMAND ----------


