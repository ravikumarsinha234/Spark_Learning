// Databricks notebook source
val rawdata=sc.textFile("/FileStore/tables/FriendsData.csv")

// COMMAND ----------

// DBTITLE 1,Task 1- Try to find average friend for each age of people.... (Start of task 1)
 rawdata.take(10)

// COMMAND ----------

val remHeader=rawdata.filter(line => !line.contains("Age"))
remHeader.take(4)

// COMMAND ----------

val frnd_rdd=remHeader.map(x => (x.split(",")(2).toInt,(1,x.split(",")(3).toInt)))
frnd_rdd.take(7)

// COMMAND ----------

val frnd_rdd_reduced=frnd_rdd.reduceByKey( (x,y) => (x._1+y._1,x._2+y._2) )
frnd_rdd_reduced.take(10)

// COMMAND ----------

val avg_frnd=frnd_rdd_reduced.mapValues(data =>data._2 /data._1)
avg_frnd.collect()

// COMMAND ----------

// DBTITLE 1,Task1 : result
for ((age,avg) <- avg_frnd.collect()) println(age+" : "+avg)

// COMMAND ----------

// DBTITLE 1,Task2- Find the maximum friend for each age....Start of task2
//TASK -2 FIND THE MAXIMUM FRIEND FOR EACH AGE

remHeader.take(10)

val frndrdd=remHeader.map(x => (x.split(",")(2).toInt,x.split(",")(3).toInt))

frndrdd.collect()



// COMMAND ----------

val initial=0
val com_max =(x:Int, y:Int) => if (x>y) x else y
val merge_max=(p1:Int, p2:Int) =>if (p1>p2) p1 else p2
val aggrdd=frndrdd.aggregateByKey(initial)((com_max),(merge_max))

// COMMAND ----------

// DBTITLE 1,Task-2 result
aggrdd.collect()

// COMMAND ----------


