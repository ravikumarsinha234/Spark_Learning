// Databricks notebook source
// https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/812815359225522/1860667054523667/4471544797058022/latest.html

//    /FileStore/tables/nasa_july.tsv
//     /FileStore/tables/nasa_august.tsv

val julydata= sc.textFile("/FileStore/tables/nasa_july.tsv")
val augustdata= sc.textFile("/FileStore/tables/nasa_august.tsv")

// COMMAND ----------

val augusthost=augustdata.map(x => x.split("\t")(0))
val julyhost=julydata.map(x => x.split("\t")(0))

// COMMAND ----------

augusthost.take(10)


// COMMAND ----------

julyhost.take(10)

// COMMAND ----------

var intersection_rdd=julyhost.intersection(augusthost)
intersection_rdd.take(10)

// COMMAND ----------

val intersection_headrem_rdd=intersection_rdd.filter(line => !line.contains("host"))
intersection_headrem_rdd.count()

// COMMAND ----------


