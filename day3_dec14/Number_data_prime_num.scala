// Databricks notebook source
// DBTITLE 1,Prime number task from number data
//https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/812815359225522/1299070255524746/4471544797058022/latest.html

val prime_num=sc.textFile("/FileStore/tables/numberData.csv")
//def headerRemover(line: String): Boolean= !(line.startsWith("host"))
val prime_num_Rdd=prime_num.filter(line=>line!="Number")

prime_num_Rdd.take(3)

// COMMAND ----------

def checkPrime(i:Int):Boolean={
if(i<=1)
  false
else if (i==2)
  true
else
  !(2 until i).exists(x=> i % x ==0)
}

// COMMAND ----------

prime_num_Rdd.map(x=>checkPrime(x.toInt)).countByValue() ///just for checking if everything is correct

// COMMAND ----------

val prime_Num=prime_num_Rdd.filter(x=>checkPrime(x.toInt))
prime_Num.collect() foreach println

// COMMAND ----------


