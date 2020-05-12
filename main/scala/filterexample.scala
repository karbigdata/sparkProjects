
//package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp}

object crimedata extends App{

  private val session = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  val spark: SparkSession = session

  spark.sparkContext.setLogLevel("ERROR")
  val df = spark.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    .csv("/home/apple/IdeaProjects/crimeDataAnalysis/crimes.csv")

  //df.printSchema()
  //dropping first col as it has invalid data
  val goodDF = df.drop("_c0")
  //goodDF.printSchema()
 // goodDF.show(3)
  //goodDF.take(10).foreach(f => println(f))

  val crimeDF = goodDF.toDF()
  //crimeDF.createOrReplaceTempView("crimedata")
  //spark.sqlContext.sql("select * from crimedata LIMIT 10").show(false)

  //Month wise count for each type of crime
  /* month1 crime1 count
  *  month1 crime2 count
  *  month2 crime1 count
  *  month2 crime2 count
  * and count in desc and all days in asc
   */
  // we need date and crime type
  //first get the required col data
  // YYYY-MM-DD HH:mm:ss
  val requiredCols = crimeDF.select(col="Date", cols="Primary Type")
  requiredCols.show(3)
  /*
  +--------------------+--------------------+
|                Date|        Primary Type|
+--------------------+--------------------+
|05/03/2016 11:40:...|             BATTERY|
|05/03/2016 09:40:...|             BATTERY|
|05/03/2016 11:31:...|PUBLIC PEACE VIOL...|
+--------------------+--------------------+
*/
  val requiredColsFormatted = requiredCols
    .withColumn("Date", unix_timestamp(col("Date"), "MM/dd/yyyy HH:mm:ss")
    .cast("timestamp"))
  val requiredColsFormattedMonth = requiredColsFormatted
    .withColumn("Month", col("Date")
      .substr(0,7))
      .withColumnRenamed("Primary Type", "Primary_Type")

  requiredColsFormattedMonth.show(3)
  requiredColsFormatted.printSchema()
/*  
  +-------------------+--------------------+-------+
|               Date|        Primary_Type|  Month|
+-------------------+--------------------+-------+
|2016-05-03 11:40:00|             BATTERY|2016-05|
|2016-05-03 09:40:00|             BATTERY|2016-05|
|2016-05-03 11:31:00|PUBLIC PEACE VIOL...|2016-05|
+-------------------+--------------------+-------+
*/
  
  //Now spark sql to query the data
  requiredColsFormattedMonth.createOrReplaceTempView("crimedata")

  val monthlyCrimeCount = spark.sqlContext.sql("select Month, Primary_Type, count(1) as monthlyCrimeCount from crimedata " +
                      "group by Month, Primary_Type order by Month, monthlyCrimeCount desc")
  monthlyCrimeCount.show(50)

  /*
  
+-------+--------------------+-----------------+
|  Month|        Primary_Type|monthlyCrimeCount|
+-------+--------------------+-----------------+
|2012-01|               THEFT|             5711|
|2012-01|             BATTERY|             4307|
|2012-01|           NARCOTICS|             3271|
|2012-01|     CRIMINAL DAMAGE|             2660|
|2012-01|            BURGLARY|             1757|
|2012-01|       OTHER OFFENSE|             1537|
|2012-01| MOTOR VEHICLE THEFT|             1440|
|2012-01|             ASSAULT|             1294|
|2012-01|  DECEPTIVE PRACTICE|             1093|
|2012-01|             ROBBERY|             1011|
|2012-01|   CRIMINAL TRESPASS|              662|
|2012-01|   WEAPONS VIOLATION|              317|
|2012-01|OFFENSE INVOLVING...|              227|
|2012-01|        PROSTITUTION|              194|
+-------+--------------------+-----------------+
only showing top 50 rows
*/
  
  //save the final DF to a file
  monthlyCrimeCount.write
    .option("sep", "|")
    .option("header", "true")
    .csv("/home/apple/IdeaProjects/crimeDataAnalysis/output/csv2")

   //this is giving null pointer exception 

}
