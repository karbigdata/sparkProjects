
//package com.sparkbyexamples.spark.dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, unix_timestamp}

object filterexample extends App{

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
  //requiredCols.show(3)

  val requiredColsFormatted = requiredCols
    .withColumn("Date", unix_timestamp(col("Date"), "MM/dd/yyyy HH:mm:ss")
    .cast("timestamp"))
  val requiredColsFormattedMonth = requiredColsFormatted
    .withColumn("Month", col("Date")
      .substr(0,7))
      .withColumnRenamed("Primary Type", "Primary_Type")

  requiredColsFormattedMonth.show(3)
  requiredColsFormatted.printSchema()

  //Now spark sql to query the data
  requiredColsFormattedMonth.createOrReplaceTempView("crimedata")

  val monthlyCrimeCount = spark.sqlContext.sql("select Month, Primary_Type, count(1) as monthlyCrimeCount from crimedata " +
                      "group by Month, Primary_Type order by Month, monthlyCrimeCount desc")
  monthlyCrimeCount.show(50)

  //save the final DF to a file
  monthlyCrimeCount.write
    .option("sep", "|")
    .option("header", "true")
    .csv("/home/apple/IdeaProjects/crimeDataAnalysis/output/csv2")

   //this is giving null pointer exception 

}
