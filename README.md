# Spark Scala Moving Average
[![Made with Scala](https://img.shields.io/badge/Made%20with-Scala-yellow.svg)](https://img.shields.io/badge/Made%20with-Scala-yellow.svg) [![Powered by Spark](https://img.shields.io/badge/Powered%20by-Spark-red.svg)](https://img.shields.io/badge/Powered%20by-Spark-red.svg) [![Build Status](https://travis-ci.org/joemccann/dillinger.svg?branch=master)](https://travis-ci.org/joemccann/dillinger) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) 

The purpose of this project is to present a moving average solution with Spark.

## Overview
In fact, this example is simple if you already know the secondary sort algorithm. More details please refer to my project, [spark-scala-secondary-sort](https://github.com/yungshun317/spark-scala-secondary-sort).

The data format is like: `<name-as-string>, <date-as-timestamp>, <value-as-double>`. Our goal is to obtain: `<name-as-string>, <date-as-timestamp>, <moving-average-as-double>`.
1. Initialize `SparkContext`.
2. Read input data with `sc.textFile()`. 
3. Sort the values based on the time stamp using secondary sort.
4. Group data based on the stock symbol.
5. Apply the moving average algorithm.
```scala
val movingAverage = groupByStockSymbol.mapValues(values => {
  val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val queue = new scala.collection.mutable.Queue[Double]()
  
  for (timeSeriesData <- values) yield {
    queue.enqueue(timeSeriesData.closingStockPrice)
    if (queue.size > brodcastWindow.value)
      queue.dequeue
    (dateFormat.format(new java.util.Date(timeSeriesData.timeStamp)), (queue.sum / queue.size))
  }
})
```
6. Save the output data.

## Moving Average
**Time series** data represents the values of a variable over a period of time, such as a second, minute, hour, day, week, month, quarter, or year. Typically, time series data occurs whenever the same measurements are recorded over a period of time. For example, the closing price of a company stock is time series data over minutes, hours, or days.

The mean (or average) of time series data (observations equally spaced in time, such as per hour or per day) from several consecutive periods is called the **moving average**. It's called "moving" because the average is continually recomputed as new time series data becomes available, and it progresses by dropping the earliest value and adding the most recent. 

## Up & Running
In IntelliJ, execute `sbt package`.

Run `spark-submit`.
```sh
~/workspace/scala/spark-scala-moving-average$ spark-submit --master local --class yungshun.chang.movingaverage.MovingAverage target/scala-2.11/spark-scala-moving-average_2.11-0.1.jar 2 1 datasets/input/stock_prices.csv datasets/output
```

Print the result.
```sh
~/workspace/scala/spark-scala-moving-average$ cat datasets/output/*
IBM,2013-09-25,189.47
IBM,2013-09-26,189.845
IBM,2013-09-27,188.57
IBM,2013-09-30,186.05
GOOG,2004-11-02,194.87
GOOG,2004-11-03,193.26999999999998
GOOG,2004-11-04,188.185
GOOG,2013-07-17,551.625
GOOG,2013-07-18,914.615
GOOG,2013-07-19,903.64
AAPL,2013-10-03,483.41
AAPL,2013-10-04,483.22
AAPL,2013-10-07,485.39
AAPL,2013-10-08,484.345
AAPL,2013-10-09,483.765
```

## Spark SQL
We can achieve the same goal with `spark-shell` using Spark SQL.
```sh
scala> val df = spark.read.option("header", "false").option("inferSchema", "true").option("dateFormat", "yyyy-MM-dd").csv("/home/yungshun/workspace/scala/spark-scala-moving-average/datasets/input/stock_prices.csv").toDF("stock_symbol", "timestamp", "closing_price")
df: org.apache.spark.sql.DataFrame = [stock_symbol: string, timestamp: timestamp ... 1 more field]

scala> df.show()
+------------+-------------------+-------------+
|stock_symbol|          timestamp|closing_price|
+------------+-------------------+-------------+
|        GOOG|2004-11-04 00:00:00|        184.7|
|        GOOG|2004-11-03 00:00:00|       191.67|
|        GOOG|2004-11-02 00:00:00|       194.87|
|        AAPL|2013-10-09 00:00:00|       486.59|
|        AAPL|2013-10-08 00:00:00|       480.94|
|        AAPL|2013-10-07 00:00:00|       487.75|
|        AAPL|2013-10-04 00:00:00|       483.03|
|        AAPL|2013-10-03 00:00:00|       483.41|
|         IBM|2013-09-30 00:00:00|       185.18|
|         IBM|2013-09-27 00:00:00|       186.92|
|         IBM|2013-09-26 00:00:00|       190.22|
|         IBM|2013-09-25 00:00:00|       189.47|
|        GOOG|2013-07-19 00:00:00|        896.6|
|        GOOG|2013-07-18 00:00:00|       910.68|
|        GOOG|2013-07-17 00:00:00|       918.55|
+------------+-------------------+-------------+

scala> import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window

scala> import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions._

scala> val windowSpec = Window.partitionBy("stock_symbol").orderBy("timestamp").rowsBetween(-1, 0)
windowSpec: org.apache.spark.sql.expressions.WindowSpec = org.apache.spark.sql.expressions.WindowSpec@30078038

scala> val movingAverageDF = df.withColumn("moving_average", avg(df("closing_price")).over(windowSpec)).drop("closing_price")
movingAverageDF: org.apache.spark.sql.DataFrame = [stock_symbol: string, timestamp: timestamp ... 1 more field]

scala> movingAverageDF.show()
+------------+-------------------+------------------+                           
|stock_symbol|          timestamp|    moving_average|
+------------+-------------------+------------------+
|        AAPL|2013-10-03 00:00:00|            483.41|
|        AAPL|2013-10-04 00:00:00|            483.22|
|        AAPL|2013-10-07 00:00:00|            485.39|
|        AAPL|2013-10-08 00:00:00|           484.345|
|        AAPL|2013-10-09 00:00:00|           483.765|
|        GOOG|2004-11-02 00:00:00|            194.87|
|        GOOG|2004-11-03 00:00:00|193.26999999999998|
|        GOOG|2004-11-04 00:00:00|           188.185|
|        GOOG|2013-07-17 00:00:00|           551.625|
|        GOOG|2013-07-18 00:00:00|           914.615|
|        GOOG|2013-07-19 00:00:00|            903.64|
|         IBM|2013-09-25 00:00:00|            189.47|
|         IBM|2013-09-26 00:00:00|           189.845|
|         IBM|2013-09-27 00:00:00|            188.57|
|         IBM|2013-09-30 00:00:00|            186.05|
+------------+-------------------+------------------+
```
I think the key is setting a **window spec** as `val windowSpec = Window.partitionBy("stock_symbol").orderBy("timestamp").rowsBetween(-1, 0)` and then applying this `windowSpec` with `over` in `df.withColumn("moving_average", avg(df("closing_price")).over(windowSpec))`.

## Tech

This project uses:

* [IntelliJ](https://www.jetbrains.com/idea/) - a Java integrated development environment (IDE) for developing computer software developed by JetBrains.
* [Spark](https://spark.apache.org/) - a unified analytics engine for large-scale data processing.

## Todos

 - More Spark SQL projects.

## License
[Spark Scala Moving Average](https://github.com/yungshun317/spark-scala-moving-average) is released under the [MIT License](https://opensource.org/licenses/MIT) by [yungshun317](https://github.com/yungshun317).
