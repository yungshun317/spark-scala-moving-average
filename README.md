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

## Tech

This project uses:

* [IntelliJ](https://www.jetbrains.com/idea/) - a Java integrated development environment (IDE) for developing computer software developed by JetBrains.
* [Spark](https://spark.apache.org/) - a unified analytics engine for large-scale data processing.

## Todos

 - More Spark SQL projects.

## License
[Spark Scala Moving Average](https://github.com/yungshun317/spark-scala-moving-average) is released under the [MIT License](https://opensource.org/licenses/MIT) by [yungshun317](https://github.com/yungshun317).
