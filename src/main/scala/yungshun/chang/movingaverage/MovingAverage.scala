package yungshun.chang.movingaverage

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object MovingAverage {
  def main(args: Array[String]): Unit = {
    if (args.size < 4) {
      println("Usage: MovingAverage <window> <number-of-partitions> <input-dir> <output-dir>")
      sys.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("MovingAverage")
    val sc = new SparkContext(sparkConf)

    val window = args(0).toInt
    val numPartitions = args(1).toInt
    val input = args(2)
    val output = args(3)

    val brodcastWindow = sc.broadcast(window)

    val rawData = sc.textFile(input)

    val valueTokey = rawData.map(line => {
      val tokens = line.split(",")
      val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
      val timestamp = dateFormat.parse(tokens(1)).getTime
      (CompositeKey(tokens(0), timestamp), TimeSeriesData(timestamp, tokens(2).toDouble))
    })

    val sortedData = valueTokey.repartitionAndSortWithinPartitions(new CompositeKeyPartitioner(numPartitions))
  }
}

case class CompositeKey(stockSymbol: String, timeStamp: Long)
case class TimeSeriesData(timeStamp: Long, closingStockPrice: Double)

object CompositeKey {
  implicit def ordering[A <: CompositeKey]: Ordering[A] = {
    Ordering.by(fk => (fk.stockSymbol, fk.timeStamp))
  }
}

import org.apache.spark.Partitioner

class CompositeKeyPartitioner(partitions: Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case k: CompositeKey => math.abs(k.stockSymbol.hashCode % numPartitions)
    case null            => 0
    case _               => math.abs(key.hashCode % numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: CompositeKeyPartitioner => h.numPartitions == numPartitions
    case _                          => false
  }

  override def hashCode: Int = numPartitions
}
