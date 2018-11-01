import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: WordCount InputDir OutputDir")
    }
    val path = args(0);
    val times = args(1).toInt;
    val pathout = args(2);
    val sc = new SparkContext(new SparkConf().setAppName("MoviePlot"))

    // Databricks notebook source
    val read = sc.textFile(path)
    val header = read.first
    val data = read.filter(line => line != header).map(x => x.replaceAll("\"", ""))

    val oriToDest = data.map(x => ((x.split(","))(1), 1.0))
    val destToOri = data.map(x => ((x.split(","))(4), (x.split(","))(1)))
    oriToDest.take(10)

    // COMMAND ----------

    val oriToDest2 = oriToDest.reduceByKey(_+_)
    val N = oriToDest2.count().toDouble
    var oriToDest3 = oriToDest.reduceByKey(_+_).map(x => (x._1, 10.0))
    val destToOri2 = destToOri.groupByKey()
    oriToDest2.take(10)

    // COMMAND ----------

    val map1 = oriToDest2.collect().toMap

    // COMMAND ----------

    import scala.collection.immutable
    def prCal(dest: String, oris: Iterable[String], map1: immutable.Map[String, Double], map2: immutable.Map[String, Double]): Double = {
      var sum = 0.0
      oris.foreach { ori =>
        sum = sum + map2.getOrElse(ori, 0.0) / map1.getOrElse(ori, 0.0)
      }
      var res = 0.15 * 1 / N + 0.85 * sum
      res
    }

    // COMMAND ----------

    var x = 0;
    for(x <- 1 to times ){
      val map2 = oriToDest3.collect().toMap
      oriToDest3 = destToOri2.map(x => (x._1, prCal(x._1, x._2, map1, map2)))
      print(x)
    }
    oriToDest3.saveAsTextFile(pathout)

    // COMMAND ----------
  }
}
