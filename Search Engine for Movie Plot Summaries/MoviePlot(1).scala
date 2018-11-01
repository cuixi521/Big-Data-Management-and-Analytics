import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.feature.StopWordsRemover

object MoviePlot {
  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      println("Usage: WordCount InputDir OutputDir")
    }
    val path = args(0);
    val word = args(1);
    val sc = new SparkContext(new SparkConf().setAppName("MoviePlot"))

    val movieInput = sc.textFile(path + "movie.metadata.tsv")
    val plotInput = sc.textFile(path + "plot_summaries.txt").map(x=>(x.split("\t")(0),x.split("\t")(1)))

    val movieID_Name = movieInput.map(x => (x.split("\t")(0),x.split("\t")(2)))

    val stopWordSet = StopWordsRemover.loadDefaultStopWords("english").toSet
    val plottmp = plotInput.map(x => (x._1, x._2.toLowerCase().split("""\W+""").filter(x => stopWordSet.contains(x)==false).filter(x => x.length >= 3)))
    val N = plottmp.count()
    val plot = plottmp.flatMap(x => (x._2.map((_,(x._1,1))))).map(x => ((x._1,x._2._1),x._2._2))

    val WtoID = plot.reduceByKey(_+_).map(x => (x._1._1, x._1._2))

    val idf = WtoID.groupByKey().map(x => (x._1, Math.log (N / x._2.size.toDouble)))

    val countInDoc = plot.reduceByKey(_+_).map(x => (x._1._2, (x._1._1, x._2)))

    val J = countInDoc.join(plottmp)

    val tf = J.map(x => (x._2._1._1, (x._1, x._2._1._2.toDouble / x._2._2.length.toDouble)))

    val res = tf.join(idf).map(x => (x._1, (x._2._1._1, x._2._1._2 * x._2._2))).groupByKey()

    val look = res.lookup(word).flatMap(x => x)

    val fres = sc.parallelize(look)
    val ffres = fres.join(movieID_Name).map(x => (x._2._2, x._2._1)).sortBy(-_._2)
    ffres.saveAsTextFile(path + "part1")
  }
}



