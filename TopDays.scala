package project
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TopDays {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("TopDays").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("tweets_created.txt")
    val dates = lines.map(_.split(",")).map(x => (x(1).trim, 1)).persist()
    val grouped = dates.groupByKey().sortByKey().mapValues(_.toList.reduce({
      (x,y)=> x + y
    })
    ).persist()
    val result = grouped.sortBy(r => r._2,false).take(25)
    result.foreach({case(x,y) => println(x + ", " + y )})
  }

}
