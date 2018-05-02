package project
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object TopMonths {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("TopMonths").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("tweets_created.txt")
    val months = lines.map(_.split(",")).map{
      case x if x(1).split("-").length > 1 => (x(1).split("-")(0) + "-" +
        x(1).split("-")(1), 1)
      case x => (None, 1)
    }.filter(x => x._1 != None).persist()
    //lines.map
    val grouped = months.groupByKey().mapValues(_.toList.reduce({
      (x,y)=> x + y
    })
    ).persist()
    val result = grouped.sortBy(r => r._2,false).take(5)
    result.foreach({case(x,y) => println(x + ", " + y )})


  }


}
