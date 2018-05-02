package project
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object TopRetweets {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("TopRetweets").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("retweets.txt")
    //lines.foreach(println)
    val retweets = lines.map(_.split(",")).map{
      case x if x.length > 3 => (x(2).toDouble,x(3))
      case x => (0.0,None)
    }.filter(x => x._2 != None).persist()
    val result = retweets.sortBy(r => r._1,false).take(25)
    result.foreach({case(x,y) => println(x + ", " + y )})

  }
}
