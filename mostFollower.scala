import org.apache.spark.SparkContext._
import scala.io._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object mostFollower {
  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("asgn5").setMaster("local[20]")
    val sc= new SparkContext(conf)



    val fileRdd = sc.textFile("allen_data.txt")
    val splitRdd = fileRdd.map(line=> line.split(","))
    //fileRdd.foreach(println(_))


    val yourRdd = splitRdd.flatMap(arr => {
      val name = arr(0)
      val followerCount = arr(1)
      val timeZone = arr(2)
      val lang = arr(3)
      val createdAt = arr(4)
      val friendCount = arr(5)
      //val title = arr(0)
      //val text = arr(1)
      val words = name.split(",")
      words.map(word => (name, followerCount, timeZone, lang, friendCount, createdAt))

      }
    ).persist
     val result = yourRdd.collect.sortBy(_._2.toDouble).reverse.foreach(println(_))


  }

}
