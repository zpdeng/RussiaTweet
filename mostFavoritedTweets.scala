import org.apache.spark.SparkContext._
import scala.io._

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection._

object mostFavoritedTweets {
  def main(args: Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("asgn5").setMaster("local[4]")
    val sc= new SparkContext(conf)


   // val string = "b,c,,,a"
    //println(string.split(",", -1)(4))


    val fileRdd = sc.textFile("tweets.csv")
    val splitRdd = fileRdd.map(line=> line.split("," )).filter(x=> x.size >= 11)


    val yourRdd = splitRdd.flatMap(arr => {
      val userName = arr(1)
      val dateCreated = arr(3)
      val retweetCount = arr(4)
      val favoriteCount = arr(6)
      val text = arr(7)
      val hashtag = arr(10)
      //val friendCount = arr(5)
      //val title = arr(0)
      //val text = arr(1)
      val words = userName.split(",")
      words.map(word => (userName, dateCreated, retweetCount, favoriteCount))

    }
    ).persist

    //yourR
    //val filteredResult = yourRdd.filter((3) = "#N/A")
    val result = yourRdd.collect.sortBy(_._3).reverse
    result.foreach(println(_))
    //splitRdd.collect.foreach(x=> println(x(3)+ ", " + x(4) + ", "+ x(6)+", "+ x(7)))
    /*
    splitRdd.collect.foreach(x=>
      if(x.size <= 6 ){

      }
    )*/
    //fileRdd.foreach(println(_))

    /*
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
      words.map(word => (name, followerCount, timeZone, lang, createdAt, friendCount))

    }
    ).persist
    val result = yourRdd.collect.sortBy(_._2.toDouble).reverse
    result.foreach(println(_))
    */
  }

}
