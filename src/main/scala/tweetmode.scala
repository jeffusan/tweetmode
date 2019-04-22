
import config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import pureconfig.generic.auto._
import twitter4j.FilterQuery

object tweetmode {

  case class TweetCreds(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String)
  def main(args: Array[String]): Unit = {

    val config = pureconfig.loadConfigOrThrow[AppConfig]("tweetmode")

    System.setProperty("twitter4j.oauth.consumerKey", config.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", config.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", config.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.accessTokenSecret)

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[2]")
    }
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val filters = args.takeRight(args.length - 4)

    val stream = TwitterUtils.createStream(ssc, None, filters)
    val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))

    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
      .map{case (topic, count) => (count, topic)}
      .transform(_.sortByKey(false))


    // Print popular hashtags
    topCounts60.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
    })

    ssc.start()
    ssc.awaitTermination()
    println("Hello, world!")
  }

}
