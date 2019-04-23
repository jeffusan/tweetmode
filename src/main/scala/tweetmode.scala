
import config.AppConfig
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import pureconfig.generic.auto._
import twitter4j.{FilterQuery, Status}

object tweetmode {

  case class TweetCreds(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String)

  def streamingContext: StreamingContext = {

    val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster("local[*]")
    }
    new StreamingContext(sparkConf, Seconds(2))
  }

  def streamFromEnv(ssc: StreamingContext): ReceiverInputDStream[Status] = {
    val config = pureconfig.loadConfigOrThrow[AppConfig]("tweetmode")

    System.setProperty("twitter4j.oauth.consumerKey", config.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", config.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", config.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.accessTokenSecret)
    TwitterUtils.createStream(ssc, None)
  }

  def main(args: Array[String]): Unit = {

    LogManager.getRootLogger.setLevel(Level.WARN)

    val ssc = streamingContext
    val stream = streamFromEnv(ssc)

    processStream(stream)

    ssc.start()
    ssc.awaitTermination()
  }

  def processStream(stream: ReceiverInputDStream[Status]) = {
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
  }

}
