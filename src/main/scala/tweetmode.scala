
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import pureconfig.generic.auto._
import twitter4j.{FilterQuery, Status}
import cats.data.State

object tweetmode {

  case class AppConfig(
                        consumerKey: String,
                        consumerSecret: String,
                        accessToken: String,
                        accessTokenSecret: String,
                        appName: String,
                        sparkMaster: String,
                        streamMicrobatchSeconds: Int)

  case class TweetModeState(tweets: Seq[Tweet], batchRate: Int, batchCount: Int = 1) {

    def tweetCount: Int = tweets.size

    override def toString: String = {
      s"""
        |tweetcount: $tweetCount,
        |seconds: ${batchRate * batchCount}
      """.stripMargin
    }
  }

  case class Emoji(character: String)

  case class HashTag(value: String)

  case class Domain(value: String)

  case class Tweet(value: String)


  def streamingContext(config: AppConfig): StreamingContext = {

    val sparkConf = new SparkConf().setAppName(config.appName)
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster(config.sparkMaster)
    }
    new StreamingContext(sparkConf, Seconds(config.streamMicrobatchSeconds))
  }

  def streamFromEnv(ssc: StreamingContext, config: AppConfig): ReceiverInputDStream[Status] = {

    System.setProperty("twitter4j.oauth.consumerKey", config.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", config.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", config.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.accessTokenSecret)
    TwitterUtils.createStream(ssc, None)
  }

  def main(args: Array[String]): Unit = {

    val config = pureconfig.loadConfigOrThrow[AppConfig]("tweetmode")

    LogManager.getRootLogger.setLevel(Level.WARN)

    val ssc = streamingContext(config)
    ssc.checkpoint("./checkpoint")
    val stream = streamFromEnv(ssc, config)

    processStream(stream, config)

    ssc.start()
    ssc.awaitTermination()
  }

  def processStream(stream: ReceiverInputDStream[Status], config: AppConfig) = {
    implicit val c = config
    stream.map(s => Tweet(s.getText))
      .map(t => (TweetModeState(Seq.empty, config.streamMicrobatchSeconds), t))
      .updateStateByKey(updateFunction)
      .foreachRDD(rdd => println(rdd.take(1).head._2))
  }

  def updateFunction(tweets: Seq[Tweet], state: Option[TweetModeState])(implicit config: AppConfig): Option[TweetModeState] = state match {
    case None => Some(TweetModeState(tweets, config.streamMicrobatchSeconds))
    case Some(s) => Some(s.copy(tweets = s.tweets ++ tweets, batchCount = s.batchCount + 1))
  }
}
