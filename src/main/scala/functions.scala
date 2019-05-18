import algebras.{AppConfig, Tweet, TweetModeState}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import pureconfig.generic.auto._

object functions {

  def loadConfig(name: String): Option[AppConfig] = {
    Some(pureconfig.loadConfigOrThrow[AppConfig](name))
  }

  def streamingContext(config: AppConfig): Option[StreamingContext] = {

    val sparkConf = new SparkConf().setAppName(config.appName)
    if (!sparkConf.contains("spark.master")) {
      sparkConf.setMaster(config.sparkMaster)
    }
    val ssc = new StreamingContext(sparkConf, Seconds(config.streamMicrobatchSeconds))
    ssc.checkpoint("./checkpoint")
    Some(ssc)
  }

  def streamFromEnv(ssc: StreamingContext, config: AppConfig): Option[ReceiverInputDStream[Status]] = {

    System.setProperty("twitter4j.oauth.consumerKey", config.consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", config.consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", config.accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", config.accessTokenSecret)
    Some(TwitterUtils.createStream(ssc, None))
  }

  def processStream(stream: ReceiverInputDStream[Status], config: AppConfig) = {
    implicit val c = config
    stream.map(s => Tweet(s))
      .map(t => (TweetModeState(Seq.empty, config.streamMicrobatchSeconds), t))
      .updateStateByKey(updateFunction)
      .foreachRDD(rdd => println(rdd.take(1).head._2))

  }

  def updateFunction(tweets: Seq[Tweet], state: Option[TweetModeState])(implicit config: AppConfig): Option[TweetModeState] = state match {
    case None => Some(TweetModeState(tweets, config.streamMicrobatchSeconds))
    case Some(s) => Some(s.copy(tweets = s.tweets ++ tweets, batchCount = s.batchCount + 1))
  }

}
