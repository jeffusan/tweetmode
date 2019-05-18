
import org.apache.log4j.{Level, LogManager}
import functions.{loadConfig, processStream, streamFromEnv, streamingContext}

object tweetmode {

  def main(args: Array[String]): Unit = for {
    config <- loadConfig("tweetmode")
    _ = LogManager.getRootLogger.setLevel(Level.ERROR)
    ssc <- streamingContext(config)
    stream <- streamFromEnv(ssc, config)
  } yield {
    processStream(stream, config)

    ssc.start()
    ssc.awaitTermination()
  }

}
