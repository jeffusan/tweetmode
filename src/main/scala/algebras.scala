import twitter4j.Status

object algebras {

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

    def tweetsPerSecond: Int = tweetCount / (batchRate * batchCount)

    def tweetsPerMinute: Int = tweetsPerSecond * 60

    def tweetsPerHour: Int = tweetsPerMinute * 60

    def domainCount: Int = tweets.flatMap(t => t.domains).size

    def hashTagCount: Int = tweets.count(_.hasHashTag)

    def emojiCount: Int = tweets.count(_.hasEmoji)

    def mediaCount: Int = tweets.count(_.hasMedia)

    override def toString: String = {
      s"""
         |tweetcount: $tweetCount,
         |tweets per second: $tweetsPerSecond
         |tweets per minute: $tweetsPerMinute
         |tweets per hour: $tweetsPerHour
      """.stripMargin
    }
  }

  case class Emoji(character: String)

  case class HashTag(value: String)

  case class Domain(value: String)

  case class Tweet(value: Status) {

    def domains: Seq[Domain] = value.getURLEntities.map(e => Domain(e.getExpandedURL))

    def hasHashTag: Boolean = value.getHashtagEntities.length > 0

    def hasMedia: Boolean = value.getMediaEntities.length > 0

    def hasEmoji: Boolean = value.getSymbolEntities.length > 0
  }
}
