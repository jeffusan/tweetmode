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

    def domainCount: Int = tweets.flatMap(_.domains).size

    def hashTagCount: Int = tweets.flatMap(_.hashTags).size

    def emojiCount: Int = tweets.flatMap(_.emojis).size

    def mediaCount: Int = tweets.flatMap(_.media).size

    private def round(v: Double): Double = BigDecimal(v).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble

    private def percentOfTweets(p: Int): Double = round((p * 100.0f) / tweetCount)

    override def toString: String =
      s"""
         |tweetcount: $tweetCount,
         |tweets per second: $tweetsPerSecond
         |tweets per minute: $tweetsPerMinute
         |tweets per hour: $tweetsPerHour
         |percentage domains: ${percentOfTweets(domainCount)}
         |percentage emojis: ${percentOfTweets(emojiCount)}
         |percentage hashtag: ${percentOfTweets(hashTagCount)}
      """.stripMargin
  }

  case class Emoji(character: String)

  case class HashTag(value: String)

  case class Domain(value: String)

  case class Tweet(value: Status) {

    def domains: Seq[Domain] = value.getURLEntities.map(e => Domain(e.getExpandedURL))

    def hashTags: Seq[HashTag] = value.getHashtagEntities.map(h => HashTag(h.getText))

    def media: Seq[String] = value.getMediaEntities.map(m => m.getExpandedURL)

    def emojis: Seq[Emoji] = value.getSymbolEntities.map(s => Emoji(s.getText))
  }
}
