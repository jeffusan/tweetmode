name := "tweetmode"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies ++= Seq(
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.2",
  "org.apache.spark" %% "spark-tags" % "2.3.2",
  "org.twitter4j" % "twitter4j-stream" % "4.0.6",
  "org.apache.spark" %% "spark-streaming" % "2.3.2",
  "com.github.pureconfig" %% "pureconfig" % "0.10.2",
  "org.typelevel" %% "cats-core" % "1.6.0",
  "org.typelevel" %% "cats-effect" % "1.2.0",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.0" % Test)

