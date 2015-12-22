
name := "BTC_Project"

version := "1.0"

scalaVersion := "2.11.6"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies +=
  "com.typesafe.akka" %% "akka-actor" % "2.3.7"

libraryDependencies +=
  "com.typesafe.akka" %% "akka-remote" % "2.3.7"
