name := "KaspaCore"

ThisBuild / version := "1.1"
ThisBuild / scalaVersion := "2.12.16"
ThisBuild / assemblyJarName := "kaspacore.jar"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

// https://mvnrepository.com/artifact/org.apache.commons/commons-pool2
libraryDependencies += "org.apache.commons" % "commons-pool2" % "2.11.1"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-kms
libraryDependencies += "com.amazonaws" % "aws-java-sdk-kms" % "1.12.279"

// https://mvnrepository.com/artifact/org.scalaz/scalaz-core
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.3.6"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.2" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.2.0" % Test

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "3.2.0"

// https://mvnrepository.com/artifact/io.confluent/kafka-avro-serializer
libraryDependencies += "io.confluent" % "kafka-avro-serializer" % "7.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-token-provider-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-token-provider-kafka-0-10" % "3.2.0"

// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "3.2.0"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "3.2.0"

// https://mvnrepository.com/artifact/com.snowplowanalytics/scala-maxmind-iplookups
libraryDependencies += "com.snowplowanalytics" %% "scala-maxmind-iplookups" % "0.7.2"

assembly / assemblyMergeStrategy := {
  {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  }
}
