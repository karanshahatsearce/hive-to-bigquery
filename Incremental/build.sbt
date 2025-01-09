name := "Incremental"
version := "1.0"
scalaVersion := "2.12.14"
val sparkVersion = "3.3.1"
libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.0" % "provided"
libraryDependencies += "org.scala-lang" % "scala-reflect" % "2.12.0" % "provided"
libraryDependencies += "com.github.seratch" %% "bigquery4s" % "0.8"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion  % "compile"
libraryDependencies += "org.apache.spark" %% "spark-hive" %  sparkVersion % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.3.1"
libraryDependencies += "com.google.cloud.spark" %% "spark-bigquery-with-dependencies" % "2.12-0.29" from "file:///opt/Hive-to-bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar"
libraryDependencies += "com.hortonworks.hive" %% "hive-warehouse-connector-spark3" % "1.0.0.1.18.7215.0-43" from "file:///opt/Hive-to-bigquery/hive-warehouse-connector-spark3_2.12-1.0.0.1.18.7215.0-43.jar"
libraryDependencies += "org.apache.hadoop" % "hadoop-client-api" % "3.3.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-yarn-client" % "3.3.1"
libraryDependencies += "io.delta" %% "delta-core" % "2.2.0"
//libraryDependencies += "org.apache.hive" % "hive-jdbc" % "3.1.3000.7.1.8.5-1"
// https://mvnrepository.com/artifact/org.apache.hive/hive-metastore
//libraryDependencies += "org.apache.hive" % "hive-metastore" % "3.1.3000.7.2.14.0-149"


libraryDependencies += "org.scala-lang.modules" %% "scala-collection-compat" % "2.9.0"
//libraryDependencies += "com.google.api-client" % "google-api-client" % "2.1.1"
//libraryDependencies += "com.google.apis" % "google-api-services-bigquery" % "v2-rev459-1.25.0"

  libraryDependencies ++= Seq(
      "com.google.apis"         %  "google-api-services-bigquery" % "v2-rev405-1.25.0",
      "com.google.oauth-client" %  "google-oauth-client"          % "1.25.0",
      "com.google.oauth-client" %  "google-oauth-client-jetty"    % "1.25.0",
      "com.google.http-client"  %  "google-http-client-jackson2"  % "1.25.0",
      "ch.qos.logback"          %  "logback-classic"              % "1.2.3"   % Test,
      "org.scalatest"           %% "scalatest"                    % "3.0.5"   % Test

)
