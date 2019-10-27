  
lazy val root = (project in file("."))
  .settings(
    name := "pq",
    scalaVersion := "2.13.1",
    mainClass in assembly := Some("priv.pq.Main"),
    assemblyJarName in assembly := "pq.jar",
    testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oF")
  )

libraryDependencies ++= Seq(
    "org.slf4j" % "log4j-over-slf4j" % "1.7.25",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe" % "config" % "1.3.1",
    "com.github.scopt" %% "scopt" % "3.7.1",
    "org.apache.parquet" % "parquet-avro" % "1.10.1",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2",
    "com.lihaoyi" %% "ujson" % "0.8.0",
    "com.google.cloud.bigdataoss" % "gcs-connector" % "1.9.4-hadoop3",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
    ) ++ hadoopDependencies
  
def hadoopVersion = "3.2.1"
def hadoopDependencies = Seq(
    "org.apache.hadoop" % "hadoop-mapreduce-client-common" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion).map(_.excludeAll(
    ExclusionRule(organization = "log4j"),
    ExclusionRule("org.slf4j", "slf4j-log4j12"),
    ExclusionRule(organization = "org.mortbay.jetty"),
    ExclusionRule(organization = "javax.xml.bind"),
    ExclusionRule(organization = "com.sun.jersey"),
    ExclusionRule(organization = "org.eclipse.jetty"),
    ExclusionRule(organization = "org.apache.curator"),
    ExclusionRule(organization = "org.apache.zookeeper"),
    ExclusionRule(organization = "com.fasterxml.jackson")
))
