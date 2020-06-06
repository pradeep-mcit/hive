name := "hive"

version := "0.1"

scalaVersion := "2.13.1"




libraryDependencies += "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.16.2"


//libraryDependencies += "org.apache.hive.hcatalog" % "hive-hcatalog-core" % "1.1.0-cdh5.16.2"


resolvers += "Cloudera" at "http://repository.cloudera.com/artifactory/cloudera-repos/"


// https://mvnrepository.com/artifact/com.github.wnameless.json/json-flattener
//libraryDependencies += "com.github.wnameless.json" % "json-flattener" % "0.8.1"



