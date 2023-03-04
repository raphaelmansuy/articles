import sbt._

object Dependencies {
    // Spark
    private val sparkSQL = "org.apache.spark" %% "spark-sql"
    private val sparkCore = "org.apache.spark" %% "spark-core"
    private val spark: Seq[ModuleID] = Seq(sparkSQL, sparkCore).map(_.%(Versions.spark))



		// Hadoop
		private val hadoop = "org.apache.hadoop" % "hadoop-client" % Versions.hadoop


    // Delta
    private val deltaCore = "io.delta" %% "delta-core" % Versions.deltaLake

    // DB
    private val postgreSQL = "org.postgresql" % "postgresql" % Versions.postgreSQL
    private val mySQL = "mysql" % "mysql-connector-java" % Versions.mySQL
    private val oracle = "com.oracle.database.jdbc" % "ojdbc8" % Versions.oracle
    private val db = Seq(postgreSQL, mySQL, oracle)


    val commonDependencies: Seq[ModuleID] = spark ++ db ++ Seq(deltaCore,hadoop)
}


