package com.vijay.spark.superhero.jobs


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.entity.{ConfigDetails, ConnectionConfigDetails, InputJsonDetails}
import com.vijay.spark.superhero.helper.{DbHelper, MainHelper}


object Main extends MainHelper with DbHelper with LazyLogging {


  def main(args: Array[String]): Unit = {

    logger.info("Starting Main Method")

    Option(args) match {
      case Some(args) =>
          Logger.getLogger("org").setLevel(Level.WARN)
          System.setProperty("hadoop.home.dir","C:\\Hadoop")

          implicit val inputJson: InputJsonDetails = getInputJsonDetails(args(0))
          println(inputJson)

          implicit val configuration: ConfigDetails = getConfigDetails
          println(configuration)

          implicit val connection: ConnectionConfigDetails = getConnectionConfigDetails
          println(connection)

          implicit val spark: SparkSession = getSparkSession

          logger.info("Spark Session Initialized")

          val superHero: SuperHeroJob = new SuperHeroJob

          logger.info("Running Job" + inputJson.jobname)

          superHero.process()

          closeSparkSession(spark)

      case None => logger.error("ERROR: Invalid Path")
    }

  }

}
