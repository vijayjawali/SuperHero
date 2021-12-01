package com.vijay.spark.superhero.helper

import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.entity._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession, functions}

import scala.util.{Failure, Success, Try}

trait SuperHeroJobHelper extends LazyLogging with Serializable {

  /**
   * Function to Read SuperHero Names File
   *
   * @param path : Names file path
   * @param spark : SparkSession
   * @return
   */
  def getSuperHeroNames(path : String)(implicit spark: SparkSession): Dataset[SuperHeroNames] = {

    logger.info("Started Reading Super Hero Names from File")

    val superHeroNameSchema = new StructType()
      .add("id", IntegerType, nullable = true)
      .add("name", StringType, nullable = true)

    import spark.implicits._
    Try {
      spark.read
        .schema(superHeroNameSchema)
        .option("sep", " ")
        .csv(path)
        .as[SuperHeroNames]
    } match {
      case Success(names) =>
        logger.info("Completed Reading Name File")
        names
      case Failure(exception) =>
        logger.info("Failed to read Names File")
        throw exception
    }

  }


  /**
   * Function to Read SuperHero Data File
   *
   * @param path : SuperHero Data File path
   * @param spark : SparkSession
   * @return
   */
  def getSuperHeroData(path: String)(implicit spark: SparkSession): Dataset[SuperHero] = {

    logger.info("Started Reading Super Hero Data from File")

    import spark.implicits._
    Try {
      spark.read
        .textFile(path)
        .as[SuperHero]
    } match {
      case Success(lines) =>
        logger.info("Completed Reading Data File")
        lines
      case Failure(exception) =>
        logger.info("Failed to read Data File")
        throw exception
    }
  }

  /**
   * Function to Get the NUmber of Connections
   *
   * @param superHeroData : Dataset[SuperHero]
   * @param spark : SparkSession
   * @return
   */
  def getConnections(superHeroData: Dataset[SuperHero])(implicit spark: SparkSession): DataFrame = {

    logger.info("Getting the Number of Connections for SuperHero")

    val connections = superHeroData.withColumn("id", functions.split(col("value"), " ").getItem(0))
      .withColumn("trim_value", trim(col("value")))
      .withColumn("connections", functions.length(col("trim_value")) - functions.length(regexp_replace(col("trim_value"), " ", "")) + 1)
      .groupBy("id")
      .agg(sum("connections").alias("connections"))
    connections
  }

  /**
   * Function to Order Data in Descending
   *
   * @param input : Input dataframe to order data
   * @param spark : Dataset[SuperHero]
   * @return
   */
  def orderData(input: DataFrame)(implicit spark: SparkSession): DataFrame = {

    logger.info("Ordering Connection Data")

    val maxConnections = input.orderBy(desc("connections")).limit(1)
    maxConnections
  }

  /**
   * Function to Join Connections with SuperHero Names
   *
   * @param maxConnections : Ordered Data to find Maximum connections
   * @param names : Dataset[SuperHeroNames]
   * @param spark : SparkSession
   * @return
   */
  def joinData( maxConnections: DataFrame, names: Dataset[SuperHeroNames])(implicit spark: SparkSession): DataFrame = {

    logger.info("Joining Super Hero Names with Connections")

    val superheroName = maxConnections.join(names, maxConnections("id") === names("id"), "inner").drop("id")
    superheroName
  }

  /**
   * Function to Generate Output
   *
   * @param joinedData : Dataframe to generate Output
   * @param spark : SparkSession
   * @param connection : ConnectionConfigDetails
   * @param config : ConfigDetails
   * @return
   */
  def generateOutput(joinedData: DataFrame)(implicit spark: SparkSession, connection: ConnectionConfigDetails, config: ConfigDetails): Dataset[OutputWrite] = {

    logger.info("Generating Output Dataset")

    import spark.implicits._
    val outputData: Dataset[OutputWrite] = Seq(OutputWrite(Seq(
       OutputWriteDetails(connections = joinedData.select(col("connections")).first.getLong(0),
        name = joinedData.select(col("name")).first.getString(0),
        jdbcurl = connection.jdbcurl,
        drivername = connection.drivername,
        username = connection.username,
        password = connection.password,
        jobname= config.jobname,
        jobidentifier = config.jobidentifier,
        schemaname = config.schemaname)
    ))).toDS()

    logger.info(s"Output Format: $outputData")
    logger.info(s"Output Generated : ${outputData.show(false)}")
    outputData
  }

  /**
   * Function to Write Output
   *
   * @param data : Dataset to write output
   * @param path : output data path
   * @param format : format to write data
   * @tparam T : Generic Dastaset
   */
  def writeOuptut[T](data:Dataset[T],path: String, format: String): Unit = {

    logger.info("Writing Output Data")

    Try {
      data.coalesce(1)
        .write
        .format(format)
        .mode(SaveMode.Append)
        .save(path)
    } match {
      case Success(value) =>
        logger.info("Successfully Output Wriiten")
        logger.info(s"$value")
      case Failure(exception) =>
        logger.info("Unable to Write Output")
        throw exception
    }
  }
}
