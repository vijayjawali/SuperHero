package com.vijay.spark.superhero.helper

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.entity.{ConfigDetails, ConnectionConfigDetails, InputJsonDetails}
import org.apache.spark.sql.SparkSession
import org.json4s.Formats
import org.json4s.native.JsonParser

import scala.util.{Failure, Success, Try}

trait MainHelper extends LazyLogging with Serializable {

  /**
   * Function to Inititate Spark Session
   *
   * @return
   */
  def getSparkSession: SparkSession = {

    logger.info("Creating Spark Session")

    Try {
      SparkSession
        .builder
        .master("local[*]")
        .appName("SuperHero")
        .getOrCreate()
    } match {
      case Success(spark) =>
        logger.info("Spark Session Created")
        spark
      case Failure(exception) =>
        logger.error("Spark Session not Created")
        throw exception
    }
  }

  /**
   * Function to Parse Input JSON file
   *
   * @param path : JSON file path
   * @return
   */
  def getInputJsonDetails(path: String): InputJsonDetails = {

    implicit val formats: Formats = org.json4s.DefaultFormats + new InputDetailsSerializer

    logger.info("Getting Input JSON Details")

    Try {
       scala.io.Source.fromFile(path)
    } match {
      case Success(jsonRequest) =>
        val jsonRequestString = jsonRequest.mkString
        val parsedJson = JsonParser.parse(jsonRequestString)
        val inputJson: InputJsonDetails = parsedJson.extract[InputJsonDetails]

        logger.info("Completed Parsing Input JSON")
        logger.info(s"$inputJson")

        inputJson
      case Failure(exception) =>
        logger.error(s"No JSON Found at $path")
        throw exception
    }
  }

  /**
   * Function to Parse Configuration Details for Spark Job
   *
   * @return
   */
  def getConfigDetails: ConfigDetails = {

    logger.info("Getting Configuration Details")

    Try {
      ConfigFactory.load("jobconfig")
    } match {
      case Success(jobconfig) =>
        val jobidentifier = jobconfig.getString("jobconfigurationdetails.jobidentifier")
        val jobname = jobconfig.getString("jobconfigurationdetails.jobname")
        val schemaname = jobconfig.getString("jobconfigurationdetails.schemaname")
        val conf = ConfigDetails(jobidentifier, jobname, schemaname)

        logger.info("Completed Parsing Job Configuration")
        logger.info(s"$conf")

        conf
      case Failure(exception) =>
        logger.error("JobConfig not Found")
        throw exception
    }
  }

  /**
   * Function to Parse Connection Details of Database
   *
   * @return
   */
  def getConnectionConfigDetails: ConnectionConfigDetails = {

    logger.info("Getting Connection Details")

    Try {
      ConfigFactory.load("connection")
    } match {
      case Success(conn) =>
        val jdbcurl = conn.getString("dbDetails.default.jdbcurl")
        val drivername = conn.getString("dbDetails.default.drivername")
        val username = conn.getString("dbDetails.default.username")
        val password = conn.getString("dbDetails.default.password")
        val connection = ConnectionConfigDetails(jdbcurl, drivername, username, password)

        logger.info("Completed Parsing Connection Details")
        logger.info(s"$connection")

        connection
      case Failure(exception) =>
        logger.error("Connection Configuration not found")
        throw exception
    }
  }

  /**
   * Function to Close Spark Session
   *
   * @param spark : SparkSession
   */
  def closeSparkSession(spark: SparkSession): Unit = {

    logger.info("Closing Spark Session")
    spark.close()
    logger.info("Spark Session Closed")
  }
}
