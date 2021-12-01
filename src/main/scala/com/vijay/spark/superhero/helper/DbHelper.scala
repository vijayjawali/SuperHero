package com.vijay.spark.superhero.helper

import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.exceptions.NoRecordsFoundException
import com.vijay.spark.superhero.entity.ConnectionConfigDetails
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.{Failure, Success, Try}



trait DbHelper extends LazyLogging with Serializable {

  /**
   * Function to Read Table Data from Database
   *
   * @param query : SQL Query
   * @param schemaName : Database Schema Name
   * @param spark : SparkSession
   * @param dbDetails : Database Details in the format ConnectionConfigDetails
   * @return
   */
  def readTable(query: String,
                schemaName: String)
               (implicit spark: SparkSession, dbDetails: ConnectionConfigDetails): DataFrame = {

    logger.info("Reading Table From Database")

    val schema_name = "SCHEMA_NAME"
    val prop = new java.util.Properties()
    prop.setProperty("user", dbDetails.username)
    prop.setProperty("password", dbDetails.password)
    prop.setProperty("driver", dbDetails.drivername)

    Try{
      spark.sqlContext.read.jdbc(dbDetails.jdbcurl,query.replaceAll(schema_name,schemaName),prop)
    } match {
      case Success(dataFrame: DataFrame) =>
        if(dataFrame.head(1).isEmpty) {
          throw NoRecordsFoundException("No Records Found in Table" + query)
        }

        logger.info("Table Read Successful")
        logger.info(s"Number of Records read : ${dataFrame.count()}")

        dataFrame
      case Failure(exception) =>
        logger.error("Error While Reading Table" + query)
        throw exception
    }
  }

}
