package com.vijay.spark.superhero.jobs

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.entity.{ConfigDetails, ConnectionConfigDetails, InputJsonDetails}
import com.vijay.spark.superhero.helper.SuperHeroJobHelper
import com.vijay.spark.superhero.jobs.Main.readTable
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

class SuperHeroJob(implicit val spark: SparkSession,
                   inputJson: InputJsonDetails,
                   config: ConfigDetails,
                   connection: ConnectionConfigDetails)
  extends LazyLogging with SuperHeroJobHelper with Serializable {


  implicit val schemaname: String = Try{
    println(connection)
    println(config)
    config.schemaname
  } match {
    case Success(value) => value
    case Failure(exception) => throw exception
  }

  def process(): Unit = {

    readTable(ConfigFactory.load("jobconfig").getString("sqlqueries.query_one"),schemaname)

    val superHeroNames = getSuperHeroNames(inputJson.inputdatapath.head.datasetpath)

    val superHeroData = getSuperHeroData(inputJson.inputdatapath(1).datasetpath)

    val connections = getConnections(superHeroData)

    val maxConnections = orderData(connections)

    val joinedData = joinData(maxConnections, superHeroNames)

    val output = generateOutput(joinedData)

    writeOuptut(output, inputJson.outputdatapath,"JSON")


  }
}
