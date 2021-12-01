package com.vijay.spark.superhero

import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.entity.{ConnectionConfigDetails, InputDetails, InputJsonDetails}
import com.vijay.spark.superhero.helper.{DbHelper, MainHelper, SuperHeroJobHelper}

trait SuperHeroTestHelper extends SuperHeroJobHelper
  with DbHelper with MainHelper with LazyLogging with Serializable {

  val inputJson: InputJsonDetails = InputJsonDetails(jobname = "SuperHero",
    inputdatapath = Seq(InputDetails(datasetid = "MARVEL_NAMES",
      datasetpath = "F:/Datasets/Marvel-names.txt")),
    outputdatapath = "F:/Datasets/Output")

  val jobconfigurationdetails: Option[Map[String,String]] = Some(Map(
    "jobname" -> "SuperHeroJob",
    "jobidentifier" -> "SuperHeroJob",
    "schemaname" -> "NLW98739"))

  val sqlqueries: Option[Map[String,String]] = Some(Map(
    "query_one" -> """(SELECT DISTINCT(launch_site) FROM SPACEXTBL) query_one""",
    "query_two" -> """(SELECT * FROM SPACEXTBL WHERE LAUNCH_SITE LIKE 'CCA%' LIMIT 5) query_two"""))

  val dbDetails: ConnectionConfigDetails = ConnectionConfigDetails(
    jdbcurl = "jdbc:db2://8e359033-a1c9-4643-82ef-8ac06f5107eb.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:30120/bludb:sslConnection=true;",
    drivername = "com.ibm.db2.jcc.DB2Driver",
    username = "nlw98739",
    password = "K5mNmB43c8DN2Qot"
  )
}
