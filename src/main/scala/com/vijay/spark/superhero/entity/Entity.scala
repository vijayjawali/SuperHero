package com.vijay.spark.superhero.entity

case class SuperHeroNames(id: Int, name: String)

case class SuperHero(value: String)

case class InputDetails(datasetid: String, datasetpath: String)

case class InputJsonDetails(jobname: String, inputdatapath: Seq[InputDetails], outputdatapath: String)

case class ConfigDetails(jobname: String, jobidentifier: String, schemaname: String)

case class ConnectionConfigDetails(jdbcurl: String, drivername: String, username: String, password: String)

case class OutputWrite(outputdata: Seq[OutputWriteDetails])

case class OutputWriteDetails(connections: Long, name: String, jdbcurl: String, drivername: String, username: String, password: String, jobname: String, jobidentifier: String, schemaname: String)