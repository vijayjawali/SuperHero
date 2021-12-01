package com.vijay.spark.superhero

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import com.vijay.spark.superhero.entity._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import java.util.Properties

class SuperHeroTest extends AnyFunSuite
  with BeforeAndAfterAll with SuperHeroTestHelper
  with LazyLogging with Serializable {


  implicit var spark: SparkSession = _
  var dbProperties: Properties = _
  var connection: ConnectionConfigDetails = _
  var input: InputJsonDetails = _
  var result: DataFrame = _
  var output: Dataset[OutputWrite] = _


  override protected def beforeAll(): Unit = {

    spark = SparkSession
      .builder()
      .master("local")
      .appName("SuperHero")
      .getOrCreate()

    val User = dbDetails.username
    val URL = dbDetails.jdbcurl
    val password = dbDetails.password
    val driver = dbDetails.drivername

    dbProperties = new Properties()

    dbProperties.setProperty("user",User)
    dbProperties.setProperty("jdbcurl",URL)
    dbProperties.setProperty("driver",driver)
    dbProperties.setProperty("password",password)
  }

  test("Validate Connection Configuration Details"){

    connection = ConnectionConfigDetails(
      username = ConfigFactory.load("connectionTest").getString("dbDetails.default.username"),
      jdbcurl = ConfigFactory.load("connectionTest").getString("dbDetails.default.jdbcurl"),
      password = ConfigFactory.load("connectionTest").getString("dbDetails.default.password"),
      drivername = ConfigFactory.load("connectionTest").getString("dbDetails.default.drivername"))

    assert(connection.username === "C")
    assert(connection.jdbcurl === "A")
    assert(connection.password === "D")
    assert(connection.drivername === "B")

    connection = getConnectionConfigDetails

    assert(dbDetails.username === "nlw98739")
    assert(dbDetails.jdbcurl === "jdbc:db2://8e359033-a1c9-4643-82ef-8ac06f5107eb.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:30120/bludb:sslConnection=true;")
    assert(dbDetails.password === "K5mNmB43c8DN2Qot")
    assert(dbDetails.drivername === "com.ibm.db2.jcc.DB2Driver")

  }

  test("Validate Input JSON Details"){

    input = InputJsonDetails(
      jobname = inputJson.jobname,
        inputdatapath = Seq(InputDetails(
          datasetid = inputJson.inputdatapath.head.datasetid,
          datasetpath = inputJson.inputdatapath.head.datasetpath)),
      outputdatapath = inputJson.outputdatapath
    )

    assert(input.jobname === "SuperHero")
    assert(input.outputdatapath === "F:/Datasets/Output")
    assert(input.inputdatapath.head.datasetid === "MARVEL_NAMES")
    assert(input.inputdatapath.head.datasetpath === "F:/Datasets/Marvel-names.txt")

    input = getInputJsonDetails("src/test/resources/inputTest.json")

    assert(input.jobname === "A")
    assert(input.outputdatapath === "G")
    assert(input.inputdatapath.head.datasetid === "B")
    assert(input.inputdatapath.head.datasetpath === "C")
    assert(input.inputdatapath(1).datasetid === "D")
    assert(input.inputdatapath(1).datasetpath === "E")

  }

  test("Validate Database Output"){

    result = readTable(sqlqueries.get("query_one"),
      jobconfigurationdetails.get("schemaname"))(spark, dbDetails)

    assert(result.select("launch_site").first.getString(0) === "CCAFS LC-40")

  }

  test("Validate the names fetched from SuperHero Names File"){
    result = getSuperHeroNames("src/test/resources/SuperHeroNames.txt").toDF()

    assert(result.count() === 10)
    assert(result.select("id").first.getInt(0) === 1)
  }

  test("Validate the data fetched from SuperHero Data File"){

    result = getSuperHeroData("src/test/resources/SuperHeroData.txt").toDF()

    assert(result.count() === 10)
    assert(result.select("value").first.getString(0).substring(0,4) === "5988")

  }

  test("Validate the calculation of connections"){
    result = getConnections(
      getSuperHeroData("src/test/resources/SuperHeroData.txt"))

    assert(result.filter(col("id") === 5985).select("connections").first.getLong(0) === 20)

  }

  test("Validate the maximum connection"){
    result = orderData(
      getConnections(
        getSuperHeroData("src/test/resources/SuperHeroData.txt")))

    assert(result.select(col("id")).first.getString(0) === "5986")
    assert(result.select(col("connections")).first.getLong(0) === 143)

  }

  test("Validate the joining of data between maximum connection and SuperHero Data"){
    result = joinData(
      orderData(
        getConnections(
          getSuperHeroData("src/test/resources/SuperHeroData.txt"))),
            getSuperHeroNames("src/test/resources/SuperHeroNames.txt"))

    assert(result.select(col("connections")).first.getLong(0) === 143)
    assert(result.select(col("name")).first.getString(0) === "VALINOR")
  }

  test("Validate the output generated"){
    output = generateOutput(
      joinData(
        orderData(
          getConnections(
            getSuperHeroData("src/test/resources/SuperHeroData.txt"))),
                getSuperHeroNames("src/test/resources/SuperHeroNames.txt")))(spark,dbDetails,
                    ConfigDetails(jobname = jobconfigurationdetails.get("jobname"),
                                  jobidentifier = jobconfigurationdetails.get("jobidentifier"),
                                  schemaname = jobconfigurationdetails.get("schemaname")))

    assert(output.head().outputdata.head.connections === 143)
    assert(output.head().outputdata.head.name === "VALINOR")
    assert(output.head().outputdata.head.jdbcurl === "jdbc:db2://8e359033-a1c9-4643-82ef-8ac06f5107eb.bs2io90l08kqb1od8lcg.databases.appdomain.cloud:30120/bludb:sslConnection=true;")
    assert(output.head().outputdata.head.drivername === "com.ibm.db2.jcc.DB2Driver")
    assert(output.head().outputdata.head.username === "nlw98739")
    assert(output.head().outputdata.head.password === "K5mNmB43c8DN2Qot")
    assert(output.head().outputdata.head.jobname === "SuperHeroJob")
    assert(output.head().outputdata.head.jobidentifier === "SuperHeroJob")
    assert(output.head().outputdata.head.schemaname === "NLW98739")
  }

  test("Validate the Output Write"){
    val output: Unit = writeOuptut(
      joinData(
        orderData(
          getConnections(
            getSuperHeroData("src/test/resources/SuperHeroData.txt"))),
              getSuperHeroNames("src/test/resources/SuperHeroNames.txt")),
                                "src/test/resources/output",
                                "JSON")
    assert(output.equals())
  }

  override protected def afterAll(): Unit = {
    spark.stop()
  }
}
