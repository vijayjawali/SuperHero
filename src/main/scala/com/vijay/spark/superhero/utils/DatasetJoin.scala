package com.vijay.spark.superhero.utils

import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}


case class Person(id: Long, name: String, cityId: Long)

case class City(id: Long, name: String)

case class Output(id: Long, name: String)

object DatasetJoin {
  def main(args: Array[String]): Unit = {

    implicit def personEncoder: Encoder[Person] = Encoders.product[Person]

    implicit def cityEncoder: Encoder[City] = Encoders.product[City]

    implicit def outputEncoder: Encoder[Output] = Encoders.product[Output]

    val personSchema = new StructType()
      .add("id", LongType, nullable = true)
      .add("name", StringType, nullable = true)
      .add("cityId", LongType, nullable = true)

    val citySchema = new StructType()
      .add("id", LongType, nullable = true)
      .add("name", StringType, nullable = true)

    val spark = SparkSession
      .builder
      .master("local[*]")
      .appName("jointest")
      .getOrCreate()

    val family: Dataset[Person] = spark.read
      .schema(personSchema)
      .option("sep", ",")
      .csv("src/test/resources/person.csv")
      .as[Person](personEncoder)

    val cities: Dataset[City] = spark.read
      .schema(citySchema)
      .option("sep", ",")
      .csv("src/test/resources/city.csv")
      .as[City](cityEncoder)

    val joined = family.joinWith(cities, family("cityId") === cities("id"), "left_outer")

    joined.show(false)
    val output = joined.map {
      case (a: Person, null) => Output(a.id, null)
      case (a: Person, b: City) => Output(a.id, b.name)
    }
    output.show()

  }
}





