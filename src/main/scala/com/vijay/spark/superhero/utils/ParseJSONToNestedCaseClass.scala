package com.vijay.spark.superhero.utils

import org.json4s.native.JsonParser
import org.json4s.{CustomSerializer, JField, JObject, JString}

class AddressSerializer extends CustomSerializer[Address](format => (
  {
    case JObject(
    JField("street", JString(street))
      :: JField("city", JString(city))
      :: JField("state", JString(state))
      :: JField("zip", JString(zip))
      :: Nil
    ) => Address(street, city, state, zip)
  },
  {
    case address: Address =>
      JObject(
        JField("street", JString(address.street))
          :: JField("city", JString(address.city))
          :: JField("state", JString(address.state))
          :: JField("zip", JString(address.zip))
          :: Nil
      )
  }
))

case class Address(street: String, city: String, state: String, zip: String)

// User contains a nested Address instance
case class User(id: Long, name: String, email: String, address: List[Address])

object NestedClass extends App{

  implicit val formats = org.json4s.DefaultFormats + new AddressSerializer

  val jsonRequestString = s"""
                             |{
                             |  "id": 5407,
                             |  "name": "Clementine Bauch",
                             |  "email": "Sincere@april.biz",
                             |  "address": [{
                             |    "street": "87473 Kulas Light",
                             |    "city": "Gwenborough",
                             |    "state": "CA",
                             |    "zip": "92998"
                             |  },
                             |  {
                             |    "street": "87473 Kulas Light",
                             |    "city": "Gwenborough",
                             |    "state": "CA",
                             |    "zip": "92998"
                             |  }]
                             |}
   """.stripMargin

  val parsedJson = JsonParser.parse(jsonRequestString)
  val user = parsedJson.extract[User]

  println(user.address(1).city)
}
