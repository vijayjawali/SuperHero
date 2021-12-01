package com.vijay.spark.superhero.helper

import com.vijay.spark.superhero.entity.InputDetails
import org.json4s.{CustomSerializer, Formats, JField, JObject, JString}

class InputDetailsSerializer extends CustomSerializer[InputDetails]((format: Formats) => (
  {
    case JObject(
    JField("datasetid", JString(datasetid))
      :: JField("datasetpath", JString(datasetpath))
      :: Nil
    ) => InputDetails(datasetid, datasetpath)
  },
  {
    case inputdatapath: InputDetails =>
      JObject(
        JField("datasetid", JString(inputdatapath.datasetid))
          :: JField("datasetpath", JString(inputdatapath.datasetpath))
          :: Nil
      )
  }
))
