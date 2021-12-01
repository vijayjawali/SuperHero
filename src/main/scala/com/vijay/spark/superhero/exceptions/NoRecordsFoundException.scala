package com.vijay.spark.superhero.exceptions

case class NoRecordsFoundException(message: String,
                                   private val cause: Throwable = None.orNull)
  extends Exception(message, cause)
