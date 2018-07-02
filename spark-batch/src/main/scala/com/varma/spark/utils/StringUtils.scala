package com.varma.spark.utils

import scala.util.Try

/**
  * Created by varma on 7/1/2018.
  */
object StringUtils {

  def getStringOrDefault(str: AnyRef, default: String): String = {
    if (Try(str.toString).isSuccess && notNullString(str.toString)) str.toString else default;
  }

  def notNullString(str: String): Boolean = {
    if (str != null && (str ne null) && str.trim != "")
      return true;
    return false;
  }

}
