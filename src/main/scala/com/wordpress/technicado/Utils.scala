package com.wordpress.technicado

import org.apache.commons.configuration.PropertiesConfiguration

object Utils {

  protected val properties = new PropertiesConfiguration()

  def readConfig(propertyFile: String): Unit = {
    properties.load(propertyFile)
  }

  def getString(propertyName: String): String =
    properties.getString(propertyName)

}
