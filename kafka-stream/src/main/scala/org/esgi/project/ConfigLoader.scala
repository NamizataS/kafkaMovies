package org.esgi.project

import java.io.FileNotFoundException
import java.util.Properties

object ConfigLoader {
  /***
   *
   * @param propertiesFilename
   * @return
   */
  def loadPropertiesFile(propertiesFilename: String): Properties = {
    val properties = new Properties()
    val resources = getClass.getResourceAsStream(s"/$propertiesFilename")
    try {
      properties.load(resources)
    } catch {
      case _:Throwable => throw new RuntimeException(s"$propertiesFilename cannot be found.")
    }
    properties
  }

}
