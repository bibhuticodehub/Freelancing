package org.scaladatabaseutils.yaml

import scala.io.Source
import org.ho.yaml.Yaml
import java.io.{ File, FileInputStream }

object YamlReaderUtils {

  def readConfigFile(path: String): String = {
    val stream = new FileInputStream(new File(path))
    Source.fromInputStream(stream).mkString
  }

  def convertYamlToMap(yamlString: String): java.util.HashMap[String, Any] = {
    Yaml.loadType(yamlString, classOf[java.util.HashMap[String, Any]])
  }
}