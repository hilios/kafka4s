package io.kafka4s.effect

import java.util.Properties

import cats.implicits._
import com.typesafe.config.ConfigFactory

import scala.collection.JavaConverters._

package object properties {

  private[kafka4s] def fromConfig(path: String): Either[Throwable, Properties] =
    for {
      configObj <- Either.catchNonFatal {
        ConfigFactory.load().getObject(path).unwrapped().asScala.toMap
      }
    } yield
      configObj.foldLeft[Properties](new Properties()) {
        case (props, item) =>
          val (key, value)  = item
          val normalizedKey = key.replaceAll(raw"-", raw".")
          props.put(normalizedKey, value)
          props
      }

  object implicits extends GetterImplicits {
    implicit class MapOps[A](val map: Map[String, A]) extends AnyVal {

      def toProperties: Properties = {
        map.foldLeft(new Properties()) {
          case (props, item) =>
            val (key, value) = item
            props.put(key, value)
            props
        }
      }
    }

    implicit class PropertiesOps(val properties: Properties) extends AnyVal {
      def getter[T](key: String)(implicit G: Getter[T]): Getter.Result[T] = G.get(properties, key)
    }
  }
}
