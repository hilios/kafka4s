import mdoc.MdocPlugin.autoImport.mdocVariables
import microsites.MicrositesPlugin.autoImport.{
  micrositeBaseUrl,
  micrositeDescription,
  micrositeName,
  micrositePalette,
  micrositeUrl
}
import sbt.Keys.version
import sbt._

object Microsite {

  val settings = Seq(
    micrositeName := "Kafka4s",
    micrositeDescription := "",
    micrositeUrl := "https://kafka4s.github.io/kafka4s",
    micrositeBaseUrl := "/kafka4s",
    micrositePalette := Map(
      "brand-primary"   -> "#E05236",
      "brand-secondary" -> "#3F3242",
      "brand-tertiary"  -> "#2D232F",
      "gray-dark"       -> "#453E46",
      "gray"            -> "#837F84",
      "gray-light"      -> "#E3E2E3",
      "gray-lighter"    -> "#F4F3F4",
      "white-color"     -> "#FFFFFF"
    ),
    mdocVariables := Map(
      "VERSION" -> version.value
    )
  )
}
