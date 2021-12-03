import sttp.client3._
import sttp.client3.sprayJson._
import spray.json._

object WeatherUtils extends App {

  object WeatherJsonProtocol extends DefaultJsonProtocol{
    implicit val coordFormat = jsonFormat(Coord, "lat", "lon")
    implicit val windFormat = jsonFormat(Wind, "speed", "deg")
    implicit val cloudFormat = jsonFormat(Clouds, "all")
    implicit val sysFormat = jsonFormat(Sys, "type", "id", "country", "sunrise", "sunset")
    implicit val mainFormat = jsonFormat(Main, "temp", "feels_like", "temp_min", "temp_max", "pressure", "humidity")
    implicit val weatherFormat = jsonFormat(Weather, "id", "main", "description", "icon")
    implicit val reportFormat = jsonFormat(ChicagoWeatherReport, "coord", "weather", "base", "main", "visibility", "wind", "clouds", "dt", "sys", "timezone", "id", "name", "cod")
  }

  def getCurrentChicagoWeatherReport: Option[ChicagoWeatherReport] = {
    val backend = HttpURLConnectionBackend()
    implicit val weatherReportFormat: RootJsonFormat[ChicagoWeatherReport] = WeatherJsonProtocol.reportFormat
    val response: Identity[Response[Either[ResponseException[String, Exception],ChicagoWeatherReport]]] = basicRequest
      .get(uri"http://api.openweathermap.org/data/2.5/weather?id=4887398&appid=a4d869eb14da51c5b4cc95b63838e676&units=imperial")
      .response(asJson[ChicagoWeatherReport])
      .send(backend)
    println("Weather requested")
    println(response.body)
    response.body.fold(_ => None, x => Some(x))
  }
}
