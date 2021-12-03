case class Main (
  temp: Double,
  feels_like: Double,
  temp_min: Double,
  temp_max: Double,
  pressure: Int,
  humidity: Int)

case class Clouds (all: Int)

case class Coord (
  lon: Double,
  lat: Double)

case class ChicagoWeatherReport (
  coord: Coord,
  weather: Seq[Weather],
  base: String,
  main: Main,
  visibility: Int,
  wind: Wind,
  clouds: Clouds,
  dt: Int,
  sys: Sys,
  timezone: Int,
  id: Int,
  name: String,
  cod: Int) {

  def getTemperatureType : Int = {
    if (main.temp < 15) {0}
    else if (main.temp < 39) {1}
    else if (main.temp < 59) {2}
    else if (main.temp < 79) {3}
    else {4}
  }

  def getWeatherType : String = {
    val weather_code = weather(0).id
    if (200 to 300 contains weather_code) { "thunder"}
    else if (300 to 500 contains weather_code) {"rain"}
    else if (600 to 700 contains weather_code) {"snow"}
    else if (800 to 900 contains weather_code) {"clear"}
    else {
      println(s"Unknown weather code $weather_code processed as CLEAR")
      "clear"
    }
  }
}

case class Sys (
  `type`: Int,
  id: Int,
  country: String,
  sunrise: Int,
  sunset: Int)

case class Weather (
  id: Int,
  main: String,
  description: String,
  icon: String)

case class Wind (
  speed: Double,
  deg: Int)

