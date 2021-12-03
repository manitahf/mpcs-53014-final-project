import org.scalatest.flatspec.AnyFlatSpec

class WeatherUtilsTest extends AnyFlatSpec {
  "WeatherUtils" should "get the current weather report" in {
    val report = WeatherUtils.getCurrentChicagoWeatherReport
    assert(report.isDefined && report.get.isInstanceOf[ChicagoWeatherReport])
  }

  it should "correctly return the temperature type" in {
    val report = ChicagoWeatherReport(Coord(41.85,-87.65),List(Weather(804,"Clouds","overcast clouds","04n")),"stations",Main(46.63,46.63,42.55,48.45,1009,87),10000,Wind(1.99,225),Clouds(90),1638410724,Sys(2,2005153,"US",1638363547,1638397249),-21600,4887398,"Chicago",200)
    assert(report.getTemperatureType == 2)
  }

  it should "correctly return the weather type" in {
    val report = ChicagoWeatherReport(Coord(41.85,-87.65),List(Weather(804,"Clouds","overcast clouds","04n")),"stations",Main(46.63,46.63,42.55,48.45,1009,87),10000,Wind(1.99,225),Clouds(90),1638410724,Sys(2,2005153,"US",1638363547,1638397249),-21600,4887398,"Chicago",200)
    assert(report.getWeatherType == "clear")
  }
}
