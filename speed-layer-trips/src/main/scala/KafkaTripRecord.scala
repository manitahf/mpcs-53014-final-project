import scala.reflect.runtime.universe._

case class KafkaTripRecord(
  trip_date: String,
  pickup_area: String,
  dropoff_area: String,
  trip_total: Long,
  trip_miles: Long,
  trip_seconds: Long,
  trip_type: String)
