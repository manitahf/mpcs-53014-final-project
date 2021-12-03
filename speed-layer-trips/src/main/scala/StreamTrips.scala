import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Increment, Put}
import org.apache.hadoop.hbase.util.Bytes
import java.time


object StreamTrips {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val tripStats = hbaseConnection.getTable(TableName.valueOf("manita_trip_stats_by_route_and_weather"))
  var weatherReport = WeatherUtils.getCurrentChicagoWeatherReport
  var weatherLastUpdated = time.Instant.now()

  def updateTripStats(ktr: KafkaTripRecord): String = {
//    If no weather available or weather has not been fetched in 5 minutes, then fetch new weather report
    if(weatherReport.isEmpty || time.Instant.now().isAfter(weatherLastUpdated.plusSeconds(30))) {
      weatherReport = WeatherUtils.getCurrentChicagoWeatherReport
    }
//    If no weather available, return
    if (weatherReport.isEmpty)
      return "Weather currently unavailable";
    val weather = weatherReport.get.getWeatherType
    val temperature = weatherReport.get.getTemperatureType
    val current_stats = tripStats.get(new Get(Bytes.toBytes(ktr.pickup_area + ktr.dropoff_area + weather + temperature)))
    val family = ktr.trip_type
    if (current_stats.isEmpty) {
      val new_put = new Put(Bytes.toBytes(ktr.pickup_area + ktr.dropoff_area + weather + temperature))
      new_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("trips"), Bytes.toBytes(1))
      new_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("total_price"), Bytes.toBytes(ktr.trip_total))
      new_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("total_distance"), Bytes.toBytes(ktr.trip_miles))
      new_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("total_duration"), Bytes.toBytes(ktr.trip_seconds))
      new_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("min_price"), Bytes.toBytes(ktr.trip_total))
      new_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("max_price"), Bytes.toBytes(ktr.trip_total))
      tripStats.put(new_put)
    } else {
      val inc = new Increment(Bytes.toBytes(ktr.pickup_area + ktr.dropoff_area + weather + temperature))
      inc.addColumn(Bytes.toBytes(family), Bytes.toBytes("trips"), 1)
      inc.addColumn(Bytes.toBytes(family), Bytes.toBytes("total_price"), ktr.trip_total)
      inc.addColumn(Bytes.toBytes(family), Bytes.toBytes("total_distance"), ktr.trip_miles)
      inc.addColumn(Bytes.toBytes(family), Bytes.toBytes("total_duration"), ktr.trip_seconds)
      tripStats.increment(inc)

      val current_min = Bytes.toLong(current_stats.getValue(Bytes.toBytes(family), Bytes.toBytes("min_price")))
      val current_max = Bytes.toLong(current_stats.getValue(Bytes.toBytes(family), Bytes.toBytes("max_price")))
      if (ktr.trip_total < current_min) {
        val min_put = new Put(Bytes.toBytes(ktr.pickup_area + ktr.dropoff_area + weather + temperature))
        min_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("min_price"), Bytes.toBytes(ktr.trip_total))
        tripStats.put(min_put)
      }
      if (ktr.trip_total > current_max) {
        val max_put = new Put(Bytes.toBytes(ktr.pickup_area + ktr.dropoff_area + weather + temperature))
        max_put.addColumn(Bytes.toBytes(family), Bytes.toBytes("max_price"), Bytes.toBytes(ktr.trip_total))
        tripStats.put(max_put)
      }
    }
    return s"Updated speed layer with ${ktr.trip_type} trip from ${ktr.pickup_area} to ${ktr.dropoff_area}: ${ktr.trip_date.slice(0,ktr.trip_date.indexOf('(')-1)}"
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: StreamTrips <brokers>
           |  <brokers> is a list of one or more Kafka brokers
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamTrips")
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("manita_trips")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);

    val ktrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[KafkaTripRecord]))

    // Update speed table
    val processedFlights = ktrs.map(updateTripStats)
    processedFlights.print()
    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }
}
