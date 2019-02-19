import java.io.IOException
import java.net.{ConnectException, MalformedURLException, URL}
import org.apache.spark.sql.SparkSession

case class Historical_Measures(station_name: String,
                               year: Int,
                               mm: Int,
                               tmax_degC: Float,
                               tmin_degC: Float,
                               af_days: Int,
                               rain_mm: Float,
                               sun_hours: Float)

object HistoricalWeatherAnalysis extends App {
  def readFileFromURL(station_url: String): String = {
    var fileContents: String = ""
    try {
      fileContents = scala.io.Source
        .fromURL(new URL(station_url))
        .getLines()
        .mkString(sep = "\n")
    } catch {
      case ex: IOException           => println(ex + " for " + station_url)
      case ex: MalformedURLException => println(ex + " for " + station_url)
      case ex: ConnectException      => println(ex + " for " + station_url)
      case ex: Exception             => println(ex + " for " + station_url)
    }
    fileContents
  }

  def filterNonDigitCharacters(line: String): String = {
    return line
      .replaceAll("---", "0")
      .replaceAll("([A-Za-z]|#|\\*|\\$)", "")
      .trim
      .replaceAll("( )+", ",")
  }

  def addDeaultsToLine(line: String): String = {
    val ELEMENTS_IN_LINE: Int = 8
    val DEFAULT_VALUE: String = ",0"
    val comma_count: Int = line.count(x => x == ',')
    return if (comma_count < ELEMENTS_IN_LINE - 1)
      line + (DEFAULT_VALUE * (ELEMENTS_IN_LINE - 1 - comma_count))
    else line
  }

  val baseURL: String =
    "https://www.metoffice.gov.uk/pub/data/weather/uk/climate/stationdata/"
  val START_POSITION_REFERENCE: String = "hours"
  val stations: List[String] =
    List(
      "aberporth",
      "armagh",
      "ballypatrick",
      "bradford",
      "braemar",
      "camborne",
      "cambridge",
      "cardiff",
      "chivenor",
      "cwmystwyth",
      "dunstaffnage",
      "durham",
      "eastbourne",
      "eskdalemuir",
      "heathrow",
      "hurn",
      "lerwick",
      "leuchars",
      "lowestoft",
      "manston",
      "nairn",
      "newtonrigg",
      "oxford",
      "paisley",
      "ringway",
      "rossonwye",
      "shawbury",
      "sheffield",
      "southampton",
      "stornoway",
      "suttonbonington",
      "tiree",
      "valley",
      "waddington",
      "whitby",
      "wickairport",
      "yeovilton"
    )

  val startTimeMillis = System.currentTimeMillis()

  val stationHistoricalData =
    stations
      .map(x => (x, readFileFromURL(baseURL + x + "data.txt")))
      .filterNot(x => x._2.isEmpty())
      .map(x0 => (x0._1, x0._2.split("\n").toList))
      .map(x0 =>
        (x0._1, x0._2.dropWhile(!_.contains(START_POSITION_REFERENCE)).tail))
      .map(x1 => (x1._1, x1._2.map(x => filterNonDigitCharacters(x))))
      .map(x2 => (x2._2.filter(_ != "").map(x => x2._1 + "," + x)))
      .flatten
      .map(x3 => addDeaultsToLine(x3))

  val durationSeconds = (System.currentTimeMillis() - startTimeMillis) / 1000
  println(
    "Total Data Create time :" + durationSeconds
      + " secs. Total No of List Elements :" + stationHistoricalData.length)

  val spark_session = SparkSession
    .builder()
    .appName("UK Historical Weather Data Analysis")
    .master("local[*]")
    .getOrCreate()
  spark_session.sparkContext.setLogLevel("Error")
  spark_session.conf.set("spark.sql.shuffle.partitions", 4)

  val df = spark_session.createDataFrame(
    stationHistoricalData
      .map(x => x.split(","))
      .map(
        x =>
          Historical_Measures(x(0),
                              x(1).toInt,
                              x(2).toInt,
                              x(3).toFloat,
                              x(4).toFloat,
                              x(5).toInt,
                              x(6).toFloat,
                              x(7).toFloat)))

  println("No of Data Frame Rdd Partitions " + df.rdd.getNumPartitions)

  df.createOrReplaceTempView("Data_Source")

  //Rank Stations they have been online
  spark_session
    .sql(
      "select station_name ,cnt,dense_rank()over(order by cnt desc) " +
        "as rank from " +
        "(select station_name,count(1) as cnt from Data_Source " +
        "group by station_name)")
    .show(37)

  //Rank stations by rainfall and-or sunshine
  spark_session
    .sql(
      "select station_name ,rain_mm,sun_hours," +
        "dense_rank()over(order by rain_mm desc) as rank_rain_fall, " +
        "dense_rank()over(order by sun_hours desc) as rank_sun_hours " +
        "from (select station_name,sum(rain_mm) as rain_mm, " +
        "sum(sun_hours)as sun_hours from Data_Source group by station_name)")
    .show(37)

  //Worst rainfall and best sunshine for each station with year
  spark_session
    .sql(
      "select station_name, year,rain_mm,sun_hours from (" +
        " select station_name, year,rain_mm,sun_hours," +
        " min(rain_mm)over(partition by station_name) min_rain_mm," +
        " max(sun_hours)over(partition by station_name) max_sun_hours from (" +
        " select station_name,year, sum(rain_mm)rain_mm," +
        " sum(sun_hours)sun_hours from Data_Source where rain_mm > 0 " +
        " and sun_hours >0 group by station_name,year))" +
        " where (rain_mm = min_rain_mm or sun_hours = max_sun_hours)")
    .show(100)

  //Averages for month of may across all stations,best and worst year
  spark_session
    .sql(
      "select * from (" +
        " select year,avg_sun_hours," +
        " max(avg_sun_hours)over() as max_avg_yr_sun_hours," +
        " min(avg_sun_hours)over() as min_avg_yr_sun_hours" +
        " from (select year,avg(sun_hours) as avg_sun_hours " +
        " from Data_Source where mm=5 and sun_hours >0 group by year)" +
        " ) where (avg_sun_hours = max_avg_yr_sun_hours " +
        " or avg_sun_hours= min_avg_yr_sun_hours)")
    .show(10)

  spark_session.stop
}
