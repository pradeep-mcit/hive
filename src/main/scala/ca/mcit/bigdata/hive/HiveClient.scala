
package ca.mcit.bigdata.hive
import java.sql.{Connection, DriverManager}
import java.util.Calendar
object HiveClass  extends App {

  val driverName: String = "org.apache.hive.jdbc.HiveDriver"
  Class.forName(driverName)
  val connection: Connection = DriverManager.getConnection("jdbc:hive2://172.16.129.58:10000", "cloudera", "cloudera")
  val stmt = connection.createStatement()
  println(Calendar.getInstance().getTime)
  stmt.execute("set hive.exec.dynamic.partition.mode=nonstrict")
  stmt.execute("set hive.exec.dynamic.partition=true")
  stmt.execute("DROP table IF EXISTS pradeep.enriched_trip")
  stmt.execute("CREATE table pradeep.enriched_trip ( " +
    "route_id Int,         " +
    "service_id String,   " +
    "trip_id String,      " +
    "trip_headsign String," +
    "direction_id Int,    " +
    "shape_id Int,        " +
    "note_fr String,      " +
    "note_en String,      " +
    "date string,          " +
    "exception_type string," +
    "start_time String,    " +
    "end_time String,      " +
    "headway_sec Int)" +
    "COMMENT 'This table contains enriched data from Trips,Calendar dates and Frequencies of STM GTFS' " +
    " PARTITIONED BY (wheelchair_accessible Int) " +
    " STORED AS PARQUET " +
    " TBLPROPERTIES('parquet.compression'='GZIP')")

  stmt.execute("Insert overwrite table pradeep.enriched_trip PARTITION(wheelchair_accessible) " +
    "select /*+ MAPJOIN(calendar,frequencies) */ trip.route_id as route_id,trip.service_id as service_id, trip.trip_id as trip_id,trip.trip_headsign as trip_headsign,trip.direction_id as direction_id,trip.shape_id as shape_id, trip.note_fr as note_fr,trip.note_en as note_en, calendar.date as date,calendar.exception_type as exception_type,frequencies.start_time as start_time, frequencies.end_time as end_time,frequencies.headway_secs as headway_secs,trip.wheelchair_accessible as wheelchair_accessible " +
    "from pradeep.ext_trips trip " +
    "left join pradeep.ext_calendar_dates calendar on trip.service_id = calendar.service_id " +
    "left join pradeep.ext_frequencies frequencies on trip.trip_id = frequencies.trip_id")

  /*val enrichedTrip: ResultSet = stmt.executeQuery("SELECT * FROM  pradeep.enriched_trip")
  while (enrichedTrip.next()) {
    println("route_id:" + enrichedTrip.getInt(1), "service_id:" + enrichedTrip.getString(2), "trip_id:" + enrichedTrip.getString(3), "trip_headsign:" + enrichedTrip.getString(4), "direction_id:" + enrichedTrip.getInt(5), "shape_id:" + enrichedTrip.getInt(6), "note_fr:" + enrichedTrip.getString(7), "note_er:" + enrichedTrip.getString(8), "date:" + enrichedTrip.getString(9), "exception_type:" + enrichedTrip.getString(10), "start_time:" + enrichedTrip.getString(11), "end_time:" + enrichedTrip.getString(12), "headway_sec:" + enrichedTrip.getInt(13), "wheelchair_accessible:" + enrichedTrip.getInt(14))
  }

   */
  println(Calendar.getInstance().getTime)
  stmt.close()
  connection.close()
}


