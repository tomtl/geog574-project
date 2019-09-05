spark-shell --packages org.datasyslab:geospark:1.2.0,org.datasyslab:geospark-sql_2.3:1.2.0,org.datasyslab:geospark-viz_2.3:1.2.0

// setup
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.types.{StructField, StructType, DoubleType, IntegerType, StringType}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

val conf = new SparkConf()
conf.setAppName("GeoSparkApp") // Change this to a proper name
conf.setMaster("local[*]") // Delete this if run in cluster mode
// Enable GeoSpark custom Kryo serializer
conf.set("spark.serializer", classOf[KryoSerializer].getName)
conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)

var sparkSession = SparkSession.builder().
    config("spark.serializer",classOf[KryoSerializer].getName).
    config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
    master("local[*]").appName("myGeoSparkSQLdemo").getOrCreate()
GeoSparkSQLRegistrator.registerAll(sparkSession)

// raw flight statistics schema
var flightsSchema = StructType(Array(
    StructField("DEPARTURES_SCHEDULED", DoubleType, true),
    StructField("DEPARTURES_PERFORMED", DoubleType, false),
    StructField("PAYLOAD", DoubleType, true),
    StructField("SEATS", DoubleType, false),
    StructField("PASSENGERS", DoubleType, false),
    StructField("FREIGHT", DoubleType, true),
    StructField("MAIL", DoubleType, true),
    StructField("DISTANCE", DoubleType, false),
    StructField("RAMP_TO_RAMP", DoubleType, true),
    StructField("AIR_TIME", DoubleType, true),
    StructField("UNIQUE_CARRIER", StringType, true),
    StructField("AIRLINE_ID", IntegerType, true),
    StructField("UNIQUE_CARRIER_NAME", StringType, true),
    StructField("UNIQUE_CARRIER_ENTITY", StringType, true),
    StructField("REGION", StringType, true),
    StructField("CARRIER", StringType, false),
    StructField("CARRIER_NAME", StringType, true),
    StructField("CARRIER_GROUP", IntegerType, true),
    StructField("CARRIER_GROUP_NEW", IntegerType, true),
    StructField("ORIGIN_AIRPORT_ID", IntegerType, true),
    StructField("ORIGIN_AIRPORT_SEQ_ID", IntegerType, true),
    StructField("ORIGIN_CITY_MARKET_ID", IntegerType, true),
    StructField("ORIGIN", StringType, false),
    StructField("ORIGIN_CITY_NAME", StringType, true),
    StructField("ORIGIN_STATE_ABR", StringType, true),
    StructField("ORIGIN_STATE_FIPS", StringType, true),
    StructField("ORIGIN_STATE_NM", StringType, true),
    StructField("ORIGIN_COUNTRY", StringType, true),
    StructField("ORIGIN_COUNTRY_NAME", StringType, true),
    StructField("ORIGIN_WAC", IntegerType, true),
    StructField("DEST_AIRPORT_ID", IntegerType, true),
    StructField("DEST_AIRPORT_SEQ_ID", IntegerType, true),
    StructField("DEST_CITY_MARKET_ID", IntegerType, true),
    StructField("DEST", StringType, false),
    StructField("DEST_CITY_NAME", StringType, true),
    StructField("DEST_STATE_ABR", StringType, true),
    StructField("DEST_STATE_FIPS", StringType, true),
    StructField("DEST_STATE_NM", StringType, true),
    StructField("DEST_COUNTRY", StringType, true),
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("DEST_WAC", IntegerType, true),
    StructField("AIRCRAFT_GROUP", IntegerType, true),
    StructField("AIRCRAFT_TYPE", IntegerType, false),
    StructField("AIRCRAFT_CONFIG", IntegerType, true),
    StructField("YEAR", IntegerType, true),
    StructField("QUARTER", IntegerType, true),
    StructField("MONTH", IntegerType, false),
    StructField("DISTANCE_GROUP", IntegerType, true),
    StructField("CLASS", StringType, true),
    StructField("DATA_SOURCE", StringType, true)
))

val flightsFullDF = spark.read.format("csv").schema(flightsSchema).option("header", "true").load("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/data/dot_airline/90313096_T_T100_SEGMENT_ALL_CARRIER/90313096_T_T100_SEGMENT_ALL_CARRIER_2018_All.csv"
)

import org.apache.spark.sql.functions._

flightsFullDF.createOrReplaceTempView("flightsfulldf")

// flight statistics table
var flightsDF = flightsFullDF.where("ORIGIN_COUNTRY = 'US' and DEST_COUNTRY = 'US'").selectExpr(
        "ORIGIN as origin",
        "DEST as dest",
        "PASSENGERS as passengers",
        "SEATS as seats",
        "MONTH as month",
        "AIRCRAFT_TYPE as aircraft_code",
        "CARRIER as carrier",
        "DISTANCE as distance",
        "DEPARTURES_PERFORMED as departures").
    withColumn("id",monotonicallyIncreasingId)


flightsDF.createOrReplaceTempView("flights")

// carriers table
var carrierDF = flightsFullDF.selectExpr(
    "CARRIER as carrier_code",
    "CARRIER_NAME as carrier_name"
).distinct().orderBy("carrier_code")

carrierDF.createOrReplaceTempView("carrier")

// aircraft table

var aircraftSchema = StructType(Array(
    StructField("AC_TYPEID", IntegerType, false),
    StructField("AC_GROUP", StringType, true),
    StructField("SSD_NAME", StringType, true),
    StructField("MANUFACTURER", StringType, false),
    StructField("LONG_NAME", StringType, false),
    StructField("SHORT_NAME", StringType, false),
    StructField("BEGIN_DATE", StringType, true),
    StructField("END_DATE", StringType, true)
))

var aircraftFullDF = spark.read.format("csv").schema(aircraftSchema).option("header", "true").load("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/data/dot_airline/90313096_T_AIRCRAFT_TYPES/90313096_T_AIRCRAFT_TYPES_All_All.csv"
)
aircraftFullDF.createOrReplaceTempView("aircraftfulldf")

var aircraftDF = aircraftFullDF.selectExpr(
    "AC_TYPEID as aircraft_code",
    "SHORT_NAME as short_name",
    "LONG_NAME as long_name",
    "MANUFACTURER as manufacturer"
)
aircraftDF.createOrReplaceTempView("aircraft")

// airports
var airportSchema = StructType(Array(
    StructField("AIRPORT_SEQ_ID", IntegerType, true),
    StructField("AIRPORT_ID", IntegerType, true),
    StructField("AIRPORT", StringType, false),
    StructField("DISPLAY_AIRPORT_NAME", StringType, false),
    StructField("DISPLAY_AIRPORT_CITY_NAME_FULL", StringType, true),
    StructField("AIRPORT_WAC_SEQ_ID2", IntegerType, true),
    StructField("AIRPORT_WAC", IntegerType, true),
    StructField("AIRPORT_COUNTRY_NAME", StringType, false),
    StructField("AIRPORT_COUNTRY_CODE_ISO", StringType, true),
    StructField("AIRPORT_STATE_NAME", StringType, true),
    StructField("AIRPORT_STATE_CODE", StringType, true),
    StructField("AIRPORT_STATE_FIPS", IntegerType, true),
    StructField("CITY_MARKET_SEQ_ID", IntegerType, true),
    StructField("CITY_MARKET_ID", IntegerType, true),
    StructField("DISPLAY_CITY_MARKET_NAME_FULL", StringType, false),
    StructField("CITY_MARKET_WAC_SEQ_ID2",  IntegerType,  true),
    StructField("CITY_MARKET_WAC", IntegerType, true),
    StructField("LAT_DEGREES", IntegerType, true),
    StructField("LAT_HEMISPHERE", StringType, true),
    StructField("LAT_MINUTES", IntegerType, true),
    StructField("LAT_SECONDS", IntegerType, true),
    StructField("LATITUDE", DoubleType, false),
    StructField("LON_DEGREES", IntegerType, true),
    StructField("LON_HEMISPHERE", StringType, true),
    StructField("LON_MINUTES", IntegerType, true),
    StructField("LON_SECONDS", IntegerType, true),
    StructField("LONGITUDE", DoubleType, false),
    StructField("UTC_LOCAL_TIME_VARIATION", IntegerType, true),
    StructField("AIRPORT_START_DATE", StringType, true),
    StructField("AIRPORT_THRU_DATE", StringType, true),
    StructField("AIRPORT_IS_CLOSED", IntegerType, true),
    StructField("AIRPORT_IS_LATEST", IntegerType, true)
))

var airportFullDF = spark.read.format("csv").schema(airportSchema).option("header", "true").load("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/data/dot_airline/90313096_T_MASTER_CORD/90313096_T_MASTER_CORD_All_All.csv")

import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.Point;
import org.apache.spark.sql.geosparksql.expressions.ST_Point

var airportDF = airportFullDF.selectExpr(
    "AIRPORT as airport_code",
    "DISPLAY_AIRPORT_NAME as airport_name",
    "LATITUDE as latitude",
    "LONGITUDE as longitude",
    "DISPLAY_CITY_MARKET_NAME_FULL as market",
    "AIRPORT_COUNTRY_NAME as country",
    "ST_Point(CAST(LONGITUDE AS Decimal(24,20)), CAST(LATITUDE AS Decimal(24,20))) as geometry"
).where("AIRPORT_IS_LATEST = 1")

airportDF.createOrReplaceTempView("airport")

// major airlines by passenger miles
spark.sql("""
    select
        carrier.carrier_name,
        cast(sum(passengers * distance) as long) as passenger_miles
    from flights
    join carrier
        on flights.carrier = carrier.carrier_code
    group by 1
    order by 2 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/carrier_passenger_miles.csv")

// major airports by passenger count
spark.sql("""
    select
        airport.airport_code,
        airport.airport_name,
        cast(sum(flights.passengers) as long) as passenger_count
    from airport
    join flights
        on airport.airport_code = flights.origin or airport.airport_code = flights.dest
    group by 1, 2
    order by 3 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/airport_passengers.csv")

// major cities by passenger count
spark.sql("""
    select
        airport.market,
        cast(sum(flights.passengers) as long) as passenger_count
    from airport
    join flights
        on airport.airport_code = flights.origin or airport.airport_code = flights.dest
    group by 1
    order by 2 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/city_passengers.csv")

// major routes by passenger count
spark.sql("""
    select
        origin,
        dest,
        cast(sum(passengers) as long) as passenger_count
    from flights
    group by 1, 2
    order by 3 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/route_passenger_count.csv")

// major routes by passenger miles
spark.sql("""
    select
        origin,
        dest,
        cast(sum(passengers * distance) as long) as passenger_miles
    from flights
    group by 1, 2
    order by 3 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/route_passenger_miles.csv")

// major city routes by passenger miles
spark.sql("""
    select
        origin.market as origin_market,
        dest.market dest_market,
        cast(sum(flights.passengers * flights.distance) as long) as passenger_miles
    from flights
    join airport as origin
        on flights.origin = origin.airport_code
    join airport as dest
        on flights.dest = dest.airport_code
    group by 1, 2
    order by 3 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/route_city_passenger_miles.csv")

// market - market competition data frame
var marketRouteCompDF = spark.sql("""
    with carrier_pct_of_route_passengers_table as (
        select distinct
            origin_airport.market as origin_market,
            dest_airport.market as dest_market,
            round(100.0 * sum(flights.passengers) over (partition by origin_airport.market, dest_airport.market, carrier) / sum(flights.passengers) over (partition by origin_airport.market, dest_airport.market), 2) as carrier_pct_of_route_passengers
        from flights
        join airport as origin_airport
            on flights.origin = origin_airport.airport_code
        join airport as dest_airport
            on flights.dest = dest_airport.airport_code
        order by 1, 2
    )
    select
        origin_market,
        dest_market,
        max(carrier_pct_of_route_passengers) as max_carrier_share
    from carrier_pct_of_route_passengers_table
    group by 1, 2
    order by 1, 2
""")
marketRouteCompDF.createOrReplaceTempView("marketRouteComp")

// market to market route competition
spark.sql("""
    select
        origin_airport.market as origin_market,
        dest_airport.market dest_market,
        competition.max_carrier_share,
        cast(sum(flights.passengers * flights.distance) as long) as passenger_miles
    from flights
    join airport as origin_airport
        on flights.origin = origin_airport.airport_code
    join airport as dest_airport
        on flights.dest = dest_airport.airport_code
    join marketRouteComp as competition
        on origin_airport.market = competition.origin_market
        and dest_airport.market = competition.dest_market
    group by 1, 2, 3
    order by 4 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/market_route_competition.csv")

// carrier market share per passenger mile
var routeCompDF = spark.sql("""
    select distinct
        origin,
        dest,
        carrier,
        distance,
        sum(passengers) over (partition by origin, dest) as route_passengers,
        sum(passengers) over (partition by origin, dest, carrier) as carrier_passengers,
        round(100.0 * sum(passengers) over (partition by origin, dest, carrier) / sum(passengers) over (partition by origin, dest), 2) as carrier_pct_of_route_passengers
    from flights
    order by 6 desc
""")
routeCompDF.createOrReplaceTempView("routeComp")

spark.sql("""
    select
        carrier.carrier_name,
        cast(sum(carrier_passengers * distance) as long) as total_passenger_miles,
        round((
            sum(carrier_pct_of_route_passengers * carrier_passengers * distance) /
            sum(carrier_passengers * distance)
        ), 2) as route_share_per_passenger_mile
    from routecomp as route
    join carrier
        on route.carrier = carrier.carrier_code
    group by 1
    order by 2 desc
    limit 100
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/carrier_market_share.csv")

// major routes by passenger miles with linestring geometry
spark.sql("""
    select
        flights.origin,
        flights.dest,
        cast(
            ST_GeomFromWKT('LINESTRING(' || origin.longitude || ' ' || origin.latitude || ', ' || dest.longitude || ' ' || dest.latitude || ')')
        as varchar(100)) as geom,
        cast(sum(passengers * distance) as long) as passenger_miles,
        cast(sum(passengers) as long) as passengers
    from flights
    join airport as origin on flights.origin = origin.airport_code
    join airport as dest on flights.dest = dest.airport_code
    group by 1, 2, 3
    order by 4 desc
    limit 10000
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/route_passenger_count_geom.csv")

// major airports by passenger count with geom
spark.sql("""
    select
        airport.airport_code,
        airport.airport_name,
        cast(ST_GeomFromWKT(
            'POINT(' || airport.longitude || ' ' || airport.latitude || ')'
        ) as varchar(100)) as geom,
        cast(sum(flights.passengers) as long) as passenger_count
    from airport
    join flights
        on airport.airport_code = flights.origin or airport.airport_code = flights.dest
    group by 1, 2, 3
    order by 4 desc
    limit 10000
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/airport_passengers_geom.csv")

// major cities by passenger count with geom
spark.sql("""
    with airports_table as (
    select
        airport.airport_code,
        airport.airport_name,
        airport.market,
        airport.latitude,
        airport.longitude,
        cast(sum(flights.passengers) as long) as passenger_count
    from airport
    join flights
        on airport.airport_code = flights.origin or airport.airport_code = flights.dest
    group by 1, 2, 3, 4, 5
    having sum(flights.passengers) > 1000000
    order by airport.market
    limit 10000
    )
    select
        market,
        avg(longitude) as lng,
        avg(latitude) as lat,
        cast(ST_GeomFromWKT(
            'POINT(' || avg(longitude) || ' ' || avg(latitude) || ')'
        ) as varchar(100)) as geom,
        sum(passenger_count) as passengers
    from airports_table
    group by 1
    order by sum(passenger_count) desc
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/market_passengers_geom.csv")

// market - market route passengers with geom
spark.sql("""
    select
        origin_airport.market as origin_market,
        dest_airport.market dest_market,
        competition.max_carrier_share,
        cast(sum(flights.passengers * flights.distance) as long) as passenger_miles,
        cast(sum(flights.passengers) as long) as passengers,
        cast(
            ST_GeomFromWKT('LINESTRING(' || avg(origin_airport.longitude) || ' ' || avg(origin_airport.latitude) || ', ' || avg(dest_airport.longitude) || ' ' || avg(dest_airport.latitude) || ')')
        as varchar(100)) as geom
    from flights
    join airport as origin_airport
        on flights.origin = origin_airport.airport_code
    join airport as dest_airport
        on flights.dest = dest_airport.airport_code
    join marketRouteComp as competition
        on origin_airport.market = competition.origin_market
        and dest_airport.market = competition.dest_market
    group by 1, 2, 3
    having sum(flights.passengers * flights.distance) > 100000000
    order by 4 desc
    limit 10000
""").write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/market_route_passengers_geom.csv")
