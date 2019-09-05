// SEGMENT DATA
val flightsDF = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/data/dot_airline/90313096_T_T100_SEGMENT_ALL_CARRIER/90313096_T_T100_SEGMENT_ALL_CARRIER_2018_All.csv")

flightsDF.createOrReplaceTempView("segment_table")

spark.sql("select origin, dest, carrier_name, passengers, seats, month from segment_table where origin = 'BOS' and dest = 'SAN' and month = 1 order by 3").show

// MARKET data
val flightsMarketDF = spark.read.format("csv").option("inferSchema", "true").option("header", "true").load("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/data/dot_airline/90313096_T_T100_MARKET_ALL_CARRIER/90313096_T_T100_MARKET_ALL_CARRIER_2018_All.csv")

flightsMarketDF.createOrReplaceTempView("market_table")

spark.sql("select origin, dest, carrier_name, passengers, month from market_table where origin = 'BOS' and dest = 'SAN' and month = 1 order by 1, 2, 3").show()

flightsMarketDF.printSchema()
root
 |-- PASSENGERS: double (nullable = true)
 |-- FREIGHT: double (nullable = true)
 |-- MAIL: double (nullable = true)
 |-- DISTANCE: double (nullable = true)
 |-- UNIQUE_CARRIER: string (nullable = true)
 |-- AIRLINE_ID: integer (nullable = true)
 |-- UNIQUE_CARRIER_NAME: string (nullable = true)
 |-- UNIQUE_CARRIER_ENTITY: string (nullable = true)
 |-- REGION: string (nullable = true)
 |-- CARRIER: string (nullable = true)
 |-- CARRIER_NAME: string (nullable = true)
 |-- CARRIER_GROUP: integer (nullable = true)
 |-- CARRIER_GROUP_NEW: integer (nullable = true)
 |-- ORIGIN_AIRPORT_ID: integer (nullable = true)
 |-- ORIGIN_AIRPORT_SEQ_ID: integer (nullable = true)
 |-- ORIGIN_CITY_MARKET_ID: integer (nullable = true)
 |-- ORIGIN: string (nullable = true)
 |-- ORIGIN_CITY_NAME: string (nullable = true)
 |-- ORIGIN_STATE_ABR: string (nullable = true)
 |-- ORIGIN_STATE_FIPS: integer (nullable = true)
 |-- ORIGIN_STATE_NM: string (nullable = true)
 |-- ORIGIN_COUNTRY: string (nullable = true)
 |-- ORIGIN_COUNTRY_NAME: string (nullable = true)
 |-- ORIGIN_WAC: integer (nullable = true)
 |-- DEST_AIRPORT_ID: integer (nullable = true)
 |-- DEST_AIRPORT_SEQ_ID: integer (nullable = true)
 |-- DEST_CITY_MARKET_ID: integer (nullable = true)
 |-- DEST: string (nullable = true)
 |-- DEST_CITY_NAME: string (nullable = true)
 |-- DEST_STATE_ABR: string (nullable = true)
 |-- DEST_STATE_FIPS: integer (nullable = true)
 |-- DEST_STATE_NM: string (nullable = true)
 |-- DEST_COUNTRY: string (nullable = true)
 |-- DEST_COUNTRY_NAME: string (nullable = true)
 |-- DEST_WAC: integer (nullable = true)
 |-- YEAR: integer (nullable = true)
 |-- QUARTER: integer (nullable = true)
 |-- MONTH: integer (nullable = true)
 |-- DISTANCE_GROUP: integer (nullable = true)
 |-- CLASS: string (nullable = true)
 |-- DATA_SOURCE: string (nullable = true)
 |-- _c41: string (nullable = true)

flightSegmentsDF.printSchema()

|-- DEPARTURES_SCHEDULED: double (nullable = true)
|-- DEPARTURES_PERFORMED: double (nullable = true)
|-- PAYLOAD: double (nullable = true)
|-- SEATS: double (nullable = true)
|-- PASSENGERS: double (nullable = true)
|-- FREIGHT: double (nullable = true)
|-- MAIL: double (nullable = true)
|-- DISTANCE: double (nullable = true)
|-- RAMP_TO_RAMP: double (nullable = true)
|-- AIR_TIME: double (nullable = true)
|-- UNIQUE_CARRIER: string (nullable = true)
|-- AIRLINE_ID: integer (nullable = true)
|-- UNIQUE_CARRIER_NAME: string (nullable = true)
|-- UNIQUE_CARRIER_ENTITY: string (nullable = true)
|-- REGION: string (nullable = true)
|-- CARRIER: string (nullable = true)
|-- CARRIER_NAME: string (nullable = true)
|-- CARRIER_GROUP: integer (nullable = true)
|-- CARRIER_GROUP_NEW: integer (nullable = true)
|-- ORIGIN_AIRPORT_ID: integer (nullable = true)
|-- ORIGIN_AIRPORT_SEQ_ID: integer (nullable = true)
|-- ORIGIN_CITY_MARKET_ID: integer (nullable = true)
|-- ORIGIN: string (nullable = true)
|-- ORIGIN_CITY_NAME: string (nullable = true)
|-- ORIGIN_STATE_ABR: string (nullable = true)
|-- ORIGIN_STATE_FIPS: integer (nullable = true)
|-- ORIGIN_STATE_NM: string (nullable = true)
|-- ORIGIN_COUNTRY: string (nullable = true)
|-- ORIGIN_COUNTRY_NAME: string (nullable = true)
|-- ORIGIN_WAC: integer (nullable = true)
|-- DEST_AIRPORT_ID: integer (nullable = true)
|-- DEST_AIRPORT_SEQ_ID: integer (nullable = true)
|-- DEST_CITY_MARKET_ID: integer (nullable = true)
|-- DEST: string (nullable = true)
|-- DEST_CITY_NAME: string (nullable = true)
|-- DEST_STATE_ABR: string (nullable = true)
|-- DEST_STATE_FIPS: integer (nullable = true)
|-- DEST_STATE_NM: string (nullable = true)
|-- DEST_COUNTRY: string (nullable = true)
|-- DEST_COUNTRY_NAME: string (nullable = true)
|-- DEST_WAC: integer (nullable = true)
|-- AIRCRAFT_GROUP: integer (nullable = true)
|-- AIRCRAFT_TYPE: integer (nullable = true)
|-- AIRCRAFT_CONFIG: integer (nullable = true)
|-- YEAR: integer (nullable = true)
|-- QUARTER: integer (nullable = true)
|-- MONTH: integer (nullable = true)
|-- DISTANCE_GROUP: integer (nullable = true)
|-- CLASS: string (nullable = true)
|-- DATA_SOURCE: string (nullable = true)

 spark.sql("select carrier_name, sum(passengers) from segment_table where dest_country = 'US' group by 1 order by 2 desc").show ()

StructType(Array(
    StructField("DEPARTURES_SCHEDULED", DoubleType, true),
    StructField("DEPARTURES_PERFORMED", DoubleType, true),
    StructField("PAYLOAD", DoubleType, true),
    StructField("SEATS", DoubleType, true),
    StructField("PASSENGERS", DoubleType, true),
    StructField("FREIGHT", DoubleType, true),
    StructField("MAIL", DoubleType, true),
    StructField("DISTANCE", DoubleType, true),
    StructField("RAMP_TO_RAMP", DoubleType, true),
    StructField("AIR_TIME", DoubleType, true),
    StructField("UNIQUE_CARRIER", StringType, true),
    StructField("AIRLINE_ID", IntegerType, true),
    StructField("UNIQUE_CARRIER_NAME", StringType, true),
    StructField("UNIQUE_CARRIER_ENTITY", StringType, true),
    StructField("REGION", StringType, true),
    StructField("CARRIER", StringType, true),
    StructField("CARRIER_NAME", StringType, true),
    StructField("CARRIER_GROUP", IntegerType, true),
    StructField("CARRIER_GROUP_NEW", IntegerType, true),
    StructField("ORIGIN_AIRPORT_ID", IntegerType, true),
    StructField("ORIGIN_AIRPORT_SEQ_ID", IntegerType, true),
    StructField("ORIGIN_CITY_MARKET_ID", IntegerType, true),
    StructField("ORIGIN", StringType, true),
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
    StructField("DEST", StringType, true),
    StructField("DEST_CITY_NAME", StringType, true),
    StructField("DEST_STATE_ABR", StringType, true),
    StructField("DEST_STATE_FIPS", StringType, true),
    StructField("DEST_STATE_NM", StringType, true),
    StructField("DEST_COUNTRY", StringType, true),
    StructField("DEST_COUNTRY_NAME", StringType, true),
    StructField("DEST_WAC", IntegerType, true),
    StructField("AIRCRAFT_GROUP", IntegerType, true),
    StructField("AIRCRAFT_TYPE", IntegerType, true),
    StructField("AIRCRAFT_CONFIG", IntegerType, true),
    StructField("YEAR", IntegerType, true),
    StructField("QUARTER", IntegerType, true),
    StructField("MONTH", IntegerType, true),
    StructField("DISTANCE_GROUP", IntegerType, true),
    StructField("CLASS", StringType, true),
    StructField("DATA_SOURCE", StringType, true)
))

spark.sql("select month, sum(passengers) from segment_table where dest_country = 'US' group by 1 order by 1").show()

scala> spark.sql("select origin, dest, departures_performed, carrier_name, passengers, seats, aircraft_type, distance, month from flightsFullDF where origin = 'BOS' and dest = 'SAN' and month = 1").show()

spark.sql("""
    select
        origin.airport_code,
        dest.airport_code,
        st_distance(
            st_transform(origin.geometry, 'epsg:4326', 'epsg:5070'),
            st_transform(dest.geometry, 'epsg:4326', 'epsg:5070')
        ) /1609.340 as distance_miles
    from airport as origin
    cross join airport as dest
    where origin.airport_code = 'BOS'
        and dest.airport_code = 'SAN'
""").show

capacity weighted competition
1000 passengers have 2 choices, 1000 passengers have 1 choice
((1000 * 2) + (1000 * 1)) / (2000) = 3000 / 2000 = 1.5

1500 passengers have 2 choices, 500 have 1
((1500 * 2) + (500 * 1)) / (2000) = 3500 / 2000 = 1.75

// major carriers share
spark.sql("""
with major_carrier_table as (
    select distinct (origin, dest)
        origin,
        dest,
        carrier,
        sum(passengers) as passenger_count
    from flights
    where origin = 'JFK' and dest = 'LAX'
    group by 1, 2, 3
    order by 1, 2, 4 desc
),
total_passengers_table as (
    select
        origin,
        dest,
        sum(passengers) as passenger_count
    from flights
    where origin = 'JFK' and dest = 'LAX'
    group by 1, 2
    order by 1, 2
)
select
    route.origin,
    route.dest,
    route.passenger_count,
    (major.passenger_count / route.passenger_count) as major_share
from total_passengers_table as route
join major_carrier_table as major
    on route.origin = major.origin and route.dest = major.dest
order by 3 desc
""").show(20)

// route competition
// window function
spark.sql("""
    select distinct
        origin,
        dest,
        carrier,
        sum(passengers) over (partition by origin, dest) as route_passengers,
        sum(passengers) over (partition by origin, dest, carrier) as carrier_passengers
    from flights
    where origin = 'JFK' and dest = 'LAX'
    order by 5 desc
""").show

// check carrier totals
spark.sql("""
    select
        origin,
        dest,
        carrier,
        sum(passengers) as carrier_passengers
    from flights
    where origin = 'JFK' and dest = 'LAX'
    group by 1, 2, 3
    order by 4 desc
""").show

// check route totals
spark.sql("""
    select
        origin,
        dest,
        sum(passengers) as carrier_passengers
    from flights
    where origin = 'JFK' and dest = 'LAX'
    group by 1, 2
""").show

spark.sql("""
    select distinct
        origin,
        dest,
        carrier,
        sum(passengers) over (partition by origin, dest) as route_passengers,
        sum(passengers) over (partition by origin, dest, carrier) as carrier_passengers,
        round(100.0 * sum(passengers) over (partition by origin, dest, carrier) / sum(passengers) over (partition by origin, dest), 2) as pct_of_route_passengers
    from flights
    where origin = 'JFK' and dest = 'LAX'
    order by 5 desc
""").show

// route competition
spark.sql("""
    with carrier_pct_of_route_passengers_table as (
        select distinct
            origin,
            dest,
            round(100.0 * sum(passengers) over (partition by origin, dest, carrier) / sum(passengers) over (partition by origin, dest), 2) as carrier_pct_of_route_passengers
        from flights
        where origin = 'BOS'
    )
    select
        origin,
        dest,
        max(carrier_pct_of_route_passengers) as max_carrier_share
    from carrier_pct_of_route_passengers_table
    group by 1, 2
    order by 1, 2
""").show

// market - market competition
spark.sql("""
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
        where flights.origin = 'BOS'
        order by 1, 2
    )
    select
        origin_market,
        dest_market,
        max(carrier_pct_of_route_passengers) as max_carrier_share
    from carrier_pct_of_route_passengers_table
    group by 1, 2
    order by 1, 2
""").show

// average competition for a carrier
spark.sql("""
    select distinct
        origin,
        dest,
        carrier,
        distance,
        sum(passengers) over (partition by origin, dest) as route_passengers,
        sum(passengers) over (partition by origin, dest, carrier) as carrier_passengers,
        round(100.0 * sum(passengers) over (partition by origin, dest, carrier) / sum(passengers) over (partition by origin, dest), 2) as carrier_pct_of_route_passengers
    from flights
    where origin = 'JFK' and dest = 'LAX'
    order by 6 desc
""").show

spark.sql("""
    select
        carrier,
        sum(carrier_passengers * distance) as total_passenger_miles,
        sum(carrier_pct_of_route_passengers * carrier_passengers * distance) / sum(carrier_passengers * distance) as route_share_per_passenger_mile
    from routecomp
    group by 1
    order by 2 desc
""").show()

// check using JetBlue
spark.sql("""
    select *
    from routecomp
    where carrier = 'B6'
    order by carrier_passengers DESC
    limit 20
""").show

// major routes by passenger miles with linestring geometry
SELECT ST_GeomFromText('LINESTRING(-71.160281 42.258729,-71.160837 42.259113,-71.161144 42.25932)')

spark.sql("""
    select
        flights.origin,
        flights.dest,
        cast(
            ST_GeomFromWKT('LINESTRING(' || origin.longitude || ' ' || origin.latitude || ', ' || dest.longitude || ' ' || dest.latitude || ')')
        as varchar(100)) as geom,
        cast(sum(passengers * distance) as long) as passenger_miles
    from flights
    join airport as origin on flights.origin = origin.airport_code
    join airport as dest on flights.dest = dest.airport_code
    group by 1, 2, 3
    order by 4 desc
    limit 100
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
    limit 100
""").show

.write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/airport_passengers.csv")

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
""").show(20)

write.format("csv").mode("overwrite").option("sep", ",").option("header", "true").save("/Users/tomlee/OneDrive - UW-Madison/Classes/GEOG574/Assignments/project/results/airport_passengers_geom.csv")

// market - market route passengers with geom
spark.sql("""
    select
        origin_airport.market as origin_market,
        dest_airport.market dest_market,
        competition.max_carrier_share,
        cast(sum(flights.passengers * flights.distance) as long) as passenger_miles,
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
    limit 100
""").show(20)

// Dane County Regional Airport, Madison WI airport passengers
spark.sql("""
    select
        airport.airport_code,
        airport.airport_name,
        cast(sum(flights.passengers) as long) as passenger_count
    from airport
    join flights
        on airport.airport_code = flights.origin or airport.airport_code = flights.dest
    where airport.airport_code = 'MSN'
    group by 1, 2
    order by 3 desc
""").show
