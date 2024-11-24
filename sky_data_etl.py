from pyspark.sql import SparkSession
from pyspark.sql.functions import when, coalesce, col, current_timestamp, mean, count, expr
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType
)
import requests
from datetime import datetime
from cassandra.cluster import Cluster

class OpenSkyETL:
    def __init__(self, cassandra_host='localhost', keyspace='aviation'):
        # Initialize Spark Session
        self.spark = SparkSession.builder \
            .appName("OpenSky-ETL") \
            .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
            .getOrCreate()

        # Schema for OpenSky data
        self.schema = StructType([
            StructField("icao24", StringType(), True),
            StructField("callsign", StringType(), True),
            StructField("origin_country", StringType(), True),
            StructField("time_position", TimestampType(), True),
            StructField("last_contact", TimestampType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("baro_altitude", DoubleType(), True),
            StructField("on_ground", BooleanType(), True),
            StructField("velocity", DoubleType(), True),
            StructField("true_track", DoubleType(), True),
            StructField("vertical_rate", DoubleType(), True),
            StructField("geo_altitude", DoubleType(), True),
            StructField("squawk", StringType(), True),
            StructField("spi", BooleanType(), True),
            StructField("position_source", StringType(), True),
            StructField("ingestion_time", TimestampType(), False)  # Non-nullable field
        ])

        # Initialize Cassandra connection
        self.cassandra_host = cassandra_host
        self.keyspace = keyspace
        self.init_cassandra()

    def init_cassandra(self):
        """Initialize Cassandra keyspace and table."""
        cluster = Cluster([self.cassandra_host])
        session = cluster.connect()

        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)

        session.execute(f"DROP TABLE IF EXISTS {self.keyspace}.flights")

        session.execute(f"""
            CREATE TABLE {self.keyspace}.flights (
                icao24 text,
                callsign text,
                origin_country text,
                time_position timestamp,
                last_contact timestamp,
                longitude double,
                latitude double,
                baro_altitude double,
                on_ground boolean,
                velocity double,
                true_track double,
                vertical_rate double,
                geo_altitude double,
                squawk text,
                spi boolean,
                position_source text,
                ingestion_time timestamp,
                PRIMARY KEY ((icao24, ingestion_time), time_position)
            )
        """)

        session.shutdown()
        cluster.shutdown()

    def safe_float_convert(self, value):
        """Safely convert value to float."""
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0

    def safe_bool_convert(self, value):
        """Safely convert value to boolean."""
        return bool(value) if value is not None else False

    def safe_timestamp_convert(self, timestamp):
        """Safely convert timestamp."""
        try:
            return datetime.fromtimestamp(timestamp) if timestamp else datetime.now()
        except (ValueError, TypeError):
            return datetime.now()

    def extract(self):
        """Extract data from OpenSky API."""
        url = "https://opensky-network.org/api/states/all"
        response = requests.get(url)

        if response.status_code != 200:
            raise Exception(f"API request failed with status code: {response.status_code}")

        data = response.json()
        current_time = datetime.now()

        flights = []
        for state in data.get('states', []):
            if not state[0]:
                continue

            flight = {
                'icao24': str(state[0]),
                'callsign': str(state[1]).strip() if state[1] else 'UNKNOWN',
                'origin_country': str(state[2]) if state[2] else 'UNKNOWN',
                'time_position': self.safe_timestamp_convert(state[3]),
                'last_contact': self.safe_timestamp_convert(state[4]),
                'longitude': self.safe_float_convert(state[5]),
                'latitude': self.safe_float_convert(state[6]),
                'baro_altitude': self.safe_float_convert(state[7]),
                'on_ground': self.safe_bool_convert(state[8]),
                'velocity': self.safe_float_convert(state[9]),
                'true_track': self.safe_float_convert(state[10]),
                'vertical_rate': self.safe_float_convert(state[11]),
                'geo_altitude': self.safe_float_convert(state[13]),
                'squawk': str(state[14]) if state[14] else 'UNKNOWN',
                'spi': self.safe_bool_convert(state[15]),
                'position_source': str(state[16]) if state[16] else 'UNKNOWN',
                'ingestion_time': current_time
            }
            flights.append(flight)

        return flights

    def transform(self, data):
        """Transform data using Spark."""
        df = self.spark.createDataFrame(data, self.schema)

        transformed_df = df.na.fill({
            'callsign': 'UNKNOWN',
            'origin_country': 'UNKNOWN',
            'squawk': 'UNKNOWN',
            'position_source': 'UNKNOWN'
        })

        numeric_columns = [f.name for f in self.schema.fields if isinstance(f.dataType, DoubleType)]
        for col_name in numeric_columns:
            transformed_df = transformed_df.na.fill({col_name: 0.0})

        transformed_df = transformed_df.withColumn(
            'time_position', coalesce(col('time_position'), current_timestamp())
        ).withColumn(
            'last_contact', coalesce(col('last_contact'), current_timestamp())
        )

        transformed_df = transformed_df.filter(
            col('icao24').isNotNull() &
            col('time_position').isNotNull() &
            col('ingestion_time').isNotNull()
        )

        return transformed_df

    def calculate_statistics(self, df):
        """Calculate statistics for numeric fields."""
        statistics = {}
        for column in df.columns:
            if isinstance(df.schema[column].dataType, DoubleType):
                mean_value = df.select(mean(col(column))).first()[0]
                statistics[column] = {
                    'mean': mean_value,
                    'min': df.selectExpr(f"min({column})").first()[0],
                    'max': df.selectExpr(f"max({column})").first()[0],
                    'count': df.select(count(col(column))).first()[0]
                }
                # For mode calculation
                mode_result = df.groupBy(column).count().orderBy(col('count').desc()).first()
                statistics[column]['mode'] = mode_result[column] if mode_result else None

        return statistics

    def load(self, df):
        """Load data into Cassandra."""
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .mode("append") \
            .options(table="flights", keyspace=self.keyspace) \
            .save()

    def run_pipeline(self):
        """Run the complete ETL pipeline."""
        try:
            print("Starting ETL pipeline...")
            raw_data = self.extract()

            if not raw_data:
                print("No valid data extracted from API")
                return

            transformed_data = self.transform(raw_data)

            # Calculate and display statistics
            statistics = self.calculate_statistics(transformed_data)
            for column, stats in statistics.items():
                print(f"Statistics for {column}:")
                print(f"  Mean: {stats['mean']}")
                print(f"  Min: {stats['min']}")
                print(f"  Max: {stats['max']}")
                print(f"  Count: {stats['count']}")
                print(f"  Mode: {stats['mode']}")

            count = transformed_data.count()
            print(f"Number of records to be loaded: {count}")

            if count > 0:
                print("Loading data into Cassandra...")
                self.load(transformed_data)
                print("ETL pipeline completed successfully!")
            else:
                print("No valid records to load")

        except Exception as e:
            print(f"Error in ETL pipeline: {str(e)}")
            raise
        finally:
            self.spark.stop()

if __name__ == "__main__":
    etl = OpenSkyETL()
    etl.run_pipeline()