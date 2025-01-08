import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    logging.info("Keyspace created successfully!")

def create_tables(session):
    # Table for User Table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.user (
        user_id UUID PRIMARY KEY,
        dob_id TEXT,
        registered_id TEXT,
        location_id TEXT,
        login_id UUID,
        contact_id TEXT,
        gender TEXT,
        nationality TEXT
    );
    """)
    logging.info("Table 'user' created successfully!")

    # Table for Location Table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.location (
        location_id TEXT PRIMARY KEY,
        street TEXT,
        city TEXT,
        state TEXT,
        country TEXT,
        postcode TEXT
    );
    """)
    logging.info("Table 'location' created successfully!")

    # Table for Contact Table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.contact (
        contact_id TEXT PRIMARY KEY,
        phone TEXT,
        cell TEXT,
        email TEXT
    );
    """)
    logging.info("Table 'contact' created successfully!")

    # Table for Login Table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.login (
        login_id UUID PRIMARY KEY,
        username TEXT,
        password TEXT,
        salt TEXT,
        md5 TEXT,
        sha1 TEXT,
        sha256 TEXT
    );
    """)
    logging.info("Table 'login' created successfully!")

    # Table for Registered Table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.register (
        registered_id TEXT PRIMARY KEY,
        registered_date TEXT,
        years_since_registration INT
    );
    """)
    logging.info("Table 'register' created successfully!")

    # Table for Date of Birth Table
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.date_of_birth (
        dob_id TEXT PRIMARY KEY,
        dob TEXT,
        age INT
    );
    """)
    logging.info("Table 'date_of_birth' created successfully!")

def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                    "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1,"
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
                    "org.scala-lang.modules:scala-collection-compat_2.12:2.11.0") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        spark_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        return None

def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
            .option("subscribe", "users_created") \
            .option("failOnDataLoss", "false") \
            .load()
        logging.info("Kafka DataFrame created successfully!")
        return spark_df
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created: {e}")
        return None




def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("dob_id", StringType(), True),
        StructField("registered_id", StringType(), True),
        StructField("location_id", StringType(), True),
        StructField("login_id", StringType(), True),
        StructField("contact_id", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("nationality", StringType(), True),
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("cell", StringType(), True),
        StructField("email", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
        StructField("salt", StringType(), True),
        StructField("md5", StringType(), True),
        StructField("sha1", StringType(), True),
        StructField("sha256", StringType(), True),
        StructField("registered_date", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("years_since_registration", IntegerType(), True)
    ])

    try:
        # Parse Kafka messages into DataFrame
        parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data"))

        # Flatten DataFrame
        parsed_df = parsed_df.select("data.*")
        logging.info("DataFrame schema applied successfully!")
        return parsed_df
    except Exception as e:
        logging.error(f"Failed to apply schema to DataFrame: {e}")
        return None
    
def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        logging.info("Cassandra connection established successfully!")
        return session
    except Exception as e:
        logging.error(f"Failed to connect to Cassandra: {e}")
        return None

if __name__ == "__main__":
    # Create Spark connection
    spark_conn = create_spark_connection()

    if spark_conn:
        # Connect to Kafka
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            # Process Kafka DataFrame
            selection_df = create_selection_df_from_kafka(spark_df)
            if selection_df:
                session = create_cassandra_connection()
                if session:
                    create_keyspace(session)
                    create_tables(session)

                    # Split the data into two DataFrames
                    user = selection_df.select(
                        "user_id", "dob_id", "location_id", "login_id", 
                        "contact_id", "gender", "nationality", "registered_id"
                    )

                    location = selection_df.select(
                        "location_id", "street", "city", "state", 
                        "country", "postcode"
                    )

                    contact = selection_df.select(
                        "contact_id", "phone", "cell", "email"
                    )

                    login = selection_df.select(
                        "login_id", "username", "password", "salt",
                        "md5", "sha1", "sha256"
                    )

                    register = selection_df.select(
                        "registered_id", "registered_date", "years_since_registration"
                    )

                    dob = selection_df.select(
                        "dob_id", "dob", "age"
                    )

                    # Write personal details to Cassandra
                    try:
                        user_stream_query = (user.writeStream
                                                 .format("org.apache.spark.sql.cassandra")
                                                 .option('checkpointLocation', '/tmp/personal_checkpoint')
                                                 .option('keyspace', 'spark_streams')
                                                 .option('table', 'user')
                                                 .outputMode("append")
                                                 .start())
                        logging.info("Streaming personal details started successfully!")
                    except Exception as e:
                        logging.error(f"Personal details streaming query failed: {e}")

                    # Write location details to Cassandra
                    try:
                        location_stream_query = (location.writeStream
                                                 .format("org.apache.spark.sql.cassandra")
                                                 .option('checkpointLocation', '/tmp/location_checkpoint')
                                                 .option('keyspace', 'spark_streams')
                                                 .option('table', 'location')
                                                 .outputMode("append")
                                                 .start())
                        logging.info("Streaming location details started successfully!")
                    except Exception as e:
                        logging.error(f"Location details streaming query failed: {e}")

                    try:
                        contact_stream_query = (contact.writeStream
                                                 .format("org.apache.spark.sql.cassandra")
                                                 .option('checkpointLocation', '/tmp/contact_checkpoint')
                                                 .option('keyspace', 'spark_streams')
                                                 .option('table', 'contact')
                                                 .outputMode("append")
                                                 .start())
                        logging.info("Streaming contact details started successfully!")
                    except Exception as e:
                        logging.error(f"Contact details streaming query failed: {e}")

                    try:
                        login_stream_query = (login.writeStream
                                                 .format("org.apache.spark.sql.cassandra")
                                                 .option('checkpointLocation', '/tmp/login_checkpoint')
                                                 .option('keyspace', 'spark_streams')
                                                 .option('table', 'login')
                                                 .outputMode("append")
                                                 .start())
                        logging.info("Streaming login details started successfully!")
                    except Exception as e:
                        logging.error(f"Login details streaming query failed: {e}")

                    try:
                        register_stream_query = (register.writeStream
                                                 .format("org.apache.spark.sql.cassandra")
                                                 .option('checkpointLocation', '/tmp/register_checkpoint')
                                                 .option('keyspace', 'spark_streams')
                                                 .option('table', 'register')
                                                 .outputMode("append")
                                                 .start())
                        logging.info("Streaming register details started successfully!")
                    except Exception as e:
                        logging.error(f"Register details streaming query failed: {e}")

                    try:
                        dob_stream_query = (dob.writeStream
                                                 .format("org.apache.spark.sql.cassandra")
                                                 .option('checkpointLocation', '/tmp/dob_checkpoint')
                                                 .option('keyspace', 'spark_streams')
                                                 .option('table', 'date_of_birth')
                                                 .outputMode("append")
                                                 .start())
                        logging.info("Streaming date of birth details started successfully!")
                    except Exception as e:
                        logging.error(f"Date of birth details streaming query failed: {e}")

                    # Await termination
                    user_stream_query.awaitTermination()
                    location_stream_query.awaitTermination()
                    contact_stream_query.awaitTermination()
                    login_stream_query.awaitTermination()
                    register_stream_query.awaitTermination()
                    dob_stream_query.awaitTermination()
