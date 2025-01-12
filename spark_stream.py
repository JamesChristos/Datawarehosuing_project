import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import mysql.connector
from pyspark.sql.functions import to_date
from pyspark.sql.functions import to_timestamp

# Set up logging
logging.basicConfig(level=logging.INFO)

def create_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='mysql',
            database='datawarehousing'
        )
        logging.info("MySQL connection established successfully!")
        return connection
    except mysql.connector.Error as e:
        logging.error(f"Failed to connect to MySQL: {e}")
        return None

def create_tables(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("""
        CREATE DATABASE IF NOT EXISTS datawarehousing;
        """)
        cursor.execute("USE datawarehousing;")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS user (
            user_id VARCHAR(36) PRIMARY KEY,
            dob_id VARCHAR(255),
            registered_id VARCHAR(255),
            location_id VARCHAR(255),
            login_id VARCHAR(36),
            contact_id VARCHAR(255),
            gender VARCHAR(10),
            nationality VARCHAR(50)
        );
        """)
        logging.info("Table 'user' created successfully!")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS location (
            location_id VARCHAR(255) PRIMARY KEY,
            street VARCHAR(255),
            city VARCHAR(255),
            state VARCHAR(255),
            country VARCHAR(255),
            postcode VARCHAR(50),
            coordinates_latitude VARCHAR(255),
            coordinates_longitude VARCHAR(255)
        );
        """)
        logging.info("Table 'location' created successfully!")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS contact (
            contact_id VARCHAR(255) PRIMARY KEY,
            phone VARCHAR(20),
            cell VARCHAR(20),
            email VARCHAR(255)
        );
        """)
        logging.info("Table 'contact' created successfully!")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS login (
            login_id VARCHAR(36) PRIMARY KEY,
            username VARCHAR(255),
            password VARCHAR(255),
            salt VARCHAR(255),
            md5 VARCHAR(255),
            sha1 VARCHAR(255),
            sha256 VARCHAR(255)
        );
        """)
        logging.info("Table 'login' created successfully!")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS register (
            registered_id VARCHAR(255) PRIMARY KEY,
            registered_date DATE,
            years_since_registration INT
        );
        """)
        logging.info("Table 'register' created successfully!")

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS date_of_birth (
            dob_id VARCHAR(255) PRIMARY KEY,
            dob DATE,
            age INT
        );
        """)
        logging.info("Table 'date_of_birth' created successfully!")

        connection.commit()
    except mysql.connector.Error as e:
        logging.error(f"Failed to create tables: {e}")
    finally:
        cursor.close()

def create_spark_connection():
    try:
        spark_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', 
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,"
                    "mysql:mysql-connector-java:8.0.20") \
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
        StructField("years_since_registration", IntegerType(), True),
        StructField("coordinates_latitude", StringType(), True),
        StructField("coordinates_longitude", StringType(), True)
    ])

    try:
        parsed_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*")
        logging.info("DataFrame schema applied successfully!")
        return parsed_df
    except Exception as e:
        logging.error(f"Failed to apply schema to DataFrame: {e}")
        return None

def write_batch_to_mysql(batch_df, table_name):
    try:
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:mysql://localhost:3306/datawarehousing") \
            .option("dbtable", table_name) \
            .option("user", "root") \
            .option("password", "mysql") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        logging.info(f"Batch successfully written to {table_name}.")
    except Exception as e:
        logging.error(f"Failed to write batch to {table_name}: {e}")

def process_stream_with_mysql(df, table_name):
    """
    Processes streaming DataFrame using foreachBatch and writes to MySQL.
    """
    query = df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_batch_to_mysql(batch_df, table_name)) \
        .outputMode("append") \
        .start()
    return query

if __name__ == "__main__":
    spark_conn = create_spark_connection()

    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)

        if spark_df:
            selection_df = create_selection_df_from_kafka(spark_df)

            if selection_df is not None:
                connection = create_mysql_connection()
                if connection:
                    create_tables(connection)

                # Process each table
                user = selection_df.select("user_id", "dob_id", "registered_id", "location_id", "login_id", "contact_id", "gender", "nationality")
                user_query = process_stream_with_mysql(user, "user")

                location = selection_df.select("location_id", "street", "city", "state", "country", "postcode", "coordinates_latitude", "coordinates_longitude")
                location_query = process_stream_with_mysql(location, "location")

                contact = selection_df.select("contact_id", "phone", "cell", "email")
                contact_query = process_stream_with_mysql(contact, "contact")

                login = selection_df.select("login_id", "username", "password", "salt", "md5", "sha1", "sha256")
                login_query = process_stream_with_mysql(login, "login")

                register = selection_df.select("registered_id", to_timestamp(col("registered_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("registered_date"), "years_since_registration")
                register_query = process_stream_with_mysql(register, "register")

                date_of_birth = selection_df.select("dob_id", to_timestamp(col("dob"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("dob"), "age")
                dob_query = process_stream_with_mysql(date_of_birth, "date_of_birth")

                # Await termination for all queries
                spark_conn.streams.awaitAnyTermination()
