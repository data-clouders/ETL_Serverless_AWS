import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import substring
from pyspark.sql.functions import col, to_date, date_format, to_timestamp, udf, StringType, dayofmonth, month, year
from datetime import datetime
from pyspark.sql.utils import AnalysisException
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import IntegerType, DateType, DoubleType, DecimalType


# Parse command-line arguments
args = getResolvedOptions(sys.argv,
                            ['JOB_NAME',
                            'INPUT_BUCKET',
                            'OUTPUT_BUCKET'])

# Create a SparkContext and GlueContext
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

# Create a Glue Job instance
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def convert_date_columns(df, columns_to_cast):
    
    for col_name in columns_to_cast:
        df = df.withColumn(col_name, substring(df[col_name], 1, 10))
        df = df.withColumn(col_name, df[f"{col_name}"].cast(DateType()))
    return df


def convert_numbers_columns(df, columns_to_cast):
    
    for col_name in columns_to_cast:
        df = df.withColumn(col_name, col(col_name).cast(DoubleType()))

    return df


def drop_columns(df, columns_to_drop):
    return df.drop(*columns_to_drop)


def validate_and_write_parquet(df, database_name, table_name, output_s3_path, spark):
    # Check if the db exists
    try:
        spark.sql(f"USE {database_name}")
    except:
        print(f"Database {database_name} does not exist. Creating the database.")
        spark.sql(f"CREATE DATABASE {database_name}")
        
        
    # Check if the table exists
    try:
        spark.table(table_name)
        print(f"Table {table_name} exists. Overwriting the table.")
        # If table exists, overwrite it
        df.write.option('path',output_s3_path).format("parquet").mode("overwrite").saveAsTable(f"{database_name}.{table_name}")

    except AnalysisException:
        print(f"Table {table_name} does not exist. Creating the table.")
        df.write.option('path',output_s3_path).format("parquet").saveAsTable(f"{database_name}.{table_name}")


def get_part_date(df,column_date):
    for column in column_date:
        df = df.withColumn(f"day_{column}", dayofmonth(df[f"{column}"])) \
            .withColumn(f"month_{column}", month(df[f"{column}"])) \
            .withColumn(f"year_{column}", year(df[f"{column}"]))
    return df


def main():
    
    # USA
    df_input_usa = f"{args['INPUT_BUCKET']}Usa/"
    output_s3_usa = f"{args['OUTPUT_BUCKET']}Usa/"
    df_usa_read = spark.read.parquet(df_input_usa)
    
    # COL
    df_input_col = f"{args['INPUT_BUCKET']}Col/"
    output_s3_col = f"{args['OUTPUT_BUCKET']}Col/"
    df_col_read = spark.read.parquet(df_input_col)

    # USA
    colums_to_delete_usa = ["date","states","recovered"]
    columns_to_cast_number_usa = ["death", "deathincrease", "hospitalized", "hospitalizedcumulative", "hospitalizedcurrently", "hospitalizedincrease", "inicucumulative", "inicucurrently", "negative", "negativeincrease", "onventilatorcumulative", "onventilatorcurrently", "pending", "positive", "positiveincrease", "totaltestresults", "totaltestresultsincrease"]
    columns_to_cast_date_usa = [ "datechecked", "lastModified"]
    columns_to_get_part_date_usa = ["datechecked","lastModified"]

    # COL
    colums_to_delete_col = ["per_etn_"]
    columns_to_cast_number_col = ["edad"]
    columns_to_cast_date_col = ["fecha_de_notificaci_n", "fecha_diagnostico", "fecha_inicio_sintomas", "fecha_muerte", "fecha_recuperado", "fecha_reporte_web"]
    columns_to_get_part_date_col = ["fecha_de_notificaci_n","fecha_diagnostico"]

    # USA
    df_usa = drop_columns(df_usa_read, colums_to_delete_usa)
    df_usa = convert_numbers_columns(df_usa, columns_to_cast_number_usa)
    df_usa = convert_date_columns(df_usa, columns_to_cast_date_usa)
    df_usa = get_part_date(df_usa,columns_to_get_part_date_usa)

    # COL
    df_col = drop_columns(df_col_read, colums_to_delete_col)
    df_col = convert_numbers_columns(df_col, columns_to_cast_number_col)
    df_col = convert_date_columns(df_col, columns_to_cast_date_col)
    df_col = get_part_date(df_col,columns_to_get_part_date_col)

    # Escribir data
    validate_and_write_parquet(df_usa, "database_usa", "table_usa", output_s3_usa, spark)
    validate_and_write_parquet(df_col, "database_col", "table_col", output_s3_col, spark)


if __name__ == "__main__":
    main()