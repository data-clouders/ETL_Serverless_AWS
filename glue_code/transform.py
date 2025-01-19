import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date
from datetime import datetime
from pyspark.sql.utils import AnalysisException
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Parse command-line arguments
args = getResolvedOptions(sys.argv,
                            ['JOB_NAME',
                             'INPUT_BUCKET',
                             'OUTPUT_BUCKET'])

# Create a SparkContext and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Create a Glue Job instance
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Function to read JSON from S3 using PySpark
def read_json_from_s3(spark, s3_path):
    try:
        # Read data from S3 and create a DataFrame
        df = spark.read.json(s3_path)
        count = df.count()
        print(f"Number of records read from {s3_path}: {count}")
        return df
    except AnalysisException as e:
        print(f"Error reading from {s3_path}: {str(e)}")
        return None

# Function to validate the DataFrame and write to Parquet
def validate_and_write_parquet(df, output_s3_path):
    if df is None or df.count() == 0:
        print("DataFrame is empty. Skipping write operation.")
        return
    
    # Write the DataFrame to Parquet format
    df.write.parquet(output_s3_path, mode="overwrite")
    print(f"Data written to {output_s3_path}")

# Main logic
def main():
    # Generate timestamp to be used in S3 paths
    timestamp = datetime.now().strftime('%Y-%m-%d')
    
    # Construct S3 paths for input and output data
    input_s3_usa = f"{args['INPUT_BUCKET']}Usa/datos_response_{timestamp}.json"
    input_s3_col = f"{args['INPUT_BUCKET']}Col/datos_response_{timestamp}.json"
    output_s3_usa = f"{args['OUTPUT_BUCKET']}Usa/"
    output_s3_col = f"{args['OUTPUT_BUCKET']}Col/"
    
    # Read data from S3 for USA and Colombia
    df_usa = read_json_from_s3(spark, input_s3_usa)
    df_col = read_json_from_s3(spark, input_s3_col)

    print(df_usa)

if __name__ == "__main__":
    main()

