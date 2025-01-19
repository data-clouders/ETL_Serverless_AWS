import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
import datetime

args = getResolvedOptions(sys.argv,
                            ['JOB_NAME',
                            'key',
                            'INPUT_BUCKET',
                            'OUTPUT_BUCKET'])


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def read_json_from_s3(glue_context, s3_path):

    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [s3_path] },
        format="json"
    )
    return dynamic_frame


def validate_and_write_parquet(glue_context, dynamic_frame, output_s3_path):
    
    count = dynamic_frame.count()
    if count == 0:
        raise ValueError("El DynamicFrame no contiene datos. Validación fallida.")
    
    glue_context.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_s3_path},
        format="parquet"
    )


def main():
    timestamp = datetime.now().strftime('%Y-%m-%d')    
    input_bucket_s3_usa = f"{args['INPUT_BUCKET']}Usa/datos_response_{timestamp}.json"
    input_bucket_s3_col = f"{args['INPUT_BUCKET']}Col/datos_response_{timestamp}.json"

    output_bucket_s3_usa = f"{args['OUTPUT_BUCKET']}Usa/"
    output_bucket_s3_col = f"{args['OUTPUT_BUCKET']}Col/"
    
    dynamic_frame_raw_usa = read_json_from_s3(glue_context, input_bucket_s3_usa)
    dynamic_frame_raw_col = read_json_from_s3(glue_context, input_bucket_s3_col)
    
    validate_and_write_parquet(glue_context, dynamic_frame_raw_usa, output_bucket_s3_usa)
    validate_and_write_parquet(glue_context, dynamic_frame_raw_col, output_bucket_s3_col)
    job.commit()


if __name__ == "__main__":
    main()