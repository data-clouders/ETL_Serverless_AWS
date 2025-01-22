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
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.legacy.allowNonEmptyLocationInCTAS", "true")

# Create a Glue Job instance
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def convert_date_columns():
    pass
# COL
# fecha_de_notificacion, fecha_diagnostico, fecha_inicio_sintomas, fecha_muerte, fecha_recuperado, fecha_reporte_web
# USA
# datechecked (dd/mm/aaaa), date (dd/mm/aaaa), 

def convert_numbers_columns():
    pass
# COL
# Edad
# USA
""" 
    death, deathincrease, hospitalized, hospitalizedcumulative, hospitalizedcurrently, 
    hospitalizedincrease, inicucumulative, inicucurrently, negative, negativeincrease, 
    onventilatorcumulative, onventilatorcurrently, pending, positive, positiveincrease
    recovered, totaltestresults, totaltestresultsincrease

"""


def convert_category_columns():
    pass
# COL
# Sexo, Fuente_tipo_contagio



def drop_columns():
    pass
# USA
# lastmodified, states



def main():
    pass


if __name__ == "__main__":
    main()