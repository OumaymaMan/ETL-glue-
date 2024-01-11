import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lit, udf, monotonically_increasing_id
from pyspark.sql.types import IntegerType
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Fonctions UDF pour l'extraction des données temporelles
def extract_year(date_id):
    match = re.match(r'a(\d{4})', date_id)
    return int(match.group(1)) if match else None

def extract_month(date_id):
    match = re.search(r'm(\d{2})', date_id)
    return int(match.group(1)) if match else None

def extract_quarter(date_id):
    match = re.search(r'q(\d)', date_id)
    return int(match.group(1)) if match else None

extract_year_udf = udf(extract_year, IntegerType())
extract_month_udf = udf(extract_month, IntegerType())
extract_quarter_udf = udf(extract_quarter, IntegerType())

# Liste des noms de table dans le catalogue de données Glue à parcourir
table_names = ["ccpi_a", "ccpi_m", "ccpi_q"]

# DataFrame pour la dimension temporelle
dim_temp_df = spark.createDataFrame([], schema="id_date INT, Year INT, Quarter INT, Month INT")

# Parcourir chaque table et extraire les informations de date
for table_name in table_names:
    table_df = glueContext.create_dynamic_frame.from_catalog(database="data", table_name=table_name).toDF()
    # Extraction des colonnes de date et création d'un identifiant unique pour chaque ligne
    for column_name in table_df.columns:
        if re.match(r'a(\d{4})(m\d{2}|q\d)?', column_name):
            temp_df = table_df.select(lit(column_name).alias("DateID"))
            temp_df = temp_df.withColumn("Year", extract_year_udf(col("DateID")))
            temp_df = temp_df.withColumn("Quarter", extract_quarter_udf(col("DateID")))            
            temp_df = temp_df.withColumn("Month", extract_month_udf(col("DateID")))
            temp_df = temp_df.withColumn("id_date", monotonically_increasing_id())
            dim_temp_df = dim_temp_df.union(temp_df.select("id_date", "Year", "Quarter", "Month"))

# Suppression des doublons
dim_temp_df = dim_temp_df.dropDuplicates(["id_date"])

# Conversion du DataFrame Spark en DynamicFrame pour l'écriture dans Redshift
dynamic_frame_to_write = DynamicFrame.fromDF(dim_temp_df, glueContext, "dynamic_frame_to_write")

# Définition des options de connexion à Redshift
redshift_write_options = {
    "url": "jdbc:redshift://***TODO****",
    "dbtable": "dim_temporelle",
    "user": "admin",
    "password": "***TODO****",
    "redshiftTmpDir": "s3://***TODO****"
}

# Écriture du DynamicFrame dans Redshift
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_to_write,
    connection_type="redshift",
    connection_options=redshift_write_options,
    transformation_ctx="datasink4"
)

job.commit()