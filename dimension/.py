import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from awsglue.dynamicframe import DynamicFrame

# Initialisation des paramètres du job Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Lire les données de chaque source et sélectionner uniquement les colonnes nécessaires
sources = ["ccpi_a", "ecpi_a", "fcpi_a", "hcpi_a", "ppi_a"]
dataframes = [glueContext.create_dynamic_frame.from_catalog(database="data", table_name=source).toDF().select("country_code", "country") for source in sources]

# Fusionner tous les DataFrames en un seul
combined_df = dataframes[0]
for dataframe in dataframes[1:]:
    combined_df = combined_df.unionByName(dataframe)

# Suppression des doublons
combined_df = combined_df.dropDuplicates(["country_code"])

# Conversion en DynamicFrame
transformed_dynamic_frame = DynamicFrame.fromDF(combined_df, glueContext, "transformed_df")

# Définition des options de connexion à Redshift
redshift_write_options = {
    "url": "jdbc:redshift://***TODO****",
    "dbtable": "dim_geographique",
    "user": "***TODO****",
    "password": "***TODO****",
    "redshiftTmpDir": "s3://***TODO****"
}

# Écriture du DynamicFrame dans Redshift
glueContext.write_dynamic_frame.from_options(
    frame=transformed_dynamic_frame,
    connection_type="redshift",
    connection_options=redshift_write_options,
    transformation_ctx="datasink4"
)

# Finalisation du job
job.commit()
