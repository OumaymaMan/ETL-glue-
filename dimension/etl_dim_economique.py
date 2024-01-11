import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr
from awsglue.dynamicframe import DynamicFrame

# Initialisation des paramètres du job Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Lecture des données de chômage et PIB depuis le catalogue de données Glue
df_chomage = glueContext.create_dynamic_frame.from_catalog(database="dimension", table_name="chomage_csv").toDF()
df_pib = glueContext.create_dynamic_frame.from_catalog(database="dimension", table_name="pib_csv").toDF()

def transform_and_stack_df(df, year_columns, value_column_name):
    escaped_year_columns = ["`{}`".format(year) for year in year_columns]
    stack_expr = "stack(" + str(len(year_columns)) + ", " + ', '.join(["'{}', {}".format(year, col) for year, col in zip(year_columns, escaped_year_columns)]) + ") as (Year, "+ value_column_name + ")"
    return df.select("country_name", "country_code", F.expr(stack_expr)).where(value_column_name + " is not null")

# Transformation des DataFrames pour chômage et PIB
year_columns_chomage = [c for c in df_chomage.columns if c.startswith('a') and c[1:].isdigit()]
year_columns_pib = [c for c in df_pib.columns if c.startswith('a') and c[1:].isdigit()]

chomage_transformed = transform_and_stack_df(df_chomage, year_columns_chomage, "chomage_value")
pib_transformed = transform_and_stack_df(df_pib, year_columns_pib, "pib_value")

# Retirer le préfixe 'a' des années
chomage_transformed = chomage_transformed.withColumn("Year", expr("substring(Year, 2, 4)"))
pib_transformed = pib_transformed.withColumn("Year", expr("substring(Year, 2, 4)"))

# Jointure des DataFrames transformés sur 'country_code' et 'Year'
joined_df = chomage_transformed.join(pib_transformed, ["country_code", "Year"], "inner") \
                               .select(
                                   "Year",
                                   "country_code",
                                   chomage_transformed["country_name"],  # Utiliser 'country_name' de 'chomage_transformed'
                                   "chomage_value",
                                   "pib_value"
                               )

# Ajout de la colonne 'id_economique' et suppression des doublons
joined_df = joined_df.withColumn("id_economique", F.concat_ws("_", col("country_code"), col("Year"))) \
                     .dropDuplicates(["id_economique"])

# Sélection des colonnes pour le DataFrame final
joined_df = joined_df.select(
    col("id_economique"),
    col("Year"),
    col("country_code"),
    col("country_name"),  
    col("chomage_value"),
    col("pib_value")
)

# Conversion du DataFrame Spark en DynamicFrame pour l'écriture dans Redshift
dynamic_frame_to_write = DynamicFrame.fromDF(joined_df, glueContext, "dynamic_frame_to_write")
# Définition des options de connexion à Redshift
redshift_write_options = {
    "url": "jdbc:redshift://***TODO****",
    "dbtable": "dim_economique",
    "user": "***TODO****",
    "password": "***TODO*****",
    "redshiftTmpDir": "s3://***TODO****"
}

# Écriture du DynamicFrame dans Redshift
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_to_write,
    connection_type="redshift",
    connection_options=redshift_write_options,
    transformation_ctx="datasink4"
)

# Finalisation du job
job.commit()
