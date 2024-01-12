import sys
import re  
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from functools import reduce  

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Fonction pour traiter les données annuelles et mensuelles
def process_data(source_dyf, date_pattern, date_format):
    # Transformer le DynamicFrame en DataFrame pour les opérations de manipulation des données
    source_df = source_dyf.toDF()

    # Sélectionner les colonnes 'country_code' et les colonnes de dates selon le pattern
    date_columns = [col for col in source_df.columns if re.match(date_pattern, col)]
    
    print(f"date_columns : {date_columns}")

    # Préparer le DataFrame final pour la table de faits
    fact_rows = []
    for date_column in date_columns:
        year_value = date_column[1:5]
       
        fact_rows.append(
            source_df.withColumn("ID_Date", lit(date_column))
                     .withColumn("id_economique", concat(col("country_code"), lit("_"), lit(year_value)))
                     .withColumn("inflation_value", col(date_column))
                     .withColumn("id_secteur", lit("G")) 
                     .select("ID_Date", "id_secteur", "id_economique", "country_code", "inflation_value")
        )

    # Union de toutes les lignes pour créer le DataFrame complet pour la table de faits
    return reduce(DataFrame.union, fact_rows)

# Lire les données annuelles depuis la table hcpi_a via le catalogue de données Glue
annual_source_dyf = glueContext.create_dynamic_frame.from_catalog(database="data", table_name="hcpi_a")
annual_fact_df = process_data(annual_source_dyf, r'a\d{4}$', r'a(\d{4})')

annual_fact_df.show(100)

# Lire les données mensuelles depuis la table hcpi_m via le catalogue de données Glue
monthly_source_dyf = glueContext.create_dynamic_frame.from_catalog(database="data", table_name="hcpi_m")
monthly_fact_df = process_data(monthly_source_dyf, r'a\d{4}m\d{2}$', r'a(\d{4})m(\d{2})')

monthly_fact_df.show(100)

# Lire les données trimestrielles
quarterly_source_dyf = glueContext.create_dynamic_frame.from_catalog(database="data", table_name="hcpi_q")
quarterly_fact_df = process_data(quarterly_source_dyf, r'a\d{4}q\d$', r'a(\d{4})q(\d)')

# Union des données annuelles, mensuelles et trimestrielles
combined_fact_df = annual_fact_df.union(monthly_fact_df).union(quarterly_fact_df)

# Afficher les deux premières lignes du DataFrame combiné pour le débogage
combined_fact_df.show(2)

# Convertir le DataFrame combiné en DynamicFrame
combined_fact_dyf = DynamicFrame.fromDF(combined_fact_df, glueContext, "combined_fact_dyf")

# Afficher les deux premières lignes du DynamicFrame combiné pour le débogage
combined_fact_dyf.toDF().show(300)

# Define the Redshift connection options
redshift_write_options = {
    "url": "jdbc:redshift://**TODO**",
    "dbtable": "fact_inflation",
    "user": "***TODO****",
    "password": "***TODO****",
    "redshiftTmpDir": "s3://***TODO****"
}

# Write the DynamicFrame to Redshift
glueContext.write_dynamic_frame.from_options(
    frame = combined_fact_dyf,
    connection_type = "redshift",
    connection_options = redshift_write_options,
    transformation_ctx = "datasink4"
)

job.commit()
