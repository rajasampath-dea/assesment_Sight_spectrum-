from configparser import ConfigParser
import os
import re
import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("IMDb ETL") \
    .getOrCreate()
CONF_FILE = "C:/conf/configuration.ini" # where I will store all my credientials and environment variables
BASE_PATH = "C:/data/temp/imdb/"  #extract path after unziping
conf_parse = ConfigParser()
conf_parse.read(CONF_FILE)
#Add Redshift connection
print("Start connecting to Redshift.......................")                                                       
redshift_deid_user = conf_parse.get("conf", "redshift_deid_user")
redshift_deid_password = conf_parse.get("conf", "redshift_deid_password")
redshift_deid_host = conf_parse.get("conf", "redshift_deid_host")
redshift_deid_db_port = conf_parse.get("conf", "redshift_deid_port")
redshift_deid_db = conf_parse.get("conf", "redshift_deid_db")
iam_role = conf_parse.get("conf", "iam_role")
ctx = "host=%s port=%s dbname=%s user=%s password=%s" % (
redshift_deid_host, redshift_deid_db_port, redshift_deid_db, redshift_deid_user, redshift_deid_password)
rsconn = psycopg2.connect(ctx)
rs_cur = rsconn.cursor()  
print("==> Connected to redshift ...") 
try:
    # Load raw datasets
    
    title_basics = spark.read.csv(f"{BASE_PATH}/title.basics.tsv", sep="\t", header=True)
    title_ratings = spark.read.csv(f"{BASE_PATH}/title.ratings.tsv", sep="\t", header=True)
    principals = spark.read.csv(f"{BASE_PATH}/title.principals.tsv", sep="\t", header=True)
    
    #Clean and transform the IMDb dataset (titles, ratings, and episodes). I dont see episode files in the IMDb dataset. so switching it to principals
    # Clean Titles
    titles_clean = title_basics \
        .select(
            col("tconst"),
            col("titleType"),
            col("primaryTitle"),
            col("originalTitle"),
            col("isAdult").cast("int"),
            col("startYear").cast("int"),
            col("endYear").cast("int"),
            col("runtimeMinutes").cast("int"),
            col("genres")
        ) 
        
    #I ave attached the IAM role to EC2 with S3 resource policy. so no need for access_key_is and secret_access_key_id
    titles_clean.write \
        .mode("overwrite") \
        .partitionBy("titleType", "startYear") \
        .parquet("s3://imdb-lakehouse-bucket/data_lake/titles", compression="snappy")    
    
    # Ratings
    
    ratings_clean = title_ratings \
        .select(
            col("tconst"),
            col("averageRating").cast("float"),
            col("numVotes").cast("int")
        )
        
    ratings_clean.write \
        .mode("overwrite") \
        .parquet("s3://imdb-lakehouse-bucket/data_lake/ratings", compression="snappy")         #I am not using partition here as it seems fact table and data are measures
        
        
    # principals
    
    principals_clean = principals \
        .select(
            col("tconst"),
            col("ordering"),
            col("nconst"),
            col("category"),
            col("job"),
            col("characters")
        )
    
    principals_clean.write \
        .mode("overwrite") \
        .partitionBy("tconst") \
        .parquet("s3://imdb-lakehouse-bucket/data_lake/principals", compression="snappy")
    

#    create_tbl_title_basics = """ DROP TABLE IF EXISTS imdb.title_basics;
#            CREATE TABLE imdb.title_basics (
#                        tconst VARCHAR(20) NOT NULL,
#                        titleType VARCHAR(50),
#                        primaryTitle VARCHAR(512),
#                        originalTitle VARCHAR(512),
#                        isAdult INT,
#                        startYear INT,
#                        endYear INT,
#                        runtimeMinutes INT,
#                        genres VARCHAR(200)
#                    ) DISTSTYLE KEY
#                    DISTKEY (tconst)
#                    SORTKEY (titleType, startYear);
#                """
#    rs_cur.execute(create_tbl_title_basics) 
#    rs_conn.commit()
#    
#    create_tbl_title_ratings = """ DROP TABLE IF EXISTS imdb.title_ratings;
#            CREATE TABLE imdb.title_ratings (
#                        tconst VARCHAR(20) NOT NULL,
#                        averageRating FLOAT4,
#                        numVotes INT
#                    )   DISTSTYLE AUTO            
#            
#            """
#    rs_cur.execute(create_tbl_title_ratings) 
#    rs_conn.commit()
#    
#    create_tbl_principals = """ DROP TABLE IF EXISTS imdb.principals;
#            CREATE TABLE imdb.principals 
#                tconst VARCHAR(20) NOT NULL,
#                ordering INT,
#                nconst VARCHAR(20),
#                category VARCHAR(50),
#                job VARCHAR(255),
#                characters VARCHAR(512)
#                )       
#                        
#                        """        
#    rs_cur.execute(create_tbl_principals) 
#    rs_conn.commit()
#
#    cp_cmd_title_basics = """COPY imdb.title_basics
#            FROM 's3://imdb-lakehouse/titles/'
#            IAM_ROLE '{}'
#            FORMAT AS PARQUET""".format(iam_role)
#    rs_cur.execute(cp_cmd_title_basics) 
#    rs_conn.commit()            
#
#    cp_cmd_title_rating = """COPY imdb.title_ratings
#            FROM 's3://imdb-lakehouse/ratings/'
#            IAM_ROLE '{}'
#            FORMAT AS PARQUET""".format(iam_role)
#    rs_cur.execute(cp_cmd_title_rating) 
#    rs_conn.commit()            
#
#    cp_cmd_principals = """COPY imdb.principals
#                FROM 's3://imdb-lakehouse/principals/'
#                IAM_ROLE '{}'
#                FORMAT AS PARQUET""".format(iam_role)
#    rs_cur.execute(cp_cmd_principals) 
#    rs_conn.commit()
except Exception as e:
    print(f"error {str(e)}")
    if rsconn:
        rsconn.rollback()
    sys.exit(1)

finally:
    if rs_cur:
        rs_cur.close()
    if rsconn:
        rsconn.close()
    spark.stop()

  