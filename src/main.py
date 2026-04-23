import os
import sys
from utils.spark_session import get_spark_session
from extraction.reader import DBReader

def main():
    spark = get_spark_session("SQL-to-S3-Pipeline")
    bucket_name = os.getenv('S3_BUCKET')
    s3_path = f"s3a://{bucket_name}/dim_identity_output/"
    
    print(f"Starting pipeline. Destination: {s3_path}")
    
    try:
        reader = DBReader(spark)
        print("Extracting data from SQL Server...")
        df = reader.read_table()
        
        df.show(3) # Show a preview in logs
        
        print("Writing data to AWS S3...")
        df.write.mode("overwrite").parquet(s3_path)
        
        print("SUCCESS: Pipeline Complete.")
        
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
