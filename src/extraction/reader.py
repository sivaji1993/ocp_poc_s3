import os

class DBReader:
    def __init__(self, spark):
        self.spark = spark
        self.server = os.getenv('DB_SERVER', 'ABCD\\EFGH303')
        self.user = os.getenv('DB_USER')
        self.pwd = os.getenv('DB_PASS')
        self.database = os.getenv('DB_NAME', 'AI DB')

    def read_table(self):
        jdbc_url = f"jdbc:sqlserver://{self.server};databaseName={self.database};encrypt=true;trustServerCertificate=true;"
        print(f"Connecting to SQL Server via Spark JDBC at: {self.server}")

        return self.spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "[IAM IADW].[Dim Identity]") \
            .option("user", self.user) \
            .option("password", self.pwd) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
