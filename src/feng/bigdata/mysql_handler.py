from pyspark.sql.connect.functions import lower


class MySQLHandler:
    def __init__(self, spark, host, port, database, user, password):
        self.spark = spark
        self.url = (
            f"jdbc:mysql://{host}:{port}/{database}"
            "?useSSL=false"
            "&autoReconnect=true"
            "&socketTimeout=600000"
            "&useCursorFetch=true"       # ← enable server-side cursors
            "&defaultFetchSize=1000"
            "&tcpKeepAlive=true"
        )
        self.properties = {
            "user": user,
            "password": password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def read_table(self, table_name):

        return self.spark.read.jdbc(
            url=self.url,
            table=table_name,
            properties=self.properties,
            lowerBound=100000,
            upperBound=999999,
            numPartitions=5
        )

    def write_table(self, df, table_name, mode='append'):
        df.write.jdbc(url=self.url, table=table_name, mode=mode, properties=self.properties)
