from src.feng.bigdata import SparkHandler
from src.feng.bigdata import MySQLHandler
from src.feng.config.config import (
    SPARK_MASTER,
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_DATABASE,
    MYSQL_USER,
    MYSQL_PASSWORD
)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from dotenv import load_dotenv
import os

def main():
    # 2. Start Spark
    spark_handler = SparkHandler(app_name="comp500_job", master_url=SPARK_MASTER)
    spark = spark_handler.start_session()

    # Define date range for filtering
    START_DATE = os.getenv("START_DATE", "1995-01-01")
    END_DATE = os.getenv("END_DATE", "2025-01-01")

    # 3. Fetch comp_500 from MySQL
    mysql = MySQLHandler(
        spark=spark,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    comp_500 = (
        mysql
          .read_table("ent_company_info")
          .filter(F.col("BICS_LEVEL_1") == 10)
          .select("ID_BB_COMPANY")
          .distinct()
    )
    comp_list = [r.ID_BB_COMPANY for r in comp_500.collect()]

    # 4. Define join keys & window spec
    keys = [
        "LATEST_PERIOD_END_DT_FULL_RECORD",
        "ID_BB_COMPANY",
        "ID_BB_UNIQUE",
        "FILING_STATUS",
        "ACCOUNTING_STANDARD",
        "EQY_CONSOLIDATED",
        "EQY_FUND_CRNCY",
        "FISCAL_YEAR_PERIOD"
    ]
    window_spec = (
        Window.partitionBy(*keys)
              .orderBy(F.col("REVISION_ID").desc())
    )

    # 5. Helper to load, rank and filter a staging table
    def load_latest(table_name):
        df = (
            mysql.read_table(f"staging.{table_name}")
                 .where(F.col("ID_BB_COMPANY").isin(comp_list))
                .where(
                    (F.col("LATEST_PERIOD_END_DT_FULL_RECORD") > START_DATE) &
                    (F.col("LATEST_PERIOD_END_DT_FULL_RECORD") < END_DATE))
                 .withColumn("revision_rank", F.rank().over(window_spec))
                 .where(F.col("revision_rank") == 1)
        )
        return df

    # 6. Load & filter all staging tables
    is_df      = load_latest("bbgftp_fundamentals_is").alias("a")
    cr_df      = load_latest("bbgftp_fundamentals_cr").alias("b")
    cr2_df     = load_latest("bbgftp_fundamentals_cr2").alias("c")
    cf_df      = load_latest("bbgftp_fundamentals_cf").alias("d")
    bs_df      = load_latest("bbgftp_fundamentals_bs").alias("e")
    sard_bs_df = load_latest("bbgftp_fundamentals_sard_bs").alias("f")
    sard_is_df = load_latest("bbgftp_fundamentals_sard_is").alias("g")
    sard_cf_df = load_latest("bbgftp_fundamentals_sard_cf").alias("h")
    from functools import reduce

    def join_no_dup(left, right, on, how="left"):
        # find columns in common (minus the join keys)
        overlap = set(left.columns).intersection(right.columns) - set(on)
        # drop those from the right-hand side
        right_clean = right.drop(*overlap)
        return left.join(right_clean, on=on, how=how)

    to_join = [cr_df, cr2_df, cf_df, bs_df, sard_bs_df, sard_is_df, sard_cf_df]
    joined = reduce(lambda acc, df: join_no_dup(acc, df, keys), to_join, is_df)

    # # 7. Chain left-joins
    # joined = (
    #     is_df
    #       .join(cr_df,      keys, how="left")
    #       .join(cr2_df,     keys, how="left")
    #       .join(cf_df,      keys, how="left")
    #       .join(bs_df,      keys, how="left")
    #       .join(sard_bs_df, keys, how="left")
    #       .join(sard_is_df, keys, how="left")
    #       .join(sard_cf_df, keys, how="left")
    # )

    # 8. Final date filter and select columns
    result = (
        joined
          .select("a.*", "b.*", "c.*", "d.*", "e.*", "f.*", "g.*", "h.*")
    )
    from collections import Counter
    cols = joined.columns
    counts = Counter(cols)

    # all column names that appear more than once
    dups = [name for name, cnt in counts.items() if cnt > 1]
    print("Duplicated columns:", dups)

    # if you also want to see how many times each is duplicated:
    for name in dups:
        print(f"{name} appears {counts[name]} times")

    # 9. Show or write out
    result.show(50, truncate=False)
    # e.g. result.write.parquet("/path/to/output")

    result \
      .coalesce(1) \
      .write \
      .option("header", True) \
      .mode("overwrite") \
      .csv("./data/comp500_result.csv")

    # 10. Stop Spark
    spark_handler.stop_session()

if __name__ == "__main__":
    main()



# if __name__ == "__main__":
#     # Load environment variables
#     load_dotenv()
#
#     # Initialize Spark
#     spark_handler = SparkHandler(app_name="test", master_url=SPARK_MASTER)
#     spark = spark_handler.start_session()
#
#     # Initialize MySQL handler
#     mysql_handler = MySQLHandler(
#         spark=spark,
#         host=MYSQL_HOST,
#         port=MYSQL_PORT,
#         database=MYSQL_DATABASE,
#         user=MYSQL_USER,
#         password=MYSQL_PASSWORD
#     )
#
#     ent_info = mysql_handler.read_table("ent_company_info")\
#                     .filter(F.col("BICS_LEVEL_1") == 10)\
#                     .select("ID_BB_COMPANY")\
#                     .distinct()
#
#     comp_list = [row.ID_BB_COMPANY for row in ent_info.collect()]
