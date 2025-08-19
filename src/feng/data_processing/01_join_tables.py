from statsmodels.graphics.tukeyplot import results

from src.feng.bigdata import SparkHandler
from src.feng.bigdata import MySQLHandler
from src.feng.bigdata.spark_utils import save_to_parquet
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
from pyspark.sql import SparkSession, DataFrame

from dotenv import load_dotenv
import os
from functools import reduce
from collections import Counter
from pathlib import Path

def get_comps_list(spark: SparkSession, IND_CODE: int):

    mysql = MySQLHandler(
        spark=spark,
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=MYSQL_DATABASE,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD
    )
    if IND_CODE != -99:
        comp_500 = (
            mysql
              .read_table("ent_company_info")
              .filter(F.col("BICS_LEVEL_1") == IND_CODE)
              .select("ID_BB_COMPANY")
              .distinct()
        )
    elif IND_CODE == -99:
        comp_500 = (
            mysql
              .read_table("ent_company_info")
              # .select("ID_BB_COMPANY")
              # .distinct()
        )
    comp_list = [r.ID_BB_COMPANY for r in comp_500.collect()]
    return comp_list, comp_500[['ID_BB_COMPANY', 'BICS_LEVEL_1']]


def load_parquet_file(
    spark: SparkSession,
    filepath: str,
    alias_name: str,
    comp_list: list,
    START_DATE: str,
    END_DATE: str,
):
    """
    Reads the given parquet file, filters by company & date,
    ranks revisions, keeps rank==1, and aliases the DataFrame.
    """
    return (
        spark.read.parquet(filepath)
             .where(F.col("ID_BB_COMPANY").isin(comp_list))
             .where(
                 (F.col("LATEST_PERIOD_END_DT_FULL_RECORD") > START_DATE) &
                 (F.col("LATEST_PERIOD_END_DT_FULL_RECORD") < END_DATE)
             )
             .alias(alias_name)
    )

def main(IND_CODE):

    START_DATE = os.getenv("START_DATE", "1995-01-01")
    END_DATE = os.getenv("END_DATE", "2025-06-30")

    spark_handler = SparkHandler(app_name="comp500_job", master_url=SPARK_MASTER)
    spark = spark_handler.start_session()

    # Load industry comps
    comp_list, industry_code_df = get_comps_list(spark, IND_CODE)

    # Load Data
    is_df = load_parquet_file(spark,"./data/fundamentals_is_20250523.parquet","is",comp_list,START_DATE,END_DATE)
    cr_df = load_parquet_file(spark,"./data/fundamentals_cr_20250521.parquet", "cr",comp_list,START_DATE,END_DATE)
    cr2_df = load_parquet_file(spark,"./data/fundamentals_cr2_20250522.parquet","cr2",comp_list,START_DATE,END_DATE)
    cf_df = load_parquet_file(spark,"./data/fundamentals_cf_20250521.parquet","cf",comp_list,START_DATE,END_DATE)
    bs_df = load_parquet_file(spark,"./data/fundamentals_bs_20250520.parquet","bs",comp_list,START_DATE,END_DATE)
    sard_bs_df = load_parquet_file(spark,"./data/fundamentals_sard_bs_20250524.parquet","sard_bs",comp_list,START_DATE,END_DATE)
    sard_is_df = load_parquet_file(spark,"./data/fundamentals_sard_is_20250524.parquet","sard_is",comp_list,START_DATE,END_DATE)
    sard_cf_df = load_parquet_file(spark,"./data/fundamentals_sard_cf_20250524.parquet","sard_cf",comp_list,START_DATE,END_DATE)

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
    results = is_df.join(cr_df, on=keys, how="left")
    results = results.join(cf_df, on=keys, how="left")
    results = results.join(bs_df, on=keys, how="left")
    results = results.join(sard_bs_df, on=keys, how="left")
    results = results.join(sard_is_df, on=keys, how="left")
    results = results.join(sard_cf_df, on=keys, how="left")
    results = results.join(cr2_df, on=keys, how="left")
    results = results.join(industry_code_df, on='ID_BB_COMPANY', how="left")
    # def join_no_dup(left, right, on, how="left"):
    #     # find columns in common (minus the join keys)
    #     overlap = set(left.columns).intersection(right.columns) - set(on)
    #     # drop those from the right-hand side
    #     right_clean = right.drop(*overlap)
    #     return left.join(right_clean, on=on, how=how)
    #
    # to_join = [cr_df, cr2_df, cf_df, bs_df, sard_bs_df, sard_is_df, sard_cf_df]
    # joined = reduce(lambda acc, df: join_no_dup(acc, df, keys), to_join, is_df)
    #
    # result = (
    #     joined
    #       .select("is.*", "cr.*", "cr2.*", "cf.*", "bs.*", "sard_bs.*", "sard_is.*", "sard_cf.*")
    # )
    # cols = joined.columns
    # counts = Counter(cols)
    # 9. Show or write out
    print(len(results.columns))
    results.show(50, truncate=False)
    df = results.select('BICS_LEVEL_1', 'LATEST_PERIOD_END_DT_FULL_RECORD', 'ID_BB_COMPANY', 'ID_BB_UNIQUE',
       'FILING_STATUS', 'ACCOUNTING_STANDARD', 'EQY_CONSOLIDATED',
       'EQY_FUND_CRNCY', 'FISCAL_YEAR_PERIOD', 'is.SECURITY_DESCRIPTION', 'bs.RCODE',
       'bs.NFIELDS', 'bs.TICKER', 'bs.EXCH_CODE', 'bs.TICKER_AND_EXCH_CODE',
       'bs.ID_BB_SECURITY', 'bs.EQY_FUND_IND', 'bs.ANNOUNCEMENT_DT',
       'bs.FUNDAMENTAL_ENTRY_DT', 'bs.FUNDAMENTAL_UPDATE_DT', 'ARD_TOTAL_REVENUES',
       'CF_CASH_FROM_OPER', 'EBITDA', 'ARD_CAPITAL_EXPENDITURES', 'IS_EPS')
    # df.write.partitionBy('BICS_LEVEL_1').format('parquet').save('./data/industry_code')
    df = df.toPandas()
    file_name = JOINED_TABLE_DIR / f'joined_ind_{IND_CODE}_selected.parquet'
    df.to_parquet(file_name, index=False)

    # save_to_parquet(result, f'./data/joined_ind_{IND_CODE}.parquet')
    # print(is_df.count(), cr_df.count(), cr2_df.count(), cf_df.count(), bs_df.count(), sard_bs_df.count(), sard_is_df.count(), sard_cf_df.count())
if __name__ == "__main__":
    P = Path(__file__).resolve()
    PROJECT_DIR = P.parents[3]
    JOINED_TABLE_DIR = PROJECT_DIR / 'data' / 'joined_tables'
    JOINED_TABLE_DIR.mkdir(parents=True, exist_ok=True)
    ind_code = -99
    main(ind_code)
    # file_name = JOINED_TABLE_DIR / f'joined_ind_{ind_code}_selected.csv'
    # import pandas as pd
    # df = pd.read_csv(file_name)
    # df