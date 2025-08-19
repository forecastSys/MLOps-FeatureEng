import numpy as np
from src.feng.data_processing.dp_utils import *
import pandas as pd
from dotenv import load_dotenv
load_dotenv()
import warnings
warnings.filterwarnings('ignore')

raw_data_path = os.environ['BBG_RAW_DATA_PATH']
final_data_path = os.environ['BBG_PROCESSED_DATA_PATH']

def read_process_raw_data():

    ## Read raw data
    raw_df = pd.read_csv(raw_data_path)
    raw_df = raw_df[raw_df.columns[~raw_df.columns.str.contains(r'\.\d+$')]]

    quarter_al_df = primary_quarterly_data_prep(raw_df)

    ## Adding industry information
    industry_mappings = read_industry_code_mapping()
    df_company_info, industry_cols = read_company_info()
    df1 = merge_with_company_info(quarter_al_df, industry_cols, industry_mappings, df_company_info)


    ## Union process with custom aggregate
    df2 = choose_quarter_data_for_comp(df1)
    return df2

def create_lead_features(df2: pd.DataFrame):

    create_lag_list = []

    # y_cols = ['SALES_REV_TURN','CF_CASH_FROM_OPER','EBITDA']
    y_cols = ['SALES_REV_TURN', 'CF_CASH_FROM_OPER', 'EBITDA', 'ARD_CAPITAL_EXPENDITURES']
    lead_y = []
    for y in y_cols:
        lead_y.append(y + '_lead1')

    for id, sub_df in df2.groupby('ID_BB_UNIQUE'):
        sub_df = sub_df.sort_values(['Year', 'Quarter'], ascending=True)
        for y in y_cols:
            sub_df[y + '_lead1'] = sub_df[y].shift(-1)
            # lead_y.append(y + '_lead1')
            sub_df[y + '_last_year_same_quarter'] = sub_df[y].shift(4)
        sub_df['ANNOUNCEMENT_DT_lead1'] = sub_df['ANNOUNCEMENT_DT'].shift(-1)
        sub_df['LATEST_PERIOD_END_DT_FULL_RECORD_lead1'] = sub_df['LATEST_PERIOD_END_DT_FULL_RECORD'].shift(-1)
        sub_df['Year_lead1'] = sub_df['Year'].shift(-1)
        sub_df['Quarter_lead1'] = sub_df['Quarter'].shift(-1)
        sub_df = sub_df.drop(sub_df.index[-1])
        create_lag_list.append(sub_df)

    df3 = pd.concat(create_lag_list, ignore_index=True)
    df3.replace(0, np.nan, inplace=True)
    df3.replace("NULL", np.nan, inplace=True)

    # original data was from 1991 - 2024, keep all data available, for example lag4 features
    df4 = df3[df3['Year'] >= 1990]

    outlier = df4['ARD_CAPITAL_EXPENDITURES'].max()
    print(outlier)
    df4 = df4[df4['ARD_CAPITAL_EXPENDITURES'] != outlier]
    df4 = df4[df4['ARD_CAPITAL_EXPENDITURES_lead1'] != outlier]

    df4['ANNOUNCEMENT_DT_lead1'] = pd.to_datetime(df4['ANNOUNCEMENT_DT_lead1'], format='%Y-%m-%d')
    df4.sort_values(by=['ANNOUNCEMENT_DT_lead1'], inplace=True)
    return df4

def data_cleaning_main():

    df = read_process_raw_data()
    df = create_lead_features(df)
    df.to_csv(final_data_path, index=False)

if __name__ == '__main__':
    data_cleaning_main()