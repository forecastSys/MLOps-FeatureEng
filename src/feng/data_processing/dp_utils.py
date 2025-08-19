import os

import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.arima.model import ARIMA
import itertools
from dotenv import load_dotenv
load_dotenv()

################## Primary data preprocessing ################

def primary_data_prep(df):
    # Converting the 'FUNDAMENTAL_UPDATE_DT' column to a datetime format (assuming dates are in YYYYMMDD format)
    df['FUNDAMENTAL_UPDATE_DT'] = pd.to_datetime(df['FUNDAMENTAL_UPDATE_DT'], format='%Y%m%d')

    # Filtering the DataFrame to include only rows with "Annual" fiscal year periods.
    # This is determined by checking if the 'FISCAL_YEAR_PERIOD' column ends with " A".
    df_annual = df[df["FISCAL_YEAR_PERIOD"].str.endswith(" A")]

    # Extracting the year from the 'FISCAL_YEAR_PERIOD' column (e.g., "2023 A" -> 2023)
    # Adding the extracted year as a new column called 'Year'
    df_annual['Year'] = df_annual['FISCAL_YEAR_PERIOD'].str.extract(r'(\d{4})').astype(int)

    # Filtering the DataFrame to include only rows where the year is 2000 or later
    # df_annual_after_2000 = df_annual[df_annual['Year'] >= 2000]

    # Sorting the filtered DataFrame by 'ID_BB_UNIQUE' (company identifier) and 'Year' in ascending order
    # Resetting the index of the sorted DataFrame
    df_annual_sorted_after_2000 = df_annual.sort_values(by=['ID_BB_UNIQUE', 'Year']).reset_index(drop=True)

    return df_annual_sorted_after_2000


################## Data Imputation ##################
## Marking columns with high rate of missing values ##
## Impute missing data for the rest ##

def mark_high_missing_columns(df, threshold=0.8):
    """
    Replaces columns where more than 'threshold' fraction of values are missing
    with np.nan.

    Parameters:
        df (pd.DataFrame): The input DataFrame.
        threshold (float): The fraction of allowed missing values. Columns with
                           a higher fraction of missing values will be replaced
                           with np.nan.

    Returns:
        pd.DataFrame: The DataFrame with specified columns replaced by np.nan.
    """
    # Calculate the fraction of missing values in each column
    missing_fraction = df.isna().mean()
    print("Missing Fraction: ", missing_fraction)

    # Identify columns where the missing fraction exceeds the threshold
    cols_to_mark = missing_fraction[missing_fraction > threshold].index

    # Replace entire columns with np.nan
    df[cols_to_mark] = 0

    return df


def handle_missing_start(series):
    """
    Handles missing values at the start of the series using backfill.
    """
    first_valid_idx = series.first_valid_index()
    if first_valid_idx is not None and first_valid_idx != series.index[0]:
        # Get the positional index of the first valid index
        first_valid_pos = series.index.get_loc(first_valid_idx)
        # Backfill missing values at the start
        series.iloc[:first_valid_pos] = series.iloc[first_valid_pos]
    return series


def handle_missing_end(series):
    """
    Handles missing values at the end of the series using linear regression
    based on the last continuous block of valid data, with X_train starting from 0.
    """
    last_valid_idx = series.last_valid_index()
    if last_valid_idx is not None and last_valid_idx != series.index[-1]:
        # Get the positional index of the last valid index
        last_valid_pos = series.index.get_loc(last_valid_idx)

        # Find the start of the last continuous block of valid data
        start_pos = last_valid_pos
        while start_pos > 0 and not pd.isna(series.iloc[start_pos - 1]):
            start_pos -= 1

        # Extract the last continuous block of valid data
        y_train = series.iloc[start_pos:last_valid_pos + 1].values

        # Adjust X_train to start from 0
        X_train = np.arange(0, len(y_train)).reshape(-1, 1)

        # Check if we have enough data to train
        if len(y_train) > 1:
            # Build linear regression model
            model = LinearRegression()
            model.fit(X_train, y_train)

            # Number of missing values at the end
            num_missing = len(series) - (last_valid_pos + 1)

            # Prepare X_pred starting from where X_train left off
            X_pred = np.arange(len(y_train), len(y_train) + num_missing).reshape(-1, 1)

            # Predict missing values at the end
            y_pred = model.predict(X_pred)

            # Fill missing values
            series.iloc[last_valid_pos + 1:] = y_pred
        else:
            # Not enough data to train a model; use the last valid value
            series.iloc[last_valid_pos + 1:] = series.iloc[last_valid_pos]
    return series


def handle_missing_between(series):
    """
    Handles consecutive missing values between two data points using interpolation.
    """
    series.interpolate(method='linear', inplace=True)
    return series


def impute_series(series):
    """
    Imputes missing values in a pandas Series according to the specified rules.
    """
    series = handle_missing_start(series)
    series = handle_missing_end(series)
    series = handle_missing_between(series)
    return series


def impute_missing_values(df, cols_to_exclude):
    """
    Applies the imputation process to each column in the DataFrame.
    """
    for col in df.columns:
        if col not in cols_to_exclude:
            df[col] = impute_series(df[col])
    return df


##################### Data Preprocessing #####################
## Choosing more relevant/recent data for each TICKER, Fiscal year ##

def custom_agg(x, update_dt_col, eqy_consolidated_col, filing_status_col, accounting_standard_col):
    """
    Aggregates values in a column by applying prioritization rules to resolve cases with multiple unique values.

    Parameters:
        x (Pandas Series): The main column for aggregation, containing raw data.
        update_dt_col (Pandas Series): A column with update timestamps to prioritize the latest rows.
        eqy_consolidated_col (Pandas Series): A column indicating if a record is consolidated ('Y' or 'N').
        filing_status_col (Pandas Series): A column with filing status values prioritized as follows:
            - 'MR' (Most Recent): Priority 1
            - 'OR': Priority 2
            - 'PR': Priority 3
            - 'RS': Priority 4
            - 'ER': Priority 5
        accounting_standard_col (Pandas Series): A column indicating the accounting standard prioritized as follows:
            - 'IAS/IFRS': Priority 1
            - 'US GAAP': Priority 2

    Returns:
        Single value (any type): The resolved value from column `x`.
        np.nan: If no resolution can be achieved or if `x` is empty after dropping NaN values.
    """

    # Define the priority mapping for 'FILING_STATUS'
    filing_status_priority = {
        'MR': 1,
        'OR': 2,
        'PR': 3,
        'RS': 4,
        'ER': 5
    }

    # Define the priority mapping for 'ACCOUNTING_STANDARD'
    accounting_standard_priority = {
        'IAS/IFRS': 1,
        'US GAAP': 2
    }

    x = x.dropna()
    if x.empty:
        return np.nan

    unique_values = x.unique()

    if len(unique_values) == 1:
        return unique_values[0]

    # Rule-Based Selection of Value: When there are more than one unique_values we apply the rules to filter out the correct value.
    # Step 1: Try to resolve by latest FUNDAMENTAL_UPDATE_DT
    dt_values = update_dt_col.loc[x.index]
    max_date = dt_values.max()
    latest_mask = dt_values == max_date
    latest_indices = x.index[latest_mask]
    latest_values = x.loc[latest_indices]
    unique_latest_values = latest_values.unique()

    if len(unique_latest_values) == 1:
        return unique_latest_values[0]

    # Step 2: If undecided, prefer EQY_CONSOLIDATED == 'Y'
    eqy_values = eqy_consolidated_col.loc[latest_indices]
    y_mask = eqy_values == 'Y'
    if y_mask.any():
        y_indices = latest_indices[y_mask]
        y_values = x.loc[y_indices]
        unique_y_values = y_values.unique()
        if len(unique_y_values) == 1:
            return unique_y_values[0]
        else:
            # Step 3: If still undecided, use FILING_STATUS priority
            filing_status_values = filing_status_col.loc[y_indices]
            priorities = filing_status_values.map(filing_status_priority)
            min_priority = priorities.min()
            priority_mask = priorities == min_priority
            priority_indices = y_indices[priority_mask]
            priority_values = x.loc[priority_indices]
            unique_priority_values = priority_values.unique()
            if len(unique_priority_values) == 1:
                return unique_priority_values[0]
            else:
                # Step 4: If still undecided, use ACCOUNTING_STANDARD priority
                accounting_standard_values = accounting_standard_col.loc[priority_indices]
                acc_priorities = accounting_standard_values.map(accounting_standard_priority)
                min_acc_priority = acc_priorities.min()
                acc_priority_mask = acc_priorities == min_acc_priority
                acc_priority_indices = priority_indices[acc_priority_mask]
                acc_priority_values = x.loc[acc_priority_indices]
                unique_acc_priority_values = acc_priority_values.unique()
                if len(unique_acc_priority_values) == 1:
                    return unique_acc_priority_values[0]
                else:
                    return np.nan
    else:
        # If no EQY_CONSOLIDATED == 'Y', proceed to FILING_STATUS
        filing_status_values = filing_status_col.loc[latest_indices]
        priorities = filing_status_values.map(filing_status_priority)
        min_priority = priorities.min()
        priority_mask = priorities == min_priority
        priority_indices = latest_indices[priority_mask]
        priority_values = x.loc[priority_indices]
        unique_priority_values = priority_values.unique()
        if len(unique_priority_values) == 1:
            return unique_priority_values[0]
        else:
            # Step 4: If still undecided, use ACCOUNTING_STANDARD priority
            accounting_standard_values = accounting_standard_col.loc[priority_indices]
            acc_priorities = accounting_standard_values.map(accounting_standard_priority)
            min_acc_priority = acc_priorities.min()
            acc_priority_mask = acc_priorities == min_acc_priority
            acc_priority_indices = priority_indices[acc_priority_mask]
            acc_priority_values = x.loc[acc_priority_indices]
            unique_acc_priority_values = acc_priority_values.unique()
            if len(unique_acc_priority_values) == 1:
                return unique_acc_priority_values[0]
            else:
                return np.nan


def choose_data_for_comp(df_merged):
    result = []
    cols_not_processing = ['TICKER', 'EXCH_CODE',
                           'TICKER_AND_EXCH_CODE', 'ID_BB_COMPANY', 'ID_BB_SECURITY',
                           'ID_BB_UNIQUE', 'EQY_FUND_IND', 'FISCAL_YEAR_PERIOD',
                           # 'LATEST_PERIOD_END_DT_FULL_RECORD',
                           'FUNDAMENTAL_ENTRY_DT', 'FUNDAMENTAL_UPDATE_DT', 'EQY_CONSOLIDATED',
                           'FILING_STATUS', 'ACCOUNTING_STANDARD', 'Year']
    print(cols_not_processing)

    # Group the merged DataFrame by 'TICKER' and process each group
    for ticker, sub_df in df_merged.groupby('ID_BB_UNIQUE'):
        # Print the current TICKER for tracking/debugging purposes
        print(f"ID_BB_UNIQUE: {ticker}")

        # Process each group of rows for the current TICKER, grouped further by 'Year'
        df_result = sub_df.groupby('Year').apply(
            lambda year: pd.Series(
                {
                    'Year': year.name,  # Extract the 'Year' as the index name
                    # Apply the custom aggregation function to each column in cols_to_process
                    **{
                        col: custom_agg(
                            year[col],  # Column data for processing
                            year['FUNDAMENTAL_UPDATE_DT'],  # Update date information
                            year['EQY_CONSOLIDATED'],  # Consolidation status
                            year['FILING_STATUS'],  # Filing status
                            year['ACCOUNTING_STANDARD']  # Accounting standard
                        )
                        for col in df_merged.columns if col not in cols_not_processing
                    }
                }
            )
        ).reset_index(drop=True)  # Flatten the result by resetting the index

        # Insert the 'TICKER' column to associate the processed data with the corresponding ticker
        df_result.insert(1, 'ID_BB_UNIQUE',
                         list(sub_df['ID_BB_UNIQUE'])[0])  # Use the first TICKER value in the sub-group

        # Append the processed DataFrame for this TICKER to the result list
        result.append(df_result)

    df_combined = pd.concat(result, ignore_index=True)

    return df_combined

def primary_quarterly_data_prep(df):
    # Converting the 'FUNDAMENTAL_UPDATE_DT' column to a datetime format (assuming dates are in YYYYMMDD format)
    df['LATEST_PERIOD_END_DT_FULL_RECORD'] = pd.to_datetime(df['LATEST_PERIOD_END_DT_FULL_RECORD'], format='%Y%m%d')
    df['ANNOUNCEMENT_DT'] = pd.to_datetime(df['ANNOUNCEMENT_DT'], format='%Y%m%d')

    # Filtering the DataFrame to include only quarter data.
    # This is determined by checking if the 'FISCAL_YEAR_PERIOD' column ends with " Q1" or other quarters.
    df_quarter = df[df['FISCAL_YEAR_PERIOD'].str.contains(r' Q', regex=True)]

    # Extracting the year from the 'FISCAL_YEAR_PERIOD' column (e.g., "2023 A" -> 2023)
    # Adding the extracted year as a new column called 'Year'
    df_quarter['Year'] = df_quarter['FISCAL_YEAR_PERIOD'].str.extract(r'(\d{4})').astype(int)
    df_quarter['Quarter'] = df_quarter['FISCAL_YEAR_PERIOD'].str.extract(r'Q(\d)').astype(int)

    # Filtering the DataFrame to include only rows where the year is 2000 or later
    # df_annual_after_2000 = df_annual[df_annual['Year'] >= 2000]

    # Sorting the filtered DataFrame by 'ID_BB_UNIQUE' (company identifier) and 'Year' in ascending order
    # Resetting the index of the sorted DataFrame
    df_quarter_sorted = df_quarter.sort_values(by=['ID_BB_UNIQUE', 'Year', 'Quarter'], ascending=True).reset_index(drop=True)

    return df_quarter_sorted


def choose_quarter_data_for_comp(df_merged):
    result = []
    cols_not_processing = ['TICKER', 'EXCH_CODE',
                           'TICKER_AND_EXCH_CODE', 'ID_BB_COMPANY', 'ID_BB_SECURITY',
                           'ID_BB_UNIQUE', 'EQY_FUND_IND', 'FISCAL_YEAR_PERIOD',
                           'ANNOUCEMENT_DT'
                           'FUNDAMENTAL_ENTRY_DT', 'FUNDAMENTAL_UPDATE_DT', 'EQY_CONSOLIDATED',
                           'FILING_STATUS', 'ACCOUNTING_STANDARD', 'Year']
    print(cols_not_processing)

    # Group the merged DataFrame by 'TICKER' and process each group
    for ticker, sub_df in df_merged.groupby('ID_BB_UNIQUE'):
        # Print the current TICKER for tracking/debugging purposes
        print(f"ID_BB_UNIQUE: {ticker}")

        # Process each group of rows for the current TICKER, grouped further by 'Year'
        df_result = sub_df.groupby(['Year','Quarter']).apply(
            lambda year_quarter: pd.Series(
                {
                    'Year': year_quarter.name[0],
                    'Quarter': year_quarter.name[1],# Extract the 'Year' as the index name
                    # Apply the custom aggregation function to each column in cols_to_process
                    **{
                        col: custom_agg(
                            year_quarter[col],  # Column data for processing
                            year_quarter['FUNDAMENTAL_UPDATE_DT'],  # Update date information
                            year_quarter['EQY_CONSOLIDATED'],  # Consolidation status
                            year_quarter['FILING_STATUS'],  # Filing status
                            year_quarter['ACCOUNTING_STANDARD']  # Accounting standard
                        )
                        for col in df_merged.columns if col not in cols_not_processing
                    }
                }
            )
        ).reset_index(drop=True)  # Flatten the result by resetting the index

        # Insert the 'TICKER' column to associate the processed data with the corresponding ticker
        df_result.insert(1, 'ID_BB_UNIQUE',
                         list(sub_df['ID_BB_UNIQUE'])[0])  # Use the first TICKER value in the sub-group

        # Append the processed DataFrame for this TICKER to the result list
        result.append(df_result)

    df_combined = pd.concat(result, ignore_index=True)

    return df_combined


##################### Add Company and Industry Info #####################

def read_industry_code_mapping():
    # Reading the Excel file containing industry code mappings into a DataFrame
    # Assuming the sheet 'industry code mapping' contains the relevant mappings
    df_industry_code_mapping = pd.read_excel(os.environ['COMPANY_INFO_PATH'], sheet_name='industry code mapping')

    # Creating dictionaries to map numeric codes to descriptive industry labels for various industry levels
    industry_sector_mapping = dict(
        zip(df_industry_code_mapping['Industry_sector_num'], df_industry_code_mapping['Industry_sector']))
    industry_group_mapping = dict(
        zip(df_industry_code_mapping['Industry_group_num'], df_industry_code_mapping['Industry_group']))
    industry_subgroup_mapping = dict(
        zip(df_industry_code_mapping['Industry_subgroup_num'], df_industry_code_mapping['Industry_subgroup']))
    industry_level4_mapping = dict(
        zip(df_industry_code_mapping['Industry_level_4_num'], df_industry_code_mapping['Industry_level_4']))
    industry_level5_mapping = dict(
        zip(df_industry_code_mapping['Industry_level_5_num'], df_industry_code_mapping['Industry_level_5']))
    industry_level6_mapping = dict(
        zip(df_industry_code_mapping['Industry_level_6_num'], df_industry_code_mapping['Industry_level_6']))
    industry_level7_mapping = dict(
        zip(df_industry_code_mapping['Industry_level_7_num'], df_industry_code_mapping['Industry_level_7']))

    # Storing all the mapping dictionaries in a list for easier handling or iteration
    industry_mappings = [
        industry_sector_mapping,
        industry_group_mapping,
        industry_subgroup_mapping,
        industry_level4_mapping,
        industry_level5_mapping,
        industry_level6_mapping
    ]

    return industry_mappings


def read_company_info():
    # Reading the 'company info' sheet from the Excel file into a DataFrame
    df_company_info = pd.read_csv('/data/zhuanghao/MyGithub/AutoML-ForecastingPipeline/data/data/data/input/company_info.csv')

    # Extracting the ticker symbol from the 'Ticker' column
    # Assuming the ticker symbol is followed by " US" (e.g., "AAPL US"), and we need only the symbol part
    # df_company_info['TICKER'] = df_company_info['Ticker'].str.extract(r'(.*) US')

    # Specifying the columns related to industry classifications for further processing
    industry_cols = ['INDUSTRY_SECTOR_NUM', 'INDUSTRY_GROUP_NUM', 'INDUSTRY_SUBGROUP_NUM', 'Industry_level_4_num',
                     'Industry_level_5_num', 'Industry_level_6_num']

    return df_company_info, industry_cols


def merge_with_company_info(bbg_df, industry_cols, industry_mappings, df_company_info):
    industry_cols_to_merge = []  # List to store names of the new mapped columns

    # Iterate over each column and its corresponding mapping
    for col, mapping in zip(industry_cols, industry_mappings):
        new_col_name = col + '_mapped'  # Generate new column name
        industry_cols_to_merge.append(new_col_name)  # Add new column name to the list

        # Create the new column by mapping values using the provided dictionary
        df_company_info[new_col_name] = df_company_info[col].map(mapping)

    # Combine the industry mapped columns and the 'TICKER' column into a list
    columns_to_select = industry_cols_to_merge + ['ID_BB_UNIQUE']

    # Perform a left join to merge the two DataFrames on the 'TICKER' column
    df_merged = pd.merge(
        bbg_df,
        df_company_info[columns_to_select],
        on='ID_BB_UNIQUE',
        how='left'
    )

    # Display the merged DataFrame
    return df_merged

def impute_for_column(column):
    for i in range(len(column)):
        if pd.isna(column[i]):
            previous_data = column[:i]

            # if len(previous_data) < 2:
            #     continue
            model = ARIMA(previous_data, order=(1, 0, 0))
            fitted_model = model.fit()

            predicted_value = fitted_model.forecast(steps=1).iloc[0]
            column[i] = predicted_value
    return column


def impute_for_test_set(df, cols_not_impute):
    ## Inplace imputation for every column in test dataset, using AR(1)
    for col in df.columns:
        if col not in cols_not_impute:
            print("Imputing test set data for column {}".format(col))
            df[col] = impute_for_column(df[col])
    return df



########################### Feature Engineering ###########################
def create_rolling_avg(X_df, feature_columns, window_size = 3):
    final_df_list = []
    for comp, sub_df in X_df.groupby('ID_BB_UNIQUE'):
        for col in feature_columns:
            sub_df[col + '(rolling_avg_' + str(window_size) + ')'] = sub_df[col].rolling(window=window_size, min_periods=1).mean()
        final_df_list.append(sub_df)
    final_df = pd.concat(final_df_list, ignore_index=True)
    return final_df

def create_ema(X_df, feature_columns, span = 3):
    final_df_list = []
    for comp, sub_df in X_df.groupby('ID_BB_UNIQUE'):
        for col in feature_columns:
            sub_df[col + '(ema_' + str(span) + ')'] = sub_df[col].ewm(span=span, adjust=False).mean()
        final_df_list.append(sub_df)
    final_df = pd.concat(final_df_list, ignore_index=True)
    return final_df

def create_first_order_diff(X_df, feature_columns):
    final_df_list = []
    for comp, sub_df in X_df.groupby('ID_BB_UNIQUE'):
        for col in feature_columns:
            sub_df[col + '(q2q_diff)'] = sub_df[col].diff()
        final_df_list.append(sub_df)
    final_df = pd.concat(final_df_list, ignore_index=True)
    return final_df

def create_growth_rate(X_df, feature_columns):
    final_df_list = []
    for comp, sub_df in X_df.groupby('ID_BB_UNIQUE'):
        for col in feature_columns:
            sub_df[col + '(q2q_pct_change)'] = sub_df[col].pct_change() * 100
            sub_df[col + '(q2q_cumulative_growth)'] = (1 + sub_df[col + '(q2q_pct_change)'] / 100).cumprod()
        final_df_list.append(sub_df)
    final_df = pd.concat(final_df_list, ignore_index=True)
    return final_df

def create_interactive_features(X_df, feature_columns):
    # for col1 in feature_columns:
    #     for col2 in feature_columns:
    #         if col1 != col2:
    #             X_df[col1 + '/' + col2] = X_df[col1] / X_df[col2]
    #             X_df[col1 + '*' + col2] = X_df[col1] * X_df[col2]
    #             X_df[col1 + '/' + col2] = X_df[col1 + '/' + col2].replace([np.inf, -np.inf], np.nan)

    for col1, col2 in itertools.combinations(feature_columns, 2):
        # Create new features: col1/col2 and col1*col2 using vectorized operations
        X_df[col1 + '/' + col2] = np.divide(X_df[col1], X_df[col2], where=X_df[col2] != 0)
        X_df[col1 + '*' + col2] = X_df[col1] * X_df[col2]

        # Handle division by zero (inf values)
        X_df[col1 + '/' + col2] = X_df[col1 + '/' + col2].replace([np.inf, -np.inf], np.nan)

    return X_df

def create_log(X_df, feature_columns):
    for col in feature_columns:
        X_df[col + '(log)'] = np.log(X_df[col])
    return X_df



################# Industry Statistics ###################
def create_industry_stats_for_year(X_df, feature_columns, ind_col, year):
    print("Current year: ", year)
    print("Processing industry level: ", ind_col)
    X_df = X_df[X_df['Year'] == year]
    df_list = []
    for ind, sub_df in X_df.groupby(ind_col, dropna=False):
        print("Processing group: ", ind)
        print("Length of data: ", len(sub_df))
        for col in feature_columns:
            if col in sub_df.columns:
                if pd.isna(ind):  # If the group corresponds to NaN in ind_col
                    sub_df[col + '(' + str(ind_col) + '_mean)'] = np.nan
                    sub_df[col + '(' + str(ind_col) + '_median)'] = np.nan
                    sub_df[col + '(' + str(ind_col) + '_min)'] = np.nan
                    sub_df[col + '(' + str(ind_col) + '_max)'] = np.nan
                    sub_df[col + '(' + str(ind_col) + '_std)'] = np.nan
                    sub_df[col + '(' + str(ind_col) + '_q1)'] = np.nan
                    sub_df[col + '(' + str(ind_col) + '_q3)'] = np.nan
                else:  # Compute actual statistics for valid groups
                    sub_df[col + '(' + str(ind_col) + '_mean)'] = sub_df[col].mean()
                    sub_df[col + '(' + str(ind_col) + '_median)'] = sub_df[col].median()
                    sub_df[col + '(' + str(ind_col) + '_min)'] = sub_df[col].min()
                    sub_df[col + '(' + str(ind_col) + '_max)'] = sub_df[col].max()
                    sub_df[col + '(' + str(ind_col) + '_std)'] = sub_df[col].std()
                    sub_df[col + '(' + str(ind_col) + '_q1)'] = sub_df[col].quantile(0.25)
                    sub_df[col + '(' + str(ind_col) + '_q3)'] = sub_df[col].quantile(0.75)

        df_list.append(sub_df)

    final_df = pd.concat(df_list, ignore_index=True)
    return final_df

