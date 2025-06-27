import numpy as np
import pandas as pd
from pandas import DataFrame, ExcelFile

mf_component_section_mapping = {
    "EQUITY & EQUITY RELATED": {
        "sublist": {
            "a) Listed/awaiting listing on Stock Exchanges": "listed_on_exchange",
            "b) Unlisted": "unlisted_on_exchange",
            "c) Foreign Securities and /or overseas ETF": "listed_overseas"
        }
    },
    "DEBT INSTRUMENTS": {
        "sublist": {
            "a) Listed/awaiting listing on the stock exchanges": "listed_on_exchange",
            "b) Privately Placed/Unlisted": "unlisted_on_exchange",
            "c) Securitised Debt Instruments": "sdi_securities",
            "d) Central Government Securities": "central_govt_securities",
            "e) State Government Securities": "state_govt_securities"
        }
    },
    "MONEY MARKET INSTRUMENTS": {
        "sublist": {
            "a) Commercial Paper": "commercial_paper",
            "b) Certificate of Deposits": "certificate_of_deposits",
            "c) Treasury Bills": "treasury_bills",
            "d) Bills Re- Discounting": "bills_re_discounting",
            "e) STRIPS": "strips"
        }
    },
    "OTHERS": {
        "sublist": {
            "a) Mutual Fund Units / Exchange Traded Funds": "mf_or_exchange_traded_funds",
            "b) Alternative Investment Funds": "alt_investment_finds",
            "c) Gold": "gold",
            "d) Short Term Deposits": "short_term_deposits",
            "e) Term Deposits Placed as Margins": "term_deposits_placed_as_Margin",
            "f) TREPS / Reverse Repo Investments": "treps"
        }
    },
    "Other Current Assets / (Liabilities)": {
        "sublist": {}
    },
    "DERIVATIVES": {
        "sublist": {
            "Stock Futures": "stock_futures",
            "INTEREST RATE SWAPS": "interest_rate_swaps",
            "Index Futures": "index_futures"
        }
    }
}


def col_rename(df: DataFrame):
    df.columns = df.columns.str.replace(r'Name of.*', 'inst_name', regex=True)
    df.columns = df.columns.str.replace(r'Rating.*', 'sector', regex=True)
    df.columns = df.columns.str.replace(r'Market[\s\S]*', 'market_value_lakhs', regex=True)
    df.columns = df.columns.str.replace(r'YTM.*', 'ytm_percent', regex=True)
    df.columns = df.columns.str.replace(r'YTC.*', 'ytc_percent', regex=True)
    df.columns = df.columns.str.replace(r'Notes.*', 'notes_symbols', regex=True)
    df = df.rename(columns={
        "ISIN": "isin",
        "Quantity": "quantity",
        "% to AUM": "aum_percent",
    })

    return df


def generate_holdings_map_ds(df: DataFrame):
    ommit_rows = ['', 'Notes & Symbols :-', 'GRAND TOTAL (AUM)', 'Name of the Instrument', 'Derivatives Total', 'Total']
    section_dataframes = {}

    current_section_type = None
    current_sub_section_type = None

    for idx, row in df.iterrows():

        holdings = None

        if row.iloc[0] == 'Notes & Symbols :-':
            break
        else:
            if row.iloc[0] in mf_component_section_mapping:
                if current_section_type != row.iloc[0]:
                    current_section_type = row.iloc[0]
                    current_sub_section_type = ''
            elif row.iloc[0] in mf_component_section_mapping[current_section_type]['sublist']:
                # current_sub_section_type = row[0]
                current_sub_section_type = mf_component_section_mapping[current_section_type]['sublist'][row.iloc[0]]
            elif row.iloc[0] not in ommit_rows:
                holdings = {
                    'inst_name': row['inst_name'],
                    'isin': row['isin'],
                    'sector': row['sector'],
                    'quantity': row['quantity'],
                    # 'quantity':row['quantity'].str.extract(r'\((\d+)\)')
                    'market_value_lakhs': row['market_value_lakhs'] if row['market_value_lakhs'] != 'NIL' else 0.0,
                    'aum_percent': row['aum_percent'],
                    # 'esg_score':row['esg_score'] if ,
                    'esg_score': row.get('esg_score', '') or '',
                    'notes_symbols': row['notes_symbols'],
                    'ytm_percent': row['ytm_percent'],
                    'ytc_percent': row['ytc_percent']
                }
                section_dataframes.setdefault(current_section_type, {}).setdefault(current_sub_section_type, []).append(
                    holdings)

    return section_dataframes


def convert_ds_to_df(ds_map):
    flat_rows = []

    for section_type, subsections in ds_map.items():
        for sub_section_type, holdings_list in subsections.items():
            for holding in holdings_list:
                flat_rows.append({
                    'section_type': section_type,
                    'sub_section_type': sub_section_type,
                    **holding  # expands the holding dict into flat columns
                })

    return pd.DataFrame(flat_rows)


def post_process(df: DataFrame, mf_fund_id: int, mf_fund_code: str, disclose_month: int, disclose_year: int):
    # rename the section types
    df['section_type'] = df['section_type'].replace(
        {
            'EQUITY & EQUITY RELATED': 'equities',
            'OTHERS': 'other',
            'Other Current Assets / (Liabilities)': 'assets_or_liabilities',
            'DERIVATIVES': 'derivatives',
            'MONEY MARKET INSTRUMENTS': 'money_market_inst'
        }
    )

    df['mf_fund_id'] = mf_fund_id
    df['mf_fund_code'] = mf_fund_code
    df['fund_holder'] = 'SBI'

    # remove all NaN

    df = df.fillna(0.0)

    # marking as asset or liability
    df['type'] = np.where(
        (df['market_value_lakhs'] < 0),
        'liability',  # value if condition is True
        'asset'  # value if condition is False
    )

    # populate disclosure month and year
    df['disclose_month'] = disclose_month
    df['disclose_year'] = disclose_year

    return df


def generate_mf_ds(mf_code_id: int, mf_code_str: str, xls_file_fp: ExcelFile, disclosure_month: int,
                   disclosure_year: int):
    # read the sheet
    mf_df = xls_file_fp.parse(mf_code_str, header=5)

    mf_df.dropna(how='all',
                 inplace=True)  # drop completly blank rows , make changes directly to the df -- inplace = true
    mf_df = mf_df.loc[:, ~mf_df.columns.str.contains('BRSR')]  # not interested in any BRSR col

    # rename cols
    mf_df = col_rename(df=mf_df)

    interested_cols = ['inst_name', 'isin', 'sector', 'quantity', 'market_value_lakhs', 'aum_percent', 'ytm_percent',
                       'ytc_percent', 'notes_symbols']

    # pick interested cols
    df_int = mf_df[interested_cols]

    # generate intermediate ds
    holdings_ds = generate_holdings_map_ds(df_int)

    # convert to dataframe
    holdings_df = convert_ds_to_df(holdings_ds)

    # post process
    holdings_df = post_process(df=holdings_df, mf_fund_id=mf_code_id, mf_fund_code=mf_code_str,
                               disclose_month=disclosure_month,
                               disclose_year=disclosure_year)

    return holdings_df
