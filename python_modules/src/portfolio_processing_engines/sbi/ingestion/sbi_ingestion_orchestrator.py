import pandas as pd
from portfolio_processing_engines.sbi.ingestion.bq_writer import write_portfolio_to_bq
from portfolio_processing_engines.sbi.parsing_engine.core_parser import parse_portfolio


def create_complete_portfolio_ds(parsed_ds):
    portfolio_detailed_dfs = []
    for key, values in parsed_ds.items():
        mf_code_str = key
        mf_code_id = values['mf_code_id']
        mf_name = values['mf_name']
        mf_portfolio = values['mf_portfolio']
        print(f'storing data for .. {key}')
        # write_portfolio_to_bq(df=mf_portfolio, schema_name=mf_name)
        portfolio_detailed_dfs.append(mf_portfolio)

    # combining data  frames to reduce write time
    combined_df = pd.concat(portfolio_detailed_dfs, ignore_index=True)

    return portfolio_detailed_dfs, combined_df


def orchestrate_portfolio_disclosure_process(file_path, mode, storage, month: int, year: int):
    if mode == 'local':
        file_path = '/prototypes/sbi/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx'

        parsed_ds = parse_portfolio(xls_file_path=file_path, mode=mode)

        print('parsing of portfolio sheet complete....')

        portfolio_ds, combined_portfolio_df = create_complete_portfolio_ds(parsed_ds)

        if storage == 'bq':
            write_portfolio_to_bq(df=combined_portfolio_df, schema_name='all_schemas')
        else:
            print('Current supported storage mechanism only set to BQ')

    if mode == 'gcp':
        gcs_strorage_path = file_path

        parsed_ds = parse_portfolio(xls_file_path=file_path, mode=mode, month=month, year=year)

        print('parsing of portfolio sheet complete....')

        portfolio_ds, combined_portfolio_df = create_complete_portfolio_ds(parsed_ds)

        if storage == 'bq':
            write_portfolio_to_bq(df=combined_portfolio_df, schema_name='all_schemas')
            print(f'BQ writw complete, written {combined_portfolio_df.__len__()} records')

        else:
            print('Current supported storage mechanism only set to BQ')


def get_month_and_year(file_path: str):
    import re
    import calendar
    match = re.search(r"(\d{1,2})(st|nd|rd|th)-([a-z]+)-(\d{4})", file_path)
    if match:
        day, _, month_str, year = match.groups()
        month_str_cap = month_str.capitalize()
        month_num = list(calendar.month_name).index(month_str_cap)  # month_name[1] = "January"
        return month_num, year

    else:
        print("Could not parse date.")


if __name__ == "__main__":
    sheet_local_path = '/prototypes/sbi/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx'
    gcs_path = 'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx'

    gcs_file_path_list = [
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-31st-january-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-31st-march-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-30th-april-2025.xlsx',
        'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-31st-may-2025.xlsx'
    ]

    for file_path in gcs_file_path_list:
        month, year = get_month_and_year(file_path)
        orchestrate_portfolio_disclosure_process(file_path=file_path, mode='gcp', storage='bq', month=month, year=year)
