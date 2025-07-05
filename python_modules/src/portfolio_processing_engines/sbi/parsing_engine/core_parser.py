import pandas as pd
from folio_sheet_parser import generate_mf_ds
from pandas import ExcelFile, DataFrame


def parse_index_sheet(xls_file_fp: ExcelFile):
    return xls_file_fp.parse('Index', header=2)


def create_index_parsed_ds(index_df: DataFrame):
    scheme_index_df = {}
    for idx, row in index_df.iterrows():
        scheme_index_df[row[0]] = {
            'scheme_code': row[1],
            'scheme_name': row[2],
        }

    return scheme_index_df


def create_folio_ds(xls_file_fp: ExcelFile, index_df: DataFrame):
    folio_core_ds = {}
    for idx, row in index_df.iterrows():
        mf_code_id = row.iloc[0]
        mf_code = row.iloc[1]
        mf_name = row.iloc[2]
        folio_parsed_df = generate_mf_ds(mf_code_id=mf_code_id, mf_code_str=mf_code, xls_file_fp=xls_file_fp,
                                         disclosure_month=2,
                                         disclosure_year=2025)

        folio_core_ds[mf_code] = {
            'mf_name': mf_name,
            'mf_code_id': mf_code_id,
            'mf_portfolio': folio_parsed_df
        }

    return folio_core_ds


def parse_portfolio(xls_file_path: str, mode: str):
    if mode == 'local':
        xls_file = pd.ExcelFile(xls_file_path)
        return create_folio_ds(xls_file_fp=xls_file, index_df=parse_index_sheet(xls_file))

    elif mode == 'gcp':
        xls_file = pd.ExcelFile(xls_file_path, engine='openpyxl')
        print('Excel file read..')
        return create_folio_ds(xls_file_fp=xls_file, index_df=parse_index_sheet(xls_file))

    else:
        return 'Mode not supported'
