import pandas as pd
from google.cloud import bigquery
from pandas.core.interchange.dataframe_protocol import DataFrame


def write_portfolio_to_bq(df: DataFrame, schema_name: str):
    project_id = "sab-dev-projx-dc-6466"
    dataset_id = "mf_portfolio_details"
    table_name = "sbi_detailed_portfolio"
    table_id = f"{project_id}.{dataset_id}.{table_name}"

    #######################
    # avoiding 0.0 problem in pure text cols
    df['inst_name'] = df['inst_name'].apply(lambda x: '' if pd.isna(x) else str(x))
    df['sector'] = df['sector'].apply(lambda x: '' if pd.isna(x) else str(x))
    df['aum_percent'] = df['aum_percent'].apply(lambda x: '' if pd.isna(x) else str(x))
    df['isin'] = df['isin'].apply(lambda x: '' if pd.isna(x) else str(x))
    df["disclose_year"] = df["disclose_year"].astype(int)

    #######################

    # import pyarrow as pa
    # for col in df.columns:
    #     try:
    #         pa.array(df[col])
    #     except Exception as e:
    #         print(f"col error '{col}' .... error ...{e}")
    #######################

    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Waits for the upload to finish

    print(f"Data successfully uploaded to {table_id} for ...", schema_name)

    return None
