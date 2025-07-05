from fastapi import FastAPI
from portfolio_processing_engines.sbi.ingestion.sbi_ingestion_orchestrator import \
    orchestrate_portfolio_disclosure_process

app = FastAPI()

app


@app.get("/dummy")
async def root():
    return {"message": "Hello World"}


@app.get("/dummy/trigger/sbi")
async def trigger_sbi():
    print('triggering SBI portroflio parsing')
    gcs_path = 'gs://projx-tb-extracts-staging/pxsbi/all-schemes-monthly-portfolio---as-on-28th-february-2025.xlsx'
    orchestrate_portfolio_disclosure_process(file_path=gcs_path, mode='gcp', storage='bq')
    return {"message": "triggered SBI test"}
