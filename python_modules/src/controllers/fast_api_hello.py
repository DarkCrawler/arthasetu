from fastapi import FastAPI

app = FastAPI()

app
@app.get("/dummy")
async def root():
    return {"message": "Hello World"}
