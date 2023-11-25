import pandas as pd
import uvicorn
from fastapi.responses import JSONResponse
from fastapi import FastAPI, UploadFile
from src import Model, FeatureBuilder
 

app = FastAPI()
model = Model()
builder = FeatureBuilder()

@app.post("/uploadfile/")
async def create_upload_file(file: UploadFile):
    try:
        df = pd.read_parquet(file.file)

    except:
        return f'Ошибка. Расширение файла должно быть .gzip'
    
    df = builder.prepare_data(df)
    result = model.predict(df)
    response_data = result.to_dict(orient='records')
    return JSONResponse(content=response_data)


if __name__ == "__main__":
    uvicorn.run("app:app", reload=True, port=8000)