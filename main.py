import fastapi
import typing
import gero
import os
import io
import dotenv
import json
import pandas as pd

if not "host" in os.environ:
    import dotenv
    dotenv.load_dotenv(".env")

conn = gero.create_connection(
    user=os.environ.get("user"),
    password=os.environ.get("password"),
    host=os.environ.get("host"),
    port=os.environ.get("port"),
    database=os.environ.get("database")
)
app = fastapi.FastAPI()

@app.get("/surveys/{instrument_uuid}")
async def get_surveys(instrument_uuid: str, output: typing.Optional[str] = "json"):
    surveys = gero.get_surveys_df(conn, instrument_uuid)
    if output == "json":
        surveys_json = fastapi.encoders.jsonable_encoder(json.loads(surveys.to_json(orient="records")))
        response = fastapi.responses.JSONResponse(content=surveys_json)
    if output == "excel":
        output = io.BytesIO()
        with pd.ExcelWriter(output) as writer:
            surveys.to_excel(writer, index = False)
        headers = {
            'Content-Disposition': f'attachment; filename="{instrument_uuid}.xlsx"'
        }
        response = fastapi.responses.StreamingResponse(iter([output.getvalue()]), headers=headers)
    return response

@app.get("/instruments")
async def list_instruments():
    instruments = gero.list_instruments(conn)
    instruments_json = fastapi.encoders.jsonable_encoder(instruments)
    response = fastapi.responses.JSONResponse(content=instruments_json)
    return response
