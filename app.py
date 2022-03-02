import json
import statistics
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from models import Base, SessionLocal, engine, CreditData

Base.metadata.create_all(bind=engine)

app = FastAPI()


# Grab all the information about a user.
@app.get("/user/{user_id}")
async def get_user(user_id: str):
    db = SessionLocal()
    user = db.query(CreditData).filter(CreditData.uuid == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")
    db.close()
    return jsonable_encoder(user)


# Given a credit tag, calculate the mean, median, and standard deviation.
# Only records having positive numbers are used for processing.
@app.get("/tag/{tag}")
async def get_tag_data(tag: str):
    with engine.connect() as conn:
        try:
            result = conn.execute("Select {} from credit_data where {} > 0".format(tag, tag))
        except Exception as e:
            raise HTTPException(status_code=400, detail="Invalid tag: {}.".format(tag))
        records = [row[0] for row in result]
        data = {
            'mean': statistics.mean(records),
            'median': statistics.median(records),
            'std_dev': statistics.stdev(records)
        }
        return jsonable_encoder(data)
