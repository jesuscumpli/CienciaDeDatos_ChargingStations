import math
import random

import pandas as pd
import datetime
import json
from pymongo import MongoClient
import numpy as np

client = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = client.chargingEVSE_V4
collection = db.chargingSessions

df = pd.read_csv("usage.csv")

newDF = pd.DataFrame(
    columns=["site", "cardNumber", "cpId", "connectorType", "cost", "energy", "tariff", "durationCharge",
             "durationSession", "startDate", "startTimestamp", "endDate", "endTimestamp", "class", "anomaly"])

for index, session in df.iterrows():

    try:
        object = {}
        session = session.to_dict()
        object["connectorType"] = session["Connector"]
        # object["connector"] = random.randint(1, 5)
        object["cpId"] = session["CP ID"]
        object["energy"] = session["Total kWh"]
        object["model"] = session["Model"]
        object["site"] = session["Site"]
        object["anomaly"] = 0
        object["class"] = "Normal"
        object["cardNumber"] = random.choice(["CPS628777", "CPS622821", "CPS628777", "CPS604738H"])

        startDate = session["Start Date"]
        startTime = session["Start Time"]
        endDate = session.get("End Date", None)
        endTime = session.get("End Time", None)

        startDateObject = datetime.datetime.strptime(startDate, "%d/%m/%Y")
        startTimeObject = datetime.datetime.strptime(startTime, "%H:%M")
        startObject = startDateObject.replace(hour=startTimeObject.hour, minute=startTimeObject.minute)

        endDateObject = datetime.datetime.strptime(endDate, "%d/%m/%Y")
        endTimeObject = datetime.datetime.strptime(endTime, "%H:%M")
        endObject = endDateObject.replace(hour=endTimeObject.hour, minute=endTimeObject.minute)

        object["startDate"] = startObject
        object["endDate"] = endObject
        object["startTimestamp"] = startObject.timestamp()
        object["endTimestamp"] = endObject.timestamp()

        object["tariff"] = 0  # free
        object["durationSession"] = object["endTimestamp"] - object["startTimestamp"]
        object["durationCharge"] = object["endTimestamp"] - object["startTimestamp"]

        n = random.uniform(0, 1)
        if n > 0.5:
            object["tariff"] = round(random.uniform(0.05, 1.3), 2)

        object["cost"] = round(object["tariff"] * object["energy"], 2)

        # collection.insert_one(object)
        series = pd.Series(object)
        row_df = pd.DataFrame([series])
        newDF = pd.concat([row_df, newDF], ignore_index=True)
    except:
        print("Error")
        continue

newDF.to_excel(r'./ChargingSessionsUssage.xlsx', index=False)