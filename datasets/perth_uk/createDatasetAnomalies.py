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

samples = df.sample(frac=0.25)

for index, session in samples.iterrows():

    try:
        object = {}
        session = session.to_dict()
        object["connector"] = session["Connector"]
        object["idCP"] = session["CP ID"]
        object["energy"] = session["Total kWh"]
        object["model"] = session["Model"]
        object["site"] = session["Site"]

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

        object["startTime"] = startObject
        object["endTime"] = endObject
        object["startTimestamp"] = startObject.timestamp()
        object["endTimestamp"] = endObject.timestamp()

        object["tariff"] = 0  # free
        object["durationSession"] = object["endTimestamp"] - object["startTimestamp"]
        object["anomaly"] = 0
        object["class"] = "Normal"

        n = random.uniform(0, 1)
        if n > 0.5:
            object["tariff"] = round(random.uniform(0.05, 0.6), 2)

        object["cost"] = round(object["tariff"] * object["energy"], 2)

        if object["energy"] is None or object["energy"] < 0:
            object["class"] = "Discharge"
            object["anomaly"] = 1

        if object["durationSession"] is None or object["durationSession"] < 0:
            object["class"] = "DurationNegative"
            object["anomaly"] = 1

        if object["energy"] == 0 and object["durationSession"] > 0:
            object["class"] = "ZeroEnergy"
            object["anomaly"] = 1

        if object["anomaly"] == 1:
            continue

        if (object["cost"] > 0):
            i = random.randint(0, 5)
        else:
            i = random.randint(0, 3)

        # Anomaly
        if i == 0:
            kwhSum = random.uniform(1.75, 5) * object["energy"]
            object["energy"] = round(kwhSum, 3)
            object["cost"] = round(object["tariff"] * object["energy"], 2)
            object["class"] = "OverKwhDelivered"
            object["anomaly"] = 1
        elif i == 1:
            kwhSum = random.uniform(0.6, 1) * object["energy"]
            object["energy"] -= kwhSum
            object["energy"] = round(object["energy"], 3)
            object["cost"] = round(object["tariff"] * object["energy"], 2)
            object["class"] = "UnderKwhDelivered"
            object["anomaly"] = 1
        elif i == 2:
            object["tariff"] = round(random.uniform(0.7, 1.5), 2)
            object["cost"] = round(object["tariff"] * object["energy"], 2)
            object["class"] = "OverTariff"
            object["anomaly"] = 1
        elif i == 3:
            sum = random.uniform(0.75, 1) * object["durationSession"]
            object["tariff"] = round(random.uniform(0.01, 0.03), 2)
            object["cost"] = round(object["tariff"] * object["energy"], 2)
            object["class"] = "UnderTariff"
            object["anomaly"] = 1
        elif i == 4:
            sum = random.uniform(1.75, 5) * object["cost"]
            object["cost"] += round(sum, 2)
            object["cost"] = round(object["cost"], 2)
            object["class"] = "OverCosts"
            object["anomaly"] = 1
        elif i == 5:
            sum = random.uniform(0.5, 1) * object["cost"]
            object["cost"] -= round(sum, 2)
            object["cost"] = round(object["cost"], 2)
            object["class"] = "UnderCosts"
            object["anomaly"] = 1

        collection.insert_one(object)
    except:
        continue
