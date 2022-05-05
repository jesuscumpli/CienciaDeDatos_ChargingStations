import math
import random

import pandas as pd
import datetime
import json
from pymongo import MongoClient

df = pd.read_excel("transactionsNetherlands.xlsx")

newDF = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():
    object = session.to_dict()

    # Get Data
    connectorType = "Fast"
    maxPower = object["MaxPower"]
    energy = float(object["TotalEnergy"])
    # Time: Convert string to float
    durationCharge = object["ChargeTime"]
    durationSession = object["ConnectedTime"]

    meanPower = 0.0
    if durationCharge > 0:
        meanPower = round(energy / durationCharge, 3)

    # Get Info
    city = "-"
    country = "Netherlands"
    startDate = object["UTCTransactionStart"]
    endDate = object["UTCTransactionStop"]
    startTz = "UTC"
    endTz = "UTC"

    startDateObject = datetime.datetime.strptime(str(startDate) + startTz, "%Y-%m-%d %H:%M:%S%Z")

    weekDayStart = startDateObject.weekday()
    yearStart = startDateObject.year
    hourStart = startDateObject.hour
    minuteStart = startDateObject.minute
    startTimestamp = startDateObject.timestamp()

    endDateObject = datetime.datetime.strptime(str(endDate) + endTz, "%Y-%m-%d %H:%M:%S%Z")

    weekDayEnd = endDateObject.weekday()
    yearEnd = endDateObject.year
    hourEnd = endDateObject.hour
    minuteEnd = endDateObject.minute
    endTimestamp = endDateObject.timestamp()

    # Generate Tariff and Costs
    tariff = 0.0  # Free
    n = random.uniform(0, 1)
    if n > 0.5:
        tariff = round(random.uniform(0.1, 1.3), 2)  # Between 0.1 and 1.3 euro/kWh
    cost = round(tariff * energy, 2)

    newObject = {}

    newObject["country"] = country
    newObject["city"] = city

    newObject["connectorType"] = connectorType
    newObject["durationCharge"] = durationCharge
    newObject["durationSession"] = durationSession
    newObject["energy"] = energy
    newObject["tariff"] = tariff
    newObject["cost"] = cost
    newObject["meanPower"] = meanPower
    newObject["maxPower"] = maxPower

    newObject["startDate"] = startDateObject.strftime("%d/%m/%Y %H:%M:%S%z")
    newObject["startTimestamp"] = startTimestamp
    newObject["weekDayStart"] = weekDayStart
    newObject["yearStart"] = yearStart
    newObject["hourStart"] = hourStart
    newObject["minuteStart"] = minuteStart

    newObject["endDate"] = endDateObject.strftime("%d/%m/%Y %H:%M:%S%z")
    newObject["endTimestamp"] = endTimestamp
    newObject["weekDayEnd"] = weekDayEnd
    newObject["yearEnd"] = yearEnd
    newObject["hourEnd"] = hourEnd
    newObject["minuteEnd"] = minuteEnd

    newObject["anomaly"] = 0
    newObject["class"] = "Normal"

    # collection.insert_one(newObject)
    series = pd.Series(newObject)
    row_df = pd.DataFrame([series])
    newDF = pd.concat([row_df, newDF], ignore_index=True)

print(newDF)
newDF.to_csv(r'./Netherlands_not_Clean.csv', index=False)
