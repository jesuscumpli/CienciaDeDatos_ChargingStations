import math
import random

import pandas as pd
import datetime
import json
import numpy as np


df = pd.read_csv('cpdata-2018.csv')

newDF = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():

    object = {}
    session = session.to_dict()
    connectorType = "Slow"
    energy = session["Total kWh"]
    model = session["Model"]
    maxPower = 0
    if "22" in model:
        maxPower = 22.0
        connectorType = "Fast"
    elif "7" in model:
        maxPower = 7.0
        connectorType = "Fast"
    elif "Semi-Rapid" in model:
        maxPower = 22.0
        connectorType = "Fast"
    elif "50" in model:
        maxPower = 50.0
        connectorType = "Rapid"
    elif "Dual Rapid" in model:
        maxPower = 50.0
        connectorType = "Rapid"
    elif "Triple Rapid" in model:
        maxPower = 50.0
        connectorType = "Rapid"
    else:
        print("ERROR GETTING MODEL MAX POWER")

    startDate = session["Start Date"]
    startTime = session["Start Time"]
    endDate = session.get("End Date", None)
    endTime = session.get("End Time", None)

    startDateObject = datetime.datetime.strptime(startDate, "%d/%m/%Y")
    startTimeObject = datetime.datetime.strptime(startTime, "%H:%M")
    startDateObject = startDateObject.replace(hour=startTimeObject.hour, minute=startTimeObject.minute)
    weekDayStart = startDateObject.weekday()
    yearStart = startDateObject.year
    hourStart = startDateObject.hour
    minuteStart = startDateObject.minute
    startTimestamp = startDateObject.timestamp()

    try:
        endDateObject = datetime.datetime.strptime(endDate, "%d/%m/%Y")
        endTimeObject = datetime.datetime.strptime(endTime, "%H:%M")
        endDateObject = endDateObject.replace(hour=endTimeObject.hour, minute=endTimeObject.minute)
        weekDayEnd = endDateObject.weekday()
        yearEnd = endDateObject.year
        hourEnd = endDateObject.hour
        minuteEnd = endDateObject.minute
        endTimestamp = endDateObject.timestamp()
    except:
        print("Error endDate: " + str(endDate))
        print("Error endTime: " + str(endTime))
        continue

    durationSession = round((endTimestamp - startTimestamp) / 3600.0, 2)
    durationCharge = durationSession
    meanPower = 0.0
    if durationCharge > 0:
        meanPower = round(energy / durationCharge, 3)

    # Generate Tariff and Costs
    tariff = 0.0  # Free
    n = random.uniform(0, 1)
    if n > 0.5:
        tariff = round(random.uniform(0.1, 1.3), 2)  # Between 0.1 and 1.3 euro/kWh
    cost = round(tariff * energy, 2)

    city = "Dundee"
    country = "UK"

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

    # collection.insert_one(object)
    series = pd.Series(newObject)
    row_df = pd.DataFrame([series])
    newDF = pd.concat([row_df, newDF], ignore_index=True)

newDF.to_csv(r'./Dundee_2018_not_Clean.csv', index=False)
