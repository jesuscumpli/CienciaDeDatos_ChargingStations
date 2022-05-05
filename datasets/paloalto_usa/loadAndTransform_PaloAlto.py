import math
import random

import pandas as pd
import datetime
import json
from pymongo import MongoClient

df = pd.read_csv("paloalto.csv")

newDF = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():
    object = session.to_dict()

    # Get Data
    portType = object["Port Type"]
    connectorType = "Slow"
    maxPower = 2.8
    if portType == "Level 2":
        connectorType = "Fast"
        maxPower = 7.2
    energy = float(object["Energy (kWh)"])

    # Time: Convert string to float
    hc, mc, sc = object["Charging Time (hh:mm:ss)"].split(':')
    hs, ms, ss = object["Total Duration (hh:mm:ss)"].split(':')
    durationCharge = round(int(hc) + float(int(mc) / 60.0) + float(int(sc) / 3600.0), 2)
    durationSession = round(int(hs) + float(int(ms) / 60.0) + float(int(ss) / 3600.0), 2)
    meanPower = 0.0
    if durationCharge > 0:
        meanPower = round(energy / durationCharge, 3)
    # Get Info
    city = "Palo Alto"
    country = "USA"
    startDate = object["Start Date"]
    endDate = object["End Date"]
    startTimezone = object["Start Time Zone"]
    endTimezone = object["End Time Zone"]

    startTz = "-0700"
    if startTimezone == "PST":
        startTz = "-0800"
    try:
        startDateObject = datetime.datetime.strptime(startDate + startTz, "%m/%d/%Y %H:%M%z")
    except:
        print("Error en startDate: " + str(startDate))
        continue
    weekDayStart = startDateObject.weekday()
    yearStart = startDateObject.year
    hourStart = startDateObject.hour
    minuteStart = startDateObject.minute
    startTimestamp = startDateObject.timestamp()

    if yearStart <= 2018:  # Filter to transactions more recent
        continue

    endTz = "-0600"
    if endTimezone == "MST":
        endTz = "-0700"
    try:
        endDateObject = datetime.datetime.strptime(endDate + endTz, "%m/%d/%Y %H:%M%z")
    except:
        print("Error en endDate: " + str(endDate))
        continue
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
newDF.to_csv(r'./PaloAlto_not_Clean.csv', index=False)
