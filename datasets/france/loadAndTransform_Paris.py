import math
import random

import pandas as pd
import datetime
import json
from pymongo import MongoClient

df1 = pd.read_csv("transactions-data-April.csv", sep=";")
df2 = pd.read_csv("transactions-data-Mai.csv", sep=";")
df = pd.concat([df1, df2])

newDF = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():
    object = session.to_dict()

    # Get Data
    standardConnector = object["Type de prise"]
    connectorType = "Rapid"
    maxPower = object["Prise de courant"]
    if maxPower <= 3.7:
        connectorType = "Slow"
    if maxPower <= 22:
        connectorType = "Fast"
    if maxPower <= 50:
        connectorType = "Rapid"
    if maxPower > 50:
        connectorType = "Ultra-Rapid"
    energy = float(object["L'énergie (Wh)"])/1000.0 #To kWh
    # Time
    durationCharge = round(object["Durée (sec)"] / 3600.0, 2)
    meanPower = 0.0
    if durationCharge > 0:
        meanPower = round(energy / durationCharge, 3)
    # Get Info
    city = "Paris"
    country = "France"

    yearStart = object["Année de début"]
    dayStart = object["Jour de début"]
    monthStart = object["Mois de début"]
    hourStart = object["Heure de début "]
    minuteStart = object["Minute de Début"]
    startDate = str(dayStart) + "/" + str(monthStart) + "/" + str(yearStart) + " " + str(hourStart) + ":" + str(
        minuteStart)
    startTz = "+0200"
    startDateObject = datetime.datetime.strptime(startDate+startTz, "%d/%m/%Y %H:%M%z")
    weekDayStart = startDateObject.weekday()
    startTimestamp = startDateObject.timestamp()

    yearEnd = object["Année de fin"]
    dayEnd = object["Jour de fin"]
    monthEnd = object["Mois de fin"]
    hourEnd = object["Heure de fin"]
    minuteEnd = object["Minute de fin"]
    endDate = str(dayEnd) + "/" + str(monthEnd) + "/" + str(yearEnd) + " " + str(hourEnd) + ":" + str(
        minuteEnd)
    endTz = "+0200"
    endDateObject = datetime.datetime.strptime(endDate+endTz, "%d/%m/%Y %H:%M%z")
    weekDayEnd = endDateObject.weekday()
    endTimestamp = endDateObject.timestamp()

    durationSession = round((endTimestamp - startTimestamp)/3600.0, 2)

    # Generate Tariff and Costs
    tariff = 0.0  # Free
    n = random.uniform(0, 1)
    if n > 0.5:
        tariff = round(random.uniform(0.1, 1.3), 2)  # Between 0.1 and 1.3 euro/kWh
    cost = round(tariff * energy, 2)

    newObject = {}

    newObject["country"] = country
    newObject["city"] = city

    newObject["standardConnector"] = standardConnector
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
newDF.to_csv(r'./Paris_not_Clean.csv', index=False)
