import pandas as pd
import datetime
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false")
db = client.chargingEVSE_V4
collection = db.modelsDundee

df = pd.read_csv("cpdata-2022.csv")
df.sort_values(by='Start', ascending=False)

newDF = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():
    object = {}
    session = session.to_dict()
    connectorType = session["Connector Type"]
    energy = float(session["Consum(kWh)"])
    cpId = str(session["CP ID"])
    result = collection.find_one({"cpId": cpId})
    model = result["model"]
    maxPower = result["maxPower"]

    startDate = session["Start"]
    endDate = session.get("End", None)

    try:
        startDateObject = datetime.datetime.strptime(startDate, "%Y-%m-%d %H:%M:%S")
        weekDayStart = startDateObject.weekday()
        yearStart = startDateObject.year
        hourStart = startDateObject.hour
        minuteStart = startDateObject.minute
        startTimestamp = startDateObject.timestamp()
    except:
        try:
            startDateObject = datetime.datetime.strptime(str(startDate), "%d/%m/%Y %H:%M")
            weekDayStart = startDateObject.weekday()
            yearStart = startDateObject.year
            hourStart = startDateObject.hour
            minuteStart = startDateObject.minute
            startTimestamp = startDateObject.timestamp()
        except:
            print("Error startDate: " + str(startDate))
            continue

    try:
        endDateObject = datetime.datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")
        weekDayEnd = endDateObject.weekday()
        yearEnd = endDateObject.year
        hourEnd = endDateObject.hour
        minuteEnd = endDateObject.minute
        endTimestamp = endDateObject.timestamp()
    except:
        try:
            endDateObject = datetime.datetime.strptime(str(endDate), "%d/%m/%Y %H:%M")
            weekDayEnd = endDateObject.weekday()
            yearEnd = endDateObject.year
            hourEnd = endDateObject.hour
            minuteEnd = endDateObject.minute
            endTimestamp = endDateObject.timestamp()
        except:
            print("Error startDate: " + str(endDate))
            continue

    durationSession = round((endTimestamp - startTimestamp) / 3600.0, 2)
    duration = session["Duration"]
    if isinstance(duration, datetime.time):
        duration = duration.hour * 3600 + duration.minute * 60 + duration.second
    elif isinstance(duration, datetime.datetime):
        duration = (duration.year - 1900) * 3600 * 24 * 365 + (
                duration.month - 1) * 3600 * 24 * 30 + duration.day * 3600 * 24 + duration.hour * 3600 + duration.minute * 60 + duration.second
    elif isinstance(duration, str):
        if "1900-" in duration:
            duration = datetime.datetime.strptime(duration, '%Y-%m-%d %H:%M:%S')
            duration = (duration.year - 1900) * 3600 * 24 * 365 + (
                    duration.month - 1) * 3600 * 24 * 30 + duration.day * 3600 * 24 + duration.hour * 3600 + duration.minute * 60 + duration.second
        elif not "-" in duration:
            hours, minutes, seconds = duration.split(":")
            duration = float(hours)*3600 + float(minutes) * 60 + float(seconds)
        else:
            print("Error duration: " + str(duration))
            continue
    else:
        print("Error duration: " + str(duration))
        continue

    durationCharge = round(duration/3600.0, 2)
    meanPower = 0.0
    if durationCharge > 0:
        meanPower = round(energy / durationCharge, 3)


    cost = float(session["Amount"])
    cost = cost*1.22 #libra to euro
    tariff = 0.0
    if energy != 0:
        tariff = cost / energy

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

newDF.to_excel(r'./Dundee_2022_not_Clean.xlsx', index=False)