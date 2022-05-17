import random
import pandas as pd
import datetime

df = pd.read_csv("boulder_usa.csv")

newDF = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():
    session_object = session.to_dict()

    # Get Data
    connectorType = "Fast"
    maxPower = 7.4
    energy = float(session_object["Energy__kWh_"])
    # Time: Convert string to float
    hc, mc, sc = session_object["Charging_Time__hh_mm_ss_"].split(':')
    hs, ms, ss = session_object["Total_Duration__hh_mm_ss_"].split(':')
    durationCharge = round(int(hc) + float(int(mc) / 60.0) + float(int(sc) / 3600.0), 2)
    durationSession = round(int(hs) + float(int(ms) / 60.0) + float(int(ss) / 3600.0), 2)
    meanPower = 0.0
    if durationCharge > 0:
        meanPower = round(energy / durationCharge, 3)
    # Get Info
    city = "Boulder"
    country = "USA"
    startDate = session_object["Start_Date___Time"]
    endDate = session_object["End_Date___Time"]
    startTimezone = session_object["Start_Time_Zone"]
    endTimezone = session_object["End_Time_Zone"]

    startTz = "-0600"
    if startTimezone == "MST":
        startTz = "-0700"
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

    newObject["start.startDate"] = startDateObject.strftime("%d/%m/%Y %H:%M:%S%z")
    newObject["start.startDate"] = startDateObject.strftime("%d/%m/%Y %H:%M:%S%z")
    newObject["start.startTimestamp"] = startTimestamp
    newObject["start.weekDayStart"] = weekDayStart
    newObject["start.yearStart"] = yearStart
    newObject["start.hourStart"] = hourStart
    newObject["start.minuteStart"] = minuteStart

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
newDF.to_csv(r'./Boulder_not_Clean.csv', index=False)
