from pymongo import MongoClient
import pandas as pd

# Connection to database
client = MongoClient(
    "mongodb+srv://ismaelgo97:Yo19970923%2B@cluster0.wepwf.mongodb.net/test?retryWrites=true&w=majority", tls=True,
    tlsAllowInvalidCertificates=True)

# Get data from csv file
df = pd.read_csv("../datasets/Dataset_Clean.csv") 

db = client.charging_vehicles.data

# We import the data to the database 1 by 1
for index, session in df.iterrows():
    session = session.to_dict()
    newData = {}
    endData = {}
    startData = {}
    newData["country"] = session["country"]
    newData["city"] = session["city"]
    newData["connectorType"] = session["connectorType"]
    newData["durationSession"] = session["durationSession"]
    newData["durationCharge"] = session["durationCharge"]
    newData["energy"] = session["energy"]
    newData["tariff"] = session["tariff"]
    newData["cost"] = session["cost"]
    newData["meanPower"] = session["meanPower"]
    newData["maxPower"] = session["maxPower"]

    startData["startDate"] = session["startDate"]
    startData["startTimestamp"] = session["startTimestamp"]
    startData["weekDayStart"] = session["weekDayStart"]
    startData["yearStart"] = session["yearStart"]
    startData["hourStart"] = session["hourStart"]
    startData["minuteStart"] = session["minuteStart"]

    endData["endDate"] = session["endDate"]
    endData["endTimestamp"] = session["endTimestamp"]
    endData["weekDayEnd"] = session["weekDayEnd"]
    endData["yearEnd"] = session["yearEnd"]
    endData["hourEnd"] = session["hourEnd"]
    endData["minuteEnd"] = session["minuteEnd"]

    newData["start"] = startData
    newData["end"] = endData

    db.insert_one(newData)



