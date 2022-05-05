import pandas as pd

df = pd.read_excel("Dundee_not_Clean.xlsx")
output = "Dundee_Clean.xlsx"

dfClean = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():

    object = session.to_dict()
    durationCharge = object["durationCharge"]
    durationSession = object["durationSession"]
    maxPower = object["maxPower"]
    energy = object["energy"]
    tariff = object["tariff"]
    cost = object["cost"]
    meanPower = object["meanPower"]

    # Anomalies of duration
    if durationCharge < 0:
        object["class"] = "DurationChargeNegative"
        object["anomaly"] = 1
    if durationSession < 0:
        object["class"] = "DurationSessionNegative"
        object["anomaly"] = 1
    if durationSession < durationCharge:
        object["class"] = "DurationSessionLessThanDurationCharge"
        object["anomaly"] = 1

    if durationCharge >= 100:
        object["class"] = "DurationChargeTooLong"
        object["anomaly"] = 1

    if durationSession > 168:
        object["class"] = "DurationSessionTooLong"
        object["anomaly"] = 1

    # Anomalies of Energy
    if energy < 0:
        object["class"] = "EnergyNegative"
        object["anomaly"] = 1

    if 0 <= energy <= 0.2 and durationCharge >= 3:
        object["class"] = "NotCharge"
        object["anomaly"] = 1

    if tariff < 0:
        object["class"] = "TariffNegative"
        object["anomaly"] = 1
    if tariff >= 4:
        object["class"] = "TariffExcessive"
        object["anomaly"] = 1

    if cost < 0:
        object["class"] = "CostNegative"
        object["anomaly"] = 1
    if cost > 100:
        object["class"] = "CostExcessive"
        object["anomaly"] = 1

    if meanPower < 0:
        object["class"] = "MeanPowerNegative"
        object["anomaly"] = 1

    if meanPower > maxPower:
        object["class"] = "MeanPowerExcessive"
        object["anomaly"] = 1

    if maxPower > 22 and 0 < meanPower < 0.01:
        object["class"] = "MeanPowerTooLow"
        object["anomaly"] = 1

    series = pd.Series(object)
    row_df = pd.DataFrame([series])
    dfClean = pd.concat([row_df, dfClean], ignore_index=True)

dfClean.to_excel(output, index=False)
