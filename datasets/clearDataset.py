import pandas as pd

df = pd.read_csv("Dataset_not_Clean.csv")

df.dropna()  # Delete rows with NaN values
dfClean = pd.DataFrame(
    columns=["connectorType", "cost", "energy", "tariff", "durationCharge", "durationSession", "meanPower", "maxPower",
             "anomaly", "class",
             "weekDayStart", "hourStart", "minuteStart", "weekDayEnd", "hourEnd", "minuteEnd",
             "startDate", "endDate", "startTimestamp", "endTimestamp"
             ])

for index, session in df.iterrows():
    # Get Data by Row
    session = session.to_dict()
    durationCharge = session["durationCharge"]
    durationSession = session["durationSession"]
    maxPower = session["maxPower"]
    energy = session["energy"]
    tariff = session["tariff"]
    cost = session["cost"]
    meanPower = session["meanPower"]
    yearStart = session["yearStart"]

    if yearStart <= 2017:  # Filter to transactions more recent
        continue

    # Anomalies of duration
    if durationCharge < 0:
        session["class"] = "DurationChargeNegative"
        session["anomaly"] = 1
    if durationSession < 0:
        session["class"] = "DurationSessionNegative"
        session["anomaly"] = 1
    if durationSession < durationCharge:
        session["class"] = "DurationSessionLessThanDurationCharge"
        session["anomaly"] = 1

    if durationCharge >= 100:
        session["class"] = "DurationChargeTooLong"
        session["anomaly"] = 1

    if durationSession > 168:
        session["class"] = "DurationSessionTooLong"
        session["anomaly"] = 1

    # Anomalies of Energy
    if energy < 0:
        session["class"] = "EnergyNegative"
        session["anomaly"] = 1

    if 0 <= energy <= 0.2 and durationCharge >= 3:
        session["class"] = "NotCharge"
        session["anomaly"] = 1

    if tariff < 0:
        session["class"] = "TariffNegative"
        session["anomaly"] = 1
    if tariff >= 4:
        session["class"] = "TariffExcessive"
        session["anomaly"] = 1

    if cost < 0:
        session["class"] = "CostNegative"
        session["anomaly"] = 1
    if cost > 100:
        session["class"] = "CostExcessive"
        session["anomaly"] = 1

    if meanPower < 0:
        session["class"] = "MeanPowerNegative"
        session["anomaly"] = 1

    if meanPower > maxPower:
        session["class"] = "MeanPowerExcessive"
        session["anomaly"] = 1

    if maxPower > 22 and 0 < meanPower < 0.01:
        session["class"] = "MeanPowerTooLow"
        session["anomaly"] = 1

    series = pd.Series(session)
    row_df = pd.DataFrame([series])
    dfClean = pd.concat([row_df, dfClean], ignore_index=True)

dfClean.to_csv("Dataset_With_Anomalies.csv", index=False)

dfClean = dfClean[dfClean['anomaly'] == 0]
dfClean.drop(['anomaly', 'class'], axis=1, inplace=True)
dfClean.to_csv("Dataset_Clean.csv", index=False)
