import pandas as pd

df = pd.read_csv("Dataset_not_Clean.csv")

df.dropna()  # Delete rows with NaN values
df = df.loc[df['yearStart'] > 2018]  # Delete rows before 2019

# Anomalies of duration
df.loc[df['durationCharge'] < 0, 'anomaly'] = 1
df.loc[df['durationCharge'] < 0, 'class'] = "DurationChargeNegative"
df.loc[df['durationSession'] < 0, 'anomaly'] = 1
df.loc[df['durationSession'] < 0, 'class'] = "DurationSessionNegative"
df.loc[df['durationCharge'] < df['durationSession'], 'anomaly'] = 1
df.loc[df['durationCharge'] < df['durationSession'], 'class'] = "DurationSessionLessThanCharge"
df.loc[df['durationCharge'] >= 100, 'anomaly'] = 1
df.loc[df['durationCharge'] >= 100, 'class'] = "DurationChargeTooLong"
df.loc[df['durationSession'] > 168, 'anomaly'] = 1
df.loc[df['durationSession'] > 168, 'class'] = "DurationSessionTooLong"

# Anomalies of Energy
df.loc[df['energy'] < 0, 'anomaly'] = 1
df.loc[df['energy'] < 0, 'class'] = "EnergyNegative"
df.loc[(df['energy'] > 0) & (df['energy'] <= 0.2) & (df['durationCharge'] >= 3), 'anomaly'] = 1
df.loc[(df['energy'] > 0) &(df['energy'] <= 0.2) & (df['durationCharge'] >= 3), 'class'] = "NotCharge"

# Anomalies of Tariff
df.loc[df['tariff'] < 0, 'anomaly'] = 1
df.loc[df['tariff'] < 0, 'class'] = "TariffNegative"
df.loc[df['tariff'] >= 4, 'anomaly'] = 1
df.loc[df['tariff'] >= 4, 'class'] = "TariffExcessive"

# Anomalies of Cost
df.loc[df['cost'] < 0, 'anomaly'] = 1
df.loc[df['cost'] < 0, 'class'] = "CostNegative"
df.loc[df['cost'] > 100, 'anomaly'] = 1
df.loc[df['cost'] > 100, 'class'] = "CostExcessive"

# Anomalies of MeanPower
df.loc[df['meanPower'] < 0, 'anomaly'] = 1
df.loc[df['meanPower'] < 0, 'class'] = "MeanPowerNegative"
df.loc[df['meanPower'] > df['maxPower'], 'anomaly'] = 1
df.loc[df['meanPower'] > df['maxPower'], 'class'] = "MeanPowerExcessive"
df.loc[(df['maxPower'] > 22) & (df['meanPower'] > 0) & (df['meanPower'] < 0.01), 'anomaly'] = 1
df.loc[(df['maxPower'] > 22) & (df['meanPower'] > 0) & (df['meanPower'] < 0.01), 'class'] = "MeanPowerTooLow"

df.to_csv("Dataset_With_Anomalies.csv", index=False)

dfClean = df[df['anomaly'] == 0]
dfClean.drop(['anomaly', 'class'], axis=1, inplace=True)
dfClean.to_csv("Dataset_Clean.csv", index=False)
