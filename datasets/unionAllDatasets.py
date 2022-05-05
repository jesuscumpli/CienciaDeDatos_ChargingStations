import pandas as pd

df1 = pd.read_csv("boulder_usa/Boulder_not_Clean.csv")
df2 = pd.read_csv("./dundee_uk/Dundee_not_Clean.csv")
df3 = pd.read_csv("./france/Paris_not_Clean.csv")
df4 = pd.read_csv("./netherlands/Netherlands_not_Clean.csv")
df5 = pd.read_csv("./paloalto_usa/PaloAlto_not_Clean.csv")
df = pd.concat([df1, df2, df3, df4, df5])
df.to_csv(r"./Dataset_not_Clean.csv", index=False)