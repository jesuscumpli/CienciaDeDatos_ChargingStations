import pandas as pd

df1 = pd.read_excel('./2021/Dundee_2021_not_Clean.xlsx')
df2 = pd.read_excel('./2021_2022/Dundee_2022_not_Clean.xlsx')

df = pd.concat([df1, df2])

df.to_csv(r'./Dundee_not_Clean.csv', index=False)
