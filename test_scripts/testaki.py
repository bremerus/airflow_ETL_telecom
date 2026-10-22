import pandas as pd
import time

df = pd.read_csv('telecom_churn.csv')


lista2 = df['dependents'].unique().tolist()
print(lista2)


df['dependents'] = df['dependents'].str.strip().str.lower().replace({'yes' : 1, 'y' : 1, 'no' : 0, 'n' : 0})




lista2 = df['dependents'].unique().tolist()
print(lista2)