import pandas as pd
import matplotlib.pyplot as plt

#csv_path_input = "dataset/dataset_completo.csv"
csv_path_input = "download/1990-01.csv"
csv_path_output = "dataset/"

df = pd.read_csv(csv_path_input)

print("Numero totale righe = ", df.shape[0], "\n")
print("Colonne: ", df.columns, "\n")

print("min mag", df["mag"].min, "\n")
print("max mag", df["mag"].max, "\n")
print("mag", df["mag"].value_counts, "\n")
df['mag'].plot(kind='kde')
df['mag'].plot(kind='hist', edgecolor='black')

print("magType", df["magType"].value_counts, "\n")
df['magType'].plot(kind='kde')
df['magType'].plot(kind='hist', edgecolor='black')
