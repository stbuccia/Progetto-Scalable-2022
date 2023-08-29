import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db_labels = ["2010", "2000", "1990", "1980"]
    db_size = [100000, 200000, 400000, 800000]

    for i in range(4):
        csv_file = dir_path + "/dataset_from_"+ db_labels[i] +".csv"
        df = pd.read_csv(csv_file)
        df = df.sort_values(by=['year'], ascending=False)
        df = df.head(db_size[i])
        df.to_csv(dir_path +'/dataset_'+ str(i+1) +'.csv', index=False)
    