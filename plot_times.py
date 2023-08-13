#!/bin/python3

# Before execution it is necessary to cat .csv files in src/main/resources/times 
# to new times.csv file

import csv
import sys
import matplotlib.pyplot as plt

def removeDuplicatesInLegend():
    handles, labels = plt.gca().get_legend_handles_labels()
    newLabels, newHandles = [], []
    for handle, label in zip(handles, labels):
      if label not in newLabels:
        newLabels.append(label)
        newHandles.append(handle)
    plt.legend(newHandles, newLabels)


def plot_csv_data(csv_file, x_data, x_label):
    x_coords = []
    y_coords = []
    plot_types = []

    colors = {
        "aprioriseq" : "blue",
        "fpgrowth": "orange",
        "apriorispc": "green",
        "apriorimapreduce": "pink"
    }

    lines_number = {
        'dataset_2020.csv' : 60587,
        'dataset_2010.csv' : 322585,
        'dataset_1990.csv' : 773630
    }

    with open(csv_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            #x_coords.append((lines_number[row['dataset']]))
            x_coords.append(row[x_data])
            y_coords.append(float(row['time elapsed (ms)']))
            plot_types.append(row['algorithm'])

    plt.figure(figsize=(10, 6))

    for plot_type in set(plot_types):
        x_plot = [x_coords[j] for j in range(len(plot_types)) if plot_types[j] == plot_type]
        y_plot = [y_coords[j] for j in range(len(plot_types)) if plot_types[j] == plot_type]
        plt.plot(x_plot, y_plot, 1, label=plot_type, color=colors[plot_type])

    #plt.xlabel('Rows in dataset')
    plt.xlabel(x_label)
    plt.ylabel('Elapsed time in ms')
    plt.title('Elapsed time by Apriori Algorithm')
    removeDuplicatesInLegend()
    plt.grid(True)

    plt.show()

if __name__ == "__main__":
#     csv_file = "src/main/resources/times/times.csv"
    if len(sys.argv) < 2:
        print("You must specify a .csv file for plot!")
        sys.exit()
    csv_file = sys.argv[1]
    x_data = sys.argv[2]
    x_label = sys.argv[3]
    plot_csv_data(csv_file, x_data, x_label)
