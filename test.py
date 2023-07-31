#!/bin/python3
import csv
import matplotlib.pyplot as plt

def removeDuplicatesInLegend():
    handles, labels = plt.gca().get_legend_handles_labels()
    newLabels, newHandles = [], []
    for handle, label in zip(handles, labels):
      if label not in newLabels:
        newLabels.append(label)
        newHandles.append(handle)
    plt.legend(newHandles, newLabels)


def plot_csv_data(csv_file):
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
        'dataset_from_2020_01_to_2021_12.csv' : 60587,
        'dataset_from_2010_01_to_2021_12.csv' : 322585,
        'dataset_from_1990_01_to_2022_12.csv' : 773630
    }

    with open(csv_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            x_coords.append((lines_number[row['dataset']]))
            y_coords.append(float(row['time elapsed (ms)']))
            plot_types.append(row['algorithm'])

    plt.figure(figsize=(10, 6))

    for plot_type in set(plot_types):
        x_plot = [x_coords[j] for j in range(len(plot_types)) if plot_types[j] == plot_type]
        y_plot = [y_coords[j] for j in range(len(plot_types)) if plot_types[j] == plot_type]
        plt.plot(x_plot, y_plot, 1, label=plot_type, color=colors[plot_type])

    plt.xlabel('Rows in dataset')
    plt.ylabel('Elapsed time in ms')
    plt.title('Elapsed time by Apriori Algorithm')
    removeDuplicatesInLegend()
    plt.grid(True)

    plt.show()

if __name__ == "__main__":
    csv_file = "src/main/resources/times/times.csv"
    plot_csv_data(csv_file)
