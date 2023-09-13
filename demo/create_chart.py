import pandas as pd
import os
import matplotlib.pyplot as plt
import numpy as np

dir_path = os.path.dirname(os.path.realpath(__file__))

def editTimesFile():
    inputDirs = os.listdir(dir_path + "/demo_output")
    for dir in inputDirs:
        inputFiles = os.listdir(dir_path + "/demo_output/" + dir +"/times/")
        for file in inputFiles:
            if(file.startswith("part")):
                filePath = dir_path + "/demo_output/" + dir +"/times/" + file
                df = pd.read_csv(filePath)
                strList = dir.split("_")
                newMaster = strList[1] +"_" + strList[2] +"_" + strList[3]
                df['master'] = newMaster
                df.to_csv(filePath, index=False)

def mergeResult():
    csv_path_input = dir_path + "/demo_output/"
    fileNameOutput = "result_db.csv"

    # leggo tutti i nomi dei file di input e creo un unico dataframe
    sumRows = 0
    listCSV = []
    inputDirs = os.listdir(csv_path_input)
    db_mapping = {
        'dataset_4.csv': 4,
        'dataset_3.csv': 3,
        'dataset_2.csv': 2,
        'dataset_1.csv': 1
    }
    alg_mapping = {
        'aprioriseq': 'AprioriSeq',
        'aprioritailrec': 'AprioriTailRec',
        'apriorimapreduce': 'AprioriMapReduce',
        'fpgrowth': 'FPGrowth'
    }
    j = 0
    tot = len(inputDirs)
    for dir in inputDirs:
        inputFiles = os.listdir(csv_path_input + dir +"/times/")
        for file in inputFiles:
            if file.endswith('.csv'):
                df_read = pd.read_csv(csv_path_input + dir + "/times/" + file)
                df_read["dataset"] = df_read["dataset"].map(lambda x: db_mapping[x])
                df_read["algorithm"] = df_read["algorithm"].map(lambda x: alg_mapping[x])
                listCSV.append(df_read)
                sumRows = sumRows + df_read.shape[0]
                j = j + 1
                print('Reading : '+str(j) +' / ' +str(tot), end='\r')

    df = pd.concat(listCSV, ignore_index=True)
    createBarChart(df)

def convertToSecond(x):
    outputList = []
    for elem in x:
        outputList.append(elem / 1000)
    return outputList

def createBarChart(df):    
    df = df.sort_values(by=['time elapsed (ms)'], ascending=False)
    x = df['algorithm']
    y = convertToSecond(df['time elapsed (ms)'])
    barWidth = 0.2

    plt.bar(x, y, color ='blue', width=barWidth, edgecolor='grey')
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.xlabel("Algorithms")
    plt.ylabel("Time (s)")
    plt.title("")
    plt.savefig(dir_path + "/barChart.png")
    plt.clf()


if __name__ == "__main__":
    editTimesFile()
    mergeResult()
    
    
