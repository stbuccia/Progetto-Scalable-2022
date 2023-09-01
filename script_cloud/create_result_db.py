import pandas as pd
import os

def editTimesFile():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    inputDirs = os.listdir(dir_path + "/output")
    for dir in inputDirs:
        inputFiles = os.listdir(dir_path + "/output/" + dir +"/times/")
        for file in inputFiles:
            if(file.startswith("part")):
                filePath = dir_path + "/output/" + dir +"/times/" + file
                df = pd.read_csv(filePath)
                strList = dir.split("_")
                newMaster = strList[1] +"_" + strList[2] +"_" + strList[3]
                df['master'] = newMaster
                df.to_csv(filePath, index=False)

def mergeResult():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    csv_path_input = dir_path + "/output/"
    fileNameOutput = "result_db.csv"

    # leggo tutti i nomi dei file di input e creo un unico dataframe
    sumRows = 0
    listCSV = []
    inputDirs = os.listdir(csv_path_input)
    target_mapping = {
        'dataset_4.csv': 4,
        'dataset_3.csv': 3,
        'dataset_2.csv': 2,
        'dataset_1.csv': 1
    }
    j = 0
    tot = len(inputDirs)
    for dir in inputDirs:
        inputFiles = os.listdir(csv_path_input + dir +"/times/")
        for file in inputFiles:
            if file.endswith('.csv'):
                #print("Read file: ", csv_path_input + dir + "/times/" + file)
                df_read = pd.read_csv(csv_path_input + dir + "/times/" + file)
                df_read["dataset"] = df_read["dataset"].map(lambda x: target_mapping[x])
                listCSV.append(df_read)
                sumRows = sumRows + df_read.shape[0]
                j = j + 1
                print('Reading : '+str(j) +' / ' +str(tot), end='\r')

    df = pd.concat(listCSV, ignore_index=True)

    #print("Numero di file letti = ", len(listCSV))
    #print("Numero totale righe = ", df.shape[0])
    #print( sumRows == df.shape[0] )

    # scrivi file di output
    df.to_csv(dir_path + "/" + fileNameOutput, index=False) 
    print("Nuovo file creato: ", dir_path + "/" + fileNameOutput)




if __name__ == "__main__":
    doEditResult = True
    doMergeResult = True
    
    if(doEditResult): editTimesFile()
    if(doMergeResult): mergeResult()
    