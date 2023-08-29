import pandas as pd
import os

def findNumWorker(dirName):
    if("w2" in dirName):
        return "w2"
    if("w3" in dirName):
        return "w3"
    if("w4" in dirName):
        return "w4"
        


dir_path = os.path.dirname(os.path.realpath(__file__))
csv_path_input = dir_path + "/results/"
fileNameOutput = "test_results_senza_persist.csv"



# leggo tutti i nomi dei file di input e creo un unico dataframe
sumRows = 0
listCSV = []
inputDirs = os.listdir(csv_path_input)
#print("inputDirs", len(inputDirs), ":", inputDirs)
target_mapping = {
    'dataset_from_1980.csv': 1980,
    'dataset_from_1990.csv': 1990,
    'dataset_from_2000.csv': 2000,
    'dataset_from_2010.csv': 2010
}
j = 0
tot = len(inputDirs)
for dir in inputDirs:
    inputFiles = os.listdir(csv_path_input + dir +"/times/")
    #print("inputFiles", len(inputFiles), ":", inputFiles)
    for file in inputFiles:
        if file.endswith('.csv'):
            print("Read file: ", csv_path_input + dir + "/times/" + file)
            df_read = pd.read_csv(csv_path_input + dir + "/times/" + file)
            
            df_read["dataset"] = df_read["dataset"].map(lambda x: target_mapping[x])

            if("yarn" in dir):
                n_worker = findNumWorker(dir)
                df_read['master'] = "yarn_" + n_worker
                

            listCSV.append(df_read)
            sumRows = sumRows + df_read.shape[0]
            j = j + 1
            print('Reading : '+str(j) +' / ' +str(tot), end='\r')

df = pd.concat(listCSV, ignore_index=True)

print("Numero di file letti = ", len(listCSV))
print("Numero totale righe = ", df.shape[0])
print( sumRows == df.shape[0] )

print(df)

# scrivi file di output
df.to_csv(dir_path + "/" + fileNameOutput, index=False) 
print("Nuovo file creato: ", dir_path + "/" + fileNameOutput)