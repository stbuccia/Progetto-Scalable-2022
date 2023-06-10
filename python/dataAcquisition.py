from fileinput import filename
import pandas as pd
import os

sumRows = 0

def Diff(li1, li2):
    return list(set(li1) - set(li2)) + list(set(li2) - set(li1))

csv_path_input = "download/"
csv_path_output = "dataset/"

# leggo tutti i nomi dei file di input e creo un unico dataframe
listCSV = []
inputFiles = os.listdir(csv_path_input)
j = 0
tot = len(inputFiles)
for f in inputFiles:
    df_read = pd.read_csv(csv_path_input + f)
    listCSV.append(df_read)
    sumRows = sumRows + df_read.shape[0]
    j = j + 1
    print('Reading : '+str(j) +' / ' +str(tot), end='\r')

df = pd.concat(listCSV, ignore_index=True)

print("Numero di file letti = ", len(listCSV))
print("Numero totale righe = ", df.shape[0])
print( sumRows == df.shape[0] )


# aggiungo una nuova colonna 'year'
df['year'] = df['time'].apply(lambda x: x[:4])

# elimino le colonne inutili
labels = df.columns
labelsToKeep = ['year', 'latitude', 'longitude', 'depth', 'mag']
labelsToDrop = Diff(labels, labelsToKeep)
df.drop(labels=labelsToDrop, axis=1, inplace=True)

# scrivi file di output
start = inputFiles[0][:7].replace('-','_')
end = inputFiles[len(inputFiles)-1][:7].replace('-','_')
#fileName = 'dataset_from_' +start +'_to_' +end +'.csv'
fileName = 'test_2.5.csv'
df.to_csv(csv_path_output + fileName, index=False) 
print("Nuovo file creato: ", csv_path_output + fileName)