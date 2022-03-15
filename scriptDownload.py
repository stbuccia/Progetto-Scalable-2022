import os
from urllib import request
from urllib.error import HTTPError, URLError

startYear = 2020
endYear = 2021

local_path = 'download/'
if not os.path.exists(local_path):
    os.makedirs(local_path)

errorFile = []
j = 0
tot = (endYear - startYear + 1) * 12
print('Downloading : 0' +' / ' +str(tot), end='\r')

listMM = ['01','02','03','04','05','06','07','08','09','10','11','12']
dd = '01'


for year in range(startYear, endYear+1):
    for month in range(len(listMM)):
        startTime = str(year) +'-' +listMM[month] +'-' +dd
        if( month != 11 ):
            endTime = str(year) +'-' +listMM[ (month+1)%12 ] +'-' +dd
        else:
            endTime = str(year+1) +'-' +listMM[ (month+1)%12 ] +'-' +dd

        # Define the remote file to retrieve
        remote_url = 'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=' +startTime +'&endtime=' +endTime +'&eventtype=earthquake&minmagnitude=2.5'

        # Define the local filename to save data
        fileName = str(year) +'-' +listMM[month] +'.csv'

        # Download remote and save locally
        try:
            (filename, headers) = request.urlretrieve(remote_url, local_path + fileName)
        except (HTTPError,URLError) as e:
            errorFile.append("Failed to download '{}'. '{}'".format(remote_url, e.reason))

        j = j + 1
        print('Downloading : '+str(j) +' / ' +str(tot), end='\r')

# Print errors
for i in range(len(errorFile)):
    print( errorFile[i])

