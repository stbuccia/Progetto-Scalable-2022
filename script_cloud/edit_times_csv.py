import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

if __name__ == "__main__":
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



