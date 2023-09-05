import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
db_labels = ["1", "2", "3", "4"]
db_size = ["100000", "200000", "400000", "800000"]
algorithms = ["AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth"]
master_types = ["yarn_w2_c1", "yarn_w2_c2", "yarn_w4_c2", "yarn_w4_c4"]
n_core = ["2", "4", "8", "16"]


def createBarChart1(x1, y1, label1, xlabel, ylabel, title, filename):
    colors = {'100000':'red', '200000':'green', '400000':'blue', '800000':'#FFD700'}         
    labels = list(colors.keys())
    handles = [plt.Rectangle((0,0),1,1, color=colors[label]) for label in labels]
    plt.legend(handles, labels,  title="Db size")

    for i in range(0,4):
        plt.bar(x1[i], y1[i], color=colors[db_size[i]], label=label1, width = 0.5)

    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(filename+".png")
    plt.clf()


def createBarChart3(
        x1, y1, 
        x2, y2, 
        x3, y3, 
        label1, label2, label3,  
        xlabel,
        ylabel,
        title, filename):
     
    algorithmsMap = {'AprioriTailRec':'green', 'AprioriMapReduce':'blue', 'FPGrowth':'#FFD700'}         
    labels = algorithms[1:]
    handles = [plt.Rectangle((0,0),1,1, color=algorithmsMap[label]) for label in labels]
    plt.legend(handles, labels,  title="Algorithms")

    barWidth = 0.2
    br1 = np.arange(len(y1))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]
    
    plt.bar(br1, y1, color ='green', width=barWidth, edgecolor='grey', label=label1)
    plt.bar(br2, y2, color ='blue', width=barWidth, edgecolor='grey', label=label2)
    plt.bar(br3, y3, color ='#FFD700', width=barWidth, edgecolor='grey', label=label3)
    
    plt.xticks([r + barWidth for r in range(len(y1))], x1)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(filename+".png")
    plt.clf()


def createBarChart4(
        x1, y1, 
        x2, y2, 
        x3, y3, 
        x4, y4, 
        label1, label2, label3, label4, 
        xlabel,
        ylabel,
        title, filename):
     
    algorithmsMap = {'AprioriSeq':'red', 'AprioriTailRec':'green', 'AprioriMapReduce':'blue', 'FPGrowth':'#FFD700'}      
    labels = algorithms
    handles = [plt.Rectangle((0,0),1,1, color=algorithmsMap[label]) for label in labels]
    plt.legend(handles, labels,  title="Algorithms")

    barWidth = 0.2
    br1 = np.arange(len(y1))
    br2 = [x + barWidth for x in br1]
    br3 = [x + barWidth for x in br2]
    br4 = [x + barWidth for x in br3]
    
    plt.bar(br1, y1, color ='red', width=barWidth, edgecolor='grey', label=label1)
    plt.bar(br2, y2, color ='green', width=barWidth, edgecolor='grey', label=label2)
    plt.bar(br3, y3, color ='blue', width=barWidth, edgecolor='grey', label=label3)
    plt.bar(br4, y4, color ='#FFD700', width=barWidth, edgecolor='grey', label=label4)
    
    plt.xticks([r + barWidth for r in range(len(y1))], x1)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.savefig(filename+".png")
    plt.clf()


def createLineChart1(x1, y1, label1, xlabel, ylabel, title, filename):
    plt.plot(x1, y1, label = label1, linestyle="-", marker = 'o')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.title(title)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.savefig(filename+".png")
    plt.clf()


def createLineChart3(
        x1, y1, 
        x2, y2, 
        x3, y3,  
        label1, label2, label3, 
        xlabel,
        ylabel,
        title, filename):
    plt.plot(x1, y1, label = label1, linestyle="-", marker = 'o')
    plt.plot(x2, y2, label = label2, linestyle="--", marker = 's')
    plt.plot(x3, y3, label = label3, linestyle="-.", marker = 'v')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.title(title)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.savefig(filename+".png")
    plt.clf()


def createLineChart4(
        x1, y1, 
        x2, y2, 
        x3, y3, 
        x4, y4, 
        label1, label2, label3, label4, 
        xlabel,
        ylabel,
        title, filename):
    plt.plot(x1, y1, label = label1, linestyle="-", marker = 'o')
    plt.plot(x2, y2, label = label2, linestyle="--", marker = 's')
    plt.plot(x3, y3, label = label3, linestyle="-.", marker = 'v')
    plt.plot(x4, y4, label = label4, linestyle=":", marker = 'x')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.title(title)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    plt.savefig(filename+".png")
    plt.clf()


def compareAlgorithms(df):
    
    for master in master_types:
        alg_list = []

        for alg in algorithms:
            query = str('master == "'+ master +'" & algorithm == "'+ alg +'"')
            d = df.query(query)[['dataset', 'time elapsed (ms)']]
            d = d.sort_values(by=['dataset'], ascending=True)
            alg_list.append(d['time elapsed (ms)'])

        
        createLineChart4(
            db_size, alg_list[0], 
            db_size, alg_list[1],
            db_size, alg_list[2], 
            db_size, alg_list[3],
            "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "dataset",
            "time (ms)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/"+fromMasterToCore(master)+"_cores"
        )

        createLineChart3(
            db_size, alg_list[1],
            db_size, alg_list[2], 
            db_size, alg_list[3],
            "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "dataset",
            "time (ms)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/"+fromMasterToCore(master)+"_cores_noSeq"
        )

        for i in range(4):
            createLineChart1(
                db_size, alg_list[i], 
                algorithms[i],
                "dataset",
                "time (ms)",
                algorithms[i] + " - " +fromMasterToCore(master)+" cores",
                dir_path+"/grafici_algoritmi/"+fromMasterToCore(master)+"_cores_"+algorithms[i]
            )
        

        # in secondi
        secondi = convertListToSecond(alg_list)

        createLineChart4(
            db_size, secondi[0], 
            db_size, secondi[1],
            db_size, secondi[2], 
            db_size, secondi[3],
            "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "dataset",
            "time (s)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/sec_"+fromMasterToCore(master)+"_cores"
        )

        createLineChart3(
            db_size, secondi[1],
            db_size, secondi[2], 
            db_size, secondi[3],
            "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "dataset",
            "time (s)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/sec_"+fromMasterToCore(master)+"_cores_noSeq"
        )

        for i in range(4):
            createLineChart1(
                db_size, secondi[i], 
                algorithms[i],
                "dataset",
                "time (s)",
                algorithms[i] + " - " +fromMasterToCore(master)+" cores",
                dir_path+"/grafici_algoritmi/sec_"+fromMasterToCore(master)+"_cores_"+algorithms[i]
            )
       

def strongScalability(df):

    for db in db_labels:
        alg_list = []

        for alg in algorithms:
            query = str('dataset == '+ db +' & algorithm == "'+ alg +'"')
            d = df.query(query)[['master', 'time elapsed (ms)']]
            d = d.sort_values(by=['master'])
            alg_list.append(d['time elapsed (ms)'])


        for i in range(4):
            createLineChart1(
                n_core, alg_list[i], 
                algorithms[i],
                "# core",
                "time (ms)",
                "Strong scalability - db"+db +" - " +algorithms[i],
                dir_path+"/grafici_strong_scalability/strong_scalability_db"+db+"_"+algorithms[i]
            )
        
        createLineChart4(
            n_core, alg_list[0], 
            n_core, alg_list[1],
            n_core, alg_list[2], 
            n_core, alg_list[3],
            "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "# core",
            "time (ms)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/strong_scalability_db"+db
        )

        createLineChart3( 
            n_core, alg_list[1],
            n_core, alg_list[2], 
            n_core, alg_list[3],
            "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "# core",
            "time (ms)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/strong_scalability_db"+db+"_noSeq"
        )

        # in secondi
        secondi = convertListToSecond(alg_list)

        for i in range(4):
            createLineChart1(
                n_core, secondi[i], 
                algorithms[i],
                "# core",
                "time (s)",
                "Strong scalability - db"+db +" - " +algorithms[i],
                dir_path+"/grafici_strong_scalability/sec_strong_scalability_db"+db+"_"+algorithms[i]
            )

        createLineChart4(
            n_core, secondi[0], 
            n_core, secondi[1],
            n_core, secondi[2], 
            n_core, secondi[3],
            "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "# core",
            "time (s)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/sec_strong_scalability_db"+db
        )

        createLineChart3( 
            n_core, secondi[1],
            n_core, secondi[2], 
            n_core, secondi[3],
            "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
            "# core",
            "time (s)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/sec_strong_scalability_db"+db+"_noSeq"
        )
       

def weakScalability(df):
    
    nCore_dbSize = list(map(lambda i: str("("+str(n_core[i])+", "+db_size[i]+")"), range(0,4)))
    alg_list = []

    for alg in algorithms:
        times = []

        for i in range(4):
            query = str('dataset == '+ db_labels[i] +' & algorithm == "'+ alg +'" & master == "'+ master_types[i] +'"' )
            x = df.query(query)['time elapsed (ms)']
            times.append(x._get_value(0, 'time elapsed (ms)'))

        alg_list.append(times)

        createBarChart1( 
            n_core, times,
            alg, 
            "# core",
            "time (ms)",
            "Weak scalability - "+alg,
            dir_path+"/grafici_weak_scalability/bar_weak_scalability_"+alg
        )   

        createLineChart1( 
            nCore_dbSize, times,
            alg, 
            "(# core, db size)",
            "time (ms)",
            "Weak scalability - "+alg,
            dir_path+"/grafici_weak_scalability/weak_scalability_"+alg
        )   
     
        # in secondi
        secondi = convertToSecond(times)
        createBarChart1( 
            n_core, secondi,
            alg, 
            "# core",
            "time (ms)",
            "Weak scalability - "+alg,
            dir_path+"/grafici_weak_scalability/sec_bar_weak_scalability_"+alg
        )   

        createLineChart1( 
            nCore_dbSize, secondi,
            alg, 
            "(# core, db size)",
            "time (s)",
            "Weak scalability - "+alg,
            dir_path+"/grafici_weak_scalability/sec_weak_scalability_"+alg
        )   

    createBarChart4(
        nCore_dbSize, alg_list[0], 
        nCore_dbSize, alg_list[1],
        nCore_dbSize, alg_list[2], 
        nCore_dbSize, alg_list[3],
        "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/bar_weak_scalability"
    )

    createBarChart3(
        nCore_dbSize, alg_list[1],
        nCore_dbSize, alg_list[2], 
        nCore_dbSize, alg_list[3],
        "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/bar_weak_scalability_noSeq"
    )
  
    createLineChart4(
        nCore_dbSize, alg_list[0], 
        nCore_dbSize, alg_list[1],
        nCore_dbSize, alg_list[2], 
        nCore_dbSize, alg_list[3],
        "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/weak_scalability"
    )

    createLineChart3(
        nCore_dbSize, alg_list[1],
        nCore_dbSize, alg_list[2], 
        nCore_dbSize, alg_list[3],
        "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/weak_scalability_noSeq"
    )

    # in secondi
    secondi = convertListToSecond(alg_list)
    createBarChart4(
        nCore_dbSize, secondi[0], 
        nCore_dbSize, secondi[1],
        nCore_dbSize, secondi[2], 
        nCore_dbSize, secondi[3],
        "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/sec_bar_weak_scalability"
    )

    createBarChart3(
        nCore_dbSize, secondi[1],
        nCore_dbSize, secondi[2], 
        nCore_dbSize, secondi[3],
        "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/sec_bar_weak_scalability_noSeq"
    )

    createLineChart4(
        nCore_dbSize, secondi[0], 
        nCore_dbSize, secondi[1],
        nCore_dbSize, secondi[2], 
        nCore_dbSize, secondi[3],
        "AprioriSeq", "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (s)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/sec_weak_scalability"
    )

    createLineChart3(
        nCore_dbSize, secondi[1],
        nCore_dbSize, secondi[2], 
        nCore_dbSize, secondi[3],
        "AprioriTailRec", "AprioriMapReduce", "FPGrowth",
        "(# core, db size)",
        "time (s)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/sec_weak_scalability_noSeq"
    )

    
def convertListToSecond(x):
    outputList = []
    for elem in x:
        outputList.append(convertToSecond(elem))
    return outputList


def convertToSecond(x):
    outputList = []
    for elem in x:
        outputList.append(elem / 1000)
    return outputList


def fromMasterToCore(master):
    if("w2_c1" in master):
        return "2"
    if("w2_c2" in master):
        return "4"
    if("w4_c2" in master):
        return "8"
    if("w4_c4" in master):
        return "16"


def computeResultFilePath():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    pathArray = dir_path.split("/")
    pathArray = pathArray[:len(pathArray)-1]
    resFilePath = ('/'.join(x for x in pathArray)) + "/script_cloud/result_db.csv"
    return resFilePath



if __name__ == "__main__":
    csv_file = computeResultFilePath()
    df = pd.read_csv(csv_file)
    compareAlgorithms(df)
    strongScalability(df)
    weakScalability(df)