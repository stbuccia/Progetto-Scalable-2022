import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os

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
    #plt.xticks(rotation = 45) 
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
    #plt.xticks(rotation = 45) 
    plt.savefig(filename+".png")
    plt.clf()



def compareAlgorithms(df, db_labels, master_types, algorithms, dir_path):
    
    

    for master in master_types:
        alg_list = []

        for alg in algorithms:
            query = str('master == "'+ master +'" & algorithm == "'+ alg +'"')
            d = df.query(query)[['dataset', 'time elapsed (ms)']]
            d = d.sort_values(by=['dataset'], ascending=True)
            alg_list.append(d['time elapsed (ms)'])

        
        createLineChart4(
            db_labels, alg_list[0], 
            db_labels, alg_list[1],
            db_labels, alg_list[2], 
            db_labels, alg_list[3],
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            "dataset",
            "time (ms)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/"+fromMasterToCore(master)+"_cores"
        )

        createLineChart3(
            db_labels, alg_list[1],
            db_labels, alg_list[2], 
            db_labels, alg_list[3],
            "apriorispc", "apriorimapreduce", "fpgrowth",
            "dataset",
            "time (ms)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/"+fromMasterToCore(master)+"_cores_noSeq"
        )

        for i in range(4):
            createLineChart1(
                db_labels, alg_list[i], 
                algorithms[i],
                "dataset",
                "time (ms)",
                algorithms[i] + " - " +fromMasterToCore(master)+" cores",
                dir_path+"/grafici_algoritmi/"+fromMasterToCore(master)+"_cores_"+algorithms[i]
            )
        

        # in secondi
        secondi = convertListToSecond(alg_list)

        createLineChart4(
            db_labels, secondi[0], 
            db_labels, secondi[1],
            db_labels, secondi[2], 
            db_labels, secondi[3],
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            "dataset",
            "time (s)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/sec_"+fromMasterToCore(master)+"_cores"
        )

        createLineChart3(
            db_labels, secondi[1],
            db_labels, secondi[2], 
            db_labels, secondi[3],
            "apriorispc", "apriorimapreduce", "fpgrowth",
            "dataset",
            "time (s)",
            fromMasterToCore(master)+" cores",
            dir_path+"/grafici_algoritmi/sec_"+fromMasterToCore(master)+"_cores_noSeq"
        )

        for i in range(4):
            createLineChart1(
                db_labels, secondi[i], 
                algorithms[i],
                "dataset",
                "time (s)",
                algorithms[i] + " - " +fromMasterToCore(master)+" cores",
                dir_path+"/grafici_algoritmi/sec_"+fromMasterToCore(master)+"_cores_"+algorithms[i]
            )
        
        
        

def strongScalability(df, db_labels, master_types, algorithms, dir_path):
    n_core = ["2", "4", "8", "16"]

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
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            "# core",
            "time (ms)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/strong_scalability_db"+db
        )

        createLineChart3( 
            n_core, alg_list[1],
            n_core, alg_list[2], 
            n_core, alg_list[3],
            "apriorispc", "apriorimapreduce", "fpgrowth",
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
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            "# core",
            "time (s)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/sec_strong_scalability_db"+db
        )

        createLineChart3( 
            n_core, secondi[1],
            n_core, secondi[2], 
            n_core, secondi[3],
            "apriorispc", "apriorimapreduce", "fpgrowth",
            "# core",
            "time (s)",
            "Strong scalability - db"+db,
            dir_path+"/grafici_strong_scalability/sec_strong_scalability_db"+db+"_noSeq"
        )


        
        
        
        

def weakScalability(df, db_labels, master_types, algorithms, dir_path):
    n_core = ["2", "4", "8", "16"]
    alg_list = []

    for alg in algorithms:
        times = []

        for i in range(4):
            query = str('dataset == '+ db_labels[i] +' & algorithm == "'+ alg +'" & master == "'+ master_types[i] +'"' )
            x = df.query(query)['time elapsed (ms)']
            times.append(x._get_value(0, 'time elapsed (ms)'))

        alg_list.append(times)


        """ ar = np.array(times)
        ar_persist = np.array(times_persist)
        print(alg,"senza", ar)
        print(alg, "con", ar_persist)  """
    
        
        createLineChart1( 
            n_core, times,
            alg, 
            "# core",
            "time (ms)",
            "Weak scalability - "+alg,
            dir_path+"/grafici_weak_scalability/weak_scalability_"+alg
        )   

        # in secondi
        secondi = convertToSecond(times)
        createLineChart1( 
            n_core, secondi,
            alg, 
            "# core",
            "time (s)",
            "Weak scalability - "+alg,
            dir_path+"/grafici_weak_scalability/sec_weak_scalability_"+alg
        )   

    
    createLineChart4(
        n_core, alg_list[0], 
        n_core, alg_list[1],
        n_core, alg_list[2], 
        n_core, alg_list[3],
        "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
        "# core",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/weak_scalability"
    )

    createLineChart3(
        n_core, alg_list[1],
        n_core, alg_list[2], 
        n_core, alg_list[3],
        "apriorispc", "apriorimapreduce", "fpgrowth",
        "# core",
        "time (ms)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/weak_scalability_noSeq"
    )

    # in secondi
    secondi = convertListToSecond(alg_list)
    createLineChart4(
        n_core, secondi[0], 
        n_core, secondi[1],
        n_core, secondi[2], 
        n_core, secondi[3],
        "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
        "# core",
        "time (s)",
        "Weak scalability",
        dir_path+"/grafici_weak_scalability/sec_weak_scalability"
    )

    createLineChart3(
        n_core, secondi[1],
        n_core, secondi[2], 
        n_core, secondi[3],
        "apriorispc", "apriorimapreduce", "fpgrowth",
        "# core",
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


if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db_labels = ["1", "2", "3", "4"]
    master_types = ["yarn_w2_c1", "yarn_w2_c2", "yarn_w4_c2", "yarn_w4_c4"]
    algorithms = ["aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth"]

    csv_file = dir_path + "/test_results.csv"
    df = pd.read_csv(csv_file)
    compareAlgorithms(df, db_labels, master_types, algorithms, dir_path)
    strongScalability(df, db_labels, master_types, algorithms, dir_path)
    weakScalability(df, db_labels, master_types, algorithms, dir_path)