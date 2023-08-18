import matplotlib.pyplot as plt
import pandas as pd
import os

def createLineChart2(x1, y1, x2, y2, label1, label2, title, filename):
    plt.plot(x1, y1, label = label1, linestyle="-")
    plt.plot(x2, y2, label = label2, linestyle="--")
    plt.xlabel("dataset")
    plt.ylabel("time elapsed (ms)")
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
        title, filename):
    plt.plot(x1, y1, label = label1, linestyle="--")
    plt.plot(x2, y2, label = label2, linestyle="-.")
    plt.plot(x3, y3, label = label3, linestyle=":")
    plt.xlabel("dataset")
    plt.ylabel("time elapsed (ms)")
    plt.legend()
    plt.title(title)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    #plt.xticks(rotation = 45) 
    plt.savefig(filename+".png")
    #plt.show()
    plt.clf()

def createLineChart4(
        x1, y1, 
        x2, y2, 
        x3, y3, 
        x4, y4, 
        label1, label2, label3, label4, 
        title, filename):
    plt.plot(x1, y1, label = label1, linestyle="-")
    plt.plot(x2, y2, label = label2, linestyle="--")
    plt.plot(x3, y3, label = label3, linestyle="-.")
    plt.plot(x4, y4, label = label4, linestyle=":")
    plt.xlabel("dataset")
    plt.ylabel("time elapsed (ms)")
    plt.legend()
    plt.title(title)
    plt.subplots_adjust(left=0.15, bottom=0.15)
    #plt.xticks(rotation = 45) 
    plt.savefig(filename+".png")
    #plt.show()
    plt.clf()

def comparePersist(df, df_persist, db_labels, dir_path):
    master_types = ["local[1]", "local[1]", "local[3]", "lo1al[4]", "yarn_w2", "yarn_w3", "yarn_w4"]
    algorithms = ["aprioriseq", "apriorispc", "apriorimapre2uce", "fpgrowth"]
    for master in master_types:
        for alg in algorithms:
            query = str('master == "'+ master +'" & algorithm == "'+ alg +'"')
            d = df.query(query)[['dataset', 'time elapsed (ms)']]
            d = d.sort_values(by=['dataset'], ascending=False)
            dp = df_persist.query(query)[['dataset', 'time elapsed (ms)']]
            dp = dp.sort_values(by=['dataset'], ascending=False)
            createLineChart2(
                db_labels, 
                d['time elapsed (ms)'], 
                db_labels, 
                dp['time elapsed (ms)'], 
                "alg", alg+"_persist", 
                alg+" "+master,
                dir_path+"/grafici_confronto_persist/"+alg+"_"+master
            )


def compareAlgorithms(df, df_persist, db_labels, dir_path):
    master_types = ["local[1]", "local[2]", "local[3]", "local[4]", "yarn_w2", "yarn_w3", "yarn_w4"]
    algorithms = ["aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth"]
    for master in master_types:
        alg_list = []
        alg_list_persist = []
        for alg in algorithms:
            query = str('master == "'+ master +'" & algorithm == "'+ alg +'"')
            d = df.query(query)[['dataset', 'time elapsed (ms)']]
            d = d.sort_values(by=['dataset'], ascending=False)
            alg_list.append(d['time elapsed (ms)'])

            dp = df_persist.query(query)[['dataset', 'time elapsed (ms)']]
            dp = dp.sort_values(by=['dataset'], ascending=False)
            alg_list_persist.append(dp['time elapsed (ms)'])

        createLineChart4(
            db_labels, alg_list[0], 
            db_labels, alg_list[1],
            db_labels, alg_list[2], 
            db_labels, alg_list[3],
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            master,
            dir_path+"/grafici_senza_persist/"+master
        )
        createLineChart4(
            db_labels, alg_list_persist[0], 
            db_labels, alg_list_persist[1],
            db_labels, alg_list_persist[2], 
            db_labels, alg_list_persist[3],
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            master,
            dir_path+"/grafici_con_persist/"+master
        )

def strongScalability(df, df_persist, db_labels, dir_path):
    master_types = ["local[1]", "local[2]", "local[3]", "local[4]", "yarn_w2", "yarn_w3", "yarn_w4"]
    algorithms = ["aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth"]

    for db in db_labels:
        alg_list = []
        alg_list_persist = []

        for alg in algorithms:
            query = str('dataset == '+ db +' & algorithm == "'+ alg +'"')
            d = df.query(query)[['master', 'time elapsed (ms)']]
            d = d.sort_values(by=['master'])
            alg_list.append(d['time elapsed (ms)'])

            dp = df_persist.query(query)[['master', 'time elapsed (ms)']]
            dp = dp.sort_values(by=['master'])
            alg_list_persist.append(dp['time elapsed (ms)'])

        createLineChart4(
            master_types, alg_list[0], 
            master_types, alg_list[1],
            master_types, alg_list[2], 
            master_types, alg_list[3],
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            "Strong scalability - "+db,
            dir_path+"/grafici_senza_persist/strong_scalability_"+db
        )

        createLineChart3( 
            master_types, alg_list[1],
            master_types, alg_list[2], 
            master_types, alg_list[3],
            "apriorispc", "apriorimapreduce", "fpgrowth",
            "Strong scalability - "+db,
            dir_path+"/grafici_senza_persist/strong_scalability_"+db+"_noSeq"
        )
         
        createLineChart4(
            master_types, alg_list_persist[0], 
            master_types, alg_list_persist[1],
            master_types, alg_list_persist[2], 
            master_types, alg_list_persist[3],
            "aprioriseq", "apriorispc", "apriorimapreduce", "fpgrowth",
            "Strong scalability - "+db,
            dir_path+"/grafici_con_persist/strong_scalability_"+db
        )

        createLineChart3( 
            master_types, alg_list_persist[1],
            master_types, alg_list_persist[2], 
            master_types, alg_list_persist[3],
            "apriorispc", "apriorimapreduce", "fpgrowth",
            "Strong scalability - "+db,
            dir_path+"/grafici_con_persist/strong_scalability_"+db+"_noSeq"
        )
        


if __name__ == "__main__":
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db_labels = ["2010", "2000", "1990", "1980"]
    csv_file_persist = dir_path + "/test_results_con_persist.csv"
    csv_file = dir_path + "/test_results_senza_persist.csv"
    df_persist = pd.read_csv(csv_file_persist)
    df = pd.read_csv(csv_file)
    comparePersist(df, df_persist, db_labels, dir_path)
    compareAlgorithms(df, df_persist, db_labels, dir_path)
    strongScalability(df, df_persist, db_labels, dir_path)