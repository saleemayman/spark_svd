import glob, os, sys
import numpy as np
from random import*
import matplotlib.pyplot as plt
import matplotlib.cm as cmx
import matplotlib.colors as colors

def get_rand_color(val):
    h,s,v = random()*6, 0.5, 243.2
    colors = []
    for i in range(val):
        h += 3.75#3.708
        tmp = ((v, v-v*s*abs(1-h%2), v-v*s)*3)[5**int(h)/3%3::int(h)%2+1][:3]
        colors.append('#' + '%02x' *3%tmp)
        if i%5/4:
            s += 0.1
            v -= 51.2
    return colors

def main(argv):
    resultsCSV = sys.argv[1]
    plotTitle = sys.argv[2]
    print resultsCSV
    
    # get csv file from current directory
    data = np.genfromtxt(resultsCSV, delimiter = ',')
    with open(resultsCSV, 'r') as file:
        headers = np.asarray(file.readline().strip().split(','))

    dataDim = data.shape
    data = data[1:dataDim[0], :]
    idxStage = int(np.where(headers == 'stage')[0][0])
    idxTask = int(np.where(headers == 'task_id')[0][0])
    idxStart = int(np.where(headers == 'start')[0][0])
    idxFinish = int(np.where(headers == 'finish')[0][0])
    idxRunTime = int(np.where(headers == 'run_time')[0][0])
    idxDeserial = int(np.where(headers == 'deserial')[0][0])
    idxSerial = int(np.where(headers == 'serial')[0][0])
    idxJVMGC = int(np.where(headers == 'jvm_gc')[0][0])
    stages = np.unique(data[:, idxStage ])
    numStages = len(stages)

    cmap = get_rand_color(len(headers))
    fig = plt.figure()
    ax = fig.add_subplot(111)

    labels = ['Deserialization', 'Serialization', 'JVM GC', 'Task Run Time', 'Stage Time']
    for i in range(0, 100):
        stage = stages[i]
        stageData = data[ np.where(data[:, idxStage] == stage)[0], :]
        tasks = stageData[:, idxTask]
        avgStageTime = np.mean(stageData[:, idxFinish] - stageData[:, idxStart])/1000
        avgDeserialTime = np.mean(stageData[:, idxDeserial])/1000
        avgSerialTime = np.mean(stageData[:, idxSerial])/1000
        avgTaskRunTime = np.mean(stageData[:, idxRunTime])/1000
        avgJVMGCTime = np.mean(stageData[:, idxJVMGC])/1000
        avgStageData = [avgStageTime, avgDeserialTime, avgSerialTime, avgJVMGCTime, avgTaskRunTime]

        yStart = 0
        ax.bar(i, avgStageTime, 0.5, bottom=yStart, align='center', color=cmap[0], label=labels[j])
        for j in range(1, len(labels)):
            ax.bar(i, avgStageData[j], 0.5, bottom=yStart, align='center', color=cmap[j], label=labels[j])
            yStart = avgStageData[j]

    ax.set_xlim([-0.50, 100 + 1])
    ax.set_xlabel('Stages')
    ax.set_ylabel('Time [sec]')
    # ax.legend(loc='best', title='Task Name')
    ax.set_title(plotTitle)
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
