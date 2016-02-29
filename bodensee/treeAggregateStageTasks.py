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
    logFileName = sys.argv[1]
    plotTitle = sys.argv[2]
    print logFileName
    
    # get csv file from current directory
    resultsCSV = sorted(glob.glob(logFileName + '.*_taskTimes.csv'))

    fig = plt.figure()
    ax1 = fig.add_subplot(111)

    temp = np.genfromtxt(resultsCSV[0], delimiter = ',')
    for j in range(1, len(resultsCSV)):
        temp1 = np.genfromtxt(resultsCSV[j], delimiter = ',')
        data = np.concatenate((temp1, temp))
        temp = data
        temp1 = []

    taskTimes = data[:, 4] - data[:, 3]
    ax1.hist(taskTimes, bins=1000)
    ax1.set_xlim([400, 2000])

    ax1.set_xlabel('Task Execution Times [msec]')
    ax1.set_ylabel('Number of Tasks')
    ax1.set_title(logFileName)
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
