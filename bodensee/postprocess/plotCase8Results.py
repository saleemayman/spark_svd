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
    
    # get all csv file from current directory
    resultsCSV = sorted(glob.glob(logFileName + '*_results.csv'))

    # read the individual csv files for logFileName
    nFiles = len(resultsCSV)
    cmap = get_rand_color(nFiles)
    markers = ['x', 'o', '^', '+', 's', 'v', '<', '>']
    xlabel = 'Number of Executors'

    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    # plot the time-line data
    width = 0
    for i in range(0, nFiles):
        timeSVD = np.genfromtxt(resultsCSV[i], delimiter = ', ')
        numCores = np.unique(timeSVD[:, 1])
        # blockSize = np.unique(timeSVD[:, 2])

        numExecutors = np.empty(len(numCores), dtype=int)
        avgSVDTime = np.empty(len(numCores), dtype=float)
        for j in range(len(numCores)):
            numExecutors[j] = timeSVD[0, 2]/numCores[j]
            singleRunData = timeSVD[ np.where(timeSVD[:, 1] == numCores[j])[0], 3]
            avgSVDTime[j] = np.mean(singleRunData)

            print 'execs: ' + str(numExecutors[j]) + ', cores: ' + str(numCores[j]) + ', avg. time: ' + str(avgSVDTime[j])

            ax.bar(j + width, avgSVDTime[j], 0.5, bottom=0, align='center', color='b')

        width = width + 0.5
        # line plot
        # ax.plot(blockSize[0:6], avgSVDTime[0:6], marker=markers[i], color=cmap[i], linewidth=2, label=str(int(numExecutors)))

    # ax.set_xlim([-0.5, 8])
    # ax.set_ylim([100, 180])
    runs = range(len(numExecutors))
    # xTicks = [val + width/2 for val in runs]
    xTicks = [val for val in runs]
    plt.xticks(xTicks, map(str, map(int, numExecutors)))
    # ax.set_ylim([50, 500])
    # ax.xaxis.set_ticks(range(0, 288+1, 24))
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Avg. Time to compute SVD [sec]')
    # ax.set_title('Compute times for different number of Executors (10M Data-set)')
    # ax.annotate('Max. Cores=64', xy=(-125, -175), xycoords='axes pixels', bbox=dict(boxstyle='square', fc='yellow', alpha=0.3))

    # remove duplicate legends
    # handles, labels = plt.gca().get_legend_handles_labels()
    # newLabels, newHandles = [], []
    # for handle, label in zip(handles, labels):
    #     if label not in newLabels:
    #         newLabels.append(label)
    #         newHandles.append(handle)
    # plt.legend(newHandles, newLabels, title='Executors:', ncol=5)

    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
