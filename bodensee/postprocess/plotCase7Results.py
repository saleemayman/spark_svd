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
    xlabel = 'Block size [kB]'

    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    # plot the time-line data
    width = 0
    for i in range(0, nFiles):
        timeSVD = np.genfromtxt(resultsCSV[i], delimiter = ', ')
        numExecutors = timeSVD[0, 3]/timeSVD[0, 1]
        blockSize = np.unique(timeSVD[:, 2])

        avgSVDTime = np.empty(len(blockSize), dtype=float)
        for j in range(len(blockSize)):
            singleRunData = timeSVD[ np.where(timeSVD[:, 2] == blockSize[j])[0], 4]
            avgSVDTime[j] = np.mean(singleRunData)
            ax.bar(j + width, avgSVDTime[j], 0.1, bottom=0, align='center', color=cmap[i], label=str(int(numExecutors)))

        width = width + 0.1
        # line plot
        # ax.plot(blockSize[0:6], avgSVDTime[0:6], marker=markers[i], color=cmap[i], linewidth=2, label=str(int(numExecutors)))

    ax.set_xlim([-0.5, 8])
    ax.set_ylim([100, 180])
    blocks = range(len(blockSize))
    xTicks = [val + width/2 for val in blocks]
    plt.xticks(xTicks, map(str, map(int, blockSize)))
    # ax.set_ylim([50, 500])
    # ax.xaxis.set_ticks(range(0, 288+1, 24))
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Avg. Time to compute SVD [sec]')

    # remove duplicate legends
    handles, labels = plt.gca().get_legend_handles_labels()
    newLabels, newHandles = [], []
    for handle, label in zip(handles, labels):
        if label not in newLabels:
            newLabels.append(label)
            newHandles.append(handle)
    plt.legend(newHandles, newLabels, title='Executors:', ncol=5)
    # ax.set_title(plotTitle)

    # ax.annotate('Workers=1\ncores/Worker=32\nMax. Cores=32',
    #              xy=(-125, -175), xycoords='axes pixels',
    #              bbox=dict(boxstyle='square', fc='yellow', alpha=0.3))
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
