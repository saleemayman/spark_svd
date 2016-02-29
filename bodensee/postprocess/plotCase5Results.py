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
    isLinePlot = int(sys.argv[3])
    print logFileName
    
    # get all csv file from current directory
    resultsCSV = sorted(glob.glob(logFileName + '*_results.csv'))

    # read the individual csv files for logFileName
    nFiles = len(resultsCSV)
    cmap = get_rand_color(nFiles)
    markers = ['x', 'o', '^', '+', 's', 'v', '<', '>']

    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    # plot the time-line data
    for i in range(0, nFiles):
        timeSVD = np.genfromtxt(resultsCSV[i], delimiter = ', ')
        numPartitions = np.unique(timeSVD[:, 2])
        parallelism = np.unique(timeSVD[:, 1])

        if (logFileName == 'case5'):
            # parallelism-to-partitions ratio
            x = parallelism/numPartitions
            # x = parallelism/32
            xlabel = 'default.parallelism/numPartitions ratio'
        else:
            # parallelism-to-cores ratio
            x = parallelism/32
            xlabel = 'tasks per core (parallelism/cores ratio)'

        avgSVDTime = np.empty(len(parallelism), dtype=float)
        parToPartitionRatio = np.empty(len(parallelism), dtype=float)
        for j in range(len(parallelism)):
            singleRunData = timeSVD[ np.where(timeSVD[:, 1] == parallelism[j])[0], 3]
            avgSVDTime[j] = np.mean(singleRunData)


        # line plot
        if (isLinePlot == 1):
            ax.plot(x, avgSVDTime, marker=markers[i], color=cmap[i], linewidth=2, label=str(int(numPartitions)))
        else:
            # scatter plot
            if (logFileName == 'case5'):
                parRatio = timeSVD[:, 1]/numPartitions
            else:
                parRatio = timeSVD[:, 1]/32
            ax.scatter(parRatio, timeSVD[:, 3], marker=markers[i], facecolors='none', edgecolors=cmap[i], s=50, label=str(int(numPartitions)))


    # ax.set_xlim([0, 288])
    # ax.set_ylim([50, 500])
    # ax.xaxis.set_ticks(range(0, 288+1, 24))
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Avg. Time to compute SVD [sec]')
    ax.legend(loc="best", title='Partitions:')
    # ax.set_title(plotTitle)

    # ax.annotate('Workers=1\ncores/Worker=32\nMax. Cores=32',
    #              xy=(-125, -175), xycoords='axes pixels',
    #              bbox=dict(boxstyle='square', fc='yellow', alpha=0.3))
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
