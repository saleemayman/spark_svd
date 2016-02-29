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

    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    # plot the time-line data
    for i in range(0, nFiles):
        timeSVD = np.genfromtxt(resultsCSV[i], delimiter = ', ')
        ax.plot(timeSVD[:, 1], timeSVD[:, 3], color=cmap[i], marker='o', label=str(int(timeSVD[0, 2])))


    ax.set_xlim([8, 45])
    # ax.set_ylim([50, 500])
    ax.xaxis.set_ticks(range(8, 45, 4))
    ax.set_xlabel('Spark Parallelism Level')
    ax.set_ylabel('Time to compute SVD [sec]')
    ax.legend(loc="best", prop={'size':8}, title='RDD Partitions')
    ax.set_title(plotTitle)

    ax.annotate('Workers=1\ncores/Worker=32\nExecutors=1',
                 xy=(-120, -130), xycoords='axes pixels',
                 bbox=dict(boxstyle='square', fc='yellow', alpha=0.3))
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])