import glob, os, sys
import numpy as np
from random import*
import matplotlib.pyplot as plt
import matplotlib.cm as cmx
import matplotlib.colors as colors


def main(argv):
    logFileName = sys.argv[1]
    plotTitle = sys.argv[2]
    print logFileName
    
    # get all csv file from current directory
    resultsCSV = sorted(glob.glob(logFileName + '*_results.csv'))
    # labels = ['6 execs (16 cores/exec, 2 exec/Worker)', '24 execs (4 cores/exec, 8 exec/Worker)', 
    #             '3 execs (32 cores/exec, 1 exec/Worker)', '48 execs (2 cores/exec, 16 exec/Worker)',
    #             '12 execs (8 cores/exec, 4 exec/Worker)']
    labels = ['6 execs (16 cores/exec)', '24 execs (4 cores/exec)', '3 execs (32 cores/exec)',
             '48 execs (2 cores/exec)', '12 execs (8 cores/exec)']

    # read the individual csv files for logFileName
    nFiles = len(resultsCSV)
    colors = ['r', 'g', 'b', 'k', 'c']

    fig = plt.figure()
    ax = fig.add_subplot(111)
    
    # plot the time-line data
    for i in range(0, nFiles):
        timeSVD = np.genfromtxt(resultsCSV[i], delimiter = ', ')
        ax.plot(timeSVD[:, 1], timeSVD[:, 2], color=colors[i], marker='o', label=labels[i])


    ax.set_xlim([0, 288])
    ax.set_ylim([50, 500])
    ax.xaxis.set_ticks(range(0, 288+1, 24))
    ax.set_xlabel('RDD Partitions')
    ax.set_ylabel('Time to compute SVD [sec]')
    # ax.legend(loc="best", prop={'size':8})
    handles, labels = ax.get_legend_handles_labels()
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0]))
    ax.legend(handles, labels)
    ax.legend(loc='best', title='Executor Configuration:', prop={'size':12})
    # ax.set_title(plotTitle)

    # ax.annotate('Workers=3\ncores/Worker=32\nMaximum Cores=96',
    #              xy=(-155, -180), xycoords='axes pixels',
    #              bbox=dict(boxstyle='square', fc='yellow', alpha=0.3))
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
