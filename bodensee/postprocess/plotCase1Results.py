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

    timeSVD = np.genfromtxt(logFileName, delimiter = ',')

    fig = plt.figure()
    ax = fig.add_subplot(111)
    ax.plot(timeSVD[:, 1], timeSVD[:, 3], color='r', marker='o')
    ax.set_xlim([0, max(timeSVD[:, 1]) + 4])
    ax.set_ylim([min(timeSVD[:, 3]) - 20, max(timeSVD[:, 3]) + 10])
    ax.set_xlabel('Cores per Executor')
    ax.set_ylabel('Time to compute SVD [sec]')
    ax.set_title(plotTitle)

for i in range(0, len(timeSVD[:, 1])):
    ax.annotate(str(int(timeSVD[i, 2])), xy=(timeSVD[i, 1], timeSVD[i, 3]), xytext=(-5, 10), textcoords='offset points')

    ax.annotate('Workers=3\nCores per Worker=32\nMax. Cores=96\n(Number of Executors labeled\nabove each data point.)',
                 xy=(-200, -100), xycoords='axes pixels',
                 bbox=dict(boxstyle='square', fc='yellow', alpha=0.3))
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])