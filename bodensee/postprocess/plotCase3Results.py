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
    timeSVD = np.genfromtxt(resultsCSV, delimiter = ', ')
    cores = [int(value) for value in np.unique(timeSVD[:, 1])]
    partitions = np.unique(timeSVD[:, 2])
    numBars = len(cores)

    cmap = get_rand_color(len(partitions))
    fig = plt.figure()
    ax = fig.add_subplot(111)

    # plot data as stacked bars for given number of cores
    for i in range(0, numBars):
        dataForCore = timeSVD[np.where(timeSVD[:,1] == cores[i])]
        dataForCore[:, 3] = dataForCore[:, 3]/sum(dataForCore[:, 3]) * 100
        numStacks = len(dataForCore[:, 2])

        yStart = 0
        for j in range(0, numStacks):
            part = int(partitions[np.where(dataForCore[j, 2] == partitions)[0]])
            ax.bar(i, dataForCore[j, 3], 0.75, bottom=yStart, align='center',
                color=cmap[np.where(dataForCore[j, 2] == partitions)[0]], label=str(part))

            ax.text(i, yStart + dataForCore[j, 3]/2, str(part) + " [" + str(int(dataForCore[j, 3])) + "%]", 
                zorder=10, rotation=00, color='k', horizontalalignment='center',
                verticalalignment='center', fontsize=9, weight='heavy')

            yStart = yStart + dataForCore[j, 3]


    ax.set_xlim([-0.50, len(cores) + 1])
    ax.set_ylim([0, 110])
    ax.xaxis.set_ticks(range(0, len(cores), 1))
    ax.set_xticklabels(map(str, cores))
    ax.set_xlabel('Cores per executor')
    ax.set_ylabel('SVD time (Normalized by the sum) [%]')

    # remove duplicate legends
    handles, labels = plt.gca().get_legend_handles_labels()
    newLabels, newHandles = [], []
    for handle, label in zip(handles, labels):
        if label not in newLabels:
            newLabels.append(label)
            newHandles.append(handle)
    plt.legend(newHandles, newLabels, title='RDD partitions')

    ax.set_title(plotTitle)
    plt.show()


if __name__ == "__main__":
    main(sys.argv[1])
