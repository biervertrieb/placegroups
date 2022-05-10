''' This module contains helper functions to visualize data and features '''
import matplotlib.pyplot as plt
import numpy as np


def plot_pixellist(pixellist: list):
    ''' plot pixels in one color '''
    plot_list_of_pixellist([pixellist])


def plot_list_of_pixellist(pixellistlist: list):
    ''' plot pixel in mixed colors '''
    fig = plt.figure()
    ax = fig.add_subplot(projection='3d')
    color = iter(plt.cm.rainbow(np.linspace(0, 1, len(pixellistlist))))
    for pixellist in pixellistlist:
        c = next(color)
        xs, ys, ts = list(pixellist)
        ax.scatter(xs, ys, ts, c)
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('time')
    ax.set_xlim([0, 2000])
    ax.set_ylim([2000, 0])
    plt.show()
