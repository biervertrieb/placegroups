''' This module contains helper functions to visualize data and features '''
import itertools
import matplotlib.pyplot as plt


def plot_pixellist(pixellist, color='blue'):
    ''' plot pixels in one color '''
    fig = plt.figure()
    ax = fig.add_subplot(projection='3d')
    xs, ys, ts = list(pixellist)
    ax.scatter(xs, ys, ts, color)
    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('time')
    ax.set_ylim(ax.get_ylim()[::-1])
    plt.show()


def plot_userlist_pixel(userpixel):
    ''' plot pixels for multiple users '''
    fig = plt.figure()
    ax = fig.add_subplot(projection='3d')

    colors = itertools.cycle(
        ["blue", "green", "red", "yellow", "cyan", "orange"])
    for pixellist in userpixel:
        xs, ys, ts = list(pixellist)
        ax.scatter(xs, ys, ts, color=next(colors))

    ax.set_xlabel('X')
    ax.set_ylabel('Y')
    ax.set_zlabel('time')
    ax.set_xlim([0, 2000])
    ax.set_ylim([2000, 0])
    plt.show()
