''' This module contains helper functions to visualize data and features '''
import matplotlib.pyplot as plt
import numpy as np

from src.models.models import RZModel


def show_and_save(subplot=None, savename=None):
    ''' Helper to save a plot figure '''
    savepath = '../reports/figures/'
    if savename is None:
        if subplot is None:
            savename = 'unnamed_fig'
        else:
            savename = (subplot.get_title()).lower().replace(' ', '_').replace(
                'ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
    plt.savefig(savepath+savename+'.jpg', dpi=300)
    plt.show()


def new_xyt_plot():
    '''Helper zum initialisieren'''
    fig = plt.figure()
    subplot = fig.add_subplot(projection='3d')
    subplot.set_xlabel('X')
    subplot.set_ylabel('Y')
    subplot.set_zlabel('Zeit')
    subplot.set_xlim([0, 2001])
    subplot.set_ylim([2001, 0])
    return subplot


def plot_pixellist(pixellist: list, color='blue', subplot=None, finish=True):
    ''' plot pixels in one color '''
    if subplot is None:
        subplot = new_xyt_plot()
    x_list, y_list, t_list = list(pixellist)
    subplot.scatter(x_list, y_list, t_list, c=color)
    if finish:
        show_and_save(subplot)
    return subplot


def plot_list_of_pixellist(pixellistlist: list, color=None, subplot=None, finish=True):
    ''' plot pixel in mixed colors (max 10 colors!) '''
    if subplot is None:
        subplot = new_xyt_plot()
    colorpicker = None
    if color is None:
        colorpicker = iter(plt.cm.tab10(
            np.linspace(0, 1, 10)))
    for pixellist in pixellistlist:
        if colorpicker is not None:
            color = [next(colorpicker)]
        plot_pixellist(pixellist, color, subplot, False)
    if finish:
        show_and_save(subplot)
    return subplot


def plot_line(x_list, y_list, t_list, color='blue', subplot=None, finish=True):
    ''' Helperfunction to draw a single line '''
    if subplot is None:
        subplot = new_xyt_plot()
    subplot.plot(x_list, y_list, t_list, c=color)
    if finish:
        show_and_save(subplot)
    return subplot


def plot_pixel_boundingbox(x_coord, y_coord, t_coord, color='blue', subplot=None, finish=True):
    ''' Helperfunction to draw the bounding box around a single pixel '''
    if subplot is None:
        subplot = new_xyt_plot()
    # draw top lines
    # x+,y+ -> x+,y- -> x-,y- -> x-,y+ -> x+,y+
    plot_line([x_coord+RZModel.max_xy_dist, x_coord+RZModel.max_xy_dist], [y_coord+RZModel.max_xy_dist, y_coord -
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord+RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord+RZModel.max_xy_dist, x_coord-RZModel.max_xy_dist], [y_coord-RZModel.max_xy_dist, y_coord -
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord+RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord-RZModel.max_xy_dist, x_coord-RZModel.max_xy_dist], [y_coord-RZModel.max_xy_dist, y_coord +
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord+RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord-RZModel.max_xy_dist, x_coord+RZModel.max_xy_dist], [y_coord+RZModel.max_xy_dist, y_coord +
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord+RZModel.max_t_dist], color, subplot, False)
    # draw bottom lines
    plot_line([x_coord+RZModel.max_xy_dist, x_coord+RZModel.max_xy_dist], [y_coord+RZModel.max_xy_dist, y_coord -
              RZModel.max_xy_dist], [t_coord-RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord+RZModel.max_xy_dist, x_coord-RZModel.max_xy_dist], [y_coord-RZModel.max_xy_dist, y_coord -
              RZModel.max_xy_dist], [t_coord-RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord-RZModel.max_xy_dist, x_coord-RZModel.max_xy_dist], [y_coord-RZModel.max_xy_dist, y_coord +
              RZModel.max_xy_dist], [t_coord-RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord-RZModel.max_xy_dist, x_coord+RZModel.max_xy_dist], [y_coord+RZModel.max_xy_dist, y_coord +
              RZModel.max_xy_dist], [t_coord-RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    # horizontal lines
    plot_line([x_coord+RZModel.max_xy_dist, x_coord+RZModel.max_xy_dist], [y_coord+RZModel.max_xy_dist, y_coord +
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord+RZModel.max_xy_dist, x_coord+RZModel.max_xy_dist], [y_coord-RZModel.max_xy_dist, y_coord -
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord-RZModel.max_xy_dist, x_coord-RZModel.max_xy_dist], [y_coord-RZModel.max_xy_dist, y_coord -
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    plot_line([x_coord-RZModel.max_xy_dist, x_coord-RZModel.max_xy_dist], [y_coord+RZModel.max_xy_dist, y_coord +
              RZModel.max_xy_dist], [t_coord+RZModel.max_t_dist, t_coord-RZModel.max_t_dist], color, subplot, False)
    if finish:
        show_and_save(subplot)
    return subplot

def hex_to_rgb(hexcolor):
    '''converts hex color value to RGB'''
    hexcolor = hexcolor.lstrip('#')
    lv = len(hexcolor)
    return tuple(int(hexcolor[i:i+lv//3], 16) for i in range(0, lv, lv//3))
