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

def parse_pixel_color(pixel_color):
    """Convert a hex color code to an integer key."""
    hex_to_key = {
        "#000000": 0,
        "#00756F": 1,
        "#009EAA": 2,
        "#00A368": 3,
        "#00CC78": 4,
        "#00CCC0": 5,
        "#2450A4": 6,
        "#3690EA": 7,
        "#493AC1": 8,
        "#515252": 9,
        "#51E9F4": 10,
        "#6A5CFF": 11,
        "#6D001A": 12,
        "#6D482F": 13,
        "#7EED56": 14,
        "#811E9F": 15,
        "#898D90": 16,
        "#94B3FF": 17,
        "#9C6926": 18,
        "#B44AC0": 19,
        "#BE0039": 20,
        "#D4D7D9": 21,
        "#DE107F": 22,
        "#E4ABFF": 23,
        "#FF3881": 24,
        "#FF4500": 25,
        "#FF99AA": 26,
        "#FFA800": 27,
        "#FFB470": 28,
        "#FFD635": 29,
        "#FFF8B8": 30,
        "#FFFFFF": 31,
    }
    color = hex_to_key[pixel_color]
    indexed_rgb = (
        (0, 0, 0),
        (0, 117, 111),
        (0, 158, 170),
        (0, 163, 104),
        (0, 204, 120),
        (0, 204, 192),
        (36, 80, 164),
        (54, 144, 234),
        (73, 58, 193),
        (81, 82, 82),
        (81, 233, 244),
        (106, 92, 255),
        (109, 0, 26),
        (109, 72, 47),
        (126, 237, 86),
        (129, 30, 159),
        (137, 141, 144),
        (148, 179, 255),
        (156, 105, 38),
        (180, 74, 192),
        (190, 0, 57),
        (212, 215, 217),
        (222, 16, 127),
        (228, 171, 255),
        (255, 56, 129),
        (255, 69, 0),
        (255, 153, 170),
        (255, 168, 0),
        (255, 180, 112),
        (255, 214, 53),
        (255, 248, 184),
        (255, 255, 255),
    )
    return indexed_rgb[color]
    
def hex_to_rgb(hexcolor):
    '''converts hex color value to RGB'''
    hexcolor = hexcolor.lstrip('#')
    lv = len(hexcolor)
    return tuple(int(hexcolor[i:i+lv//3], 16) for i in range(0, lv, lv//3))
