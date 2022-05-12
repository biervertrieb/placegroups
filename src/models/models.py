''' Classes for representing different models and their logic '''


class RZModel:
    ''' Represents the axis aligned bounding box volume around a given pixel '''
    max_xy_dist = 80
    max_t_dist = 20*60

    @staticmethod
    def is_box_colliding(pixel1: list, pixel2: list) -> bool:
        ''' checks if two boxes are colliding with each other '''
        x_1, y_1, t_1 = list(pixel1)
        x_2, y_2, t_2 = list(pixel2)
        if abs(x_1-x_2) <= 2*RZModel.max_xy_dist and abs(y_1-y_2) <= 2*RZModel.max_xy_dist and abs(t_1-t_2) <= 2*RZModel.max_t_dist:
            return True
        return False
