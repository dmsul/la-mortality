
def legend_below(ax, shrink=False, *args, **kwargs):
    if shrink:
        shrink_axes_forlegend(ax)
    # Put legend centered, just below axes
    ax.legend(*args, loc='upper center', bbox_to_anchor=(0.5, -0.1), **kwargs)


def shrink_axes_forlegend(*args):
    for ax in args:
        box = ax.get_position()
        new_box = [box.x0, box.y0 + box.height * 0.1,
                   box.width, box.height * 0.9]
        ax.set_position(new_box)
