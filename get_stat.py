"""

@author: Ladislav Ondris
         xondri07@vutbr.cz

This file plots data from PCR dataset.
"""

import numpy as np
import argparse
import matplotlib.pyplot as plt
from download import DataDownloader


def label_bars(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, -13),
                    textcoords="offset points",
                    ha='center', va='bottom', color='white')


def plot_stat(data_source, fig_location=None, show_figure=False):
    """
    Given a data_source of parsed PCR dataset, it produces
    a bar plot using matplotlib displaying the number of accidents in
    each reagion. It creates a subplot for each year.

    Parameters
    ----------
    data_source :
        The data source.
    fig_location : string, optional
        File path to save the figure to. If it None, it is not saved.
    show_figure : boolean, optional
        Shows the figure in a console if True.

    Returns
    -------
    None.

    """
    headers, features = data_source
    regions_col = _get_regions_col(headers, features)
    years_col = _get_years_col(headers, features)

    unique_years, year_indices = np.unique(years_col, return_inverse=True)
    unique_regions = np.unique(regions_col)

    fig, ax_list = plt.subplots(nrows=len(unique_years), ncols=1,
                                figsize=(0.6*len(unique_regions), 2.5*len(unique_years)),
                                sharey=True)

    counts = _get_counts_for_each_year_and_region(
                    regions_col, unique_years, year_indices)

    for i, (year, regions_counts) in enumerate(counts):
        indexofsort_ascending = np.argsort(regions_counts[:, 1].astype(int), axis=-1)
        indexofsort_descending = np.flip(indexofsort_ascending, axis=0)
        regions_counts = regions_counts[indexofsort_descending, :]

        ax = ax_list[i]
        bars = ax.bar(regions_counts[:, 0], regions_counts[:, 1].astype(int), width=0.97)
        ax.set_title(year, fontsize=14, y=0.83)
        ax.tick_params(axis="x", bottom=False)
        ax.tick_params(axis="y", left=False)
        ax.grid(axis="y", which="major", color="black", alpha=.2, linewidth=.5)

        for pos in ['right', 'top', 'bottom', 'left']:
            ax.spines[pos].set_visible(False)
        label_bars(ax, bars)

    ax_list[-1].set_xlabel("Kraj", fontsize=14)
    fig.tight_layout(pad=2)
    fig.subplots_adjust(left=0.12, top=0.95)
    fig.text(0.02, 0.5, 'Počet nehod', va='center', rotation='vertical', fontsize=14)
    fig.suptitle("Počty nehod v českých krajích", fontsize=16, y=0.98)

    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


def _get_regions_col(headers, features):
    return features[headers.index("region")]


def _get_years_col(headers, features):
    dates_col = features[headers.index("p2a")]
    return dates_col.astype("datetime64[Y]")


def _get_counts_for_each_year_and_region(regions_col, unique_years, year_indices):
    counts = []
    for i, year in enumerate(unique_years):
        regions_for_the_year = regions_col[np.argwhere(year_indices == i)]
        region_labels, region_counts = np.unique(regions_for_the_year, return_counts=True)
        regions_counts = np.stack([region_labels, region_counts], axis=1)
        counts.append([year, regions_counts])
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--fig_location', type=str,
                        help='Path to the figure including its name')
    parser.add_argument('--show_figure', help='Show figure in console',
                        action='store_true', default=False)
    args = parser.parse_args()

    data_source = DataDownloader().get_list()
    plot_stat(data_source, show_figure=args.show_figure, fig_location=args.fig_location)
