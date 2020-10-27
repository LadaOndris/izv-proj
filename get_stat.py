
import numpy as np
import matplotlib.pyplot as plt
from download import DataDownloader


def plot_stat(data_source, fig_location = None, show_figure = False):
    headers, features = data_source
    index = np.squeeze(np.argwhere(headers == "region"))
    
    regions_col = _get_regions_col(headers, features)
    years_col = _get_years_col(headers, features)
    
    unique_years, year_indices = np.unique(years_col, return_inverse=True)
    unique_regions = np.unique(regions_col)
    
    fig, ax_list = plt.subplots(nrows=len(unique_years), ncols=1, 
                                figsize=(0.5*len(unique_regions),3*len(unique_years)))
    
    counts = _get_counts_for_each_year_and_region(regions_col, unique_years, year_indices)
    maximal_count = _get_maximal_count(counts)
    
    for i, (year, regions, counts) in enumerate(counts):
        ax_list[i].bar(regions, counts)
        ax_list[i].set_title(year)
        ax_list[i].get_xaxis().set_visible(False)
        ax_list[i].set_ylim([0, maximal_count])
        ax_list[i].tick_params(axis="x", bottom=False)
        ax_list[i].grid(axis="y", which="major", color="black", alpha=.5, linewidth=.5)
        #ax_list[i].text()
        
        for pos in ['right','top','bottom','left']:
            ax_list[i].spines[pos].set_visible(False)
 
    ax_list[-1].get_xaxis().set_visible(True)
    plt.show()
    #print(index)
    #print(headers[index], features[index][-10:-1])
    
def _get_regions_col(headers, features):
    return features[np.squeeze(np.argwhere(headers == "region"))]

def _get_years_col(headers, features):
    dates_col = features[np.squeeze(np.argwhere(headers == "p2a"))]
    dates_col = np.char.split(dates_col, '-')
    dates_col = np.array([np.array(year) for year in dates_col])
    return dates_col[...,0]

def _get_counts_for_each_year_and_region(regions_col, unique_years, year_indices):
    counts = []
    for i, year in enumerate(unique_years):
        regions_for_the_year = regions_col[np.argwhere(year_indices == i)]
        region_labels, region_counts = np.unique(regions_for_the_year, return_counts=True)
        counts.append((year, region_labels, region_counts))
    return counts

def _get_maximal_count(counts):
    max_count = 0
    for year, region_labels, region_counts in counts:
        m = np.max(region_counts)
        max_count = max(max_count, m)
    return max_count
    
if __name__ == "__main__":
    data_source = DataDownloader().get_list()
    plot_stat(data_source)
    
    