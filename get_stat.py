
import numpy as np
import matplotlib.pyplot as plt
from download import DataDownloader

def label_bars(ax, rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(height),
                    xy=(rect.get_x() + rect.get_width() / 2, height),
                    xytext=(0, -15),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom', color='white')



def plot_stat(data_source, fig_location = None, show_figure = False):
    headers, features = data_source
    index = np.squeeze(np.argwhere(headers == "region"))
    
    regions_col = _get_regions_col(headers, features)
    years_col = _get_years_col(headers, features)
    
    unique_years, year_indices = np.unique(years_col, return_inverse=True)
    unique_regions = np.unique(regions_col)
    
    fig, ax_list = plt.subplots(nrows=len(unique_years), ncols=1, 
                                figsize=(0.6*len(unique_regions), 2.5*len(unique_years)),
                                constrained_layout=True)
    
    counts = _get_counts_for_each_year_and_region(regions_col, unique_years, year_indices)
    maximal_count = _get_maximal_count(counts)
    
    for i, (year, regions_counts) in enumerate(counts):
        
        indexofsort_ascending = np.argsort(regions_counts[:,1].astype(int),axis=-1)
        indexofsort_descending = np.flip(indexofsort_ascending, axis=0)
        regions_counts = regions_counts[indexofsort_descending,:]
        
        
        ax = ax_list[i]
        bars = ax.bar(regions_counts[:,0], regions_counts[:,1].astype(int), width=0.97)
        ax.set_title(year, fontsize=16)
        #ax.get_xaxis().set_visible(False)
        ax.set_ylim([0, maximal_count])
        ax.tick_params(axis="x", bottom=False)
        ax.tick_params(axis="y", left=False)
        ax.grid(axis="y", which="major", color="black", alpha=.2, linewidth=.5)
        ax.set_ylabel("Počet nehod", fontsize=14)
        ax.margins(y=1, tight=False)
        #ax_list[i].text()
        
        for pos in ['right','top','bottom','left']:
            ax.spines[pos].set_visible(False)
        
        label_bars(ax, bars)
    
    #fig.subplots_adjust(top=0.95, bottom=0)
    #fig.tight_layout(pad=2)
    fig.suptitle("Počty nehod v českých krajích v minulých letech", fontsize=16, y=0.98)
 
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()
        
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
        regions_counts = np.stack([region_labels, region_counts], axis=1)
        
        #print(regions_counts)
        #regions_counts = np.array(regions_counts, dtype=[('region', 'U3'), ('count', 'i4')])
        counts.append([year, regions_counts])
        
    return counts

def _get_maximal_count(counts):
    max_count = 0
    for year, regions_counts in counts:
        m = np.max(regions_counts[:,1].astype('int'))
        max_count = max(max_count, m)
    return max_count
    
if __name__ == "__main__":
    #data_source = DataDownloader().get_list(["OLK", "PAK", "JHM", "HKK"])
    data_source = DataDownloader().get_list()
    plot_stat(data_source, show_figure=True, fig_location="fig.png")
    
    