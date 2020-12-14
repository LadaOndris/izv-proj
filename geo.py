import numpy as np
import pandas as pd
import geopandas
import contextily as ctx
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import matplotlib.colors as colors
from sklearn.cluster import KMeans
from mpl_toolkits.axes_grid1.inset_locator import inset_axes


def _save_show_fig(fig, fig_location, show_figure):
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


def make_geo(df: pd.DataFrame) -> geopandas.GeoDataFrame:
    """d, e = pozice"""
    """p5a = lokalita (1 - v obci, 2 - mimo obec)"""
    df_clean = df[['d', 'e', 'p5a', 'region']].dropna(how='any')
    gdf = geopandas.GeoDataFrame(df_clean,
                                 geometry=geopandas.points_from_xy(df_clean["d"], df_clean["e"]),
                                 crs="EPSG:5514")
    return gdf


def plot_geo(gdf: geopandas.GeoDataFrame, fig_location: str = None, show_figure: bool = False):
    # Prepare data
    gdf_olk = gdf[gdf['region'] == 'OLK']
    gdf_olk_in = gdf_olk[gdf_olk['p5a'] == 1]
    gdf_olk_out = gdf_olk[gdf_olk['p5a'] == 2]
    # remove outlier that's outside the region
    gdf_olk_out = gdf_olk_out[gdf_olk_out['d'] < -490000.0]

    # Setup figure
    fig, ax = plt.subplots(1, 2, figsize=(20, 16))
    for axis in ax:
        for pos in ['right', 'top', 'bottom', 'left']:
            axis.spines[pos].set_visible(False)
        axis.get_xaxis().set_visible(False)
        axis.get_yaxis().set_visible(False)
    fig.suptitle("Nehody v Olomouckém kraji", fontsize=30, fontweight='bold')
    ax[0].set_title('Nehody v OLK kraji: v obci', fontsize=24)
    ax[1].set_title('Nehody v OLK kraji: mimo obec', fontsize=24)

    # Plot accidents
    gdf_olk_in.centroid.plot(ax=ax[0], markersize=5, color='#d92c26')
    gdf_olk_out.centroid.plot(ax=ax[1], markersize=5, color='#2b2f35')

    # Add background map
    ctx.add_basemap(ax[0], crs=gdf_olk.crs.to_string(), source=ctx.providers.Stamen.TonerLite)
    ctx.add_basemap(ax[1], crs=gdf_olk.crs.to_string(), source=ctx.providers.Stamen.TonerLite)

    # Plot figure
    fig.tight_layout()
    fig.subplots_adjust(top=0.90)
    _save_show_fig(fig, fig_location, show_figure)


def _cmap_subset(cmap, min, max):
    """ Create a subset of a cmap. """
    return colors.LinearSegmentedColormap.from_list(
        'trunc({n},{a:.2f},{b:.2f})'.format(n=cmap.name, a=min, b=max),
        cmap(np.linspace(min, max, 256)))


def plot_cluster(gdf: geopandas.GeoDataFrame, fig_location: str = None,
                 show_figure: bool = False):
    gdf_olk = gdf[gdf['region'] == 'OLK']
    # remove outlier that's outside the region
    gdf_olk = gdf_olk[gdf_olk['d'] < -490000.0]

    # Find clusters
    kmeans = KMeans(n_clusters=20, random_state=0)
    kmeans.fit(gdf_olk[['d', 'e']])
    cluster_centers = kmeans.cluster_centers_
    cluster_indices, cluster_counts = np.unique(kmeans.labels_, return_counts=True)
    cluster_center_areas = cluster_counts * 1.2

    # Setup figure
    fig, ax = plt.subplots(1, 1, figsize=(10, 16))
    for pos in ['right', 'top', 'bottom', 'left']:
        ax.spines[pos].set_visible(False)
    ax.get_xaxis().set_visible(False)
    ax.get_yaxis().set_visible(False)
    fig.suptitle("Nehody v Olomouckém kraji", fontsize=24, fontweight='bold')

    # Plot clusters
    cmap = _cmap_subset(cm.get_cmap('Reds'), 0.4, 1.0)
    ax.scatter(cluster_centers[:, 0], cluster_centers[:, 1], c=cluster_counts,
               cmap=cmap, s=cluster_center_areas, alpha=0.75)

    # Draw colorbar
    ax_inset = inset_axes(ax, width="100%", height="100%", loc='upper center',
                          bbox_to_anchor=(0.83, 0.71, 0.04, 0.24), bbox_transform=ax.transAxes)
    cbar = fig.colorbar(cm.ScalarMappable(cmap=cmap), cax=ax_inset)
    colorbar_tick_labels = np.linspace(cluster_counts.min(), cluster_counts.max(), 5, dtype=int)
    cbar.set_ticks(np.linspace(0, 1, 5))
    cbar.set_ticklabels(colorbar_tick_labels)
    cbar.set_label(label='Počet nehod', size=14)
    cbar.ax.tick_params(labelsize='large')

    # Plot accidents
    gdf_olk.centroid.plot(ax=ax, markersize=5, color='#2b2f35')
    # Add background map
    ctx.add_basemap(ax, crs=gdf_olk.crs.to_string(), source=ctx.providers.Stamen.TonerLite)

    # Plot figure
    fig.subplots_adjust(top=0.95, left=0.02, right=0.98, bottom=0.02)
    _save_show_fig(fig, fig_location, show_figure)


if __name__ == "__main__":
    gdf = make_geo(pd.read_pickle("accidents.pkl.gz"))
    plot_cluster(gdf, "geo2.png", True)
    plot_geo(gdf, "geo1.png", True)
