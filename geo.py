import pandas as pd
import geopandas
import seaborn as sns
import contextily as ctx
import matplotlib.pyplot as plt
from analysis import get_dataframe


def make_geo(df: pd.DataFrame) -> geopandas.GeoDataFrame:
    """d, e = pozice"""
    """p5a = lokalita (1 - v obci, 2 - mimo obec)"""
    df_clean = df[['d', 'e', 'p5a', 'region']].dropna(how='any')
    gdf = geopandas.GeoDataFrame(df_clean,
                                 geometry=geopandas.points_from_xy(df_clean["d"], df_clean["e"]),
                                 crs="EPSG:5514")
    return gdf


def plot_geo(gdf: geopandas.GeoDataFrame, fig_location: str = None, show_figure: bool = False):
    gdf_olk = gdf[gdf['region'] == 'OLK']
    gdf_olk_in = gdf_olk[gdf_olk['p5a'] == 1]
    gdf_olk_out = gdf_olk[gdf_olk['p5a'] == 2]
    # remove outlier that's outside the region
    gdf_olk_out = gdf_olk_out[gdf_olk_out['d'] < -490000.0]

    fig, ax = plt.subplots(1, 2, figsize=(20, 16))
    fig.suptitle("Nehody v Olomouckém kraji", fontsize=20)
    ax[0].set_title('Nehody v OLK kraji: v obci', fontsize=18)
    ax[1].set_title('Nehody v OLK kraji: mimo obec', fontsize=18)
    gdf_olk_in.centroid.plot(ax=ax[0], markersize=5, color='#d92c26')
    gdf_olk_out.centroid.plot(ax=ax[1], markersize=5, color='#2b2f35')

    ctx.add_basemap(ax[0], crs=gdf_olk.crs.to_string(), source=ctx.providers.Stamen.TonerLite)
    ctx.add_basemap(ax[1], crs=gdf_olk.crs.to_string(), source=ctx.providers.Stamen.TonerLite)

    fig.tight_layout()
    fig.show()

    pass


if __name__ == "__main__":
    df = get_dataframe("accidents.pkl.gz")
    gdf = make_geo(df)
    plot_geo(gdf)

