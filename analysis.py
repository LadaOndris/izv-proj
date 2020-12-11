#!/usr/bin/env python3.8
# coding=utf-8

from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
import numpy as np
import os


# muzete pridat libovolnou zakladni knihovnu ci knihovnu predstavenou na prednaskach
# dalsi knihovny pak na dotaz

def _print_categorizable_cols(df: pd.DataFrame):
    for col in df.keys():
        # print(col, len(df[col].unique()), df[col].count())
        if len(df[col].unique()) < df[col].count() / 2:
            print(F"'{col}'", end=', ')


# Ukol 1: nacteni dat
def get_dataframe(filename: str, verbose: bool = False) -> pd.DataFrame:
    df = pd.read_pickle(filename, compression='gzip')
    if verbose:
        print(F"orig_size={df.memory_usage(deep=True).sum() // 1_048_576} MB")
    # _print_categorizable_cols(df)

    categorizable_cols = ['p36', 'p37', 'p2a', 'weekday(p2a)', 'p2b', 'p6', 'p7', 'p8', 'p9', 'p10',
                          'p11', 'p12', 'p14', 'p15', 'p16', 'p17', 'p18',
                          'p19', 'p20', 'p21', 'p22', 'p23', 'p24', 'p27', 'p28', 'p34', 'p35', 'p39',
                          'p44', 'p45a', 'p47', 'p48a', 'p49', 'p50a', 'p50b', 'p51', 'p52', 'p53',
                          'p55a', 'p57', 'p58', 'h', 'i', 'j', 'k', 'l', 'n', 'o', 'p', 'q', 'r', 's', 't', 'p5a']
    for col in categorizable_cols:
        df[col] = df[col].astype('category')

    df['date'] = pd.Series(df['p2a'], dtype='datetime64[M]')

    if verbose:
        print(F"new_size={df.memory_usage(deep=True).sum() // 1_048_576} MB")
    return df


# Ukol 2: následky nehod v jednotlivých regionech
def plot_conseq(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    d = df[['p13a', 'p13b', 'p13c', 'region']]

    grouped = d.groupby('region').agg(np.sum)
    grouped['counts'] = d['region'].value_counts()
    grouped = grouped.reset_index().sort_values(by='counts', ascending=False)
    rows_count = len(grouped.index)
    pallete = sns.dark_palette("#69d", n_colors=rows_count)

    fig, axes = plt.subplots(4, 1, figsize=(6, 9))
    for axis in axes:
        axis.tick_params(axis="x", bottom=False)
        axis.tick_params(axis="y", left=False)
        axis.grid(axis="y", which="major", color="black", alpha=.2, linewidth=.5)

        for pos in ['right', 'top', 'bottom', 'left']:
            axis.spines[pos].set_visible(False)

    sns.set_style("darkgrid")
    title_y = 0.9
    g1 = sns.barplot(ax=axes[0], data=grouped, x='region', y='p13a',
                     palette=pallete)
    g1.set_title('Úmrtí', y=title_y)
    g1.set_ylabel('Počet')
    g1.set_xlabel('')

    g2 = sns.barplot(ax=axes[1], data=grouped, x='region', y='p13b',
                     palette=pallete)
    g2.set_title('Těžce zranění', y=title_y)
    g2.set_ylabel('Počet')
    g2.set_xlabel('')

    g3 = sns.barplot(ax=axes[2], data=grouped, x='region', y='p13c',
                     palette=pallete)
    g3.set_title('Lehce zranění', y=title_y)
    g3.set_ylabel('Počet')
    g3.set_xlabel('')

    g4 = sns.barplot(ax=axes[3], data=grouped, x='region', y='counts',
                     palette=pallete)
    g4.set_title('Celkem nehod', y=title_y)
    g4.set_ylabel('Počet')
    g4.set_xlabel('Kraj')

    fig.suptitle('Následky nehod v jednotlivých krajích', fontsize=16)
    fig.tight_layout()
    fig.show()


# Ukol3: příčina nehody a škoda
def plot_damage(df: pd.DataFrame, fig_location: str = None,
                show_figure: bool = False):
    regions = ['JHC', 'HKK', 'OLK', 'PLK']
    columns = ['region', 'p53', 'p12']
    df_regions = df[df['region'].isin(regions)][columns]

    df_regions['p53'] = pd.cut(df_regions['p53'], [-np.inf, 500, 2000, 5000, 10000, np.inf],
                               labels=['<50', '50 - 200', '200 - 500', '500 - 1000', '>1000'])
    df_regions['p12'] = pd.cut(df_regions['p12'], [99, 199, 299, 399, 499, 599, 699],
                               labels=['Nezaviněná řidičem', 'Nepřiměřená rychlost jízdy',
                                       'Nesprávné předjíždění', 'Nedání přednosti v jízdě', 'Nesprávný způsob jízdy',
                                       'Technická závada vozidla'])

    df_regions = df_regions.groupby(columns, as_index=False).agg('size')

    sns.set_style("whitegrid")
    g = sns.catplot(data=df_regions, x='p53', y='size', hue='p12', col='region',
                    col_wrap=2, kind='bar')
    sns.despine(top=True, right=True, left=True, bottom=True)
    g.set(yscale='log')
    g.set_xlabels('Škoda [tisíc Kč]')
    g.set_ylabels('Počet')
    g.set_titles('{col_name}', size=16)
    g.fig.suptitle('Příčiny nehod v krajích', fontsize=18)
    g._legend.set_title('Příčina nehody')
    g.tight_layout()
    g.fig.show()


# Ukol 4: povrch vozovky
def plot_surface(df: pd.DataFrame, fig_location: str = None,
                 show_figure: bool = False):
    regions = ['JHC', 'HKK', 'OLK', 'PLK']
    columns = ['region', 'date', 'p16', 'p1']
    labels = ['jiný stav',
              'suchý neznečištěný',
              'suchý znečištěný',
              'mokrý',
              'bláto',
              'náledí, ujetý sníh - posypané',
              'náledí, ujetý sníh - neposypané',
              'rozlitý olej, nafta apod.',
              'souvislý sníh',
              'náhlá změna stavu']
    rename_map = {key: label for key, label in zip(range(0, len(labels)), labels)}
    df_regions = df[df['region'].isin(regions)][columns]
    df_regions = pd.crosstab([df_regions['region'], df_regions['date']], df_regions['p16'])
    df_regions.rename(columns=rename_map, inplace=True)
    df_regions = df_regions.stack().reset_index()
    df_grouped = df_regions.groupby(
        ['region', 'p16', pd.Grouper(key='date', freq='M')]).sum()
    df_grouped = df_grouped.reset_index()

    sns.set_style("whitegrid")
    g = sns.relplot(data=df_grouped, x='date', y=df_grouped[0], hue='p16', col='region',
                    col_wrap=2, kind='line')
    sns.despine(top=True, right=True, left=True, bottom=True)
    g.set_xlabels('Datum vzniku nehody')
    g.set_ylabels('Počet nehod')
    g.set_titles('{col_name}', size=16)
    g.fig.suptitle('Stav vozovky při nehodách', fontsize=18)
    #g.axes[2].legend(loc='lower center', bbox_to_anchor=(1, 1), ncol=2, title='Stav vozovky')
    g._legend.set_title('Příčina nehody')
    g.tight_layout()
    g.fig.show()

if __name__ == "__main__":
    pass
    # zde je ukazka pouziti, tuto cast muzete modifikovat podle libosti
    # skript nebude pri testovani pousten primo, ale budou volany konkreni ¨
    # funkce.
    df = get_dataframe("accidents.pkl.gz", verbose=True)
    plot_conseq(df, fig_location="01_nasledky.png", show_figure=True)
    plot_damage(df, "02_priciny.png", True)
    plot_surface(df, "03_stav.png", True)
