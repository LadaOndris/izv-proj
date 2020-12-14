from matplotlib import pyplot as plt
import pandas as pd
import seaborn as sns
from datetime import datetime


def _save_show_fig(fig, fig_location, show_figure):
    if fig_location:
        fig.savefig(fig_location)
    if show_figure:
        fig.show()


def plot_time(df: pd.DataFrame):
    """
    Plots the relation between time and the number of accidents.
    """
    # p2b - time, p1 - index
    df_time = df[['p1', 'p2b']]
    df_time['hour'] = df_time['p2b'].str.slice(0, 2)
    df_time = df_time[df_time['hour'] != '25']
    df_time = df_time.sort_values(by='hour')
    g = sns.catplot(data=df_time, x='hour', kind='count', color='#d92c26')
    g.set_title('Nehody podle hodiny')
    g.set_ylabel('Počet')
    g.set_xlabel('Hodina')
    g.fig.show()


def plot_time_roadtype(df: pd.DataFrame, fig_location: str = None,
                       show_figure: bool = False):
    """
    Plots the relation between time and the number of accidents.
    """
    # p2b - time, p1 - index
    df_time = df[['p1', 'p2b', 'p36']]
    df_time['hour'] = pd.to_numeric(df_time['p2b'].str.slice(0, 2))
    df_time = df_time[df_time['hour'] != 25]
    df_time = df_time.groupby(['hour', 'p36']).size().reset_index()
    df_time = df_time.sort_values(by='hour')
    days = get_number_of_days(df)
    df_time['daily'] = df_time[0] / days

    fig, ax = plt.subplots(1, 1, figsize=(8, 4))
    g = sns.lineplot(ax=ax, data=df_time, x='hour', y='daily', hue='p36', palette='Set1')
    g.set_title('Nehody dle typu silnice během dne')
    g.set_ylabel('Počet nehod za hodinu')
    g.set_xlabel('Hodina')

    for pos in ['right', 'top']:
        ax.spines[pos].set_visible(False)

    fig.tight_layout()
    fig.subplots_adjust(right=0.6)
    ax.get_legend().remove()
    labels = ['dálnice', 'silnice 1. třídy', 'silnice 2. třídy', 'silnice 3. třídy',
              'křižovatky ve městech', 'komunikace sledovaná', 'komunikace místní',
              'účelová - polní a lesní cesty apod.', 'účelová - parkoviště, odpočívky apod.']
    ax.legend(labels, loc='center left', bbox_to_anchor=(0.98, 0.5), title='Typ vozovky', frameon=False)
    _save_show_fig(fig, fig_location, show_figure)


def _get_causes_df(df: pd.DataFrame):
    df_cause = df.loc[:, ['p1', 'p36', 'p12']]
    df_cause['cause'] = pd.cut(df_cause['p12'], [99, 199, 299, 399, 499, 599, 699],
                               labels=['Nezaviněná řidičem', 'Nepřiměřená rychlost jízdy',
                                       'Nesprávné předjíždění', 'Nedání přednosti v jízdě', 'Nesprávný způsob jízdy',
                                       'Technická závada vozidla'])
    return df_cause


def plot_main_causes(df: pd.DataFrame, fig_location: str = None,
                     show_figure: bool = False):
    df_cause = _get_causes_df(df)
    df_main_cause_grouped = df_cause.groupby(['cause'], as_index=False).agg('size')
    df_main_cause_grouped['cause_percentage'] = df_main_cause_grouped['size'] / len(df_cause.index) * 100

    fig, ax = plt.subplots(1, 1, figsize=(6, 4))
    ax.tick_params(axis="x", bottom=False)
    ax.tick_params(axis="y", left=False)
    ax.grid(axis="x", which="major", color="black", alpha=.2, linewidth=.5)
    for pos in ['right', 'top', 'bottom', 'left']:
        ax.spines[pos].set_visible(False)

    sns.barplot(data=df_main_cause_grouped, x='cause_percentage', y='cause', orient='h')
    ax.set_title("Příčiny nehod", fontsize=15)
    ax.set_xlabel("Procento nehod")
    ax.set_ylabel("")
    for rect in ax.patches:
        ax.text(rect.get_width(), rect.get_y() + rect.get_height() / 2, "%.1f%%" % rect.get_width(), weight='bold')
    fig.tight_layout()
    _save_show_fig(fig, fig_location, show_figure)


def table_main_concrete_causes(df: pd.DataFrame):
    """
    Displays the consequences of accidents for each road type
    in a table. Rows = road type. Cols = consequences.
    """
    df_cause = _get_causes_df(df)
    df_cause_grouped = df_cause.groupby(['p12', 'cause'], as_index=False).agg('size')
    df_cause_grouped['percentage'] = df_cause_grouped['size'] / len(df_cause.index) * 100
    df_concrete_causes = df_cause_grouped[df_cause_grouped['percentage'] > 5]
    concrete_causes_labels = {100: "",
                              204: "nepřizpůsobení rychlosti stavu vozovky",
                              401: "semaforu",
                              403: "proti příkazu dopravní značky",
                              405: "při odbočování vlevo",
                              411: "přejíždění do jiného pruhu",
                              502: "vyhýbání bez dostatečného bočního odstupu",
                              503: "nedodržení bezpečné vzdálenosti za vozidlem",
                              504: "nesprávné otáčení nebo couvání",
                              508: "řidič se plně nevěnoval řízení vozidla",
                              511: "nezvládnutí řízení vozidla",
                              516: "jiný druh nesprávného způsobu jízdy"}
    df_concrete_causes['p12'] = df_concrete_causes['p12'].replace(concrete_causes_labels)
    df_table = pd.pivot_table(data=df_concrete_causes, index=['cause', 'p12'], values='percentage')
    df_table = df_table.rename(index={'cause': 'Příčina nehody', 'p12': "Upřesnění"},
                               columns={'percentage': '%'})
    df_table.index.names = ['Příčina nehody', 'Upřesnění']
    return df_table.to_latex(float_format="%.2f")


def count_accidents_during_day(df: pd.DataFrame):
    """ Returns the number of accidents which occurred during the day. """
    return ((1 <= df['p19']) & (df['p19'] <= 3)).sum()


def count_accidents_during_night(df: pd.DataFrame):
    """ Returns the number of accidents which occurred during the night. """
    return ((4 <= df['p19']) & (df['p19'] <= 7)).sum()


def get_number_of_days(df: pd.DataFrame):
    first_day = df['p2a'].min()
    last_day = df['p2a'].max()
    interval = datetime.fromisoformat(last_day) - datetime.fromisoformat(first_day)
    return interval.days


def compute_daily_accidents(df: pd.DataFrame):
    accidents = len(df.index)
    days = get_number_of_days(df)
    return accidents / days


if __name__ == "__main__":
    df = pd.read_pickle("accidents.pkl.gz")
    plot_time_roadtype(df, "04_typ_komunikace.png", True)
    plot_main_causes(df, "05_priciny.png", True)
    day_accidents = count_accidents_during_day(df)
    night_accidents = count_accidents_during_night(df)
    daily_accidents = compute_daily_accidents(df)
    total_accidents = day_accidents + night_accidents

    print("Table of concrete causes:\n", table_main_concrete_causes(df))
    print(F"Accidents during day: {day_accidents} ({day_accidents / total_accidents * 100:.2f} %)")
    print(f"Accidents during night: {night_accidents} ({night_accidents / total_accidents * 100:.2f} %)")
    print('Total accidents:', total_accidents)
    print(F"Daily accidents: {daily_accidents:.2f}")
