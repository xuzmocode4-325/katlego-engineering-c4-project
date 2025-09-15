import io
import re
import time
import logging
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from cycler import cycler
from collections import Counter
from wordcloud import WordCloud

from psycopg import OperationalError as PsycopgError

from django.db import connections, DEFAULT_DB_ALIAS
from django.db.migrations.executor import MigrationExecutor


logger = logging.getLogger(__file__)


class VisualReportGenerator:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.palette = ["#60b0d1", "#795f5b", "#436dc2", "#ad6152", "#1b1b51" ]
        self.cmap = ListedColormap(self.palette, name="everything_data")
        self.buffer = io.BytesIO()
        self.stopwords = {
            "the","and","a","an","to","into","in","of","for","my","i","im",
            "id","i’d","i’m","aim","join","data","program", "skill","like",
            "everything", "with", "this", "from", "have", "will",
            "more", "about", "would", "that", "want", "get", "can", "also"
        }

        mpl.rcParams.update({
            "axes.prop_cycle": cycler("color", self.palette),
        })
    
    def create_report(self, out_path: str | None = None, *, return_bytes: bool = False, dpi: int = 200):
        """
        Build the report figure. If `return_bytes=True`, returns PNG bytes.
        Else, saves to `out_path` (or /tmp/report.png if not provided) and returns the path.
        """
        created_fig = False
        try:
            self.frames = self.__get_datasets()
            self._create_subplots()
            created_fig = True

            self._add_cumulative_registration()
            self._add_registration_hour_hist() 
            self._add_motivation_wordcloud()
            self._add_percent_graduated()
            self._add_aptitude_boxes()
            self._add_country_age_range_bars()
            self._add_aim_counts()
            self._add_referral_rank()  

            self.fig.tight_layout()

            if return_bytes:
                # write to in-memory buffer
                self.buffer = io.BytesIO()
                self.fig.savefig(self.buffer, format='png', dpi=dpi, bbox_inches="tight")
                self.buffer.seek(0)
                return self.buffer
            else:
                path = out_path or "/tmp/report.png"
                self.fig.savefig(path, dpi=dpi, bbox_inches="tight")
                return path
        finally:
            if created_fig:
                plt.close(self.fig)

    def __django_migrations_pending(self) -> bool:
        conn = connections[DEFAULT_DB_ALIAS]
        executor = MigrationExecutor(conn)
        plan = executor.migration_plan(executor.loader.graph.leaf_nodes())
        return bool(plan)  

    def __get_datasets(self):
        frames = {}
        if not self.__django_migrations_pending():
            try: 
                datasets = pd.read_sql(
                """
                SELECT name, category, description, query
                FROM datasets_dataset
                """, self.engine
                )
                for row in datasets.itertuples():
                    try:
                        frames[row.name] = pd.read_sql(str(row.query), self.engine)
                    except Exception as e:
                        logger.exception("Query failed for dataset %s", row.name)
                return frames
            except Exception as e:
                logger.exception("database not available, waiting 5 seconds...")
                logger.error(str(e))
                time.sleep(10)
                
    def __simple_tokenize(self, s: str) -> list[str]:
        s = s.lower()
        s = re.sub(r"[’'`]", "", s)            # normalize apostrophes
        s = re.sub(r"[^a-z0-9\s]", " ", s)     # drop punctuation
        tokens = s.split()
        return tokens
    
    def __label_segments(self, bars, values, offset_center=False):
        for bar, val in zip(bars, values):
            if val <= 0:
                continue
            x = bar.get_x() + bar.get_width()/2
            if offset_center:
                y = bar.get_y() + bar.get_height() - (bar.get_height() - val/2)  # true inner-center
            else:
                y = bar.get_y() + val/2
            self.ax4.text(x, y, f"{val:.0f}%", ha="center", va="center")

    def __group_small_slices(self, counts: pd.Series, threshold=0.10, other_label="other") -> pd.Series:
            total = counts.sum()
            small = counts[counts / total < threshold]
            large = counts[counts / total >= threshold]
            if small.empty:
                return counts.sort_values(ascending=False)
            grouped = pd.concat([large, pd.Series({other_label: small.sum()})])
            return grouped.sort_values(ascending=False)
    
    def _create_subplots(self):
        # keep a figure handle
        self.fig = plt.figure(figsize=(16.5, 11.7))
        self.ax1 = plt.subplot2grid((4, 4), (0, 0), colspan=2, rowspan=1)
        self.ax2 = plt.subplot2grid((4, 4), (1, 0), colspan=2, rowspan=1)
        self.ax3 = plt.subplot2grid((4, 4), (0, 2), colspan=2, rowspan=2)
        self.ax4 = plt.subplot2grid((4, 4), (2, 0), rowspan=2)
        self.ax5 = plt.subplot2grid((4, 4), (2, 1), rowspan=2)
        self.ax6 = plt.subplot2grid((4, 4), (2, 2))
        self.ax7 = plt.subplot2grid((4, 4), (2, 3))
        self.ax8 = plt.subplot2grid((4, 4), (3, 2), colspan=2)

    def _add_cumulative_registration(self):
        df = self.frames['student_registration'].copy()
        df["date"] = pd.to_datetime(df["registration_date"], errors="coerce")
        df = df.dropna(subset=["date"]).sort_values("date")
        daily = df.groupby("date").size().cumsum().reset_index(name="cumulative")
        self.ax1.plot(daily["date"], daily["cumulative"])
        self.ax1.set_title("Cumulative Registrations")
        self.ax1.set_xlabel("Date")
        self.ax1.set_ylabel("Registrations")
        self.ax1.spines['right'].set_visible(False)
        self.ax1.spines['top'].set_visible(False)

    def _add_registration_hour_hist(self):  # was _add_regitstation_hour_hist
        df = self.frames['student_registration'].copy()
        d = pd.to_datetime(df["registration_date"], errors="coerce")
        # handle time strings like '13:45:02' or time objects robustly
        t = pd.to_timedelta(df["registration_time"].astype(str), errors="coerce")
        ts = (d + t).dropna()
        hours = ts.dt.hour

        n, bins, patches = self.ax2.hist(hours, bins=range(0, 25), align="left", rwidth=0.9)
        self.ax2.set_title("Popular Registration Hours")
        self.ax2.set_xlabel("Hour of day (0–23)")
        self.ax2.set_ylabel("Registrations")
        self.ax2.set_xticks(range(0, 24))
        self.ax2.grid(True, axis="y")
        self.ax2.spines['right'].set_visible(False)
        self.ax2.spines['top'].set_visible(False)
        for count, patch in zip(n, patches):
            if count > 0:
                x = patch.get_x() + patch.get_width()/2
                y = patch.get_height()
                self.ax2.text(x, y, f"{int(count)}", ha="center", va="bottom")

    def _add_motivation_wordcloud(self):
        df = self.frames['student_motivation'].copy()
        tokens = []
        for t in df["motivation"].astype(str):
            tokens.extend([w for w in self.__simple_tokenize(t) if w not in self.stopwords and len(w) > 2])
        freqs = Counter(tokens)
        if not freqs:
            self.ax3.axis("off")
            self.ax3.set_title("Motivation Word Cloud (no data)")
            return
        wc = WordCloud(width=1200, height=800, background_color='white', colormap=self.cmap)
        wc.generate_from_frequencies(freqs)
        self.ax3.imshow(wc, interpolation="bilinear")
        self.ax3.axis("off")
        self.ax3.set_title("Motivation Word Cloud")

    def _add_percent_graduated(self):
        df_graduation = self.frames['graduation_rate_by_track'].copy()
        df_graduation["graduation_rate"] = df_graduation["graduation_rate"].clip(0, 100)
        df_graduation["not_graduated"] = 100 - df_graduation["graduation_rate"]

        tracks = df_graduation["track"].tolist()
        grad = df_graduation["graduation_rate"].to_list()
        notgrad = df_graduation["not_graduated"].to_list()

        bars_grad = self.ax4.bar(tracks, grad, label="Graduated")
        bars_not = self.ax4.bar(tracks, notgrad, bottom=grad, label="Not graduated")
        # Axis / labels
        self.ax4.set_ylim(0, 100)
        self.ax4.set_ylabel("Percent of students")
        self.ax4.set_title("Graduation Rate by Track")
        self.ax4.spines['right'].set_visible(False)
        self.ax4.spines['top'].set_visible(False)
        self.ax4.legend()

        self.__label_segments(bars_grad, grad, offset_center=True)
        self.__label_segments(bars_not, notgrad, offset_center=True)

    def _add_aptitude_boxes(self):
        df_aptitude = self.frames['aptitude_summary_by_track'].copy()
        stats = []
        stats = []
        for _, row in df_aptitude.iterrows():
            stats.append({
                "label": row["track"],
                "whislo": float(row["min_score"]),   # low whisker (min)
                "q1":     float(row["q1"]),          # first quartile
                "med":    float(row["median"]),      # median
                "q3":     float(row["q3"]),          # third quartile
                "whishi": float(row["max_score"]),   # high whisker (max)
                "fliers": []                         # no outliers (we only have summary stats)
            })
        self.ax5.bxp(stats, showfliers=False)

        # Optional: overlay the mean as a diamond marker for each track
        for i, (_, row) in enumerate(df_aptitude.iterrows(), start=1):
            self.ax5.plot(i, row["mean_score"], marker="D")  # no explicit colors/styles

        self.ax5.set_ylabel("Aptitude score")
        self.ax5.set_title("Aptitude Score Distribution by Track")
        self.ax5.grid(True, axis="y")

    def _add_country_age_range_bars(self):
        df = self.frames['student_count_by_country_and_age'].copy()
        pivot = df.pivot_table(index="age_range", columns="country", values="student_count", aggfunc="sum").fillna(0)
        pivot = pivot.sort_index()

        # stacked bars
        pivot.plot(kind="bar", stacked=True, ax=self.ax6)

        self.ax6.set_xlabel("Age range")
        self.ax6.set_ylabel("Student count")
        self.ax6.set_title("Students by Age Range and Country")
        self.ax6.spines['left'].set_visible(False)
        self.ax6.spines['top'].set_visible(False)
        self.ax6.spines['right'].set_visible(False)
        self.ax6.grid(True, axis="y")
        self.ax6.legend(title="Country", bbox_to_anchor=(1.02, 1), loc="upper left")

    def _add_aim_counts(self):
        df = self.frames['student_motivation'].copy()
        counts = df["aim"].dropna().astype(str).value_counts()
        counts_grp = self.__group_small_slices(counts, threshold=0.10)
        self.ax7.pie(
            counts_grp.values,
            labels=counts_grp.index,
            autopct=lambda p: f"{p:.0f}%",
            startangle=60,
            # shadow=True  # shadow off for cleaner export/print
        )
        self.ax7.set_title("Aims Distribution")
        self.ax7.axis("equal")

    def _add_referral_rank(self):  # was _add_refferal_rank
        df = self.frames['referral_analysis'].copy()
        df = df.sort_values("student_count", ascending=True)
        bars = self.ax8.barh(df["referral"], df["student_count"])
        self.ax8.set_title("Referral Types Rank")
        self.ax8.set_xlabel("Students")
        self.ax8.spines['right'].set_visible(False)
        self.ax8.spines['top'].set_visible(False)

        maxv = df["student_count"].max() if not df.empty else 0
        for b, val in zip(bars, df["student_count"]):
            x = b.get_width()
            y = b.get_y() + b.get_height()/2
            self.ax8.text(x + (0.01 * maxv), y, f"{val}", va="center")