import io
import re
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from cycler import cycler
from collections import Counter
from wordcloud import WordCloud


class VisualReportGenerator:
    def __init__(self, engine) -> None:
        self.engine = engine
        self.pallete = ["#60b0d1", "#795f5b", "#436dc2", "#ad6152", "#1b1b51" ]
        self.datasets = self.__get_datasets()
        self.frames = self.__get_frames()
        self.cmap = ListedColormap(self.pallete, name="everything_data")
        self.buffer = io.BytesIO()
        self.stopwords = {
            "the","and","a","an","to","into","in","of","for","my","i","im",
            "id","i’d","i’m","aim","join","data","program", "skill","like",
            "everything", "with", "this", "from", "have", "will",
            "more", "about", "would", "that", "want", "get", "can", "also"
        }

        mpl.rcParams.update({
            "axes.prop_cycle": cycler("color", self.pallete),
        })
    
    def create_report(self, out_path: str | None = None, *, return_bytes: bool = False, dpi: int = 200):
        """
        Build the report figure. If `return_bytes=True`, returns PNG bytes.
        Else, saves to `out_path` (or /tmp/report.png if not provided) and returns the path.
        """
        try:
            self._create_subplots()
            self._add_cumulative_registration()
            self._add_regitstation_hour_hist()
            self._add_motivation_wordcloud()
            self._add_percent_graduated()
            self._add_aptitude_boxes()
            self._add_country_age_range_bars()
            self._add_aim_counts()
            self._add_refferal_rank()
            self.fig.tight_layout()
            self.fig.savefig(self.buffer, format='png')
        finally:
            # always free memory
            plt.close(self.fig)
            self.buffer.seek(0)
            return self.buffer

    def __get_datasets(self):
        datasets = pd.read_sql(
        """
        SELECT name, category, description, query
        FROM datasets_dataset
        """, self.engine
        )
        return datasets

    def __get_frames(self):
        frames = {}
        for row in self.datasets.itertuples():
            frame = pd.read_sql(row.query, self.engine)  # <-- use self.engine
            frames[row.name] = frame
        return frames
    
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
            # center of the bar (or use a slight offset if stacked on top of something)
            if offset_center:
                y = bar.get_y() + val/2
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
        df_registration = self.frames['student_registration'].copy()
        df_registration["date"] = pd.to_datetime(df_registration["registration_date"])
        daily = df_registration.groupby("date").size().cumsum().reset_index(name="cumulative")

        # Sort chronologically and compute cumulative count
        df_registration = df_registration.sort_values("date").reset_index(drop=True)
        df_registration["cumulative"] = range(1, len(df_registration) + 1)

        self.ax1.plot(df_registration["date"], df_registration["cumulative"])
        self.ax1.set_title("Cumulative Registration (Cumulative)")
        self.ax1.set_xlabel("Date")
        self.ax1.spines['right'].set_visible(False)
        self.ax1.spines['top'].set_visible(False)
        self.ax1.set_ylabel("Registrations")

    def _add_regitstation_hour_hist(self):
        df_registration = self.frames['student_registration'].copy()
        d = pd.to_datetime(df_registration["registration_date"], errors="coerce")
        t = pd.to_timedelta(df_registration["registration_time"].astype(str), errors="coerce")
        df_registration["timestamp"] = d + t
        hours = df_registration["timestamp"].dt.hour.dropna()

        n, bins, patches = self.ax2.hist(hours, bins=range(0, 25), align="left", rwidth=0.9)
        self.ax2.spines['right'].set_visible(False)
        self.ax2.spines['top'].set_visible(False)
        self.ax2.set_xlabel("Hour of day (0–23)")
        self.ax2.set_ylabel("Registrations")
        self.ax2.set_title("Popular Registration Hours")
        self.ax2.set_xticks(range(0, 24))
        self.ax2.grid(True, axis="y")
        for count, patch in zip(n, patches):
            if count > 0:
                x = patch.get_x() + patch.get_width()/2
                y = patch.get_height()
                self.ax2.text(x, y, f"{int(count)}", ha="center", va="bottom")

    def _add_motivation_wordcloud(self):
        df_motivation = self.frames['student_motivation'].copy()        
        text_tokens = []
        for t in df_motivation["motivation"].astype(str):
            text_tokens.extend(
                [
                    w for w in self.__simple_tokenize(t) 
                    if w not in self.stopwords and len(w) > 2
                ]
            )
        freqs = Counter(text_tokens)
        wc = WordCloud(
            width=1200, height=800, background_color='white', colormap=self.cmap
        )  # no explicit colors/styles
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
        df_age = self.frames['student_count_by_country_and_age'].copy()
        # Pivot to get countries as columns, age ranges as index
        pivot = df_age.pivot_table(
            index="age_range", columns="country", values="student_count", aggfunc="sum"
        ).fillna(0)
        pivot = pivot.sort_index() 

        self.ax6.set_xlabel("Age range")
        self.ax6.tick_params(axis='y', which='both', left=False, labelleft=False)
        self.ax6.spines['left'].set_visible(False)
        self.ax6.spines['top'].set_visible(False)
        self.ax6.spines['right'].set_visible(False)
        self.ax6.set_ylabel("Student count")
        self.ax6.set_title("Students Age Ranges, by Country")
        self.ax6.legend(title="Country")

    def _add_aim_counts(self):
        df_motivation = self.frames['student_motivation'].copy()
        counts = df_motivation["aim"].value_counts()
        counts_grp = self.__group_small_slices(counts, threshold=0.10)
        self.ax7.pie(
            counts_grp.values,
            labels=counts_grp.index,
            autopct=lambda p: f"{p:.0f}%",   # show percents
            startangle=60,
            shadow=True
        )
        self.ax7.set_title("Aims Distribution")
        self.ax7.axis("equal")
    
    def _add_refferal_rank(self):
        df_refferal = self.frames['referral_analysis'].copy()
        df_refferal = df_refferal.sort_values("student_count", ascending=True)  # ascending for barh so largest is on top
        bars = self.ax8.barh(df_refferal["referral"], df_refferal["student_count"])
        self.ax8.set_title("Referral Types Rank")
        self.ax8.set_xlabel("Students")
        self.ax8.spines['right'].set_visible(False)
        self.ax8.spines['top'].set_visible(False)

        # Add value labels to the right of each bar
        for b, val in zip(bars, df_refferal["student_count"]):
            x = b.get_width()
            y = b.get_y() + b.get_height()/2
            self.ax8.text(x + max(df_refferal["student_count"])*0.01, y, f"{val}", va="center")
    
        