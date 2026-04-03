import os
import tempfile
import pandas as pd
import polars as pl
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

class VizManager:
    """Enterprise-grade High-End Visualization Engine (Pd/Pl & Plotly/Mpl)."""
    
    @staticmethod
    def _to_pandas(df):
        """Standardizes input DataFrame to Pandas for visualization engines."""
        if isinstance(df, pl.DataFrame):
            return df.to_pandas()
        return df

    @staticmethod
    def generate_plotly_html(df, plot_type, x, y):
        """Generates interactive Plotly HTML report with Tokyo Night aesthetic."""
        try:
            df = VizManager._to_pandas(df)
            fig = None
            template = "plotly_dark"
            colors = ['#7aa2f7', '#bb9af7', '#9ece6a', '#f7768e', '#e0af68', '#73daca']
            
            if plot_type == "히스토그램":
                fig = px.histogram(df, x=x, template=template, color_discrete_sequence=[colors[0]])
            elif plot_type == "산점도":
                fig = px.scatter(df, x=x, y=y, template=template, color_discrete_sequence=[colors[1]])
            elif plot_type == "박스 플롯":
                fig = px.box(df, x=x, y=y, template=template, color_discrete_sequence=[colors[2]])
            elif plot_type == "바이올린 플롯":
                fig = px.violin(df, y=y, x=x, box=True, points="all", template=template, color_discrete_sequence=[colors[3]])
            elif plot_type == "선 그래프":
                fig = px.line(df, x=x, y=y, template=template, color_discrete_sequence=[colors[4]])
            elif plot_type == "바 차트":
                fig = px.bar(df, x=x, y=y, template=template, color_discrete_sequence=[colors[5]])
            elif plot_type == "파이 차트":
                fig = px.pie(df, names=x, values=y if y else None, template=template)
            elif plot_type == "영역 차트":
                fig = px.area(df, x=x, y=y, template=template, color_discrete_sequence=[colors[0]])
            elif plot_type == "히트맵 (상관관계)":
                corr = df.select_dtypes(include=['number']).corr()
                fig = px.imshow(corr, text_auto=True, template=template, aspect="auto")
            elif plot_type == "밀도 히트맵":
                fig = px.density_heatmap(df, x=x, y=y, template=template)

            if fig:
                fig.update_layout(
                    paper_bgcolor="#1a1b26",
                    plot_bgcolor="#1a1b26",
                    font_color="#c0caf5",
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
                fig.write_html(tmp.name)
                return tmp.name, "Plotly Interactive Report Generated"
            return None, "Unsupported Plot Type"
        except Exception as e:
            return None, f"Plotly Engine Failure: {str(e)}"

    @staticmethod
    def generate_matplotlib_fig(df, plot_type, x, y):
        """Generates static high-resolution Matplotlib PNG with Tokyo Night aesthetic."""
        try:
            df = VizManager._to_pandas(df)
            plt.style.use('dark_background')
            fig, ax = plt.subplots(figsize=(10, 7), dpi=100)
            fig.patch.set_facecolor('#1a1b26')
            ax.set_facecolor('#24283b')
            
            # Professional palette
            palette = ["#7aa2f7", "#bb9af7", "#9ece6a", "#f7768e", "#e0af68", "#73daca"]
            
            if plot_type == "히스토그램":
                sns.histplot(data=df, x=x, ax=ax, color=palette[0], kde=True)
            elif plot_type == "산점도":
                sns.scatterplot(data=df, x=x, y=y, ax=ax, color=palette[1])
            elif plot_type == "박스 플롯":
                sns.boxplot(data=df, x=x, y=y, ax=ax, palette=[palette[2]])
            elif plot_type == "바이올린 플롯":
                sns.violinplot(data=df, x=x, y=y, ax=ax, palette=[palette[3]])
            elif plot_type == "선 그래프":
                sns.lineplot(data=df, x=x, y=y, ax=ax, color=palette[4])
            elif plot_type == "바 차트":
                sns.barplot(data=df, x=x, y=y, ax=ax, palette=[palette[5]])
            elif plot_type == "파이 차트":
                counts = df[x].value_counts()
                ax.pie(counts, labels=counts.index, autopct='%1.1f%%', colors=palette, startangle=140)
            elif plot_type == "영역 차트":
                df.plot.area(x=x, y=y, ax=ax, color=palette[0], alpha=0.5)
            elif plot_type == "히트맵 (상관관계)":
                corr = df.select_dtypes(include=['number']).corr()
                sns.heatmap(corr, annot=True, ax=ax, cmap='viridis', fmt='.2f')
            elif plot_type == "밀도 히트맵":
                sns.kdeplot(data=df, x=x, y=y, ax=ax, fill=True, cmap='rocket')

            plt.tight_layout()
            
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            fig.savefig(tmp.name, facecolor=fig.get_facecolor(), bbox_inches='tight')
            plt.close(fig)
            return tmp.name, "Matplotlib Static Report Generated"
        except Exception as e:
            return None, f"Matplotlib Engine Failure: {str(e)}"
