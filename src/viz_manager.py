import plotly.io as pio
import plotly.express as px
import pandas as pd
import tempfile
import os
import matplotlib.pyplot as plt
import seaborn as sns
from io import BytesIO

class VizManager:
    """Manages multi-library visualization (Plotly & Matplotlib)."""
    
    @staticmethod
    def generate_plotly_html(df, plot_type, x=None, y=None, color=None):
        """Generates interactive Plotly HTML."""
        try:
            if plot_type == "히스토그램":
                fig = px.histogram(df, x=x, marginal="box", nbins=50, title=f"Histogram: {x}")
            elif plot_type == "산점도":
                fig = px.scatter(df, x=x, y=y, color=color, trendline="ols", opacity=0.7)
            elif plot_type == "박스 플롯":
                fig = px.box(df, x=x, y=y, points="all")
            elif plot_type == "히트맵":
                corr = df.select_dtypes(include=['number']).corr()
                fig = px.imshow(corr, text_auto=True, aspect="auto", color_continuous_scale='RdBu_r')
            elif plot_type == "선 그래프":
                fig = px.line(df, x=x, y=y, markers=True)
            else:
                return None, "지원하지 않는 그래프 형식"

            fig.update_layout(template="plotly_white")
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
            tmp.close()
            pio.write_html(fig, tmp.name, auto_open=False, include_plotlyjs='cdn')
            return tmp.name, "Plotly 생성 성공"
        except Exception as e:
            return None, f"Plotly 엔진 오류: {e}"

    @staticmethod
    def generate_matplotlib_fig(df, plot_type, x=None, y=None):
        """Generates static Matplotlib Figure object."""
        try:
            from matplotlib.figure import Figure
            fig = Figure(figsize=(8, 6), dpi=100)
            ax = fig.add_subplot(111)
            sns.set_theme(style="whitegrid")
            
            if plot_type == "히스토그램":
                sns.histplot(data=df, x=x, ax=ax, kde=True, color='skyblue')
            elif plot_type == "산점도":
                sns.scatterplot(data=df, x=x, y=y, ax=ax)
            elif plot_type == "박스 플롯":
                sns.boxplot(data=df, x=x, y=y, ax=ax)
            elif plot_type == "히트맵":
                corr = df.select_dtypes(include=['number']).corr()
                sns.heatmap(corr, annot=True, cmap='RdBu_r', center=0, ax=ax)
            elif plot_type == "선 그래프":
                sns.lineplot(data=df, x=x, y=y, ax=ax, marker='o')
            
            ax.set_title(f"Matplotlib: {plot_type}")
            fig.tight_layout()
            return fig, "Matplotlib 생성 성공"
        except Exception as e:
            return None, f"Matplotlib 엔진 오류: {e}"
