import os
import tempfile
import pandas as pd
import polars as pl
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

class VizManager:
    """Enterprise-grade High-End Visualization Engine with full Theme Consistency (Pd/Pl & Plotly/Mpl)."""
    
    @staticmethod
    def _to_pandas(df):
        if isinstance(df, pl.DataFrame):
            return df.to_pandas()
        return df

    @staticmethod
    def generate_plotly_html(df, plot_type, x, y, theme="dark"):
        """Generates interactive Plotly HTML report with theme-aware Tokyo Night aesthetic."""
        try:
            df = VizManager._to_pandas(df)
            is_dark = theme.lower() == "dark"
            fig = None
            template = "plotly_dark" if is_dark else "plotly_white"
            
            # Tokyo Night palettes
            dark_colors = ['#7aa2f7', '#bb9af7', '#9ece6a', '#f7768e', '#e0af68', '#73daca']
            light_colors = ['#2e7de9', '#9854f1', '#485e30', '#f7768e', '#8c6c3e', '#3891a6']
            colors = dark_colors if is_dark else light_colors
            
            # Theme UI colors
            bg_color = "#1a1b26" if is_dark else "#f0f1f4"
            font_color = "#c0caf5" if is_dark else "#3760bf"
            
            common_kw = {"template": template, "color_discrete_sequence": colors}
            
            if plot_type == "히스토그램":
                fig = px.histogram(df, x=x, **common_kw)
            elif plot_type == "산점도":
                fig = px.scatter(df, x=x, y=y, **common_kw)
            elif plot_type == "박스 플롯":
                fig = px.box(df, x=x, y=y, **common_kw)
            elif plot_type == "바이올린 플롯":
                fig = px.violin(df, y=y, x=x, box=True, points="all", **common_kw)
            elif plot_type == "선 그래프":
                fig = px.line(df, x=x, y=y, **common_kw)
            elif plot_type == "바 차트":
                fig = px.bar(df, x=x, y=y, **common_kw)
            elif plot_type == "파이 차트":
                fig = px.pie(df, names=x, values=y if y else None, template=template, color_discrete_sequence=colors)
            elif plot_type == "영역 차트":
                fig = px.area(df, x=x, y=y, **common_kw)
            elif plot_type == "히트맵 (상관관계)":
                corr = df.select_dtypes(include=['number']).corr()
                fig = px.imshow(corr, text_auto=True, template=template, aspect="auto", color_continuous_scale="Viridis" if is_dark else "Plasma")
            
            if fig:
                fig.update_layout(
                    paper_bgcolor=bg_color,
                    plot_bgcolor=bg_color,
                    font_color=font_color,
                    margin=dict(l=20, r=20, t=40, b=20)
                )
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
                fig.write_html(tmp.name)
                return tmp.name, "Plotly Interactive Report Generated"
            return None, "Unsupported Plot Type"
        except Exception as e:
            return None, f"Plotly Engine Failure: {str(e)}"

    @staticmethod
    def generate_matplotlib_fig(df, plot_type, x, y, theme="dark"):
        """Generates static Matplotlib PNG with theme-aware Tokyo Night aesthetic."""
        try:
            df = VizManager._to_pandas(df)
            is_dark = theme.lower() == "dark"
            
            if is_dark:
                plt.style.use('dark_background')
                bg_color, card_color = '#1a1b26', '#24283b'
                palette = ["#7aa2f7", "#bb9af7", "#9ece6a", "#f7768e", "#e0af68"]
            else:
                plt.style.use('default')
                bg_color, card_color = '#f0f1f4', '#ffffff'
                palette = ["#2e7de9", "#9854f1", "#485e30", "#f7768e", "#8c6c3e"]
            
            fig, ax = plt.subplots(figsize=(10, 7), dpi=100)
            fig.patch.set_facecolor(bg_color)
            ax.set_facecolor(card_color)
            
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
                sns.barplot(data=df, x=x, y=y, ax=ax, palette=[palette[0]])
            elif plot_type == "파이 차트":
                counts = df[x].value_counts()
                ax.pie(counts, labels=counts.index, autopct='%1.1f%%', colors=palette, startangle=140)
            
            plt.tight_layout()
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".png")
            fig.savefig(tmp.name, facecolor=fig.get_facecolor(), bbox_inches='tight')
            plt.close(fig)
            return tmp.name, "Matplotlib Static Report Generated"
        except Exception as e:
            return None, f"Matplotlib Engine Failure: {str(e)}"
