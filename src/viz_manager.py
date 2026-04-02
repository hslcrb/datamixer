import os
import tempfile
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

class VizManager:
    """Visualization dispatcher."""
    
    @staticmethod
    def generate_plotly_html(df, plot_type, x, y):
        """Generates interactive Plotly HTML strings with Dark Theme."""
        try:
            fig = None
            template = "plotly_dark"
            
            if plot_type == "히스토그램":
                fig = px.histogram(df, x=x, template=template, color_discrete_sequence=['#7aa2f7'])
            elif plot_type == "산점도":
                fig = px.scatter(df, x=x, y=y, template=template, color_discrete_sequence=['#bb9af7'])
            elif plot_type == "박스 플롯":
                fig = px.box(df, x=x, y=y, template=template, color_discrete_sequence=['#9ece6a'])
            elif plot_type == "히트맵" and y:
                fig = px.density_heatmap(df, x=x, y=y, template=template)
            elif plot_type == "선 그래프":
                fig = px.line(df, x=x, y=y, template=template, color_discrete_sequence=['#f7768e'])

            if fig:
                # Add professional styling
                fig.update_layout(
                    paper_bgcolor="#1a1b26",
                    plot_bgcolor="#1a1b26",
                    font_color="#c0caf5"
                )
                tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
                fig.write_html(tmp.name)
                return tmp.name, "Plotly Dark 렌더링 성공"
            return None, "지원하지 않는 플롯 유형"
        except Exception as e:
            return None, f"Plotly 오류: {e}"

    @staticmethod
    def generate_matplotlib_fig(df, plot_type, x, y):
        """Generates static Matplotlib figures with Dark Theme."""
        try:
            plt.style.use('dark_background')
            fig, ax = plt.subplots(figsize=(8, 6))
            fig.patch.set_facecolor('#1a1b26')
            ax.set_facecolor('#24283b')
            
            if plot_type == "히스토그램":
                sns.histplot(data=df, x=x, ax=ax, color='#7aa2f7')
            elif plot_type == "산점도":
                sns.scatterplot(data=df, x=x, y=y, ax=ax, color='#bb9af7')
            elif plot_type == "박스 플롯":
                sns.boxplot(data=df, x=x, y=y, ax=ax, color='#9ece6a')
            elif plot_type == "히트맵":
                # Basic categorical heatmap
                ct = pd.crosstab(df[x], df[y]) if y else df[x].value_counts()
                sns.heatmap(ct, ax=ax, cmap='magma')
            elif plot_type == "선 그래프":
                sns.lineplot(data=df, x=x, y=y, ax=ax, color='#f7768e')
            
            plt.tight_layout()
            return fig, "Matplotlib Dark 렌더링 성공"
        except Exception as e:
            return None, f"Matplotlib 오류: {e}"
