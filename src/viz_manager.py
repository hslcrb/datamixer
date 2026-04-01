import plotly.io as pio
import plotly.express as px
import pandas as pd
import tempfile
import os

class VizManager:
    """Manages high-end interactive Plotly visualizations."""
    
    @staticmethod
    def generate_plotly_html(df, plot_type, x=None, y=None, color=None):
        """Generates Plotly HTML for interactive browser views."""
        try:
            if plot_type == "히스토그램":
                fig = px.histogram(df, x=x, marginal="box", nbins=50, title=f"Histogram: {x}")
            elif plot_type == "산점도":
                fig = px.scatter(df, x=x, y=y, color=color, trendline="ols", 
                                title=f"Scatter: {x} vs {y}", opacity=0.7)
            elif plot_type == "박스 플롯":
                fig = px.box(df, x=x, y=y, points="all", title=f"Box Plot: {x} vs {y}")
            elif plot_type == "히트맵":
                corr = df.select_dtypes(include=['number']).corr()
                fig = px.imshow(corr, text_auto=True, aspect="auto", 
                               color_continuous_scale='RdBu_r', title="Correlation Heatmap")
            elif plot_type == "선 그래프":
                fig = px.line(df, x=x, y=y, markers=True, title=f"Line Chart: {x} vs {y}")
            else:
                return None, "지원하지 않는 그래프 형식"

            # Professional Template
            fig.update_layout(template="plotly_white", margin=dict(l=40, r=40, t=100, b=40))
            
            # Temporary HTML Export
            tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".html")
            tmp.close()
            pio.write_html(fig, tmp.name, auto_open=False, include_plotlyjs='cdn')
            return tmp.name, "그래프 생성 성공"
        except Exception as e:
            return None, f"그래프 엔진 오류: {e}"
