import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

df = pd.read_excel('/Users/martin/Desktop/EdgeDownload/暑期占比数据.xlsx')

# 清洗：将行标签改为日期列，去除空值列
df['日期'] = pd.to_datetime(df['日期'])
df = df.set_index('日期')
# 选取前8个目的地
destinations = df.columns[:8] 

# 绘制折线图
fig = go.Figure()
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
          '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf',
          '#f4c542', '#aec7e8', '#ffbb78', '#98df8a', '#c5b0d5']

for i, col in enumerate(destinations):
    fig.add_trace(go.Scatter(
        x=df.index,
        y=df[col],
        mode='lines',
        name=col,
        line=dict(width=2, color=colors[i % len(colors)]),
        hovertemplate='%{x|%Y-%m-%d}<br>%{fullData.name}: %{y:.2%}<extra></extra>'
    ))

# 横轴日期格式化为中文（如“7月7日”）
fig.update_xaxes(
    tickformat='%Y-%m-%d',   # 英文月份数字，显示为“07月07日”
    tickangle=45,
    dtick='M1',              # 每月一个刻度
)
# 布局设置（高清）
fig.update_layout(
    title={
        'text': '暑期流量占比趋势 x 目的地',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 24}
    },
    xaxis_title='日期',
    yaxis_title='流量占比 (%)',
    yaxis_tickformat='.0%',
    width=1600,
    height=800,
    template='plotly_white',
    legend=dict(
        orientation='h',
        yanchor='bottom',
        y=1.02,
        xanchor='left',
        x=0,
        font=dict(size=12)
    ),
    margin=dict(l=50, r=200, t=80, b=50)
)

# 显示交互图表
fig.show()
# 保存为高清 PNG（需要安装 kaleido: pip install kaleido）
# fig.write_image('traffic_trend.png', scale=3)  # scale=3 提高分辨率

"""

# 获取目的地列表
destinations = df.columns.tolist()
n_cols = len(destinations)

# 创建子图：1行 n_cols 列
fig = make_subplots(
    rows=1, cols=n_cols,
    subplot_titles=destinations,
    shared_xaxes=True,      # 共享 X 轴（日期）
    shared_yaxes=False,     # 独立 Y 轴（因为各目的地数据量级不同）
    horizontal_spacing=0.03,# 子图间距
    x_title='日期',
    y_title='流量占比'
)

# 为每个目的地添加折线图
for i, dest in enumerate(destinations, start=1):
    fig.add_trace(
        go.Scatter(
            x=df.index,
            y=df[dest],
            mode='lines',
            name=dest,
            line=dict(width=1.5),
            showlegend=False   # 子图标题已标识，不重复显示图例
        ),
        row=1, col=i
    )
    # 设置 Y 轴格式为百分比
    fig.update_yaxes(tickformat='.0%', row=1, col=i)

# 全局布局：高清大图
fig.update_layout(
    title={
        'text': '暑期流量占比趋势 x 目的地',
        'x': 0.5,
        'xanchor': 'center',
        'font': {'size': 24}
    },
    width=4000,            # 足够宽以保证每个子图清晰
    height=600,
    template='plotly_white',
    showlegend=False
)

# 仅第一个子图显示 X 轴标题（因为共享 X 轴）
fig.update_xaxes(title_text='日期', row=1, col=1)

# 显示交互图表
fig.show()
"""