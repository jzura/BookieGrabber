"""
Euro Football Bets — Performance Dashboard
Streamlit app for tracking betting performance.
Deployed on Streamlit Cloud, reads from dashboard_data/bets.csv
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime

# ─── Config ───
st.set_page_config(
    page_title="Euro Football Bets",
    page_icon="⚽",
    layout="wide",
    initial_sidebar_state="expanded",
)

PASSWORD = st.secrets.get("password", "efb2026") if hasattr(st, 'secrets') else "efb2026"

# ─── Auth ───
def check_password():
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if st.session_state.authenticated:
        return True
    pwd = st.text_input("Password", type="password")
    if pwd == PASSWORD:
        st.session_state.authenticated = True
        st.rerun()
    elif pwd:
        st.error("Incorrect password")
    return False

if not check_password():
    st.stop()

# ─── Load Data ───
@st.cache_data(ttl=300)
def load_data():
    csv_path = Path(__file__).parent / "dashboard_data" / "bets.csv"
    if not csv_path.exists():
        st.error("Data file not found. Run export_dashboard_data.py first.")
        st.stop()

    df = pd.read_csv(csv_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df['Month'] = df['Date'].dt.to_period('M').astype(str)

    for col in ['Bet365', 'BF', 'Volume', 'RPD', 'Stake', 'Return', 'Profit', 'SM_Odds']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['Result'] = pd.to_numeric(df['Result'], errors='coerce')

    return df

df = load_data()

# Filter to staked bets
staked = df[df['Stake'].notna() & (df['Stake'] > 0)].copy()
settled = staked[staked['Profit'].notna()].copy()

# ─── Sidebar ───
st.sidebar.title("⚽ Euro Football Bets")
st.sidebar.markdown("---")

min_date = df['Date'].min().date()
max_date = df['Date'].max().date()
date_range = st.sidebar.date_input("Date Range", value=(min_date, max_date),
                                     min_value=min_date, max_value=max_date)
if len(date_range) == 2:
    staked_f = staked[staked['Date'].dt.date.between(date_range[0], date_range[1])]
    settled_f = settled[settled['Date'].dt.date.between(date_range[0], date_range[1])]
else:
    staked_f = staked
    settled_f = settled

markets = st.sidebar.multiselect("Markets", options=sorted(df['Market'].unique()),
                                   default=sorted(df['Market'].unique()))
staked_f = staked_f[staked_f['Market'].isin(markets)]
settled_f = settled_f[settled_f['Market'].isin(markets)]

st.sidebar.markdown("---")
st.sidebar.caption(f"Data: {len(df):,} rows | Staked: {len(staked_f):,} | Last: {max_date}")

# ─── KPI Cards ───
st.title("Performance Dashboard")

col1, col2, col3, col4, col5 = st.columns(5)

total_staked = settled_f['Stake'].sum()
total_profit = settled_f['Profit'].sum()
n_bets = len(settled_f)
n_wins = len(settled_f[settled_f['Return'] > 0])
win_rate = n_wins / n_bets * 100 if n_bets else 0
roi = total_profit / total_staked * 100 if total_staked else 0

col1.metric("Total Bets", f"{n_bets:,}")
col2.metric("Win Rate", f"{win_rate:.1f}%")
col3.metric("Total Staked", f"{total_staked:.0f}")
col4.metric("Total Profit", f"{total_profit:.2f}", delta=f"{roi:.1f}% ROI")
col5.metric("Avg SM Odds", f"{settled_f['SM_Odds'].mean():.2f}" if settled_f['SM_Odds'].notna().any() else "N/A")

st.markdown("---")

# ─── Cumulative Profit Chart ───
st.subheader("Cumulative Profit")

cum_profit = settled_f.sort_values('Date').copy()
cum_profit['Cum_Profit'] = cum_profit['Profit'].cumsum()
cum_profit['Bet_Number'] = range(1, len(cum_profit) + 1)

fig_cum = go.Figure()
fig_cum.add_trace(go.Scatter(
    x=cum_profit['Bet_Number'], y=cum_profit['Cum_Profit'],
    mode='lines', line=dict(color='#4CAF50', width=2),
    name='Cumulative Profit',
    hovertemplate='Bet #%{x}<br>Cum Profit: %{y:.2f}<extra></extra>'
))
fig_cum.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
fig_cum.update_layout(template='plotly_dark', height=400,
    margin=dict(l=40, r=20, t=20, b=40),
    xaxis_title="Bet Number", yaxis_title="Profit (Units)")
st.plotly_chart(fig_cum, use_container_width=True)

# ─── Monthly + By Market ───
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Monthly Performance")
    monthly = settled_f.groupby('Month').agg(
        Bets=('Profit', 'count'), Staked=('Stake', 'sum'),
        Profit=('Profit', 'sum'), Wins=('Return', lambda x: (x > 0).sum()),
    ).reset_index()
    monthly['ROI %'] = (monthly['Profit'] / monthly['Staked'] * 100).round(1)
    monthly['SR %'] = (monthly['Wins'] / monthly['Bets'] * 100).round(1)

    fig_m = go.Figure()
    colors = ['#4CAF50' if p >= 0 else '#f44336' for p in monthly['Profit']]
    fig_m.add_trace(go.Bar(x=monthly['Month'], y=monthly['Profit'], marker_color=colors))
    fig_m.update_layout(template='plotly_dark', height=350,
        margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit (Units)")
    st.plotly_chart(fig_m, use_container_width=True)
    st.dataframe(monthly[['Month', 'Bets', 'Profit', 'ROI %', 'SR %']],
                 use_container_width=True, hide_index=True)

with col_right:
    st.subheader("By Market")
    by_market = settled_f.groupby('Market').agg(
        Bets=('Profit', 'count'), Staked=('Stake', 'sum'),
        Profit=('Profit', 'sum'), Wins=('Return', lambda x: (x > 0).sum()),
    ).reset_index()
    by_market['ROI %'] = (by_market['Profit'] / by_market['Staked'] * 100).round(1)
    by_market['SR %'] = (by_market['Wins'] / by_market['Bets'] * 100).round(1)

    fig_mk = go.Figure()
    colors = ['#4CAF50' if p >= 0 else '#f44336' for p in by_market['Profit']]
    fig_mk.add_trace(go.Bar(x=by_market['Market'], y=by_market['Profit'], marker_color=colors))
    fig_mk.update_layout(template='plotly_dark', height=350,
        margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit (Units)")
    st.plotly_chart(fig_mk, use_container_width=True)
    st.dataframe(by_market[['Market', 'Bets', 'Profit', 'ROI %', 'SR %']],
                 use_container_width=True, hide_index=True)

st.markdown("---")

# ─── League Performance ───
st.subheader("League Performance")
by_league = settled_f.groupby('Competition').agg(
    Bets=('Profit', 'count'), Staked=('Stake', 'sum'),
    Profit=('Profit', 'sum'), Wins=('Return', lambda x: (x > 0).sum()),
    Avg_BF=('BF', 'mean'), Avg_Vol=('Volume', 'mean'),
).reset_index()
by_league['ROI %'] = (by_league['Profit'] / by_league['Staked'] * 100).round(1)
by_league['SR %'] = (by_league['Wins'] / by_league['Bets'] * 100).round(1)
by_league = by_league.sort_values('Profit', ascending=False)

st.dataframe(
    by_league[['Competition', 'Bets', 'Profit', 'ROI %', 'SR %', 'Avg_BF', 'Avg_Vol']].round(2),
    use_container_width=True, hide_index=True,
    column_config={'Competition': 'League', 'Avg_Vol': st.column_config.NumberColumn(format="%.0f")}
)

st.markdown("---")

# ─── BF Odds + Volume ───
col_bf, col_vol = st.columns(2)

with col_bf:
    st.subheader("Profit by BF Odds")
    bins = [0, 1.3, 1.5, 1.7, 1.9, 2.1, 2.5, 3.0, 10]
    labels = ['<1.30', '1.30-1.50', '1.50-1.70', '1.70-1.90', '1.90-2.10', '2.10-2.50', '2.50-3.00', '3.00+']
    tmp = settled_f.copy()
    tmp['BF_Bin'] = pd.cut(tmp['BF'], bins=bins, labels=labels, include_lowest=True)
    bf_stats = tmp.groupby('BF_Bin', observed=True).agg(Bets=('Profit', 'count'), Profit=('Profit', 'sum'), Staked=('Stake', 'sum')).reset_index()
    bf_stats['ROI %'] = (bf_stats['Profit'] / bf_stats['Staked'] * 100).round(1)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=bf_stats['BF_Bin'], y=bf_stats['Profit'],
        marker_color=['#4CAF50' if p >= 0 else '#f44336' for p in bf_stats['Profit']]))
    fig.update_layout(template='plotly_dark', height=300, margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit")
    st.plotly_chart(fig, use_container_width=True)

with col_vol:
    st.subheader("Profit by Volume")
    vol_bins = [0, 100, 200, 400, 700, 1100, 100000]
    vol_labels = ['0-100', '100-200', '200-400', '400-700', '700-1100', '1100+']
    tmp2 = settled_f.copy()
    tmp2['Vol_Bin'] = pd.cut(tmp2['Volume'], bins=vol_bins, labels=vol_labels, include_lowest=True)
    vol_stats = tmp2.groupby('Vol_Bin', observed=True).agg(Bets=('Profit', 'count'), Profit=('Profit', 'sum'), Staked=('Stake', 'sum')).reset_index()
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=vol_stats['Vol_Bin'], y=vol_stats['Profit'],
        marker_color=['#4CAF50' if p >= 0 else '#f44336' for p in vol_stats['Profit']]))
    fig2.update_layout(template='plotly_dark', height=300, margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit")
    st.plotly_chart(fig2, use_container_width=True)

st.markdown("---")

# ─── Recent Bets ───
st.subheader("Recent Bets")
recent = settled_f.sort_values('Date', ascending=False).head(50)
st.dataframe(
    recent[['Date', 'Market', 'Home', 'Away', 'Competition', 'BF', 'SM_Odds', 'Stake', 'Result', 'Profit']],
    use_container_width=True, hide_index=True,
    column_config={
        'Date': st.column_config.DateColumn(format="YYYY-MM-DD"),
        'BF': st.column_config.NumberColumn(format="%.2f"),
        'SM_Odds': st.column_config.NumberColumn("SM Odds", format="%.3f"),
        'Profit': st.column_config.NumberColumn(format="%.2f"),
    }
)

st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
           f"Total tracked: {len(df):,} matches | Data: {min_date} to {max_date}")