"""
Euro Football Bets — Performance Dashboard
Streamlit app for tracking betting performance.
Deployed on Streamlit Cloud, reads from dashboard_data/bets.csv
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
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
    df['DayOfWeek'] = df['Date'].dt.day_name()

    for col in ['Bet365', 'BF', 'Volume', 'RPD', 'Stake', 'Return', 'Profit', 'SM_Odds']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df['Result'] = pd.to_numeric(df['Result'], errors='coerce')

    return df

df = load_data()
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

leagues = st.sidebar.multiselect("Leagues", options=sorted(df['Competition'].dropna().unique()),
                                   default=sorted(df['Competition'].dropna().unique()))
staked_f = staked_f[staked_f['Competition'].isin(leagues)]
settled_f = settled_f[settled_f['Competition'].isin(leagues)]

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

# ═══════════════════════════════════════════════
# TAB LAYOUT
# ═══════════════════════════════════════════════
tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "📈 Overview", "🎯 SM vs BF Analysis", "📅 Time Analysis",
    "🏆 League Heatmap", "📋 Recent Bets"
])

# ═══════════════════════════════════════════════
# TAB 1: OVERVIEW
# ═══════════════════════════════════════════════
with tab1:
    # Cumulative Profit
    st.subheader("Cumulative Profit")
    cum = settled_f.sort_values('Date').copy()
    cum['Cum_Profit'] = cum['Profit'].cumsum()
    cum['Bet_Number'] = range(1, len(cum) + 1)

    fig_cum = go.Figure()
    fig_cum.add_trace(go.Scatter(
        x=cum['Bet_Number'], y=cum['Cum_Profit'],
        mode='lines', line=dict(color='#4CAF50', width=2), name='Cumulative Profit',
        hovertemplate='Bet #%{x}<br>Profit: %{y:.2f}<extra></extra>'
    ))

    # Rolling 30-bet average
    if len(cum) > 30:
        cum['Rolling30'] = cum['Profit'].rolling(30).mean() * 30
        cum['Rolling30_Cum'] = cum['Rolling30'].cumsum()
        fig_cum.add_trace(go.Scatter(
            x=cum['Bet_Number'], y=cum['Profit'].rolling(30).sum(),
            mode='lines', line=dict(color='#FF9800', width=1, dash='dot'),
            name='Rolling 30-bet P&L', opacity=0.7,
            hovertemplate='30-bet P&L: %{y:.2f}<extra></extra>'
        ))

    fig_cum.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig_cum.update_layout(template='plotly_dark', height=400,
        margin=dict(l=40, r=20, t=20, b=40),
        xaxis_title="Bet Number", yaxis_title="Profit (Units)",
        legend=dict(x=0.02, y=0.98))
    st.plotly_chart(fig_cum, use_container_width=True, key="cum_profit")

    # Monthly + By Market
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
        st.plotly_chart(fig_m, use_container_width=True, key="monthly_chart")
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
        st.plotly_chart(fig_mk, use_container_width=True, key="market_chart")
        st.dataframe(by_market[['Market', 'Bets', 'Profit', 'ROI %', 'SR %']],
                     use_container_width=True, hide_index=True)

    # League Performance
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

    # BF Odds + Volume
    col_bf, col_vol = st.columns(2)
    with col_bf:
        st.subheader("Profit by BF Odds")
        bins = [0, 1.3, 1.5, 1.7, 1.9, 2.1, 2.5, 3.0, 10]
        labels = ['<1.30', '1.30-1.50', '1.50-1.70', '1.70-1.90', '1.90-2.10', '2.10-2.50', '2.50-3.00', '3.00+']
        tmp = settled_f.copy()
        tmp['BF_Bin'] = pd.cut(tmp['BF'], bins=bins, labels=labels, include_lowest=True)
        bf_stats = tmp.groupby('BF_Bin', observed=True).agg(Bets=('Profit', 'count'), Profit=('Profit', 'sum'), Staked=('Stake', 'sum')).reset_index()
        fig = go.Figure()
        fig.add_trace(go.Bar(x=bf_stats['BF_Bin'], y=bf_stats['Profit'],
            marker_color=['#4CAF50' if p >= 0 else '#f44336' for p in bf_stats['Profit']]))
        fig.update_layout(template='plotly_dark', height=300, margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit")
        st.plotly_chart(fig, use_container_width=True, key="bf_chart")

    with col_vol:
        st.subheader("Profit by Volume")
        vol_bins = [0, 100, 200, 400, 700, 1100, 100000]
        vol_labels = ['0-100', '100-200', '200-400', '400-700', '700-1100', '1100+']
        tmp2 = settled_f.copy()
        tmp2['Vol_Bin'] = pd.cut(tmp2['Volume'], bins=vol_bins, labels=vol_labels, include_lowest=True)
        vol_stats = tmp2.groupby('Vol_Bin', observed=True).agg(Bets=('Profit', 'count'), Profit=('Profit', 'sum')).reset_index()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=vol_stats['Vol_Bin'], y=vol_stats['Profit'],
            marker_color=['#4CAF50' if p >= 0 else '#f44336' for p in vol_stats['Profit']]))
        fig2.update_layout(template='plotly_dark', height=300, margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit")
        st.plotly_chart(fig2, use_container_width=True, key="vol_chart")

# ═══════════════════════════════════════════════
# TAB 2: SM vs BF ANALYSIS
# ═══════════════════════════════════════════════
with tab2:
    sm_data = settled_f[settled_f['SM_Odds'].notna() & settled_f['BF'].notna()].copy()

    if sm_data.empty:
        st.info("No SM Odds data available yet.")
    else:
        st.subheader("SportsMarket vs Betfair Odds")
        sm_data['Odds_Diff'] = sm_data['SM_Odds'] - sm_data['BF']
        sm_data['Odds_Diff_Pct'] = ((sm_data['SM_Odds'] - sm_data['BF']) / sm_data['BF'] * 100)

        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Avg BF Odds", f"{sm_data['BF'].mean():.3f}")
        c2.metric("Avg SM Odds", f"{sm_data['SM_Odds'].mean():.3f}")
        avg_diff = sm_data['Odds_Diff'].mean()
        c3.metric("Avg Difference", f"{avg_diff:+.3f}",
                  delta=f"{'Better' if avg_diff > 0 else 'Worse'} on SM")
        pct_better = (sm_data['SM_Odds'] > sm_data['BF']).mean() * 100
        c4.metric("SM Better %", f"{pct_better:.1f}%")

        # Scatter plot: SM vs BF
        fig_scatter = go.Figure()
        fig_scatter.add_trace(go.Scatter(
            x=sm_data['BF'], y=sm_data['SM_Odds'],
            mode='markers', marker=dict(size=5, color=sm_data['Profit'],
                colorscale='RdYlGn', cmin=-2, cmax=2, showscale=True,
                colorbar=dict(title="Profit")),
            hovertemplate='BF: %{x:.3f}<br>SM: %{y:.3f}<br>Diff: %{customdata[0]:+.3f}<extra></extra>',
            customdata=sm_data[['Odds_Diff']].values
        ))
        # Perfect line
        min_o = min(sm_data['BF'].min(), sm_data['SM_Odds'].min())
        max_o = max(sm_data['BF'].max(), sm_data['SM_Odds'].max())
        fig_scatter.add_trace(go.Scatter(x=[min_o, max_o], y=[min_o, max_o],
            mode='lines', line=dict(color='gray', dash='dash'), showlegend=False))
        fig_scatter.update_layout(template='plotly_dark', height=450,
            xaxis_title="BF Odds", yaxis_title="SM Matched Odds",
            margin=dict(l=40, r=20, t=20, b=40))
        st.plotly_chart(fig_scatter, use_container_width=True, key="sm_scatter")

        # Distribution of odds difference
        col_a, col_b = st.columns(2)
        with col_a:
            st.subheader("Odds Difference Distribution")
            fig_hist = go.Figure()
            fig_hist.add_trace(go.Histogram(x=sm_data['Odds_Diff'], nbinsx=40,
                marker_color='#4CAF50', opacity=0.8))
            fig_hist.add_vline(x=0, line_dash="dash", line_color="white", opacity=0.5)
            fig_hist.update_layout(template='plotly_dark', height=300,
                xaxis_title="SM - BF (positive = better on SM)",
                margin=dict(l=40, r=20, t=10, b=40))
            st.plotly_chart(fig_hist, use_container_width=True, key="sm_hist")

        with col_b:
            st.subheader("Odds Improvement by Market")
            sm_by_market = sm_data.groupby('Market').agg(
                Bets=('Odds_Diff', 'count'),
                Avg_Diff=('Odds_Diff', 'mean'),
                Avg_Diff_Pct=('Odds_Diff_Pct', 'mean'),
            ).reset_index().round(3)
            fig_bar = go.Figure()
            colors = ['#4CAF50' if d >= 0 else '#f44336' for d in sm_by_market['Avg_Diff']]
            fig_bar.add_trace(go.Bar(x=sm_by_market['Market'], y=sm_by_market['Avg_Diff'],
                marker_color=colors))
            fig_bar.update_layout(template='plotly_dark', height=300,
                yaxis_title="Avg SM - BF Difference", margin=dict(l=40, r=20, t=10, b=40))
            st.plotly_chart(fig_bar, use_container_width=True, key="sm_market_diff")

# ═══════════════════════════════════════════════
# TAB 3: TIME ANALYSIS
# ═══════════════════════════════════════════════
with tab3:
    st.subheader("Profit by Day of Week")
    dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow = settled_f.groupby('DayOfWeek').agg(
        Bets=('Profit', 'count'), Profit=('Profit', 'sum'), Staked=('Stake', 'sum'),
    ).reindex(dow_order).reset_index()
    dow['ROI %'] = (dow['Profit'] / dow['Staked'] * 100).round(1)

    fig_dow = go.Figure()
    colors = ['#4CAF50' if p >= 0 else '#f44336' for p in dow['Profit']]
    fig_dow.add_trace(go.Bar(x=dow['DayOfWeek'], y=dow['Profit'], marker_color=colors,
        hovertemplate='%{x}<br>Profit: %{y:.2f}<br>Bets: %{customdata[0]}<br>ROI: %{customdata[1]}%<extra></extra>',
        customdata=dow[['Bets', 'ROI %']].values))
    fig_dow.update_layout(template='plotly_dark', height=350,
        margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit (Units)")
    st.plotly_chart(fig_dow, use_container_width=True, key="dow_chart")

    st.dataframe(dow[['DayOfWeek', 'Bets', 'Profit', 'ROI %']].rename(columns={'DayOfWeek': 'Day'}),
                 use_container_width=True, hide_index=True)

    st.markdown("---")

    # Rolling 30-day profit trend
    st.subheader("Rolling 30-Day Profit")
    daily = settled_f.groupby(settled_f['Date'].dt.date).agg(
        Profit=('Profit', 'sum'), Bets=('Profit', 'count')
    ).reset_index()
    daily.columns = ['Date', 'Profit', 'Bets']
    daily['Date'] = pd.to_datetime(daily['Date'])
    daily = daily.sort_values('Date')
    daily['Rolling30'] = daily['Profit'].rolling(30, min_periods=5).sum()

    fig_roll = go.Figure()
    fig_roll.add_trace(go.Scatter(x=daily['Date'], y=daily['Rolling30'],
        mode='lines', line=dict(color='#FF9800', width=2), name='30-day rolling profit',
        hovertemplate='%{x|%Y-%m-%d}<br>30-day profit: %{y:.2f}<extra></extra>'))
    fig_roll.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig_roll.update_layout(template='plotly_dark', height=350,
        margin=dict(l=40, r=20, t=10, b=40),
        xaxis_title="Date", yaxis_title="30-Day Profit (Units)")
    st.plotly_chart(fig_roll, use_container_width=True, key="rolling30")

    st.markdown("---")

    # Streak analysis
    st.subheader("Win/Loss Streaks")
    streaks = settled_f.sort_values('Date').copy()
    streaks['Win'] = streaks['Return'] > 0
    streaks['Streak_Change'] = streaks['Win'] != streaks['Win'].shift()
    streaks['Streak_ID'] = streaks['Streak_Change'].cumsum()
    streak_lengths = streaks.groupby(['Streak_ID', 'Win']).size().reset_index(name='Length')

    c1, c2, c3 = st.columns(3)
    win_streaks = streak_lengths[streak_lengths['Win'] == True]
    loss_streaks = streak_lengths[streak_lengths['Win'] == False]

    current = streaks.iloc[-1] if not streaks.empty else None
    current_streak_id = streaks['Streak_ID'].iloc[-1] if not streaks.empty else 0
    current_len = streak_lengths[streak_lengths['Streak_ID'] == current_streak_id]['Length'].values
    current_len = current_len[0] if len(current_len) > 0 else 0
    current_type = "Win" if current is not None and current['Win'] else "Loss"

    c1.metric("Best Win Streak", f"{win_streaks['Length'].max() if not win_streaks.empty else 0}")
    c2.metric("Worst Loss Streak", f"{loss_streaks['Length'].max() if not loss_streaks.empty else 0}")
    c3.metric("Current Streak", f"{current_len} {current_type}s")

# ═══════════════════════════════════════════════
# TAB 4: LEAGUE HEATMAP
# ═══════════════════════════════════════════════
with tab4:
    st.subheader("Profit Heatmap: League × Month")

    heatmap_data = settled_f.groupby(['Competition', 'Month']).agg(
        Profit=('Profit', 'sum')
    ).reset_index()
    pivot = heatmap_data.pivot_table(index='Competition', columns='Month', values='Profit', fill_value=0)

    # Sort by total profit
    pivot['_total'] = pivot.sum(axis=1)
    pivot = pivot.sort_values('_total', ascending=True)
    pivot = pivot.drop('_total', axis=1)

    if not pivot.empty:
        fig_heat = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=pivot.columns.tolist(),
            y=pivot.index.tolist(),
            colorscale=[[0, '#f44336'], [0.5, '#1A1A1A'], [1, '#4CAF50']],
            zmid=0,
            hovertemplate='%{y}<br>%{x}<br>Profit: %{z:.2f}<extra></extra>',
            colorbar=dict(title="Profit"),
        ))
        fig_heat.update_layout(
            template='plotly_dark',
            height=max(400, len(pivot) * 25),
            margin=dict(l=200, r=20, t=20, b=40),
            yaxis=dict(dtick=1),
        )
        st.plotly_chart(fig_heat, use_container_width=True, key="league_heatmap")
    else:
        st.info("Not enough data for heatmap.")

    st.markdown("---")

    # League × Market breakdown
    st.subheader("Profit by League × Market")
    lm = settled_f.groupby(['Competition', 'Market']).agg(
        Profit=('Profit', 'sum'), Bets=('Profit', 'count')
    ).reset_index()
    lm_pivot = lm.pivot_table(index='Competition', columns='Market', values='Profit', fill_value=0)
    lm_pivot['Total'] = lm_pivot.sum(axis=1)
    lm_pivot = lm_pivot.sort_values('Total', ascending=False)

    st.dataframe(lm_pivot.round(2), use_container_width=True)

# ═══════════════════════════════════════════════
# TAB 5: RECENT BETS
# ═══════════════════════════════════════════════
with tab5:
    st.subheader("Recent Settled Bets")
    n_show = st.slider("Number of bets to show", 10, 200, 50, key="recent_slider")
    recent = settled_f.sort_values('Date', ascending=False).head(n_show)
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

    # Today's open bets
    st.subheader("Open / Unsettled Bets")
    open_bets = staked_f[staked_f['Profit'].isna()].sort_values('Date', ascending=False)
    if open_bets.empty:
        st.info("No open bets.")
    else:
        st.dataframe(
            open_bets[['Date', 'Market', 'Home', 'Away', 'Competition', 'BF', 'SM_Odds', 'Stake']],
            use_container_width=True, hide_index=True,
            column_config={
                'Date': st.column_config.DateColumn(format="YYYY-MM-DD"),
                'BF': st.column_config.NumberColumn(format="%.2f"),
                'SM_Odds': st.column_config.NumberColumn("SM Odds", format="%.3f"),
            }
        )

# ─── Footer ───
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
           f"Total tracked: {len(df):,} matches | Data: {min_date} to {max_date}")