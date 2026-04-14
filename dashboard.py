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
tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
    "📈 Overview", "💰 SM Account", "🎯 SM vs BF Analysis", "📅 Time Analysis",
    "🏆 League Heatmap", "📋 Recent Bets", "📊 Advanced"
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
# TAB 2: SM ACCOUNT
# ═══════════════════════════════════════════════
with tab2:
    fx_path = Path(__file__).parent / "dashboard_data" / "fx_rate.json"
    EUR_AUD = 1.66
    if fx_path.exists():
        import json as _json
        fx = _json.loads(fx_path.read_text())
        EUR_AUD = fx.get("EUR_AUD", 1.66)

    balance_path = Path(__file__).parent / "dashboard_data" / "sm_balance.json"
    if balance_path.exists():
        import json
        bal_history = json.loads(balance_path.read_text())
        if bal_history:
            bal_df = pd.DataFrame(bal_history)
            bal_df['timestamp'] = pd.to_datetime(bal_df['timestamp'])
            bal_df['balance_aud'] = bal_df['current_balance'] * EUR_AUD
            bal_df['pl_aud'] = bal_df['today_pl'] * EUR_AUD

            latest = bal_history[-1]
            bal_eur = latest.get('current_balance', 0)
            pl_today = latest.get('today_pl', 0)

            # KPIs
            st.subheader("Account Overview")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Balance (EUR)", f"€{bal_eur:,.2f}")
            c2.metric("Balance (AUD)", f"A${bal_eur * EUR_AUD:,.2f}")
            c3.metric("Last Day P/L (AUD)", f"A${pl_today * EUR_AUD:,.2f}")
            c4.metric("EUR/AUD Rate", f"{EUR_AUD:.4f}")

            st.markdown("---")

            # Balance chart
            st.subheader("Account Balance History")
            fig_bal = go.Figure()
            fig_bal.add_trace(go.Scatter(
                x=bal_df['timestamp'], y=bal_df['balance_aud'],
                mode='lines+markers', line=dict(color='#4CAF50', width=2),
                marker=dict(size=4),
                hovertemplate='%{x|%Y-%m-%d}<br>Balance: A$%{y:,.2f}<extra></extra>'
            ))
            fig_bal.update_layout(template='plotly_dark', height=350,
                margin=dict(l=40, r=20, t=10, b=40),
                xaxis_title="Date", yaxis_title="Balance (AUD)")
            st.plotly_chart(fig_bal, use_container_width=True, key="sm_balance")

            st.markdown("---")

            # Daily P/L bars
            st.subheader("Daily P/L")
            fig_dpl = go.Figure()
            colors = ['#4CAF50' if p >= 0 else '#f44336' for p in bal_df['pl_aud']]
            fig_dpl.add_trace(go.Bar(
                x=bal_df['timestamp'], y=bal_df['pl_aud'],
                marker_color=colors,
                hovertemplate='%{x|%Y-%m-%d}<br>P/L: A$%{y:,.2f}<extra></extra>'
            ))
            fig_dpl.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            fig_dpl.update_layout(template='plotly_dark', height=300,
                margin=dict(l=40, r=20, t=10, b=40),
                xaxis_title="Date", yaxis_title="Daily P/L (AUD)")
            st.plotly_chart(fig_dpl, use_container_width=True, key="sm_daily_pl")

            st.markdown("---")

            # Account stats
            st.subheader("Account Statistics")
            total_pl_aud = bal_df['pl_aud'].sum()
            winning_days = (bal_df['today_pl'] > 0).sum()
            losing_days = (bal_df['today_pl'] < 0).sum()
            best_day = bal_df.loc[bal_df['pl_aud'].idxmax()]
            worst_day = bal_df.loc[bal_df['pl_aud'].idxmin()]
            avg_daily = bal_df['pl_aud'].mean()
            peak_bal = bal_df['balance_aud'].max()
            peak_date = bal_df.loc[bal_df['balance_aud'].idxmax(), 'timestamp']
            current_dd = bal_df['balance_aud'].iloc[-1] - peak_bal

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total P/L (AUD)", f"A${total_pl_aud:,.2f}")
            c2.metric("Winning Days", f"{winning_days} / {winning_days + losing_days}")
            c3.metric("Avg Daily P/L", f"A${avg_daily:,.2f}")
            c4.metric("Win Day Rate", f"{winning_days/(winning_days+losing_days)*100:.1f}%" if (winning_days+losing_days) > 0 else "N/A")

            c5, c6, c7, c8 = st.columns(4)
            c5.metric("Best Day", f"A${best_day['pl_aud']:,.2f}", delta=str(best_day['timestamp'].date()))
            c6.metric("Worst Day", f"A${worst_day['pl_aud']:,.2f}", delta=str(worst_day['timestamp'].date()))
            c7.metric("Peak Balance", f"A${peak_bal:,.2f}", delta=str(peak_date.date()))
            c8.metric("Drawdown from Peak", f"A${current_dd:,.2f}")

            st.markdown("---")

            # Weekly P/L
            st.subheader("Weekly P/L")
            bal_df['Week'] = bal_df['timestamp'].dt.to_period('W').astype(str)
            weekly = bal_df.groupby('Week').agg(
                PL=('pl_aud', 'sum'),
                Days=('pl_aud', 'count'),
                Staked=('daily_staked', 'sum') if 'daily_staked' in bal_df.columns else ('pl_aud', 'count'),
            ).reset_index()
            fig_wk = go.Figure()
            colors = ['#4CAF50' if p >= 0 else '#f44336' for p in weekly['PL']]
            fig_wk.add_trace(go.Bar(x=weekly['Week'], y=weekly['PL'], marker_color=colors,
                hovertemplate='%{x}<br>P/L: A$%{y:,.2f}<extra></extra>'))
            fig_wk.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
            fig_wk.update_layout(template='plotly_dark', height=300,
                margin=dict(l=40, r=20, t=10, b=80),
                xaxis_title="Week", yaxis_title="Weekly P/L (AUD)",
                xaxis_tickangle=-45)
            st.plotly_chart(fig_wk, use_container_width=True, key="sm_weekly")

    else:
        st.info("SM balance data not yet available. Will populate on next export run.")

# ═══════════════════════════════════════════════
# TAB 3: SM vs BF ANALYSIS
# ═══════════════════════════════════════════════
with tab3:
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
with tab4:
    st.subheader("Profit by Day of Week")
    dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow = settled_f.groupby('DayOfWeek').agg(
        Bets=('Profit', 'count'), Staked=('Stake', 'sum'),
        Profit=('Profit', 'sum'), Wins=('Return', lambda x: (x > 0).sum()),
    ).reindex(dow_order).reset_index()
    dow['ROI %'] = (dow['Profit'] / dow['Staked'] * 100).round(1)
    dow['SR %'] = (dow['Wins'] / dow['Bets'] * 100).round(1)
    dow['Profit'] = dow['Profit'].round(2)
    dow['Staked'] = dow['Staked'].round(0)

    col_dow1, col_dow2 = st.columns(2)

    with col_dow1:
        fig_dow = go.Figure()
        colors = ['#4CAF50' if p >= 0 else '#f44336' for p in dow['Profit']]
        fig_dow.add_trace(go.Bar(x=dow['DayOfWeek'], y=dow['Profit'], marker_color=colors,
            name='Profit (Units)',
            hovertemplate='%{x}<br>Profit: %{y:.2f}<br>Bets: %{customdata[0]}<br>ROI: %{customdata[1]}%<extra></extra>',
            customdata=dow[['Bets', 'ROI %']].values))
        fig_dow.update_layout(template='plotly_dark', height=350,
            margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit (Units)")
        st.plotly_chart(fig_dow, use_container_width=True, key="dow_chart")

    with col_dow2:
        fig_roi = go.Figure()
        roi_colors = ['#4CAF50' if r >= 0 else '#f44336' for r in dow['ROI %']]
        fig_roi.add_trace(go.Bar(x=dow['DayOfWeek'], y=dow['ROI %'], marker_color=roi_colors,
            name='ROI %',
            hovertemplate='%{x}<br>ROI: %{y:.1f}%<br>Staked: %{customdata[0]:.0f}<extra></extra>',
            customdata=dow[['Staked']].values))
        fig_roi.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
        fig_roi.update_layout(template='plotly_dark', height=350,
            margin=dict(l=40, r=20, t=10, b=40), yaxis_title="ROI %")
        st.plotly_chart(fig_roi, use_container_width=True, key="dow_roi_chart")

    st.dataframe(dow[['DayOfWeek', 'Bets', 'Staked', 'Profit', 'ROI %', 'SR %']].rename(columns={'DayOfWeek': 'Day'}),
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
with tab5:
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
with tab6:
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

# ═══════════════════════════════════════════════
# TAB 6: ADVANCED
# ═══════════════════════════════════════════════
with tab7:

    # ─── Drawdown Chart ───
    st.subheader("Drawdown Analysis")
    cum = settled_f.sort_values('Date').copy()
    cum['Cum_Profit'] = cum['Profit'].cumsum()
    cum['Peak'] = cum['Cum_Profit'].cummax()
    cum['Drawdown'] = cum['Cum_Profit'] - cum['Peak']
    cum['Bet_Number'] = range(1, len(cum) + 1)

    fig_dd = go.Figure()
    fig_dd.add_trace(go.Scatter(
        x=cum['Bet_Number'], y=cum['Drawdown'],
        mode='lines', fill='tozeroy',
        line=dict(color='#f44336', width=1),
        fillcolor='rgba(244,67,54,0.2)',
        hovertemplate='Bet #%{x}<br>Drawdown: %{y:.2f}<extra></extra>'
    ))
    fig_dd.update_layout(template='plotly_dark', height=300,
        margin=dict(l=40, r=20, t=10, b=40),
        xaxis_title="Bet Number", yaxis_title="Drawdown (Units)")
    st.plotly_chart(fig_dd, use_container_width=True, key="drawdown")

    dd_c1, dd_c2, dd_c3 = st.columns(3)
    max_dd = cum['Drawdown'].min() if not cum.empty else 0
    current_dd = cum['Drawdown'].iloc[-1] if not cum.empty else 0
    peak = cum['Peak'].max() if not cum.empty else 0
    dd_c1.metric("Max Drawdown", f"{max_dd:.2f}")
    dd_c2.metric("Current Drawdown", f"{current_dd:.2f}")
    dd_c3.metric("Peak Profit", f"{peak:.2f}")

    st.markdown("---")

    # ─── Profit by Region ───
    st.subheader("Profit by Region")
    region_map = {
        'English Premier League': 'UK & Ireland', 'English Championship League': 'UK & Ireland',
        'Scottish Premiership': 'UK & Ireland',
        'Spanish La Liga': 'Southern Europe', 'Italian Serie A': 'Southern Europe',
        'Portuguese Primeira Liga': 'Southern Europe', 'Greek Super League': 'Southern Europe',
        'German Bundesliga I': 'Central Europe', 'Austrian Bundesliga': 'Central Europe',
        'Swiss Super League': 'Central Europe', 'Czech First League': 'Central Europe',
        'Polish Ekstraklasa': 'Central Europe',
        'French Ligue 1': 'Western Europe', 'Belgian First Division A': 'Western Europe',
        'Netherlands Eredivisie': 'Western Europe',
        'Turkish Super Lig': 'Eastern Europe', 'Romanian Liga I': 'Eastern Europe',
        'Serbian SuperLiga': 'Eastern Europe', 'Croatian HNL': 'Eastern Europe',
        'Danish Superligaen': 'Scandinavia', 'Norwegian Eliteserien': 'Scandinavia',
        'Swedish Allsvenskan': 'Scandinavia',
        'UEFA Champions League': 'UEFA', 'UEFA Europa League': 'UEFA',
        'UEFA Conference League': 'UEFA', 'UEFA Champions League Q': 'UEFA',
        'UEFA Europa League Q': 'UEFA', 'UEFA Conference League Q': 'UEFA',
    }
    settled_reg = settled_f.copy()
    settled_reg['Region'] = settled_reg['Competition'].map(region_map).fillna('Other')
    by_region = settled_reg.groupby('Region').agg(
        Bets=('Profit', 'count'), Staked=('Stake', 'sum'),
        Profit=('Profit', 'sum'),
    ).reset_index()
    by_region['ROI %'] = (by_region['Profit'] / by_region['Staked'] * 100).round(1)
    by_region = by_region.sort_values('Profit', ascending=False)

    col_reg1, col_reg2 = st.columns(2)
    with col_reg1:
        fig_reg = go.Figure()
        colors = ['#4CAF50' if p >= 0 else '#f44336' for p in by_region['Profit']]
        fig_reg.add_trace(go.Bar(x=by_region['Region'], y=by_region['Profit'], marker_color=colors,
            hovertemplate='%{x}<br>Profit: %{y:.2f}<br>ROI: %{customdata[0]:.1f}%<extra></extra>',
            customdata=by_region[['ROI %']].values))
        fig_reg.update_layout(template='plotly_dark', height=300,
            margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit")
        st.plotly_chart(fig_reg, use_container_width=True, key="region_chart")

    with col_reg2:
        fig_pie = go.Figure()
        fig_pie.add_trace(go.Pie(
            labels=by_region['Region'], values=by_region['Bets'],
            marker=dict(colors=px.colors.qualitative.Set2),
            textinfo='label+percent', hole=0.4))
        fig_pie.update_layout(template='plotly_dark', height=300,
            margin=dict(l=20, r=20, t=10, b=10), showlegend=False)
        st.plotly_chart(fig_pie, use_container_width=True, key="region_pie")

    st.dataframe(by_region[['Region', 'Bets', 'Profit', 'ROI %']],
                 use_container_width=True, hide_index=True)

    st.markdown("---")

    # ─── Monthly Cumulative Overlay ───
    st.subheader("Monthly Profit Curves (Overlay)")
    monthly_cum = settled_f.copy()
    monthly_cum['MonthKey'] = monthly_cum['Date'].dt.to_period('M').astype(str)
    monthly_cum = monthly_cum.sort_values('Date')

    fig_overlay = go.Figure()
    colors_cycle = ['#4CAF50', '#2196F3', '#FF9800', '#f44336', '#9C27B0',
                    '#00BCD4', '#FFEB3B', '#E91E63', '#8BC34A', '#FF5722',
                    '#3F51B5', '#009688', '#FFC107', '#795548', '#607D8B']
    for i, (month, grp) in enumerate(monthly_cum.groupby('MonthKey')):
        grp = grp.copy()
        grp['Month_Cum'] = grp['Profit'].cumsum()
        grp['Bet_In_Month'] = range(1, len(grp) + 1)
        fig_overlay.add_trace(go.Scatter(
            x=grp['Bet_In_Month'], y=grp['Month_Cum'],
            mode='lines', name=month,
            line=dict(color=colors_cycle[i % len(colors_cycle)], width=1.5),
            hovertemplate=f'{month}<br>Bet #%{{x}}<br>Cum: %{{y:.2f}}<extra></extra>'
        ))
    fig_overlay.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)
    fig_overlay.update_layout(template='plotly_dark', height=400,
        margin=dict(l=40, r=20, t=10, b=40),
        xaxis_title="Bet # in Month", yaxis_title="Cumulative Profit",
        legend=dict(orientation='h', y=-0.2))
    st.plotly_chart(fig_overlay, use_container_width=True, key="monthly_overlay")

    st.markdown("---")

    # ─── Core vs Fade Breakdown ───
    st.subheader("Core vs Fade Performance")
    settled_cf = settled_f.copy()
    def classify_bet(row):
        if row['Market'] == 'BTTS' and row['Prediction'] == 0:
            rpd = row.get('RPD')
            if rpd is not None and not pd.isna(rpd) and rpd >= 5:
                return 'BTTS Fade (Yes)'
            return 'BTTS Core (No)'
        elif row['Market'] == '1.5G' and row['Prediction'] == 1:
            rpd = row.get('RPD')
            if rpd is not None and not pd.isna(rpd) and rpd >= 4.6:
                return '1.5G Fade (Under)'
            return '1.5G Core (Under)'
        elif row['Market'] == '1.5G' and row['Prediction'] == 0:
            return '1.5G Core (Under)'
        elif row['Market'] == '3.5G':
            return '3.5G Core (Under)'
        elif row['Market'] == '2.5G':
            return '2.5G (tracked only)'
        return 'Other'

    settled_cf['Bet_Class'] = settled_cf.apply(classify_bet, axis=1)
    by_class = settled_cf.groupby('Bet_Class').agg(
        Bets=('Profit', 'count'), Staked=('Stake', 'sum'),
        Profit=('Profit', 'sum'), Wins=('Return', lambda x: (x > 0).sum()),
    ).reset_index()
    by_class['ROI %'] = (by_class['Profit'] / by_class['Staked'] * 100).round(1)
    by_class['SR %'] = (by_class['Wins'] / by_class['Bets'] * 100).round(1)
    by_class = by_class.sort_values('Profit', ascending=False)

    fig_class = go.Figure()
    colors = ['#4CAF50' if p >= 0 else '#f44336' for p in by_class['Profit']]
    fig_class.add_trace(go.Bar(x=by_class['Bet_Class'], y=by_class['Profit'],
        marker_color=colors,
        hovertemplate='%{x}<br>Profit: %{y:.2f}<br>ROI: %{customdata[0]:.1f}%<extra></extra>',
        customdata=by_class[['ROI %']].values))
    fig_class.update_layout(template='plotly_dark', height=350,
        margin=dict(l=40, r=20, t=10, b=40), yaxis_title="Profit (Units)")
    st.plotly_chart(fig_class, use_container_width=True, key="core_fade_chart")

    st.dataframe(by_class[['Bet_Class', 'Bets', 'Staked', 'Profit', 'ROI %', 'SR %']].rename(
        columns={'Bet_Class': 'Strategy'}), use_container_width=True, hide_index=True)

    st.markdown("---")

    # ─── Commission Analysis ───
    st.subheader("Commission Analysis")
    sm_comm = settled_f[settled_f['SM_Odds'].notna() & settled_f['BF'].notna()].copy()
    if not sm_comm.empty:
        # Theoretical commission = BF odds vs SM matched odds
        sm_comm['Theo_Return_BF'] = sm_comm.apply(
            lambda r: r['Stake'] * (r['BF'] - 1) if r['Result'] == 1 else -r['Stake'], axis=1)
        sm_comm['Actual_Return_SM'] = sm_comm.apply(
            lambda r: r['Stake'] * (r['SM_Odds'] - 1) if r['Result'] == 1 else -r['Stake'], axis=1)
        sm_comm['Commission_Impact'] = sm_comm['Actual_Return_SM'] - sm_comm['Theo_Return_BF']

        total_comm = sm_comm['Commission_Impact'].sum()
        avg_comm_pct = ((sm_comm['SM_Odds'] / sm_comm['BF']).mean() - 1) * 100

        c1, c2, c3 = st.columns(3)
        c1.metric("Total Commission Impact", f"€{total_comm:.2f}")
        c2.metric("Avg Odds Reduction", f"{avg_comm_pct:.2f}%")
        c3.metric("Bets with SM Data", f"{len(sm_comm):,}")
    else:
        st.info("No SM odds data available for commission analysis.")

# ─── Footer ───
st.markdown("---")
st.caption(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')} | "
           f"Total tracked: {len(df):,} matches | Data: {min_date} to {max_date}")