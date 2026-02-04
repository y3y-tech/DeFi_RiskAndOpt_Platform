import streamlit as st
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

# Page config
st.set_page_config(
    page_title="Polymarket 2025 Analytics",
    page_icon="📊",
    layout="wide"
)

# Title
st.title("📊 Polymarket 2025 User Analytics")
st.markdown("""
*Tracking redemptions (users cashing out winnings) and split activity (liquidity providers)*
""")
st.markdown("---")

# Load data function with caching
@st.cache_data
def load_monthly_data():
    """Load all monthly data and calculate metrics including new users"""
    months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
    monthly_metrics = []
    
    # Track all users seen so far (cumulative)
    all_splitters_seen = set()
    all_redeemers_seen = set()
    all_users_seen = set()
    
    for month in months:
        month_name = f"2025-{month}"
        
        try:
            # Load splits (liquidity provision activity)
            with open(f'polymarket_data_2025/splits_{month_name}.json', 'r') as f:
                splits = json.load(f)
            
            # Load redemptions (cashing out winnings)
            with open(f'polymarket_data_2025/redemptions_{month_name}.json', 'r') as f:
                redemptions = json.load(f)
            
            # Calculate current month metrics
            splitters = set(s['stakeholder'].lower() for s in splits if 'stakeholder' in s)
            redeemers = set(r['redeemer'].lower() for r in redemptions if 'redeemer' in r)
            
            total_active = splitters.union(redeemers)
            both = splitters.intersection(redeemers)
            
            # Calculate NEW users (never seen before)
            new_splitters = splitters - all_splitters_seen
            new_redeemers = redeemers - all_redeemers_seen
            new_users = total_active - all_users_seen
            
            # Calculate returning users
            returning_splitters = splitters - new_splitters
            returning_users = total_active - new_users
            
            # Update cumulative sets
            all_splitters_seen.update(splitters)
            all_redeemers_seen.update(redeemers)
            all_users_seen.update(total_active)
            
            monthly_metrics.append({
                'month': month_name,
                'month_name': datetime.strptime(month_name, '%Y-%m').strftime('%B'),
                'unique_splitters': len(splitters),
                'unique_redeemers': len(redeemers),
                'monthly_active_users': len(total_active),
                'split_and_redeemed': len(both),
                'only_split': len(splitters - redeemers),
                'only_redeemed': len(redeemers - splitters),
                'total_splits': len(splits),
                'total_redemptions': len(redemptions),
                'redeemer_splitter_ratio': len(redeemers) / len(splitters) if len(splitters) > 0 else 0,
                # New user metrics
                'new_users': len(new_users),
                'new_splitters': len(new_splitters),
                'new_redeemers': len(new_redeemers),
                'returning_users': len(returning_users),
                'retention_rate': (len(returning_users) / len(total_active) * 100) if len(total_active) > 0 else 0,
                'cumulative_users': len(all_users_seen),
                'cumulative_splitters': len(all_splitters_seen),
                'cumulative_redeemers': len(all_redeemers_seen)
            })
            
        except FileNotFoundError:
            st.warning(f"Data not found for {month_name}")
            continue
    
    return pd.DataFrame(monthly_metrics)

# Load data
with st.spinner('Loading data...'):
    df = load_monthly_data()

# Sidebar filters
st.sidebar.header("Filters")
selected_months = st.sidebar.multiselect(
    "Select Months",
    options=df['month_name'].tolist(),
    default=df['month_name'].tolist()
)

# Add info box in sidebar
st.sidebar.markdown("---")
st.sidebar.info("""
**Terminology:**
- **Redeemers**: Users cashing out winning positions
- **Splitters**: Liquidity providers (converting USDC → YES+NO shares)
- **Active Users**: Anyone who redeemed or split
""")

# Filter data
if selected_months:
    df_filtered = df[df['month_name'].isin(selected_months)]
else:
    df_filtered = df

# Key Metrics (Top Row)
st.header("📈 Key Metrics Summary")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_active = df_filtered['monthly_active_users'].sum()
    st.metric("Total Active Wallets", f"{total_active:,}")

with col2:
    total_new = df_filtered['new_users'].sum()
    st.metric("Total New Wallets", f"{total_new:,}", delta="New")

with col3:
    cumulative_users = df_filtered['cumulative_users'].iloc[-1] if len(df_filtered) > 0 else 0
    st.metric("Cumulative Unique Wallets", f"{cumulative_users:,}")

with col4:
    avg_retention = df_filtered['retention_rate'].mean()
    st.metric("Avg Retention Rate", f"{avg_retention:.1f}%")

with col5:
    avg_ratio = df_filtered['redeemer_splitter_ratio'].mean()
    st.metric("Avg Redeemer/Splitter Ratio", f"{avg_ratio:.2f}x")

st.markdown("---")

# Main Charts
st.header("📊 User Growth & Engagement")

# Chart 1: New vs Returning Users
fig1 = go.Figure()
fig1.add_trace(go.Bar(
    x=df_filtered['month_name'],
    y=df_filtered['new_users'],
    name='New Wallets',
    marker_color='lightgreen'
))
fig1.add_trace(go.Bar(
    x=df_filtered['month_name'],
    y=df_filtered['returning_users'],
    name='Returning Wallets',
    marker_color='steelblue'
))
fig1.update_layout(
    title='New vs Returning Wallets by Month',
    xaxis_title='Month',
    yaxis_title='Unique Wallets',
    barmode='stack',
    hovermode='x unified',
    height=400
)
st.plotly_chart(fig1, use_container_width=True)

# Chart 2: Cumulative User Growth
fig2 = go.Figure()
fig2.add_trace(go.Scatter(
    x=df_filtered['month_name'],
    y=df_filtered['cumulative_users'],
    mode='lines+markers',
    name='Total Cumulative Wallets',
    line=dict(color='#1f77b4', width=3),
    marker=dict(size=10),
    fill='tozeroy'
))
fig2.update_layout(
    title='Cumulative Wallet Growth Over Time',
    xaxis_title='Month',
    yaxis_title='Cumulative Unique Wallets',
    hovermode='x unified',
    height=400
)
st.plotly_chart(fig2, use_container_width=True)

# Two column layout
col1, col2 = st.columns(2)

with col1:
    # Chart 3: New User Breakdown
    fig3 = go.Figure()
    
    # Calculate new users who only split, only redeemed, or did both
    new_only_split = df_filtered['new_splitters'] - df_filtered['new_redeemers']
    new_only_redeemed = df_filtered['new_redeemers'] - df_filtered['new_splitters']
    new_both = df_filtered['new_splitters'] + df_filtered['new_redeemers'] - df_filtered['new_users']
    
    fig3.add_trace(go.Bar(
        x=df_filtered['month_name'],
        y=new_only_redeemed,
        name='New Redeemers Only',
        marker_color='lightcoral'
    ))
    fig3.add_trace(go.Bar(
        x=df_filtered['month_name'],
        y=new_only_split,
        name='New Splitters Only',
        marker_color='lightblue'
    ))
    fig3.add_trace(go.Bar(
        x=df_filtered['month_name'],
        y=new_both,
        name='New (Both Activities)',
        marker_color='lightgreen'
    ))
    
    fig3.update_layout(
        title='New Wallet Acquisition Breakdown',
        xaxis_title='Month',
        yaxis_title='New Wallets',
        barmode='stack',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig3, use_container_width=True)

with col2:
    # Chart 4: Retention Rate
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(
        x=df_filtered['month_name'],
        y=df_filtered['retention_rate'],
        mode='lines+markers',
        name='Retention Rate',
        line=dict(color='purple', width=3),
        marker=dict(size=10)
    ))
    fig4.update_layout(
        title='Monthly Retention Rate',
        xaxis_title='Month',
        yaxis_title='Retention Rate (%)',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# Activity charts section
st.header("📊 Market Activity")

col1, col2 = st.columns(2)

with col1:
    # Splitters vs Redeemers
    fig5 = go.Figure()
    fig5.add_trace(go.Bar(
        x=df_filtered['month_name'],
        y=df_filtered['unique_splitters'],
        name='Splitters (Liquidity Providers)',
        marker_color='lightblue'
    ))
    fig5.add_trace(go.Bar(
        x=df_filtered['month_name'],
        y=df_filtered['unique_redeemers'],
        name='Redeemers (Cashing Out)',
        marker_color='lightcoral'
    ))
    fig5.update_layout(
        title='Splitters vs Redeemers by Month',
        xaxis_title='Month',
        yaxis_title='Unique Wallets',
        barmode='group',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig5, use_container_width=True)

with col2:
    # Transaction Volume
    fig6 = go.Figure()
    fig6.add_trace(go.Scatter(
        x=df_filtered['month_name'],
        y=df_filtered['total_splits'],
        mode='lines+markers',
        name='Split Transactions',
        line=dict(color='blue')
    ))
    fig6.add_trace(go.Scatter(
        x=df_filtered['month_name'],
        y=df_filtered['total_redemptions'],
        mode='lines+markers',
        name='Redemption Transactions',
        line=dict(color='red')
    ))
    fig6.update_layout(
        title='Transaction Volume',
        xaxis_title='Month',
        yaxis_title='Number of Transactions',
        hovermode='x unified',
        height=400
    )
    st.plotly_chart(fig6, use_container_width=True)

# User Behavior Breakdown
st.subheader("User Behavior Breakdown")
fig7 = go.Figure()
fig7.add_trace(go.Bar(
    x=df_filtered['month_name'],
    y=df_filtered['split_and_redeemed'],
    name='Split AND Redeemed',
    marker_color='green'
))
fig7.add_trace(go.Bar(
    x=df_filtered['month_name'],
    y=df_filtered['only_split'],
    name='Only Split',
    marker_color='blue'
))
fig7.add_trace(go.Bar(
    x=df_filtered['month_name'],
    y=df_filtered['only_redeemed'],
    name='Only Redeemed',
    marker_color='red'
))
fig7.update_layout(
    title='Wallet Activity Patterns',
    xaxis_title='Month',
    yaxis_title='Unique Wallets',
    barmode='stack',
    height=400
)
st.plotly_chart(fig7, use_container_width=True)

# Insights Section
st.markdown("---")
st.header("💡 Key Insights")

# Calculate insights
max_new_users_month = df_filtered.loc[df_filtered['new_users'].idxmax()]
max_retention_month = df_filtered.loc[df_filtered['retention_rate'].idxmax()]
max_redeemers_month = df_filtered.loc[df_filtered['unique_redeemers'].idxmax()]
total_platform_users = df_filtered['cumulative_users'].iloc[-1] if len(df_filtered) > 0 else 0

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.success(f"""
    **Best Acquisition Month**  
    {max_new_users_month['month_name']}  
    {max_new_users_month['new_users']:,} new wallets
    """)

with col2:
    st.info(f"""
    **Best Retention Month**  
    {max_retention_month['month_name']}  
    {max_retention_month['retention_rate']:.1f}% returning
    """)

with col3:
    st.warning(f"""
    **Peak Redemption Activity**  
    {max_redeemers_month['month_name']}  
    {max_redeemers_month['unique_redeemers']:,} redeemers
    """)

with col4:
    st.metric(
        "Platform Growth",
        f"{total_platform_users:,}",
        delta=f"+{df_filtered['new_users'].iloc[-1]:,} last month" if len(df_filtered) > 0 else None
    )

# Data Table
st.markdown("---")
st.header("📋 Detailed Monthly Data")

# Format the dataframe for display
display_df = df_filtered[[
    'month_name',
    'monthly_active_users',
    'new_users',
    'returning_users',
    'retention_rate',
    'unique_redeemers',
    'new_redeemers',
    'unique_splitters',
    'cumulative_users',
    'total_splits',
    'total_redemptions'
]].copy()

display_df.columns = [
    'Month',
    'Monthly Active',
    'New Wallets',
    'Returning Wallets',
    'Retention %',
    'Redeemers',
    'New Redeemers',
    'Splitters (LPs)',
    'Cumulative Total',
    'Split Txns',
    'Redemption Txns'
]

# Format retention as percentage
display_df['Retention %'] = display_df['Retention %'].apply(lambda x: f"{x:.1f}%")

st.dataframe(
    display_df,
    use_container_width=True,
    hide_index=True
)

# Download button
st.download_button(
    label="📥 Download Data as CSV",
    data=display_df.to_csv(index=False).encode('utf-8'),
    file_name='polymarket_2025_metrics.csv',
    mime='text/csv'
)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>Data Source: Polymarket Activity Subgraph (Splits & Redemptions) | Updated: 2025</p>
    <p><em>Note: Splits represent liquidity provision activity, not direct trading. Redemptions represent users cashing out winning positions.</em></p>
</div>
""", unsafe_allow_html=True)