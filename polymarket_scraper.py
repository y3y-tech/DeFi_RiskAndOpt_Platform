import requests
import json
from datetime import datetime, timedelta
from collections import defaultdict
import time
import random  # <- MISSING THIS
import os

# Polymarket subgraph endpoint
SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn"

# December 2025 date range
DEC_2025_START = datetime(2025, 12, 1)
DEC_2025_END = datetime(2026, 1, 1)

def query_graphql(query, max_retries=5):
    """Send GraphQL query with retry logic"""
    for attempt in range(max_retries):
        try:
            response = requests.post(SUBGRAPH_URL, json={'query': query})
            if response.status_code == 200:
                return response.json()
            elif response.status_code in [502, 503, 504]:
                # Server error - wait and retry
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"  Server error {response.status_code}, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                raise Exception(f"Query failed: {response.status_code} - {response.text[:200]}")
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            wait_time = (2 ** attempt) + random.uniform(0, 1)
            print(f"  Network error: {e}, retrying in {wait_time:.1f}s... (attempt {attempt + 1}/{max_retries})")
            time.sleep(wait_time)
    
    raise Exception("Max retries exceeded")

def fetch_data_for_period(start_timestamp, end_timestamp, data_type="splits"):
    """Fetch splits or redemptions for a specific time period using timestamp pagination"""
    all_data = []
    last_timestamp = end_timestamp
    batch_size = 1000
    query_count = 0
    
    # Determine query structure based on data type
    if data_type == "splits":
        stakeholder_field = "stakeholder"
        amount_field = "amount"
        query_name = "splits"
    else:  # redemptions
        stakeholder_field = "redeemer"
        amount_field = "payout"
        query_name = "redemptions"
    
    while True:
        query = f"""
        {{
          {query_name}(
            first: {batch_size}, 
            where: {{
              timestamp_gte: {start_timestamp}, 
              timestamp_lt: {last_timestamp}
            }}
            orderBy: timestamp, 
            orderDirection: desc
          ) {{
            {stakeholder_field}
            timestamp
            {amount_field}
          }}
        }}
        """
        
        query_count += 1
        result = query_graphql(query)
        
        if 'errors' in result:
            print(f"  GraphQL Error: {result['errors']}")
            break
            
        data = result['data'][query_name]
        
        if not data:
            print(f"  No more {data_type} to fetch")
            break
            
        all_data.extend(data)
        print(f"  Query {query_count}: Fetched {len(data)} {data_type}, total: {len(all_data)}, last timestamp: {data[-1]['timestamp']}")
        
        # Update last_timestamp for next iteration
        last_timestamp = int(data[-1]['timestamp'])
        
        # If we got less than batch_size, we've reached the end
        if len(data) < batch_size:
            print(f"  Reached end of {data_type} (got {len(data)} < {batch_size})")
            break
        
        # Be nice to the API
        time.sleep(0.1)
    
    return all_data

def get_periods():
    """Generate 10-day periods for December"""
    periods = []
    current_date = DEC_2025_START
    while current_date < DEC_2025_END:
        period_end = min(current_date + timedelta(days=10), DEC_2025_END)
        periods.append({
            'start': current_date,
            'end': period_end,
            'start_ts': int(current_date.timestamp()),
            'end_ts': int(period_end.timestamp()),
            'name': f"{current_date.strftime('%Y-%m-%d')}_to_{period_end.strftime('%Y-%m-%d')}"
        })
        current_date = period_end
    return periods

def fetch_splits_only():
    """Fetch only splits data"""
    os.makedirs('polymarket_data', exist_ok=True)
    periods = get_periods()
    
    print("="*80)
    print("FETCHING SPLITS ONLY")
    print("="*80)
    print(f"Fetching data in {len(periods)} periods:")
    for i, period in enumerate(periods, 1):
        print(f"  Period {i}: {period['name']}")
    print()
    
    all_splits = []
    
    for i, period in enumerate(periods, 1):
        print(f"\n{'='*80}")
        print(f"PERIOD {i}/{len(periods)}: {period['name']}")
        print(f"{'='*80}")
        
        print(f"\n[Splits] Fetching for {period['name']}...")
        period_splits = fetch_data_for_period(
            period['start_ts'],
            period['end_ts'],
            "splits"
        )
        all_splits.extend(period_splits)
        print(f"[Splits] Period total: {len(period_splits):,}")
        print(f"[Splits] Cumulative total: {len(all_splits):,}")
        
        # Save splits incrementally
        with open(f'polymarket_data/splits_{period["name"]}.json', 'w') as f:
            json.dump(period_splits, f, indent=2)
        print(f"✓ Saved: polymarket_data/splits_{period['name']}.json")
        
        # Save cumulative progress
        with open('polymarket_data/all_splits_cumulative.json', 'w') as f:
            json.dump(all_splits, f, indent=2)
        print(f"✓ Saved cumulative data")
        
        print(f"\nProgress: {i}/{len(periods)} periods complete ({i/len(periods)*100:.1f}%)")
        
        if i < len(periods):
            print("Pausing 3 seconds before next period...")
            time.sleep(3)
    
    print(f"\n✓ Total splits fetched: {len(all_splits):,}")
    return all_splits

def fetch_redemptions_only():
    """Fetch only redemptions data"""
    os.makedirs('polymarket_data', exist_ok=True)
    periods = get_periods()
    
    print("="*80)
    print("FETCHING REDEMPTIONS ONLY")
    print("="*80)
    print(f"Fetching data in {len(periods)} periods:")
    for i, period in enumerate(periods, 1):
        print(f"  Period {i}: {period['name']}")
    print()
    
    all_redemptions = []
    
    for i, period in enumerate(periods, 1):
        print(f"\n{'='*80}")
        print(f"PERIOD {i}/{len(periods)}: {period['name']}")
        print(f"{'='*80}")
        
        print(f"\n[Redemptions] Fetching for {period['name']}...")
        period_redemptions = fetch_data_for_period(
            period['start_ts'],
            period['end_ts'],
            "redemptions"
        )
        all_redemptions.extend(period_redemptions)
        print(f"[Redemptions] Period total: {len(period_redemptions):,}")
        print(f"[Redemptions] Cumulative total: {len(all_redemptions):,}")
        
        # Save redemptions incrementally
        with open(f'polymarket_data/redemptions_{period["name"]}.json', 'w') as f:
            json.dump(period_redemptions, f, indent=2)
        print(f"✓ Saved: polymarket_data/redemptions_{period['name']}.json")
        
        # Save cumulative progress
        with open('polymarket_data/all_redemptions_cumulative.json', 'w') as f:
            json.dump(all_redemptions, f, indent=2)
        print(f"✓ Saved cumulative data")
        
        print(f"\nProgress: {i}/{len(periods)} periods complete ({i/len(periods)*100:.1f}%)")
        
        if i < len(periods):
            print("Pausing 3 seconds before next period...")
            time.sleep(3)
    
    print(f"\n✓ Total redemptions fetched: {len(all_redemptions):,}")
    return all_redemptions

def load_existing_data():
    """Load existing splits and redemptions from files"""
    print("="*80)
    print("LOADING EXISTING DATA FROM FILES")
    print("="*80)
    
    splits = []
    redemptions = []
    
    # Try to load cumulative files first
    try:
        with open('polymarket_data/all_splits_cumulative.json', 'r') as f:
            splits = json.load(f)
        print(f"✓ Loaded {len(splits):,} splits from all_splits_cumulative.json")
    except FileNotFoundError:
        print("✗ all_splits_cumulative.json not found, will look for individual files")
        # Load from individual period files
        periods = get_periods()
        for period in periods:
            try:
                with open(f'polymarket_data/splits_{period["name"]}.json', 'r') as f:
                    period_splits = json.load(f)
                    splits.extend(period_splits)
                    print(f"  Loaded {len(period_splits):,} from splits_{period['name']}.json")
            except FileNotFoundError:
                print(f"  ✗ splits_{period['name']}.json not found")
    
    try:
        with open('polymarket_data/all_redemptions_cumulative.json', 'r') as f:
            redemptions = json.load(f)
        print(f"✓ Loaded {len(redemptions):,} redemptions from all_redemptions_cumulative.json")
    except FileNotFoundError:
        print("✗ all_redemptions_cumulative.json not found, will look for individual files")
        # Load from individual period files
        periods = get_periods()
        for period in periods:
            try:
                with open(f'polymarket_data/redemptions_{period["name"]}.json', 'r') as f:
                    period_redemptions = json.load(f)
                    redemptions.extend(period_redemptions)
                    print(f"  Loaded {len(period_redemptions):,} from redemptions_{period['name']}.json")
            except FileNotFoundError:
                print(f"  ✗ redemptions_{period['name']}.json not found")
    
    print(f"\nTotal loaded: {len(splits):,} splits, {len(redemptions):,} redemptions")
    return splits, redemptions

def calculate_december_metrics(splits, redemptions):
    """Calculate metrics for December 2025"""
    traders = set()
    redeemers = set()
    
    # Process splits (trades)
    for split in splits:
        wallet = split.get('stakeholder')
        if wallet:
            traders.add(wallet.lower())
    
    # Process redemptions (cash outs)
    for redemption in redemptions:
        wallet = redemption.get('redeemer')
        if wallet:
            redeemers.add(wallet.lower())
    
    # Calculate metrics
    total_active = traders.union(redeemers)
    both = traders.intersection(redeemers)
    
    metrics = {
        'month': 'December 2025',
        'unique_traders': len(traders),
        'unique_redeemers': len(redeemers),
        'monthly_active_users': len(total_active),
        'traded_and_redeemed': len(both),
        'only_traded': len(traders - redeemers),
        'only_redeemed': len(redeemers - traders),
        'total_splits': len(splits),
        'total_redemptions': len(redemptions)
    }
    
    return metrics

def run_analysis():
    """Load existing data and run analysis"""
    splits, redemptions = load_existing_data()
    
    if not splits and not redemptions:
        print("\n✗ No data found! Please run fetch_splits_only() or fetch_redemptions_only() first.")
        return
    
    print("\n" + "="*80)
    print("CALCULATING METRICS")
    print("="*80)
    
    metrics = calculate_december_metrics(splits, redemptions)
    
    # Save metrics
    with open('polymarket_data/december_2025_metrics.json', 'w') as f:
        json.dump(metrics, f, indent=2)
    
    # Print results
    print("\n=== December 2025 Engagement Metrics ===")
    print(f"Month: {metrics['month']}")
    print(f"Total Splits (Trades): {metrics['total_splits']:,}")
    print(f"Total Redemptions: {metrics['total_redemptions']:,}")
    print(f"\nUnique Wallets:")
    print(f"  - Traders: {metrics['unique_traders']:,}")
    print(f"  - Redeemers: {metrics['unique_redeemers']:,}")
    print(f"  - Total Active Users: {metrics['monthly_active_users']:,}")
    print(f"\nBehavior Breakdown:")
    print(f"  - Traded AND Redeemed: {metrics['traded_and_redeemed']:,}")
    print(f"  - Only Traded: {metrics['only_traded']:,}")
    print(f"  - Only Redeemed: {metrics['only_redeemed']:,}")
    
    print(f"\n✓ Metrics saved to: polymarket_data/december_2025_metrics.json")
    return metrics

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "splits":
            fetch_splits_only()
        elif command == "redemptions":
            fetch_redemptions_only()
        elif command == "analyze":
            run_analysis()
        else:
            print("Usage:")
            print("  python script.py splits       - Fetch only splits")
            print("  python script.py redemptions  - Fetch only redemptions")
            print("  python script.py analyze      - Run analysis on existing data")
    else:
        print("="*80)
        print("INTERACTIVE MODE")
        print("="*80)
        print("What would you like to do?")
        print("1. Fetch splits only")
        print("2. Fetch redemptions only")
        print("3. Run analysis on existing data")
        print("4. Fetch both splits and redemptions")
        
        choice = input("\nEnter choice (1-4): ").strip()
        
        if choice == "1":
            fetch_splits_only()
        elif choice == "2":
            fetch_redemptions_only()
        elif choice == "3":
            run_analysis()
        elif choice == "4":
            fetch_splits_only()
            print("\n" + "="*80)
            print("SPLITS COMPLETE - STARTING REDEMPTIONS")
            print("="*80 + "\n")
            fetch_redemptions_only()
            print("\n" + "="*80)
            print("BOTH COMPLETE - RUNNING ANALYSIS")
            print("="*80 + "\n")
            run_analysis()
        else:
            print("Invalid choice")