import requests
import json
from datetime import datetime
from collections import defaultdict
import time

# Polymarket subgraph endpoint
SUBGRAPH_URL = "https://api.goldsky.com/api/public/project_cl6mb8i9h0003e201j6li0diw/subgraphs/activity-subgraph/0.0.4/gn"  # Update this URL

# December 2025 timestamps
DEC_2025_START = int(datetime(2025, 12, 1).timestamp())  # Dec 1, 2025 00:00:00
DEC_2025_END = int(datetime(2026, 1, 1).timestamp())     # Jan 1, 2026 00:00:00

def query_graphql(query):
    """Send GraphQL query and return results"""
    response = requests.post(SUBGRAPH_URL, json={'query': query})
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Query failed: {response.status_code} - {response.text}")

def fetch_december_splits():
    """Fetch all December 2025 splits"""
    all_splits = []
    skip = 0
    batch_size = 1000
    
    while True:
        query = f"""
        {{
          splits(
            first: {batch_size}, 
            skip: {skip}, 
            where: {{timestamp_gte: {DEC_2025_START}, timestamp_lt: {DEC_2025_END}}}
            orderBy: timestamp, 
            orderDirection: desc
          ) {{
            stakeholder
            timestamp
            amount
          }}
        }}
        """
        
        print(f"Fetching December splits: skip={skip}, total so far: {len(all_splits)}")
        result = query_graphql(query)
        
        if 'errors' in result:
            print(f"Error: {result['errors']}")
            break
            
        splits = result['data']['splits']
        
        if not splits:
            print("No more December splits to fetch")
            break
            
        all_splits.extend(splits)
        skip += batch_size
        
        # Be nice to the API
        time.sleep(0.5)
    
    return all_splits

def fetch_december_redemptions():
    """Fetch all December 2025 redemptions"""
    all_redemptions = []
    skip = 0
    batch_size = 1000
    
    while True:
        query = f"""
        {{
          redemptions(
            first: {batch_size}, 
            skip: {skip}, 
            where: {{timestamp_gte: {DEC_2025_START}, timestamp_lt: {DEC_2025_END}}}
            orderBy: timestamp, 
            orderDirection: desc
          ) {{
            redeemer
            timestamp
            payout
          }}
        }}
        """
        
        print(f"Fetching December redemptions: skip={skip}, total so far: {len(all_redemptions)}")
        result = query_graphql(query)
        
        if 'errors' in result:
            print(f"Error: {result['errors']}")
            break
            
        redemptions = result['data']['redemptions']
        
        if not redemptions:
            print("No more December redemptions to fetch")
            break
            
        all_redemptions.extend(redemptions)
        skip += batch_size
        
        time.sleep(0.5)
    
    return all_redemptions

def calculate_december_metrics(splits, redemptions):
    """Calculate metrics for December 2025"""
    traders = set()
    redeemers = set()
    
    # Process splits (trades)
    for split in splits:
        traders.add(split['stakeholder'].lower())
    
    # Process redemptions (cash outs)
    for redemption in redemptions:
        redeemers.add(redemption['redeemer'].lower())
    
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

def main():
    print("Starting December 2025 data collection...")
    print(f"Date range: {datetime.fromtimestamp(DEC_2025_START)} to {datetime.fromtimestamp(DEC_2025_END)}")
    
    # Fetch all December data
    print("\n=== Fetching December Splits (Trades) ===")
    splits = fetch_december_splits()
    print(f"Total December splits fetched: {len(splits)}")
    
    print("\n=== Fetching December Redemptions (Cash Outs) ===")
    redemptions = fetch_december_redemptions()
    print(f"Total December redemptions fetched: {len(redemptions)}")
    
    # Save raw data to JSON
    with open('polymarket_december_2025_raw.json', 'w') as f:
        json.dump({
            'splits': splits,
            'redemptions': redemptions
        }, f, indent=2)
    print("\nRaw data saved to: polymarket_december_2025_raw.json")
    
    # Calculate metrics
    print("\n=== Calculating December Metrics ===")
    metrics = calculate_december_metrics(splits, redemptions)
    
    # Save metrics
    with open('polymarket_december_2025_metrics.json', 'w') as f:
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
    
    print(f"\nMetrics saved to: polymarket_december_2025_metrics.json")

if __name__ == "__main__":
    main()