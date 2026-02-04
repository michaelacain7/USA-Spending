# Combined Government Activity Monitor

A Railway-deployable application that monitors:
- **USASpending.gov** - Federal contract awards to publicly traded companies
- **Congress Financial Disclosures** - House and Senate stock trading disclosures

## Features

- Runs both monitors concurrently in separate threads
- Discord webhook alerts for new activity
- Persistent data storage (tracks seen filings/awards)
- Fuzzy company name matching for contract awards
- Market cap and materiality calculations
- Filters for current members of Congress only

## Railway Deployment

1. Create a new Railway project
2. Connect your GitHub repo or upload these files
3. Add a persistent volume mounted at `/data`
4. Configure environment variables (optional):

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DISCORD_WEBHOOK_URLS` | (built-in) | Comma-separated webhook URLs |
| `USASPENDING_INTERVAL` | `60` | Seconds between contract checks |
| `CONGRESS_INTERVAL` | `30` | Seconds between disclosure checks |
| `MIN_CONTRACT_VALUE` | `500000` | Minimum contract value ($) |
| `MIN_MATERIALITY_PERCENT` | `1.0` | Minimum % of market cap |
| `USASPENDING_LOOKBACK_DAYS` | `7` | Days to look back for awards |
| `SCHEDULE_ENABLED` | `true` | Enable 6am-6pm weekday schedule |
| `SCHEDULE_START_HOUR` | `6` | Start hour (ET) |
| `SCHEDULE_END_HOUR` | `18` | End hour (ET) |
| `DEBUG` | `false` | Enable debug logging |
| `DATA_DIR` | `/data` | Data storage directory |

### Scheduling

By default, the monitor only runs:
- **6am - 6pm Eastern Time**
- **Weekdays only** (Monday - Friday)
- **Excludes stock market holidays** (2025-2026 built-in)

This saves Railway costs since the program sleeps outside these hours.

To run 24/7 instead, set `SCHEDULE_ENABLED=false`.

### Railway Volume Setup

1. Go to your Railway project settings
2. Add a volume with mount path: `/data`
3. This persists seen filings/awards across restarts

## Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python main.py
```

## Discord Webhooks

The default webhooks are hardcoded. To use your own:
1. Set the `DISCORD_WEBHOOK_URLS` environment variable
2. Separate multiple URLs with commas

## Alerts

### Contract Awards
- Matches recipient names to public company tickers
- Calculates materiality (% of market cap)
- Color-coded by materiality level

### Congress Disclosures  
- PTR (Periodic Transaction Reports) - Stock trades
- Annual disclosures
- Amendment filings
- Filtered to current members only

## Files Structure

```
combined_monitor/
├── main.py           # Main application
├── requirements.txt  # Python dependencies
├── Procfile         # Railway process definition
├── railway.toml     # Railway configuration
└── README.md        # This file
```

## Data Files (created at runtime)

In the data directory (`/data` or `~/.gov_monitor`):
- `usaspending_companies.json` - Company database cache
- `usaspending_market_caps.json` - Market cap cache
- `usaspending_seen_awards.json` - Tracked awards
- `congress_seen_filings.json` - Tracked filings
- `congress_current_members.json` - Congress members cache
