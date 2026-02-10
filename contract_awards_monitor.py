#!/usr/bin/env python3
"""
USASpending.gov Contract Monitor
Monitors federal contract awards for public companies.

FILTERS:
- Start Date must be >= TODAY (future/new contracts only)
- Materiality must be > 1% of market cap

Usage:
    python usaspending_monitor.py
    python usaspending_monitor.py --once --lookback 7

Requirements:
    pip install requests rapidfuzz yfinance
"""

import requests
import json
import time
import os
import re
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set

# Try to import optional dependencies
try:
    from rapidfuzz import fuzz
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False
    print("[WARN] rapidfuzz not installed - using basic matching")

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    YFINANCE_AVAILABLE = False
    print("[WARN] yfinance not installed - market cap lookups disabled")


# =============================================================================
# CONFIGURATION
# =============================================================================

DISCORD_WEBHOOK_URLS = [
    "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo",
    "https://discordapp.com/api/webhooks/1464048870295076984/_ldSwGExzYM2ZRAKPXy1T1XCx9LE5WGomsmae3eTOnOw_7_7Kz73x6Lmw2UIi2XheyNZ"
]

# Data directory
DATA_DIR = Path(os.environ.get('DATA_DIR', Path.home() / ".usaspending_monitor"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Filter thresholds
MIN_CONTRACT_VALUE = float(os.environ.get('MIN_CONTRACT_VALUE', '500000'))  # $500K minimum
MIN_MATERIALITY_PERCENT = float(os.environ.get('MIN_MATERIALITY_PERCENT', '1.0'))  # 1% of market cap

# Check interval
CHECK_INTERVAL = int(os.environ.get('CHECK_INTERVAL', '60'))  # seconds
LOOKBACK_DAYS = int(os.environ.get('LOOKBACK_DAYS', '7'))

DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'


# =============================================================================
# LOGGING
# =============================================================================

def setup_logging() -> logging.Logger:
    logger = logging.getLogger("USASpending")
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(console)
    
    return logger

logger = setup_logging()


# =============================================================================
# PUBLIC COMPANY DATABASE
# =============================================================================

class PublicCompanyDatabase:
    """Database of public companies for matching contract recipients."""
    
    def __init__(self, cache_file: Path):
        self.cache_file = cache_file
        self.companies: Dict[str, dict] = {}
        self.name_to_ticker: Dict[str, str] = {}
    
    def load(self) -> bool:
        """Load companies from cache or SEC."""
        # Try cache first
        if self.cache_file.exists():
            try:
                age = time.time() - self.cache_file.stat().st_mtime
                if age < 86400 * 7:  # 7 day cache
                    with open(self.cache_file, 'r') as f:
                        data = json.load(f)
                        self.companies = data.get('companies', {})
                        self._build_name_index()
                        logger.info(f"Loaded {len(self.companies)} companies from cache")
                        return True
            except Exception as e:
                logger.warning(f"Cache load failed: {e}")
        
        # Fetch from SEC
        return self._load_from_sec()
    
    def _load_from_sec(self) -> bool:
        """Load company tickers from SEC EDGAR."""
        logger.info("Loading companies from SEC EDGAR...")
        
        try:
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {'User-Agent': 'ContractMonitor/1.0 (contact@example.com)'}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            for item in data.values():
                ticker = item.get('ticker', '').upper()
                name = item.get('title', '')
                cik = item.get('cik_str', '')
                
                if ticker and name:
                    self.companies[ticker] = {
                        'ticker': ticker,
                        'name': name,
                        'cik': cik
                    }
            
            self._build_name_index()
            self._save_cache()
            
            logger.info(f"Loaded {len(self.companies)} companies from SEC")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load from SEC: {e}")
            return False
    
    def _build_name_index(self):
        """Build index for name lookups."""
        self.name_to_ticker = {}
        for ticker, info in self.companies.items():
            name = info.get('name', '').upper()
            # Index by full name
            self.name_to_ticker[name] = ticker
            # Index by simplified name (remove INC, CORP, etc.)
            simple = self._simplify_name(name)
            if simple:
                self.name_to_ticker[simple] = ticker
    
    def _simplify_name(self, name: str) -> str:
        """Remove common suffixes from company name."""
        suffixes = [
            'INC', 'INCORPORATED', 'CORP', 'CORPORATION', 'LLC', 'LTD', 'LIMITED',
            'CO', 'COMPANY', 'COMPANIES', 'HOLDINGS', 'GROUP', 'SERVICES',
            'INTERNATIONAL', 'INTL', 'TECHNOLOGIES', 'TECHNOLOGY', 'TECH',
            'SOLUTIONS', 'SYSTEMS', 'ENTERPRISES', 'THE', 'OF', 'AND', '&'
        ]
        words = name.upper().split()
        words = [w.strip('.,') for w in words if w.strip('.,') not in suffixes]
        return ' '.join(words)
    
    def _save_cache(self):
        """Save companies to cache."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump({'companies': self.companies}, f)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")
    
    def find_match(self, recipient_name: str) -> Optional[dict]:
        """Find a matching public company for a recipient name."""
        if not recipient_name:
            return None
        
        name = recipient_name.upper().strip()
        simple_name = self._simplify_name(name)
        
        # Exact match
        if name in self.name_to_ticker:
            ticker = self.name_to_ticker[name]
            return {'ticker': ticker, 'name': self.companies[ticker]['name'], 'match_score': 100}
        
        if simple_name in self.name_to_ticker:
            ticker = self.name_to_ticker[simple_name]
            return {'ticker': ticker, 'name': self.companies[ticker]['name'], 'match_score': 95}
        
        # Fuzzy match if available
        if FUZZY_AVAILABLE and len(simple_name) > 3:
            best_match = None
            best_score = 0
            
            for company_name, ticker in self.name_to_ticker.items():
                score = fuzz.ratio(simple_name, company_name)
                if score > best_score and score >= 85:
                    best_score = score
                    best_match = ticker
            
            if best_match:
                return {'ticker': best_match, 'name': self.companies[best_match]['name'], 'match_score': best_score}
        
        return None


# =============================================================================
# MARKET CAP LOOKUP
# =============================================================================

class MarketCapLookup:
    """Look up market caps for tickers."""
    
    def __init__(self, cache_file: Path):
        self.cache_file = cache_file
        self.cache: Dict[str, dict] = {}
        self._load_cache()
    
    def _load_cache(self):
        """Load market cap cache."""
        if self.cache_file.exists():
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
            except:
                self.cache = {}
    
    def _save_cache(self):
        """Save market cap cache."""
        try:
            with open(self.cache_file, 'w') as f:
                json.dump(self.cache, f)
        except:
            pass
    
    def get_market_cap(self, ticker: str) -> Optional[float]:
        """Get market cap for a ticker."""
        # Check cache (1 hour TTL)
        if ticker in self.cache:
            cached = self.cache[ticker]
            if time.time() - cached.get('timestamp', 0) < 3600:
                return cached.get('market_cap')
        
        if not YFINANCE_AVAILABLE:
            return None
        
        try:
            stock = yf.Ticker(ticker)
            info = stock.info
            market_cap = info.get('marketCap')
            
            if market_cap:
                self.cache[ticker] = {
                    'market_cap': market_cap,
                    'timestamp': time.time()
                }
                self._save_cache()
                return market_cap
        except Exception as e:
            logger.debug(f"Market cap lookup failed for {ticker}: {e}")
        
        return None
    
    def calculate_materiality(self, contract_value: float, market_cap: Optional[float]) -> dict:
        """Calculate materiality of contract relative to market cap."""
        if not market_cap or market_cap <= 0:
            return {
                'market_cap': None,
                'market_cap_formatted': 'N/A',
                'percent_of_market_cap': None,
                'materiality_rating': 'UNKNOWN'
            }
        
        pct = (contract_value / market_cap) * 100
        
        # Format market cap
        if market_cap >= 1e12:
            mcap_str = f"${market_cap/1e12:.2f}T"
        elif market_cap >= 1e9:
            mcap_str = f"${market_cap/1e9:.2f}B"
        else:
            mcap_str = f"${market_cap/1e6:.2f}M"
        
        # Rating
        if pct >= 10:
            rating = "VERY HIGH 🔥🔥🔥"
        elif pct >= 5:
            rating = "HIGH 🔥🔥"
        elif pct >= 2:
            rating = "MEDIUM 🔥"
        elif pct >= 1:
            rating = "LOW"
        else:
            rating = "MINIMAL"
        
        return {
            'market_cap': market_cap,
            'market_cap_formatted': mcap_str,
            'percent_of_market_cap': pct,
            'materiality_rating': rating
        }


# =============================================================================
# SEEN AWARDS TRACKER
# =============================================================================

class SeenAwardsTracker:
    """Track seen awards to avoid duplicate alerts."""
    
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.seen: Set[str] = set()
        self._load()
    
    def _load(self):
        """Load seen awards from disk."""
        if self.file_path.exists():
            try:
                with open(self.file_path, 'r') as f:
                    data = json.load(f)
                    self.seen = set(data.get('seen', []))
                    logger.info(f"Loaded {len(self.seen)} previously seen awards")
            except:
                self.seen = set()
    
    def _save(self):
        """Save seen awards to disk."""
        try:
            with open(self.file_path, 'w') as f:
                # Keep only last 10000 to prevent unbounded growth
                recent = list(self.seen)[-10000:]
                json.dump({'seen': recent}, f)
        except:
            pass
    
    def is_seen(self, award_id: str) -> bool:
        """Check if award was already seen."""
        return award_id in self.seen
    
    def mark_seen(self, award_id: str):
        """Mark award as seen."""
        self.seen.add(award_id)
        self._save()


# =============================================================================
# DISCORD ALERTS
# =============================================================================

def send_discord_alert(award: dict, match: dict, materiality: dict):
    """Send Discord webhook alert for a contract award."""
    
    ticker = match['ticker']
    amount = award.get('award_amount', 0)
    amount_str = f"${amount/1e6:.1f}M" if amount >= 1e6 else f"${amount:,.0f}"
    pct = materiality.get('percent_of_market_cap')
    pct_str = f"{pct:.1f}%" if pct else "N/A"
    
    embed = {
        "title": f"🏛️ New Contract Award — ${ticker}",
        "description": f"**{match['name']}**\nTicker: **{ticker}** | Materiality: **{pct_str}** of market cap",
        "color": 0x00FF00 if (pct and pct >= 5) else 0x1E90FF,
        "fields": [
            {"name": "💰 Contract Amount", "value": amount_str, "inline": True},
            {"name": "🏦 Market Cap", "value": materiality.get('market_cap_formatted', 'N/A'), "inline": True},
            {"name": "📊 % of Market Cap", "value": pct_str, "inline": True},
            {"name": "🏢 Agency", "value": award.get('awarding_agency', 'N/A')[:100], "inline": True},
            {"name": "📅 Start Date", "value": award.get('start_date', 'N/A'), "inline": True},
            {"name": "🏷️ Ticker", "value": ticker, "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": f"USASpending.gov Monitor | Match: {match.get('match_score', 0)}%"}
    }
    
    # Add description if available
    desc = award.get('description', '')
    if desc:
        embed["fields"].append({"name": "📝 Description", "value": desc[:200], "inline": False})
    
    # Add link
    internal_id = award.get('internal_id', '')
    if internal_id:
        embed["fields"].append({
            "name": "🔗 Document",
            "value": f"[View Filing](https://www.usaspending.gov/award/{internal_id})",
            "inline": False
        })
    
    payload = {"embeds": [embed]}
    
    for webhook_url in DISCORD_WEBHOOK_URLS:
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code not in [200, 204]:
                logger.warning(f"Discord webhook failed: {response.status_code}")
        except Exception as e:
            logger.warning(f"Discord webhook error: {e}")


# =============================================================================
# USASPENDING API CLIENT
# =============================================================================

class USASpendingClient:
    """Client for USAspending.gov API."""
    
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ContractMonitor/1.0'
        })
    
    def get_recent_awards(self, days_back: int = 7) -> list:
        """Fetch recent contract awards from USAspending.gov."""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
        
        payload = {
            "filters": {
                "time_period": [
                    {
                        "start_date": start_date.strftime("%Y-%m-%d"),
                        "end_date": end_date.strftime("%Y-%m-%d")
                    }
                ],
                "award_type_codes": ["A", "B", "C", "D"],  # Contract types
            },
            "fields": [
                "Award ID",
                "Recipient Name",
                "Award Amount",
                "Total Outlays",
                "Description",
                "Start Date",
                "End Date",
                "Date Signed",
                "Awarding Agency",
                "Awarding Sub Agency",
                "Contract Award Type",
                "generated_internal_id"
            ],
            "page": 1,
            "limit": 100,
            "sort": "Award Amount",
            "order": "desc"
        }
        
        all_awards = []
        
        try:
            for page in range(1, 6):  # Up to 5 pages
                payload["page"] = page
                
                logger.debug(f"Fetching awards page {page}...")
                response = self.session.post(url, json=payload, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"API error: {response.status_code}")
                    break
                
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                
                all_awards.extend(results)
                
                if len(results) < 100:
                    break
                
                time.sleep(0.5)  # Rate limiting
            
            logger.info(f"Fetched {len(all_awards)} awards from USAspending.gov")
            
        except Exception as e:
            logger.error(f"Error fetching awards: {e}")
        
        return all_awards
    
    def parse_award(self, result: dict) -> dict:
        """Parse an award result into standardized format."""
        
        # Get amount
        amount = 0
        for field in ['Award Amount', 'Total Outlays']:
            try:
                amount = float(result.get(field, 0) or 0)
                if amount > 0:
                    break
            except:
                pass
        
        return {
            'award_id': result.get('Award ID', ''),
            'internal_id': result.get('generated_internal_id', ''),
            'recipient_name': result.get('Recipient Name', ''),
            'award_amount': amount,
            'description': result.get('Description', ''),
            'start_date': result.get('Start Date', ''),
            'end_date': result.get('End Date', ''),
            'date_signed': result.get('Date Signed', ''),
            'awarding_agency': result.get('Awarding Agency', ''),
            'award_type': result.get('Contract Award Type', ''),
        }


# =============================================================================
# MAIN MONITOR
# =============================================================================

class USASpendingMonitor:
    """Main monitor class."""
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.check_interval = self.config.get('check_interval', CHECK_INTERVAL)
        self.lookback_days = self.config.get('lookback_days', LOOKBACK_DAYS)
        self.min_contract_value = self.config.get('min_contract_value', MIN_CONTRACT_VALUE)
        self.min_materiality_percent = self.config.get('min_materiality_percent', MIN_MATERIALITY_PERCENT)
        self.debug = self.config.get('debug', DEBUG)
        
        self.client = USASpendingClient()
        self.company_db = PublicCompanyDatabase(DATA_DIR / "companies.json")
        self.market_cap = MarketCapLookup(DATA_DIR / "market_caps.json")
        self.tracker = SeenAwardsTracker(DATA_DIR / "seen_awards.json")
    
    def initialize(self) -> bool:
        """Initialize the monitor."""
        logger.info("Initializing USASpending.gov Contract Monitor...")
        logger.info(f"  Min contract value: ${self.min_contract_value:,.0f}")
        logger.info(f"  Min materiality: {self.min_materiality_percent}% of market cap")
        logger.info(f"  Start date filter: >= TODAY (future contracts only)")
        return self.company_db.load()
    
    def check_once(self) -> list:
        """Check for new contract awards once."""
        logger.info("Checking for new contract awards...")
        
        results = self.client.get_recent_awards(self.lookback_days)
        
        new_matches = []
        stats = {
            'total': len(results),
            'seen': 0,
            'no_recipient': 0,
            'low_value': 0,
            'old_start_date': 0,
            'no_match': 0,
            'low_materiality': 0,
            'passed': 0
        }
        
        # TODAY's date for filtering
        today = datetime.now().strftime("%Y-%m-%d")
        
        for result in results:
            award = self.client.parse_award(result)
            
            # Must have recipient
            if not award.get('recipient_name'):
                stats['no_recipient'] += 1
                continue
            
            # Generate unique ID
            award_id = award.get('internal_id') or award.get('award_id') or ''
            if not award_id:
                continue
            
            # Skip if already seen
            if self.tracker.is_seen(award_id):
                stats['seen'] += 1
                continue
            
            # Mark as seen immediately to avoid duplicate processing
            self.tracker.mark_seen(award_id)
            
            # FILTER 1: Minimum contract value
            amount = award.get('award_amount', 0)
            if amount < self.min_contract_value:
                stats['low_value'] += 1
                continue
            
            # FILTER 2: Start date must be >= TODAY (future/new contracts only)
            start_date = award.get('start_date', '') or award.get('date_signed', '')
            if start_date:
                # Compare date strings (YYYY-MM-DD format)
                start_date_str = start_date[:10] if len(start_date) >= 10 else start_date
                if start_date_str < today:
                    stats['old_start_date'] += 1
                    if self.debug:
                        logger.debug(f"Filtered old contract: {award.get('recipient_name')} - Start: {start_date_str}")
                    continue
            
            # Try to match to public company
            match = self.company_db.find_match(award['recipient_name'])
            if not match:
                stats['no_match'] += 1
                continue
            
            # Get market cap and calculate materiality
            ticker = match['ticker']
            market_cap = self.market_cap.get_market_cap(ticker)
            materiality = self.market_cap.calculate_materiality(amount, market_cap)
            
            # FILTER 3: Materiality must be > threshold
            pct = materiality.get('percent_of_market_cap')
            if pct is not None and pct < self.min_materiality_percent:
                stats['low_materiality'] += 1
                if self.debug:
                    logger.debug(f"Filtered low materiality: {ticker} - {pct:.2f}%")
                continue
            
            # PASSED ALL FILTERS!
            stats['passed'] += 1
            
            logger.info(f"✓ MATCH: {award['recipient_name']} -> ${ticker} ({match['match_score']}%) - ${amount/1e6:.1f}M - {pct:.2f}% of mcap")
            
            new_matches.append((award, match, materiality))
            
            # Send Discord alert
            send_discord_alert(award, match, materiality)
        
        # Log summary
        logger.info(f"Check complete. Found {stats['passed']} material awards.")
        logger.info(f"  Filtered: {stats['old_start_date']} old start date, {stats['low_materiality']} low materiality, {stats['low_value']} low value, {stats['no_match']} no match")
        
        return new_matches
    
    def run_continuous(self):
        """Run the monitor continuously."""
        logger.info(f"Starting continuous monitoring (interval: {self.check_interval}s)...")
        
        print("\n" + "=" * 60)
        print("  USASpending.gov Contract Monitor - Running")
        print(f"  Interval: {self.check_interval}s")
        print(f"  Lookback: {self.lookback_days} days")
        print(f"  Min Contract: ${self.min_contract_value:,.0f}")
        print(f"  Min Materiality: {self.min_materiality_percent}% of market cap")
        print(f"  Start Date Filter: >= TODAY only")
        print("  Press Ctrl+C to stop")
        print("=" * 60 + "\n")
        
        try:
            while True:
                try:
                    self.check_once()
                except Exception as e:
                    logger.error(f"Check error: {e}")
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            print("\nStopped.")


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse
    parser = argparse.ArgumentParser(description="USASpending.gov Contract Monitor")
    parser.add_argument('-i', '--interval', type=int, default=CHECK_INTERVAL, help=f"Check interval seconds (default: {CHECK_INTERVAL})")
    parser.add_argument('-l', '--lookback', type=int, default=LOOKBACK_DAYS, help=f"Days to look back (default: {LOOKBACK_DAYS})")
    parser.add_argument('-m', '--min-value', type=float, default=MIN_CONTRACT_VALUE, help=f"Min contract value (default: {MIN_CONTRACT_VALUE})")
    parser.add_argument('--min-materiality', type=float, default=MIN_MATERIALITY_PERCENT, help=f"Min materiality %% (default: {MIN_MATERIALITY_PERCENT})")
    parser.add_argument('--once', action='store_true', help="Run once and exit")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    args = parser.parse_args()
    
    # Create config dict to pass to monitor
    config = {
        'check_interval': args.interval,
        'lookback_days': args.lookback,
        'min_contract_value': args.min_value,
        'min_materiality_percent': args.min_materiality,
        'debug': args.debug
    }
    
    if args.debug:
        logger.setLevel(logging.DEBUG)
        for handler in logger.handlers:
            handler.setLevel(logging.DEBUG)
    
    monitor = USASpendingMonitor(config)
    if not monitor.initialize():
        logger.error("Failed to initialize")
        return 1
    
    if args.once:
        monitor.check_once()
    else:
        monitor.run_continuous()
    
    return 0


if __name__ == "__main__":
    exit(main())
