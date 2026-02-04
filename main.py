#!/usr/bin/env python3
"""
Combined Government Activity Monitor for Railway
- USASpending.gov Contract Awards Monitor
- Congress Financial Disclosure Monitor (House + Senate)

Runs both monitors concurrently with Discord webhook alerts.
"""

import os
import sys
import json
import time
import logging
import hashlib
import re
import io
import zipfile
import xml.etree.ElementTree as ET
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, List, Set
from dataclasses import dataclass, field

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests
from rapidfuzz import fuzz, process
from bs4 import BeautifulSoup

# =============================================================================
# CONFIGURATION
# =============================================================================

# Discord webhooks - can be overridden by environment variables
DISCORD_WEBHOOK_URLS = [
    url.strip() for url in os.environ.get(
        'DISCORD_WEBHOOK_URLS',
        'https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo,https://discordapp.com/api/webhooks/1464048870295076984/_ldSwGExzYM2ZRAKPXy1T1XCx9LE5WGomsmae3eTOnOw_7_7Kz73x6Lmw2UIi2XheyNZ,https://discordapp.com/api/webhooks/1466210412910346422/qVVnM5ulkUwy17I6zJlYNNleqDX8CS9ivuayd3HRMIyDPOCl4P0rijneuJI9DueqEosi'
    ).split(',') if url.strip()
]

# Data directory - Railway persistent volume or local
DATA_DIR = Path(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', os.environ.get('DATA_DIR', '/data')))
if not DATA_DIR.exists():
    DATA_DIR = Path.home() / '.gov_monitor'
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Check intervals
USASPENDING_CHECK_INTERVAL = int(os.environ.get('USASPENDING_INTERVAL', '60'))
CONGRESS_CHECK_INTERVAL = int(os.environ.get('CONGRESS_INTERVAL', '30'))

# Debug mode
DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

# Scheduling - only run 6am-6pm ET on weekdays (not holidays)
SCHEDULE_ENABLED = os.environ.get('SCHEDULE_ENABLED', 'true').lower() == 'true'
SCHEDULE_START_HOUR = int(os.environ.get('SCHEDULE_START_HOUR', '6'))
SCHEDULE_END_HOUR = int(os.environ.get('SCHEDULE_END_HOUR', '18'))
TIMEZONE = ZoneInfo('America/New_York')

# US Stock Market Holidays (2025-2026)
# Markets closed on these dates
MARKET_HOLIDAYS = {
    # 2025
    '2025-01-01',  # New Year's Day
    '2025-01-20',  # MLK Day
    '2025-02-17',  # Presidents Day
    '2025-04-18',  # Good Friday
    '2025-05-26',  # Memorial Day
    '2025-06-19',  # Juneteenth
    '2025-07-04',  # Independence Day
    '2025-09-01',  # Labor Day
    '2025-11-27',  # Thanksgiving
    '2025-12-25',  # Christmas
    # 2026
    '2026-01-01',  # New Year's Day
    '2026-01-19',  # MLK Day
    '2026-02-16',  # Presidents Day
    '2026-04-03',  # Good Friday
    '2026-05-25',  # Memorial Day
    '2026-06-19',  # Juneteenth
    '2026-07-03',  # Independence Day (observed)
    '2026-09-07',  # Labor Day
    '2026-11-26',  # Thanksgiving
    '2026-12-25',  # Christmas
}

# Current year for filing searches
CURRENT_YEAR = datetime.now().year
FILING_YEARS = [CURRENT_YEAR, CURRENT_YEAR - 1]

# =============================================================================
# LOGGING SETUP
# =============================================================================

def setup_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    console.setFormatter(logging.Formatter(f'%(asctime)s | {name[:8]:8s} | %(levelname)-7s | %(message)s', datefmt='%H:%M:%S'))
    logger.addHandler(console)
    
    return logger


# =============================================================================
# DISCORD WEBHOOK HELPER
# =============================================================================

def send_discord_webhook(embed: dict, logger: logging.Logger = None):
    """Send a Discord webhook with the given embed."""
    payload = {"embeds": [embed]}
    
    for webhook_url in DISCORD_WEBHOOK_URLS:
        try:
            response = requests.post(webhook_url, json=payload, timeout=10)
            if response.status_code not in [200, 204]:
                if logger:
                    logger.warning(f"Webhook failed: {response.status_code}")
        except Exception as e:
            if logger:
                logger.warning(f"Webhook error: {e}")


def is_scheduled_time() -> tuple[bool, str, int]:
    """
    Check if current time is within scheduled operating hours.
    Returns: (is_active, reason, seconds_until_active)
    """
    if not SCHEDULE_ENABLED:
        return True, "Schedule disabled", 0
    
    now = datetime.now(TIMEZONE)
    today_str = now.strftime('%Y-%m-%d')
    
    # Check if it's a holiday
    if today_str in MARKET_HOLIDAYS:
        # Calculate seconds until midnight, then we'll recheck
        tomorrow = (now + timedelta(days=1)).replace(hour=SCHEDULE_START_HOUR, minute=0, second=0, microsecond=0)
        seconds_until = int((tomorrow - now).total_seconds())
        return False, f"Market holiday ({today_str})", seconds_until
    
    # Check if it's a weekend (Saturday=5, Sunday=6)
    if now.weekday() >= 5:
        # Calculate seconds until Monday 6am
        days_until_monday = 7 - now.weekday()  # Saturday=2 days, Sunday=1 day
        next_monday = (now + timedelta(days=days_until_monday)).replace(hour=SCHEDULE_START_HOUR, minute=0, second=0, microsecond=0)
        seconds_until = int((next_monday - now).total_seconds())
        day_name = "Saturday" if now.weekday() == 5 else "Sunday"
        return False, f"Weekend ({day_name})", seconds_until
    
    # Check if within operating hours (6am - 6pm)
    if now.hour < SCHEDULE_START_HOUR:
        # Before 6am - wait until 6am
        start_time = now.replace(hour=SCHEDULE_START_HOUR, minute=0, second=0, microsecond=0)
        seconds_until = int((start_time - now).total_seconds())
        return False, f"Before {SCHEDULE_START_HOUR}am ET", seconds_until
    
    if now.hour >= SCHEDULE_END_HOUR:
        # After 6pm - wait until tomorrow 6am (or Monday if Friday)
        if now.weekday() == 4:  # Friday
            # Next active time is Monday
            next_start = (now + timedelta(days=3)).replace(hour=SCHEDULE_START_HOUR, minute=0, second=0, microsecond=0)
        else:
            next_start = (now + timedelta(days=1)).replace(hour=SCHEDULE_START_HOUR, minute=0, second=0, microsecond=0)
        
        # Check if next day is a holiday
        next_day_str = next_start.strftime('%Y-%m-%d')
        while next_day_str in MARKET_HOLIDAYS or next_start.weekday() >= 5:
            next_start = next_start + timedelta(days=1)
            next_day_str = next_start.strftime('%Y-%m-%d')
        
        seconds_until = int((next_start - now).total_seconds())
        return False, f"After {SCHEDULE_END_HOUR % 12}pm ET", seconds_until
    
    # We're in active hours!
    return True, "Active hours", 0


# =============================================================================
# USASPENDING MONITOR
# =============================================================================

@dataclass
class USASpendingConfig:
    """Configuration for USASpending monitor."""
    api_base: str = "https://api.usaspending.gov/api/v2"
    check_interval_seconds: int = USASPENDING_CHECK_INTERVAL
    lookback_days: int = int(os.environ.get('USASPENDING_LOOKBACK_DAYS', '7'))
    fuzzy_match_threshold: int = 85
    min_contract_value: float = float(os.environ.get('MIN_CONTRACT_VALUE', '500000'))
    min_materiality_percent: float = float(os.environ.get('MIN_MATERIALITY_PERCENT', '1.0'))
    materiality_thresholds: dict = field(default_factory=lambda: {'very_high': 10.0, 'high': 5.0, 'medium': 2.0, 'low': 1.0})
    
    # File paths
    companies_cache: Path = field(default_factory=lambda: DATA_DIR / "usaspending_companies.json")
    market_cap_cache: Path = field(default_factory=lambda: DATA_DIR / "usaspending_market_caps.json")
    seen_awards_file: Path = field(default_factory=lambda: DATA_DIR / "usaspending_seen_awards.json")


class PublicCompanyDatabase:
    """Database of public companies for matching."""
    
    MAJOR_GOVT_CONTRACTORS = {
        'LMT': {'name': 'Lockheed Martin Corporation', 'aliases': ['LOCKHEED MARTIN', 'LOCKHEED-MARTIN', 'LOCKHEED MARTIN CORP']},
        'RTX': {'name': 'RTX Corporation', 'aliases': ['RAYTHEON', 'RAYTHEON TECHNOLOGIES', 'RTX CORPORATION', 'RAYTHEON COMPANY']},
        'BA': {'name': 'The Boeing Company', 'aliases': ['BOEING', 'BOEING COMPANY', 'THE BOEING COMPANY']},
        'GD': {'name': 'General Dynamics Corporation', 'aliases': ['GENERAL DYNAMICS', 'GENERAL DYNAMICS CORP']},
        'NOC': {'name': 'Northrop Grumman Corporation', 'aliases': ['NORTHROP GRUMMAN', 'NORTHROP', 'NORTHROP GRUMMAN CORP']},
        'LHX': {'name': 'L3Harris Technologies', 'aliases': ['L3HARRIS', 'L3 HARRIS', 'HARRIS CORPORATION']},
        'HII': {'name': 'Huntington Ingalls Industries', 'aliases': ['HUNTINGTON INGALLS', 'HII']},
        'LDOS': {'name': 'Leidos Holdings', 'aliases': ['LEIDOS', 'LEIDOS HOLDINGS', 'LEIDOS INC']},
        'BAH': {'name': 'Booz Allen Hamilton', 'aliases': ['BOOZ ALLEN', 'BOOZ ALLEN HAMILTON']},
        'SAIC': {'name': 'Science Applications International', 'aliases': ['SAIC', 'SCIENCE APPLICATIONS']},
        'CACI': {'name': 'CACI International', 'aliases': ['CACI', 'CACI INTERNATIONAL']},
        'PLTR': {'name': 'Palantir Technologies', 'aliases': ['PALANTIR', 'PALANTIR TECHNOLOGIES', 'PALANTIR USG']},
        'MSFT': {'name': 'Microsoft Corporation', 'aliases': ['MICROSOFT', 'MICROSOFT CORP']},
        'AMZN': {'name': 'Amazon.com Inc', 'aliases': ['AMAZON', 'AMAZON WEB SERVICES', 'AWS']},
        'GOOGL': {'name': 'Alphabet Inc', 'aliases': ['GOOGLE', 'ALPHABET', 'GOOGLE LLC']},
        'ORCL': {'name': 'Oracle Corporation', 'aliases': ['ORACLE', 'ORACLE CORP', 'ORACLE AMERICA']},
        'IBM': {'name': 'International Business Machines', 'aliases': ['IBM', 'IBM CORP']},
        'ACN': {'name': 'Accenture', 'aliases': ['ACCENTURE FEDERAL', 'ACCENTURE']},
        'DELL': {'name': 'Dell Technologies', 'aliases': ['DELL', 'DELL TECHNOLOGIES', 'DELL FEDERAL']},
        'HPE': {'name': 'Hewlett Packard Enterprise', 'aliases': ['HPE', 'HEWLETT PACKARD ENTERPRISE']},
        'CSCO': {'name': 'Cisco Systems', 'aliases': ['CISCO', 'CISCO SYSTEMS']},
        'PANW': {'name': 'Palo Alto Networks', 'aliases': ['PALO ALTO NETWORKS', 'PALO ALTO']},
        'CRWD': {'name': 'CrowdStrike', 'aliases': ['CROWDSTRIKE', 'CROWDSTRIKE INC']},
        'FTNT': {'name': 'Fortinet', 'aliases': ['FORTINET', 'FORTINET INC']},
        'NOW': {'name': 'ServiceNow', 'aliases': ['SERVICENOW', 'SERVICE NOW']},
        'CRM': {'name': 'Salesforce', 'aliases': ['SALESFORCE', 'SALESFORCE.COM']},
        'BWXT': {'name': 'BWX Technologies', 'aliases': ['BWX', 'BWXT', 'BWX TECHNOLOGIES']},
        'KTOS': {'name': 'Kratos Defense', 'aliases': ['KRATOS', 'KRATOS DEFENSE']},
        'PSN': {'name': 'Parsons Corporation', 'aliases': ['PARSONS', 'PARSONS CORP']},
        'KBR': {'name': 'KBR Inc', 'aliases': ['KBR', 'KBR INC']},
        'J': {'name': 'Jacobs Solutions', 'aliases': ['JACOBS', 'JACOBS ENGINEERING']},
        'FLR': {'name': 'Fluor Corporation', 'aliases': ['FLUOR', 'FLUOR CORP']},
        'AAPL': {'name': 'Apple Inc', 'aliases': ['APPLE', 'APPLE INC']},
        'INTC': {'name': 'Intel Corporation', 'aliases': ['INTEL', 'INTEL CORP']},
        'AMD': {'name': 'Advanced Micro Devices', 'aliases': ['AMD', 'ADVANCED MICRO DEVICES']},
        'NVDA': {'name': 'NVIDIA Corporation', 'aliases': ['NVIDIA', 'NVIDIA CORP']},
        'TXT': {'name': 'Textron Inc', 'aliases': ['TEXTRON', 'TEXTRON INC', 'TEXTRON SYSTEMS']},
        'TDG': {'name': 'TransDigm Group', 'aliases': ['TRANSDIGM']},
        'TDY': {'name': 'Teledyne Technologies', 'aliases': ['TELEDYNE', 'TELEDYNE TECHNOLOGIES']},
        'AXON': {'name': 'Axon Enterprise', 'aliases': ['AXON', 'TASER', 'AXON ENTERPRISE']},
        'AVAV': {'name': 'AeroVironment', 'aliases': ['AEROVIRONMENT']},
        'RKLB': {'name': 'Rocket Lab', 'aliases': ['ROCKET LAB', 'ROCKETLAB']},
        'VSAT': {'name': 'Viasat', 'aliases': ['VIASAT', 'VIASAT INC']},
        'PFE': {'name': 'Pfizer Inc', 'aliases': ['PFIZER', 'PFIZER INC']},
        'JNJ': {'name': 'Johnson & Johnson', 'aliases': ['JOHNSON AND JOHNSON', 'J&J', 'JANSSEN']},
        'UNH': {'name': 'UnitedHealth Group', 'aliases': ['UNITEDHEALTH', 'UNITED HEALTH']},
        'CAT': {'name': 'Caterpillar Inc', 'aliases': ['CATERPILLAR', 'CATERPILLAR INC']},
        'HON': {'name': 'Honeywell International', 'aliases': ['HONEYWELL', 'HONEYWELL INTERNATIONAL']},
        'GE': {'name': 'GE Aerospace', 'aliases': ['GENERAL ELECTRIC', 'GE AEROSPACE', 'GE AVIATION']},
        'VZ': {'name': 'Verizon Communications', 'aliases': ['VERIZON', 'VERIZON COMMUNICATIONS']},
        'T': {'name': 'AT&T Inc', 'aliases': ['AT&T', 'ATT', 'AT&T INC']},
    }
    
    def __init__(self, config: USASpendingConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.companies = {}
        self.name_to_ticker = {}
        self.all_names = []
    
    def load(self, force_refresh: bool = False) -> bool:
        if not force_refresh and self.config.companies_cache.exists():
            try:
                with open(self.config.companies_cache, 'r') as f:
                    cached = json.load(f)
                    self.companies = cached.get('companies', {})
                    if len(self.companies) > 100:
                        self._build_lookup_tables()
                        self.logger.info(f"Loaded {len(self.companies)} companies from cache")
                        return True
            except:
                pass
        
        if self._load_from_sec():
            return True
        
        self._load_fallback()
        return True
    
    def _load_from_sec(self) -> bool:
        try:
            self.logger.info("Loading companies from SEC EDGAR...")
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {'User-Agent': 'GovMonitor/1.0 (contact@example.com)'}
            
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                return False
            
            data = response.json()
            self.companies = {}
            
            for key, company in data.items():
                ticker = company.get('ticker', '').upper().strip()
                name = company.get('title', '')
                if ticker and name:
                    self.companies[ticker] = {'name': name, 'ticker': ticker, 'aliases': []}
            
            for ticker, info in self.MAJOR_GOVT_CONTRACTORS.items():
                if ticker in self.companies:
                    self.companies[ticker]['aliases'] = info.get('aliases', [])
                else:
                    self.companies[ticker] = {'name': info['name'], 'ticker': ticker, 'aliases': info.get('aliases', [])}
            
            self._build_lookup_tables()
            self._save_cache()
            self.logger.info(f"Loaded {len(self.companies)} companies from SEC")
            return True
            
        except Exception as e:
            self.logger.warning(f"Failed to load from SEC: {e}")
            return False
    
    def _load_fallback(self):
        self.companies = {}
        for ticker, info in self.MAJOR_GOVT_CONTRACTORS.items():
            self.companies[ticker] = {'name': info['name'], 'ticker': ticker, 'aliases': info.get('aliases', [])}
        
        self._build_lookup_tables()
        self._save_cache()
        self.logger.info(f"Loaded {len(self.companies)} companies (fallback)")
    
    def _build_lookup_tables(self):
        self.name_to_ticker = {}
        self.all_names = []
        for ticker, info in self.companies.items():
            normalized = self._normalize_name(info['name'])
            self.name_to_ticker[normalized] = ticker
            self.all_names.append(normalized)
            for alias in info.get('aliases', []):
                norm_alias = self._normalize_name(alias)
                self.name_to_ticker[norm_alias] = ticker
                self.all_names.append(norm_alias)
    
    def _save_cache(self):
        try:
            with open(self.config.companies_cache, 'w') as f:
                json.dump({'companies': self.companies}, f)
        except:
            pass
    
    def _normalize_name(self, name: str) -> str:
        if not name:
            return ""
        name = name.upper().strip()
        for suffix in [' INC', ' INC.', ' CORP', ' CORP.', ' CORPORATION', ' LLC', ' LTD', ' CO', ' CO.', ' COMPANY', ' HOLDINGS', ' TECHNOLOGIES', ' TECHNOLOGY', ' INTERNATIONAL', ' SERVICES', ' SOLUTIONS', ' GROUP']:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name
    
    def find_match(self, recipient_name: str) -> Optional[dict]:
        if not recipient_name:
            return None
        
        normalized = self._normalize_name(recipient_name)
        
        if normalized in self.name_to_ticker:
            ticker = self.name_to_ticker[normalized]
            return {'ticker': ticker, 'matched_name': self.companies[ticker]['name'], 'match_score': 100, 'match_type': 'exact'}
        
        if not self.all_names:
            return None
        
        best_score = 0
        best_match = None
        
        for match_name, score, _ in process.extract(normalized, self.all_names, scorer=fuzz.token_sort_ratio, limit=3):
            if score > best_score:
                best_score = score
                best_match = match_name
        
        for match_name, score, _ in process.extract(normalized, self.all_names, scorer=fuzz.token_set_ratio, limit=3):
            if score > best_score:
                best_score = score
                best_match = match_name
        
        if best_match and best_score >= self.config.fuzzy_match_threshold:
            ticker = self.name_to_ticker[best_match]
            return {'ticker': ticker, 'matched_name': self.companies[ticker]['name'], 'match_score': best_score, 'match_type': 'fuzzy'}
        
        return None


class MarketCapService:
    FALLBACK = {
        'LMT': 130e9, 'RTX': 155e9, 'BA': 115e9, 'GD': 82e9, 'NOC': 72e9, 'LHX': 45e9,
        'HII': 12e9, 'LDOS': 20e9, 'BAH': 18e9, 'SAIC': 7e9, 'CACI': 10e9, 'PLTR': 55e9,
        'MSFT': 3100e9, 'AMZN': 2300e9, 'GOOGL': 2100e9, 'ORCL': 450e9, 'IBM': 200e9,
        'ACN': 220e9, 'DELL': 95e9, 'HPE': 25e9, 'CSCO': 230e9, 'PANW': 115e9,
        'CRWD': 85e9, 'NOW': 200e9, 'BWXT': 12e9, 'KTOS': 4.5e9,
        'PSN': 9e9, 'KBR': 9.5e9, 'J': 18e9, 'AAPL': 3400e9, 'NVDA': 3000e9, 'RKLB': 10e9,
        'TXT': 15e9, 'FLR': 8e9, 'TDG': 75e9, 'TDY': 23e9, 'AXON': 35e9, 'AVAV': 6.5e9,
        'CRM': 300e9, 'FTNT': 70e9,
    }
    
    def __init__(self, config: USASpendingConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.cache = {}
        self._load_cache()
    
    def _load_cache(self):
        if self.config.market_cap_cache.exists():
            try:
                with open(self.config.market_cap_cache, 'r') as f:
                    self.cache = json.load(f)
            except:
                pass
    
    def _save_cache(self):
        try:
            with open(self.config.market_cap_cache, 'w') as f:
                json.dump(self.cache, f)
        except:
            pass
    
    def get_market_cap(self, ticker: str) -> Optional[float]:
        ticker = ticker.upper()
        if ticker in self.cache:
            cached = self.cache[ticker]
            if datetime.now().timestamp() - cached.get('updated', 0) < 86400 and cached.get('market_cap'):
                return cached['market_cap']
        
        market_cap = self._fetch_yahoo(ticker) or self.FALLBACK.get(ticker)
        
        self.cache[ticker] = {'market_cap': market_cap, 'updated': datetime.now().timestamp()}
        self._save_cache()
        return market_cap
    
    def _fetch_yahoo(self, ticker):
        try:
            r = requests.get(f"https://query1.finance.yahoo.com/v7/finance/quote", params={'symbols': ticker}, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            return float(r.json().get('quoteResponse', {}).get('result', [{}])[0].get('marketCap', 0)) or None
        except:
            return None
    
    def calculate_materiality(self, contract_value: float, market_cap: float) -> dict:
        if not market_cap:
            return {'market_cap': None, 'market_cap_formatted': 'Unknown', 'percent_of_market_cap': None, 'materiality_rating': 'UNKNOWN', 'materiality_score': 0}
        
        pct = (contract_value / market_cap) * 100
        mcap_str = f"${market_cap/1e12:.2f}T" if market_cap >= 1e12 else f"${market_cap/1e9:.2f}B" if market_cap >= 1e9 else f"${market_cap/1e6:.2f}M"
        
        t = self.config.materiality_thresholds
        if pct >= t['very_high']: rating, score = "VERY HIGH 🔥🔥🔥", 4
        elif pct >= t['high']: rating, score = "HIGH 🔥🔥", 3
        elif pct >= t['medium']: rating, score = "MEDIUM 🔥", 2
        elif pct >= t['low']: rating, score = "LOW", 1
        else: rating, score = "MINIMAL", 0
        
        return {'market_cap': market_cap, 'market_cap_formatted': mcap_str, 'percent_of_market_cap': pct, 'materiality_rating': rating, 'materiality_score': score}


class USASpendingClient:
    def __init__(self, config: USASpendingConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({'Content-Type': 'application/json', 'User-Agent': 'GovMonitor/1.0'})
    
    def get_recent_awards(self, days_back: int = 1) -> list:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        url = f"{self.config.api_base}/search/spending_by_award/"
        
        payload = {
            "filters": {
                "time_period": [{"start_date": start_date.strftime("%Y-%m-%d"), "end_date": end_date.strftime("%Y-%m-%d")}],
                "award_type_codes": ["A", "B", "C", "D"],
            },
            "fields": ["Award ID", "Recipient Name", "Award Amount", "Total Outlays", "Description", "Start Date", "End Date", "Date Signed", "Awarding Agency", "Awarding Sub Agency", "Contract Award Type", "generated_internal_id"],
            "page": 1,
            "limit": 100,
            "sort": "Award Amount",
            "order": "desc"
        }
        
        all_awards = []
        
        try:
            for page in range(1, 6):
                payload["page"] = page
                response = self.session.post(url, json=payload, timeout=30)
                
                if response.status_code != 200:
                    self.logger.error(f"API error: {response.status_code}")
                    break
                
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                
                all_awards.extend(results)
                
                if len(results) < 100:
                    break
                
                time.sleep(0.5)
            
            self.logger.info(f"Fetched {len(all_awards)} awards from USAspending.gov")
            
        except Exception as e:
            self.logger.error(f"Error fetching awards: {e}")
        
        return all_awards
    
    def parse_award(self, result: dict) -> dict:
        amount = 0
        for field in ["Award Amount", "Total Outlays"]:
            val = result.get(field)
            if val:
                try:
                    amount = float(val)
                    if amount > 0:
                        break
                except:
                    pass
        
        return {
            'award_id': result.get("Award ID", "") or result.get("generated_internal_id", ""),
            'recipient_name': result.get("Recipient Name", ""),
            'award_amount': amount,
            'description': result.get("Description", ""),
            'start_date': result.get("Start Date", ""),
            'date_signed': result.get("Date Signed", ""),
            'award_date': result.get("Date Signed", "") or result.get("Start Date", ""),
            'awarding_agency': result.get("Awarding Agency", ""),
            'awarding_sub_agency': result.get("Awarding Sub Agency", ""),
            'award_type': result.get("Contract Award Type", ""),
            'internal_id': result.get("generated_internal_id", ""),
        }


class USASpendingTracker:
    def __init__(self, config: USASpendingConfig, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.seen_ids = set()
        self._load()
    
    def _load(self):
        if self.config.seen_awards_file.exists():
            try:
                with open(self.config.seen_awards_file, 'r') as f:
                    self.seen_ids = set(json.load(f).get('seen_ids', []))
            except:
                pass
    
    def _save(self):
        try:
            with open(self.config.seen_awards_file, 'w') as f:
                json.dump({'seen_ids': list(self.seen_ids)[-10000:]}, f)
        except:
            pass
    
    def _get_id(self, award):
        return award.get('award_id') or award.get('internal_id') or hashlib.md5(
            f"{award.get('recipient_name', '')}_{award.get('award_amount', '')}".encode()
        ).hexdigest()
    
    def is_seen(self, award):
        return self._get_id(award) in self.seen_ids
    
    def mark_seen(self, award):
        self.seen_ids.add(self._get_id(award))
        self._save()


class USASpendingMonitor:
    def __init__(self):
        self.config = USASpendingConfig()
        self.logger = setup_logging("USASPEND")
        self.client = USASpendingClient(self.config, self.logger)
        self.company_db = PublicCompanyDatabase(self.config, self.logger)
        self.market_cap = MarketCapService(self.config, self.logger)
        self.tracker = USASpendingTracker(self.config, self.logger)
        self.running = False
    
    def initialize(self) -> bool:
        self.logger.info("Initializing USAspending.gov Monitor...")
        self.logger.info(f"  Min contract: ${self.config.min_contract_value:,.0f}")
        self.logger.info(f"  Min materiality: {self.config.min_materiality_percent}%")
        return self.company_db.load()
    
    def _send_alert(self, award: dict, match: dict, materiality: dict):
        ticker = match['ticker']
        company = match['matched_name']
        amount = award.get('award_amount', 0)
        description = award.get('description', 'Contract Award')[:100]
        agency = award.get('awarding_agency', 'Unknown')
        
        amount_str = f"${amount/1e9:.2f}B" if amount >= 1e9 else f"${amount/1e6:.2f}M" if amount >= 1e6 else f"${amount/1e3:.2f}K"
        
        color = 0x3498db
        mat_text = ""
        if materiality:
            score = materiality.get('materiality_score', 0)
            color = [0x808080, 0x00cc00, 0xffcc00, 0xff6600, 0xff0000][min(score, 4)]
            pct = materiality.get('percent_of_market_cap')
            mat_text = f"**Market Cap:** {materiality.get('market_cap_formatted', 'Unknown')}\n"
            if pct:
                mat_text += f"**% of Market Cap:** {pct:.4f}%\n"
            mat_text += f"**Materiality:** {materiality.get('materiality_rating', 'Unknown')}"
        
        internal_id = award.get('internal_id', '')
        usa_url = f"https://www.usaspending.gov/award/{internal_id}" if internal_id else None
        
        embed = {
            "title": f"🚨 ${ticker} Government Contract Award",
            "color": color,
            "fields": [
                {"name": "Company", "value": company, "inline": True},
                {"name": "Amount", "value": amount_str, "inline": True},
                {"name": "Agency", "value": agency[:100], "inline": True},
                {"name": "Recipient Name", "value": award.get('recipient_name', 'N/A')[:100], "inline": False},
                {"name": "Description", "value": description[:200], "inline": False},
            ],
            "footer": {"text": f"Date Signed: {award.get('date_signed', 'N/A')} | Match: {match['match_score']}%"},
            "timestamp": datetime.utcnow().isoformat()
        }
        if mat_text:
            embed["fields"].insert(3, {"name": "Financial Impact", "value": mat_text, "inline": False})
        if usa_url:
            embed["url"] = usa_url
            embed["fields"].append({"name": "🔗 USAspending.gov", "value": f"[View Details]({usa_url})", "inline": False})
        
        send_discord_webhook(embed, self.logger)
        self.logger.info(f"Alert sent: ${ticker} - {amount_str}")
    
    def check_once(self) -> list:
        self.logger.info("Checking for new contract awards...")
        
        results = self.client.get_recent_awards(self.config.lookback_days)
        
        new_matches = []
        cutoff_date = datetime.now() - timedelta(days=self.config.lookback_days)
        
        for result in results:
            award = self.client.parse_award(result)
            
            if not award.get('recipient_name'):
                continue
            
            if self.tracker.is_seen(award):
                continue
            
            self.tracker.mark_seen(award)
            
            date_signed = award.get('date_signed') or award.get('start_date')
            if date_signed:
                try:
                    signed_dt = datetime.strptime(date_signed[:10], "%Y-%m-%d")
                    if signed_dt < cutoff_date:
                        continue
                except:
                    pass
            
            amount = award.get('award_amount', 0)
            if amount < self.config.min_contract_value:
                continue
            
            match = self.company_db.find_match(award['recipient_name'])
            
            if match:
                materiality = self.market_cap.calculate_materiality(amount, self.market_cap.get_market_cap(match['ticker']))
                
                pct = materiality.get('percent_of_market_cap')
                if pct is not None and pct < self.config.min_materiality_percent:
                    continue
                
                self.logger.info(f"MATCH: {award['recipient_name']} -> ${match['ticker']} ({match['match_score']}%)")
                
                new_matches.append((award, match, materiality))
                self._send_alert(award, match, materiality)
        
        self.logger.info(f"Check complete. Found {len(new_matches)} material awards.")
        return new_matches
    
    def run(self):
        self.running = True
        self.logger.info(f"Starting USASpending monitor (interval: {self.config.check_interval_seconds}s)...")
        
        while self.running:
            try:
                self.check_once()
            except Exception as e:
                self.logger.error(f"Check error: {e}")
            
            time.sleep(self.config.check_interval_seconds)
    
    def stop(self):
        self.running = False


# =============================================================================
# CONGRESS DISCLOSURE MONITOR
# =============================================================================

class CongressDisclosureMonitor:
    def __init__(self):
        self.logger = setup_logging("CONGRESS")
        self.seen_filings: Set[str] = set()
        self.current_members: Set[str] = set()
        self.current_member_lastnames: Set[str] = set()
        self.highest_house_doc_id: Optional[int] = None
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        })
        self.running = False
        
        # File paths
        self.data_file = DATA_DIR / "congress_seen_filings.json"
        self.members_cache_file = DATA_DIR / "congress_current_members.json"
        
        self.load_seen_filings()
        self.load_current_members()
    
    def normalize_name(self, name: str) -> str:
        name = re.sub(r'\b(Hon\.?|Mr\.?|Mrs\.?|Ms\.?|Dr\.?|Jr\.?|Sr\.?|III|II|IV)\b', '', name, flags=re.IGNORECASE)
        name = re.sub(r'[^\w\s]', '', name)
        name = ' '.join(name.lower().split())
        return name
    
    def load_current_members(self):
        try:
            cache_valid = False
            if self.members_cache_file.exists():
                cache_age = time.time() - os.path.getmtime(self.members_cache_file)
                if cache_age < 86400:
                    cache_valid = True
                    with open(self.members_cache_file, 'r') as f:
                        data = json.load(f)
                        self.current_members = set(data.get('members', []))
                        self.current_member_lastnames = set(data.get('lastnames', []))
                        self.logger.info(f"Loaded {len(self.current_members)} current members from cache")
            
            if not cache_valid:
                self.logger.info("Fetching current members list...")
                response = self.session.get("https://unitedstates.github.io/congress-legislators/legislators-current.json", timeout=30)
                if response.status_code == 200:
                    legislators = response.json()
                    
                    for leg in legislators:
                        name_data = leg.get('name', {})
                        first = name_data.get('first', '')
                        last = name_data.get('last', '')
                        official_full = name_data.get('official_full', '')
                        nickname = name_data.get('nickname', '')
                        
                        if first and last:
                            self.current_members.add(self.normalize_name(f"{first} {last}"))
                            self.current_member_lastnames.add(last.lower())
                        if official_full:
                            self.current_members.add(self.normalize_name(official_full))
                        if nickname and last:
                            self.current_members.add(self.normalize_name(f"{nickname} {last}"))
                    
                    with open(self.members_cache_file, 'w') as f:
                        json.dump({'members': list(self.current_members), 'lastnames': list(self.current_member_lastnames)}, f)
                    
                    self.logger.info(f"Loaded {len(self.current_members)} current members from API")
                    
        except Exception as e:
            self.logger.warning(f"Could not load current members: {e}")
    
    def is_current_member(self, name: str, last_name: str = None) -> bool:
        if not self.current_members:
            return True
        
        normalized = self.normalize_name(name)
        
        if normalized in self.current_members:
            return True
        
        for member_name in self.current_members:
            if normalized in member_name or member_name in normalized:
                return True
        
        if last_name and last_name.lower() in self.current_member_lastnames:
            return True
        
        return False
    
    def load_seen_filings(self):
        if self.data_file.exists():
            try:
                with open(self.data_file, 'r') as f:
                    data = json.load(f)
                    self.seen_filings = set(data.get('filings', []))
                    self.highest_house_doc_id = data.get('highest_house_doc_id')
                    self.logger.info(f"Loaded {len(self.seen_filings)} previously seen filings")
            except Exception as e:
                self.logger.warning(f"Could not load seen filings: {e}")
    
    def save_seen_filings(self):
        try:
            data = {'filings': list(self.seen_filings)}
            if self.highest_house_doc_id:
                data['highest_house_doc_id'] = self.highest_house_doc_id
            with open(self.data_file, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            self.logger.error(f"Could not save seen filings: {e}")
    
    def generate_filing_id(self, filing: Dict) -> str:
        doc_id = filing.get('DocID', '')
        source = filing.get('Source', 'unknown')
        if doc_id:
            return f"{source}_{doc_id}"
        key = f"{source}-{filing.get('Last', '')}-{filing.get('First', '')}-{filing.get('FilingType', '')}-{filing.get('FilingDate', '')}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def get_house_filings(self) -> List[Dict]:
        all_filings = []
        seen_doc_ids = set()
        
        # Live probing
        live_filings = self._probe_house_filings()
        for f in live_filings:
            doc_id = f.get('DocID', '')
            full_name = f.get('FullName', '')
            last_name = f.get('Last', '')
            
            if doc_id and doc_id not in seen_doc_ids:
                if self.is_current_member(full_name, last_name):
                    f['Source'] = 'house'
                    all_filings.append(f)
                    seen_doc_ids.add(doc_id)
        
        # ZIP files
        for year in FILING_YEARS:
            for file_type in ["FD", "PTR"]:
                filings = self._download_house_xml(year, file_type)
                for f in filings:
                    doc_id = f.get('DocID', '')
                    full_name = f.get('FullName', '')
                    last_name = f.get('Last', '')
                    
                    if doc_id and doc_id in seen_doc_ids:
                        continue
                    
                    if self.is_current_member(full_name, last_name):
                        f['Source'] = 'house'
                        all_filings.append(f)
                        if doc_id:
                            seen_doc_ids.add(doc_id)
        
        return all_filings
    
    def _probe_house_filings(self) -> List[Dict]:
        filings = []
        
        try:
            highest_seen = self.highest_house_doc_id
            
            if highest_seen is None:
                for filing_id in self.seen_filings:
                    if filing_id.startswith('house_'):
                        try:
                            doc_id = int(filing_id.replace('house_', ''))
                            if highest_seen is None or doc_id > highest_seen:
                                highest_seen = doc_id
                        except:
                            pass
                if highest_seen is None:
                    highest_seen = 20033000
            
            probe_start = highest_seen + 1
            max_probe_ahead = 50
            consecutive_misses = 0
            max_consecutive_misses = 20
            
            for doc_id in range(probe_start, probe_start + max_probe_ahead):
                if f"house_{doc_id}" in self.seen_filings:
                    consecutive_misses = 0
                    continue
                
                filing = self._fetch_and_validate_house_pdf(doc_id)
                
                if filing:
                    filings.append(filing)
                    consecutive_misses = 0
                    if not hasattr(self, 'highest_house_doc_id') or doc_id > (self.highest_house_doc_id or 0):
                        self.highest_house_doc_id = doc_id
                else:
                    consecutive_misses += 1
                
                if consecutive_misses >= max_consecutive_misses:
                    break
                
                time.sleep(0.15)
                
        except Exception as e:
            self.logger.warning(f"House probing error: {e}")
        
        return filings
    
    def _fetch_and_validate_house_pdf(self, doc_id: int) -> Optional[Dict]:
        valid_states = {'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
                        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
                        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
                        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
                        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
                        'DC', 'PR', 'GU', 'VI', 'AS', 'MP'}
        
        for year in FILING_YEARS:
            for file_type, path in [('PTR', 'ptr-pdfs'), ('FD', 'financial-pdfs')]:
                url = f"https://disclosures-clerk.house.gov/public_disc/{path}/{year}/{doc_id}.pdf"
                
                try:
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code != 200:
                        continue
                    
                    content = response.content
                    
                    if len(content) < 10000:
                        continue
                    
                    if not content.startswith(b'%PDF'):
                        continue
                    
                    text = content.decode('latin-1', errors='ignore')
                    
                    name_match = re.search(r'Name:\s*(Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\-]+)', text)
                    if not name_match:
                        continue
                    
                    name_part = name_match.group(2).strip()
                    if not name_part or len(name_part) < 5:
                        continue
                    
                    state_match = re.search(r'State/District:\s*([A-Z]{2}\d{2})', text)
                    if not state_match:
                        continue
                    
                    state_dst = state_match.group(1)
                    
                    if state_dst[:2] not in valid_states:
                        continue
                    
                    name = f"Hon. {name_part}"
                    name_parts = name_part.split()
                    first_name = name_parts[0] if name_parts else ''
                    last_name = name_parts[-1] if len(name_parts) >= 2 else ''
                    
                    filing_date = ''
                    date_match = re.search(r'Digitally Signed:.*?(\d{1,2}/\d{1,2}/\d{4})', text)
                    if date_match:
                        filing_date = date_match.group(1)
                    
                    return {
                        'Year': str(year),
                        'FileType': file_type,
                        'Prefix': 'Hon.',
                        'Last': last_name,
                        'First': first_name,
                        'Suffix': '',
                        'FilingType': f"{file_type} Original",
                        'StateDst': state_dst,
                        'FilingDate': filing_date,
                        'DocID': str(doc_id),
                        'DocURL': url,
                        'FullName': name,
                    }
                    
                except:
                    continue
        
        return None
    
    def _download_house_xml(self, year: int, file_type: str) -> List[Dict]:
        filings = []
        zip_url = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}{file_type}.ZIP"
        
        try:
            response = self.session.get(zip_url, timeout=60)
            
            if response.status_code == 200:
                with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
                    xml_files = [f for f in zf.namelist() if f.endswith('.xml')]
                    
                    for xml_file in xml_files:
                        with zf.open(xml_file) as xf:
                            tree = ET.parse(xf)
                            root = tree.getroot()
                            
                            for member in root.findall('.//Member'):
                                filing = self._parse_house_member(member, year, file_type)
                                if filing:
                                    filings.append(filing)
                    
        except Exception as e:
            if DEBUG:
                self.logger.debug(f"House ZIP error for {year} {file_type}: {e}")
        
        return filings
    
    def _parse_house_member(self, member: ET.Element, year: int, file_type: str) -> Optional[Dict]:
        try:
            def get_text(tag):
                child = member.find(tag)
                return child.text.strip() if child is not None and child.text else ''
            
            filing = {
                'Year': str(year),
                'FileType': file_type,
                'Prefix': get_text('Prefix'),
                'Last': get_text('Last'),
                'First': get_text('First'),
                'Suffix': get_text('Suffix'),
                'FilingType': get_text('FilingType'),
                'StateDst': get_text('StateDst'),
                'FilingDate': get_text('FilingDate'),
                'DocID': get_text('DocID'),
            }
            
            name_parts = [p for p in [filing['Prefix'], filing['First'], filing['Last'], filing['Suffix']] if p]
            filing['FullName'] = ' '.join(name_parts)
            
            doc_id = filing['DocID']
            if doc_id:
                if file_type == 'PTR':
                    filing['DocURL'] = f"https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc_id}.pdf"
                else:
                    filing['DocURL'] = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{year}/{doc_id}.pdf"
            
            return filing
        except:
            return None
    
    def get_senate_filings(self) -> List[Dict]:
        all_filings = []
        
        try:
            search_url = "https://efdsearch.senate.gov/search/"
            response = self.session.get(search_url, timeout=30)
            
            if response.status_code != 200:
                return []
            
            soup = BeautifulSoup(response.text, 'html.parser')
            csrf_input = soup.find('input', {'name': 'csrfmiddlewaretoken'})
            if not csrf_input:
                return []
            
            csrf_token = csrf_input.get('value', '')
            
            accept_url = "https://efdsearch.senate.gov/search/home/"
            accept_payload = {'prohibition_agreement': '1', 'csrfmiddlewaretoken': csrf_token}
            accept_headers = {'Origin': 'https://efdsearch.senate.gov', 'Referer': 'https://efdsearch.senate.gov/search/home/', 'Content-Type': 'application/x-www-form-urlencoded'}
            
            self.session.post(accept_url, data=accept_payload, headers=accept_headers, timeout=30)
            
            data_url = "https://efdsearch.senate.gov/search/report/data/"
            cookies = self.session.cookies.get_dict()
            
            search_headers = {
                'Referer': 'https://efdsearch.senate.gov/search/',
                'X-CSRFToken': cookies.get('csrftoken', csrf_token),
                'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                'X-Requested-With': 'XMLHttpRequest',
            }
            
            for report_type, report_name in [(11, 'PTR'), (7, 'Annual')]:
                search_payload = {
                    'start': '0',
                    'length': '100',
                    'report_types': f'[{report_type}]',
                    'filer_types': '[1]',
                    'submitted_start_date': f'01/01/{CURRENT_YEAR - 1} 00:00:00',
                }
                
                response = self.session.post(data_url, data=search_payload, headers=search_headers, timeout=30)
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data.get('result') == 'ok':
                            records = data.get('data', [])
                            
                            for record in records:
                                filing = self._parse_senate_record(record, report_name)
                                if filing:
                                    filing['Source'] = 'senate'
                                    all_filings.append(filing)
                    except:
                        pass
                
                time.sleep(0.5)
                
        except Exception as e:
            self.logger.error(f"Senate scraping error: {e}")
        
        return all_filings
    
    def _parse_senate_record(self, record: List, report_type: str) -> Optional[Dict]:
        try:
            if len(record) < 5:
                return None
            
            first_name = record[0] or ''
            last_name = record[1] or ''
            filer_type = record[2] or ''
            report_html = record[3] or ''
            filing_date = record[4] or ''
            
            if 'Senator' not in filer_type:
                return None
            
            full_name = f"{first_name} {last_name}".strip()
            
            if not self.is_current_member(full_name, last_name):
                return None
            
            doc_id_match = re.search(r'/search/view/(?:ptr|annual|paper)/([a-f0-9-]+)/', report_html)
            doc_id = doc_id_match.group(1) if doc_id_match else ''
            
            if '/ptr/' in report_html:
                file_type = 'PTR'
                doc_url = f"https://efdsearch.senate.gov/search/view/ptr/{doc_id}/" if doc_id else ''
            elif '/annual/' in report_html:
                file_type = 'Annual'
                doc_url = f"https://efdsearch.senate.gov/search/view/annual/{doc_id}/" if doc_id else ''
            else:
                file_type = report_type
                doc_url = ''
            
            title_match = re.search(r'>([^<]+)</a>', report_html)
            filing_type_desc = title_match.group(1) if title_match else file_type
            
            return {
                'Year': str(CURRENT_YEAR),
                'FileType': file_type,
                'First': first_name,
                'Last': last_name,
                'FullName': full_name,
                'FilingType': filing_type_desc,
                'StateDst': 'Senate',
                'FilingDate': filing_date,
                'DocID': doc_id,
                'DocURL': doc_url,
                'FilerType': filer_type,
            }
        except:
            return None
    
    def get_all_filings(self) -> List[Dict]:
        all_filings = []
        seen_doc_ids = set()
        
        self.logger.info("Fetching House filings...")
        house_filings = self.get_house_filings()
        for f in house_filings:
            doc_id = f.get('DocID')
            if doc_id and doc_id not in seen_doc_ids:
                seen_doc_ids.add(doc_id)
                all_filings.append(f)
        self.logger.info(f"House: {len(house_filings)} filings")
        
        self.logger.info("Fetching Senate filings...")
        senate_filings = self.get_senate_filings()
        for f in senate_filings:
            doc_id = f.get('DocID')
            if doc_id and doc_id not in seen_doc_ids:
                seen_doc_ids.add(doc_id)
                all_filings.append(f)
        self.logger.info(f"Senate: {len(senate_filings)} filings")
        
        return all_filings
    
    def send_discord_alert(self, filing: Dict):
        filing_type = filing.get('FilingType', '').upper()
        file_type = filing.get('FileType', '')
        source = filing.get('Source', 'unknown')
        
        if file_type == 'PTR' or 'PTR' in filing_type:
            color = 0xFF6B6B
            type_emoji = "📈"
        elif 'AMENDMENT' in filing_type or 'AMEND' in filing_type:
            color = 0xFFD93D
            type_emoji = "✏️"
        elif 'ANNUAL' in filing_type:
            color = 0x6BCB77
            type_emoji = "📊"
        else:
            color = 0x4ECDC4
            type_emoji = "📋"
        
        chamber_emoji = "🏛️ Senate" if source == 'senate' else "🏠 House"
        
        embed = {
            "title": f"{type_emoji} New {chamber_emoji} Financial Disclosure",
            "color": color,
            "fields": [
                {"name": "👤 Name", "value": filing.get('FullName', 'Unknown'), "inline": True},
                {"name": "🗺️ State/District", "value": filing.get('StateDst', 'N/A') or 'N/A', "inline": True},
                {"name": "📅 Filing Year", "value": str(filing.get('Year', 'N/A')), "inline": True},
                {"name": "📝 Filing Type", "value": filing.get('FilingType', 'Unknown'), "inline": True},
                {"name": "📆 Filed Date", "value": filing.get('FilingDate', 'Unknown') or 'Unknown', "inline": True},
                {"name": "🔢 Doc ID", "value": str(filing.get('DocID', 'N/A'))[:20], "inline": True},
            ],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": f"Congress Disclosure Monitor | {source.title()}"}
        }
        
        if filing.get('DocURL'):
            embed["fields"].append({"name": "📎 Document", "value": f"[View Filing]({filing['DocURL']})", "inline": False})
        
        send_discord_webhook(embed, self.logger)
        self.logger.info(f"Alert sent: {filing.get('FullName', 'Unknown')} ({source})")
    
    def check_for_new_filings(self, first_run: bool = False) -> int:
        self.logger.info("Checking for new filings...")
        
        filings = self.get_all_filings()
        new_count = 0
        
        for filing in filings:
            filing_id = self.generate_filing_id(filing)
            
            if filing_id not in self.seen_filings:
                self.seen_filings.add(filing_id)
                new_count += 1
                
                if not first_run:
                    self.send_discord_alert(filing)
                    time.sleep(0.5)
        
        if new_count > 0:
            self.save_seen_filings()
            if first_run:
                self.logger.info(f"Initial scan complete. Indexed {new_count} filings.")
            else:
                self.logger.info(f"Found {new_count} new filing(s)")
        else:
            self.logger.info("No new filings found")
        
        return new_count
    
    def run(self):
        self.running = True
        self.logger.info(f"Starting Congress Disclosure monitor (interval: {CONGRESS_CHECK_INTERVAL}s)...")
        
        self.logger.info("Performing initial scan...")
        self.check_for_new_filings(first_run=True)
        
        while self.running:
            try:
                time.sleep(CONGRESS_CHECK_INTERVAL)
                self.check_for_new_filings(first_run=False)
            except Exception as e:
                self.logger.error(f"Loop error: {e}")
                time.sleep(5)
    
    def stop(self):
        self.running = False
        self.save_seen_filings()


# =============================================================================
# MAIN APPLICATION
# =============================================================================

def main():
    print("=" * 60)
    print("  Combined Government Activity Monitor")
    print("=" * 60)
    print(f"  Data Directory: {DATA_DIR}")
    print(f"  USASpending Interval: {USASPENDING_CHECK_INTERVAL}s")
    print(f"  Congress Interval: {CONGRESS_CHECK_INTERVAL}s")
    print(f"  Discord Webhooks: {len(DISCORD_WEBHOOK_URLS)}")
    if SCHEDULE_ENABLED:
        print(f"  Schedule: {SCHEDULE_START_HOUR}am - {SCHEDULE_END_HOUR % 12}pm ET, Weekdays only")
        print(f"  Holidays: {len(MARKET_HOLIDAYS)} market holidays excluded")
    else:
        print(f"  Schedule: 24/7 (disabled)")
    print("=" * 60)
    
    # Initialize monitors once
    usaspending_monitor = USASpendingMonitor()
    congress_monitor = CongressDisclosureMonitor()
    
    if not usaspending_monitor.initialize():
        print("[ERROR] Failed to initialize USASpending monitor")
        return
    
    usa_thread = None
    congress_thread = None
    monitors_running = False
    
    def start_monitors():
        nonlocal usa_thread, congress_thread, monitors_running
        if monitors_running:
            return
        
        usaspending_monitor.running = True
        congress_monitor.running = True
        
        usa_thread = threading.Thread(target=usaspending_monitor.run, daemon=True, name="USASpending")
        congress_thread = threading.Thread(target=congress_monitor.run, daemon=True, name="Congress")
        
        usa_thread.start()
        congress_thread.start()
        monitors_running = True
        
        # Send startup notification
        startup_embed = {
            "title": "🚀 Government Activity Monitor Started",
            "description": f"**Monitors Active:**\n• USASpending.gov Contract Awards\n• Congress Financial Disclosures (House + Senate)\n\n**Schedule:** {SCHEDULE_START_HOUR}am - {SCHEDULE_END_HOUR % 12}pm ET, Weekdays",
            "color": 0x00FF00,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "Government Activity Monitor"}
        }
        send_discord_webhook(startup_embed)
        print(f"[{datetime.now(TIMEZONE).strftime('%H:%M:%S')}] Monitors STARTED")
    
    def stop_monitors():
        nonlocal monitors_running
        if not monitors_running:
            return
        
        usaspending_monitor.stop()
        congress_monitor.stop()
        monitors_running = False
        
        # Send pause notification
        pause_embed = {
            "title": "⏸️ Government Activity Monitor Paused",
            "description": "Outside scheduled hours. Will resume automatically.",
            "color": 0xFFAA00,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "Government Activity Monitor"}
        }
        send_discord_webhook(pause_embed)
        print(f"[{datetime.now(TIMEZONE).strftime('%H:%M:%S')}] Monitors PAUSED")
    
    print("\n[INFO] Starting schedule-aware monitoring loop...")
    print("[INFO] Press Ctrl+C to stop.\n")
    
    try:
        while True:
            is_active, reason, wait_seconds = is_scheduled_time()
            
            if is_active:
                if not monitors_running:
                    start_monitors()
                time.sleep(60)  # Check schedule every minute while active
            else:
                if monitors_running:
                    stop_monitors()
                
                # Calculate human-readable wait time
                hours = wait_seconds // 3600
                minutes = (wait_seconds % 3600) // 60
                
                if hours > 0:
                    wait_str = f"{hours}h {minutes}m"
                else:
                    wait_str = f"{minutes}m"
                
                print(f"[{datetime.now(TIMEZONE).strftime('%H:%M:%S')}] Sleeping: {reason} - resuming in {wait_str}")
                
                # Sleep in chunks so we can respond to Ctrl+C
                sleep_chunk = min(wait_seconds, 300)  # Sleep max 5 min at a time
                time.sleep(sleep_chunk)
                
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down...")
        if monitors_running:
            usaspending_monitor.stop()
            congress_monitor.stop()
        
        # Send shutdown notification
        shutdown_embed = {
            "title": "🛑 Government Activity Monitor Stopped",
            "description": "Monitor has been manually stopped.",
            "color": 0xFF0000,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "Government Activity Monitor"}
        }
        send_discord_webhook(shutdown_embed)
        
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
