#!/usr/bin/env python3
"""
USAspending.gov Contract Award Monitor
Monitors for new federal contract awards to publicly traded companies.
Uses USAspending.gov API which has actual award data with recipient names.
"""

import os
import sys
import json
import time
import logging
import hashlib
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests
from rapidfuzz import fuzz, process

try:
    from plyer import notification
    HAS_PLYER = True
except ImportError:
    HAS_PLYER = False


# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """Configuration settings."""
    
    # USAspending API
    api_base: str = "https://api.usaspending.gov/api/v2"
    
    # Monitoring Settings
    check_interval_seconds: int = 60
    lookback_days: int = 7  # How far back to look for awards
    
    # Market Hours Settings (9:30 AM - 4:00 PM ET)
    market_hours_only: bool = True
    market_open_hour: int = 9
    market_open_minute: int = 30
    market_close_hour: int = 16
    market_close_minute: int = 0
    timezone: str = "America/New_York"
    
    # Matching Settings
    fuzzy_match_threshold: int = 92  # Higher threshold to reduce false positives
    
    # Contract Filtering
    min_contract_value: float = 500_000
    
    # Materiality Settings
    materiality_thresholds: dict = field(default_factory=lambda: {
        'very_high': 10.0, 'high': 5.0, 'medium': 2.0, 'low': 1.0,
    })
    min_materiality_percent: float = 1.0  # Only alert if contract >= 1% of market cap
    require_materiality: bool = False
    
    # Alert Settings
    enable_desktop_notifications: bool = True
    enable_sound_alerts: bool = True
    enable_console_alerts: bool = True
    enable_discord_alerts: bool = True
    discord_webhook_urls: list = field(default_factory=lambda: [
        "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo",
        "https://discordapp.com/api/webhooks/1464048870295076984/_ldSwGExzYM2ZRAKPXy1T1XCx9LE5WGomsmae3eTOnOw_7_7Kz73x6Lmw2UIi2XheyNZ"
    ])
    
    # File Paths
    data_dir: Path = field(default_factory=lambda: Path.home() / ".usaspending_monitor")
    log_file: Path = field(default_factory=lambda: Path.home() / ".usaspending_monitor" / "monitor.log")
    matches_file: Path = field(default_factory=lambda: Path.home() / ".usaspending_monitor" / "matches.json")
    companies_cache: Path = field(default_factory=lambda: Path.home() / ".usaspending_monitor" / "companies.json")
    market_cap_cache: Path = field(default_factory=lambda: Path.home() / ".usaspending_monitor" / "market_caps.json")
    seen_awards_file: Path = field(default_factory=lambda: Path.home() / ".usaspending_monitor" / "seen_awards.json")
    
    def __post_init__(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)


def setup_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger("USASpending")
    logger.setLevel(logging.DEBUG)
    
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    logger.addHandler(console)
    
    file_handler = logging.FileHandler(config.log_file)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s'))
    logger.addHandler(file_handler)
    
    return logger


# =============================================================================
# USASPENDING API CLIENT
# =============================================================================

class USASpendingClient:
    """Client for USAspending.gov API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'ContractMonitor/1.0',
        })
    
    def get_recent_awards(self, days_back: int = 1) -> list:
        """Fetch recent contract awards from USAspending.gov."""
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        # Use the spending_by_award endpoint for recent contracts
        url = f"{self.config.api_base}/search/spending_by_award/"
        
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
                "recipient_id",
                "prime_award_recipient_id",
                "generated_internal_id"
            ],
            "page": 1,
            "limit": 100,
            "sort": "Award Amount",
            "order": "desc"
        }
        
        all_awards = []
        
        try:
            # Fetch multiple pages
            for page in range(1, 6):  # Up to 5 pages (500 awards max)
                payload["page"] = page
                
                self.logger.debug(f"Fetching awards page {page}...")
                response = self.session.post(url, json=payload, timeout=30)
                
                if response.status_code != 200:
                    self.logger.error(f"API error: {response.status_code} - {response.text[:200]}")
                    break
                
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                
                all_awards.extend(results)
                
                # Check if there are more pages
                if len(results) < 100:
                    break
                
                time.sleep(0.5)  # Rate limiting
            
            self.logger.info(f"Fetched {len(all_awards)} awards from USAspending.gov")
            
        except Exception as e:
            self.logger.error(f"Error fetching awards: {e}")
        
        return all_awards
    
    def parse_award(self, result: dict) -> dict:
        """Parse an award result into standardized format."""
        
        # Get award amount - try multiple fields
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
            'awardee_name': result.get("Recipient Name", ""),  # Alias for compatibility
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


# =============================================================================
# PUBLIC COMPANY DATABASE
# =============================================================================

class PublicCompanyDatabase:
    """Database of public companies for matching."""
    
    MAJOR_GOVT_CONTRACTORS = {
        'LMT': {'name': 'Lockheed Martin Corporation', 'aliases': ['LOCKHEED MARTIN', 'LOCKHEED-MARTIN', 'LOCKHEED MARTIN CORP']},
        'RTX': {'name': 'RTX Corporation', 'aliases': ['RAYTHEON', 'RAYTHEON TECHNOLOGIES', 'RTX CORPORATION', 'RAYTHEON COMPANY', 'RAYTHEON CO']},
        'BA': {'name': 'The Boeing Company', 'aliases': ['BOEING', 'BOEING COMPANY', 'THE BOEING COMPANY', 'BOEING CO']},
        'GD': {'name': 'General Dynamics Corporation', 'aliases': ['GENERAL DYNAMICS', 'GENERAL DYNAMICS CORP', 'GENERAL DYNAMICS INFORMATION TECHNOLOGY']},
        'NOC': {'name': 'Northrop Grumman Corporation', 'aliases': ['NORTHROP GRUMMAN', 'NORTHROP', 'NORTHROP GRUMMAN CORP', 'NORTHROP GRUMMAN SYSTEMS']},
        'LHX': {'name': 'L3Harris Technologies', 'aliases': ['L3HARRIS', 'L3 HARRIS', 'HARRIS CORPORATION', 'L3 TECHNOLOGIES', 'L3HARRIS TECHNOLOGIES']},
        'HII': {'name': 'Huntington Ingalls Industries', 'aliases': ['HUNTINGTON INGALLS', 'HII']},
        'LDOS': {'name': 'Leidos Holdings', 'aliases': ['LEIDOS', 'LEIDOS HOLDINGS', 'LEIDOS INC']},
        'BAH': {'name': 'Booz Allen Hamilton', 'aliases': ['BOOZ ALLEN', 'BOOZ ALLEN HAMILTON', 'BOOZ ALLEN HAMILTON INC']},
        'SAIC': {'name': 'Science Applications International', 'aliases': ['SAIC', 'SCIENCE APPLICATIONS']},
        'CACI': {'name': 'CACI International', 'aliases': ['CACI', 'CACI INTERNATIONAL', 'CACI INC']},
        'PLTR': {'name': 'Palantir Technologies', 'aliases': ['PALANTIR', 'PALANTIR TECHNOLOGIES', 'PALANTIR USG']},
        'MSFT': {'name': 'Microsoft Corporation', 'aliases': ['MICROSOFT', 'MICROSOFT CORP', 'MICROSOFT CORPORATION']},
        'AMZN': {'name': 'Amazon.com Inc', 'aliases': ['AMAZON', 'AMAZON WEB SERVICES', 'AWS', 'AMAZON.COM']},
        'GOOGL': {'name': 'Alphabet Inc', 'aliases': ['GOOGLE', 'ALPHABET', 'GOOGLE LLC', 'GOOGLE INC']},
        'ORCL': {'name': 'Oracle Corporation', 'aliases': ['ORACLE', 'ORACLE CORP', 'ORACLE AMERICA']},
        'IBM': {'name': 'International Business Machines', 'aliases': ['IBM', 'IBM CORP', 'IBM CORPORATION']},
        'ACN': {'name': 'Accenture', 'aliases': ['ACCENTURE FEDERAL', 'ACCENTURE', 'ACCENTURE FEDERAL SERVICES']},
        'DELL': {'name': 'Dell Technologies', 'aliases': ['DELL', 'DELL TECHNOLOGIES', 'DELL INC', 'DELL FEDERAL']},
        'HPE': {'name': 'Hewlett Packard Enterprise', 'aliases': ['HPE', 'HEWLETT PACKARD ENTERPRISE', 'HP ENTERPRISE']},
        'HPQ': {'name': 'HP Inc', 'aliases': ['HP', 'HP INC', 'HEWLETT PACKARD', 'HEWLETT-PACKARD']},
        'CSCO': {'name': 'Cisco Systems', 'aliases': ['CISCO', 'CISCO SYSTEMS']},
        'PANW': {'name': 'Palo Alto Networks', 'aliases': ['PALO ALTO NETWORKS', 'PALO ALTO']},
        'CRWD': {'name': 'CrowdStrike', 'aliases': ['CROWDSTRIKE', 'CROWDSTRIKE INC']},
        'FTNT': {'name': 'Fortinet', 'aliases': ['FORTINET', 'FORTINET INC']},
        'NOW': {'name': 'ServiceNow', 'aliases': ['SERVICENOW', 'SERVICE NOW']},
        'CRM': {'name': 'Salesforce', 'aliases': ['SALESFORCE', 'SALESFORCE.COM']},
        'ATNI': {'name': 'ATN International', 'aliases': ['ATN', 'ATN INTERNATIONAL', 'ATNI']},
        'BWXT': {'name': 'BWX Technologies', 'aliases': ['BWX', 'BWXT', 'BWX TECHNOLOGIES']},
        'KTOS': {'name': 'Kratos Defense', 'aliases': ['KRATOS', 'KRATOS DEFENSE']},
        'PSN': {'name': 'Parsons Corporation', 'aliases': ['PARSONS', 'PARSONS CORP']},
        'KBR': {'name': 'KBR Inc', 'aliases': ['KBR', 'KBR INC']},
        'J': {'name': 'Jacobs Solutions', 'aliases': ['JACOBS', 'JACOBS ENGINEERING', 'JACOBS SOLUTIONS']},
        'FLR': {'name': 'Fluor Corporation', 'aliases': ['FLUOR', 'FLUOR CORP']},
        'AAPL': {'name': 'Apple Inc', 'aliases': ['APPLE', 'APPLE INC']},
        'INTC': {'name': 'Intel Corporation', 'aliases': ['INTEL', 'INTEL CORP']},
        'AMD': {'name': 'Advanced Micro Devices', 'aliases': ['AMD', 'ADVANCED MICRO DEVICES']},
        'NVDA': {'name': 'NVIDIA Corporation', 'aliases': ['NVIDIA', 'NVIDIA CORP']},
        'QCOM': {'name': 'Qualcomm', 'aliases': ['QUALCOMM']},
        'TXT': {'name': 'Textron Inc', 'aliases': ['TEXTRON', 'TEXTRON INC', 'TEXTRON SYSTEMS']},
        'TDG': {'name': 'TransDigm Group', 'aliases': ['TRANSDIGM']},
        'HWM': {'name': 'Howmet Aerospace', 'aliases': ['HOWMET', 'HOWMET AEROSPACE']},
        'TDY': {'name': 'Teledyne Technologies', 'aliases': ['TELEDYNE', 'TELEDYNE TECHNOLOGIES']},
        'AXON': {'name': 'Axon Enterprise', 'aliases': ['AXON', 'TASER', 'AXON ENTERPRISE']},
        'AVAV': {'name': 'AeroVironment', 'aliases': ['AEROVIRONMENT']},
        'MRCY': {'name': 'Mercury Systems', 'aliases': ['MERCURY SYSTEMS', 'MERCURY']},
        'RKLB': {'name': 'Rocket Lab', 'aliases': ['ROCKET LAB', 'ROCKETLAB']},
        'VSAT': {'name': 'Viasat', 'aliases': ['VIASAT', 'VIASAT INC']},
        'IRDM': {'name': 'Iridium Communications', 'aliases': ['IRIDIUM', 'IRIDIUM COMMUNICATIONS']},
        'MAXR': {'name': 'Maxar Technologies', 'aliases': ['MAXAR', 'MAXAR TECHNOLOGIES']},
        'JNJ': {'name': 'Johnson & Johnson', 'aliases': ['JOHNSON AND JOHNSON', 'JOHNSON & JOHNSON', 'J&J', 'JANSSEN']},
        'PFE': {'name': 'Pfizer Inc', 'aliases': ['PFIZER', 'PFIZER INC']},
        'MRK': {'name': 'Merck & Co', 'aliases': ['MERCK', 'MERCK & CO', 'MERCK SHARP']},
        'ABBV': {'name': 'AbbVie Inc', 'aliases': ['ABBVIE', 'ABBVIE INC']},
        'LLY': {'name': 'Eli Lilly', 'aliases': ['ELI LILLY', 'LILLY', 'ELI LILLY AND COMPANY']},
        'UNH': {'name': 'UnitedHealth Group', 'aliases': ['UNITEDHEALTH', 'UNITED HEALTH', 'UNITEDHEALTH GROUP']},
        'CVS': {'name': 'CVS Health', 'aliases': ['CVS', 'CVS HEALTH', 'CVS PHARMACY']},
        'CAT': {'name': 'Caterpillar Inc', 'aliases': ['CATERPILLAR', 'CATERPILLAR INC']},
        'DE': {'name': 'Deere & Company', 'aliases': ['JOHN DEERE', 'DEERE', 'DEERE & COMPANY']},
        'HON': {'name': 'Honeywell International', 'aliases': ['HONEYWELL', 'HONEYWELL INTERNATIONAL']},
        'GE': {'name': 'GE Aerospace', 'aliases': ['GENERAL ELECTRIC', 'GE AEROSPACE', 'GE AVIATION', 'GE']},
        'FDX': {'name': 'FedEx Corporation', 'aliases': ['FEDEX', 'FEDERAL EXPRESS', 'FEDEX CORP']},
        'UPS': {'name': 'United Parcel Service', 'aliases': ['UPS', 'UNITED PARCEL']},
        'VZ': {'name': 'Verizon Communications', 'aliases': ['VERIZON', 'VERIZON COMMUNICATIONS', 'VERIZON BUSINESS']},
        'T': {'name': 'AT&T Inc', 'aliases': ['AT&T', 'ATT', 'AT&T INC']},
        'TMUS': {'name': 'T-Mobile US', 'aliases': ['T-MOBILE', 'TMOBILE', 'T-MOBILE US']},
    }
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.companies = {}
        self.name_to_ticker = {}
        self.all_names = []
    
    def load(self, force_refresh: bool = False) -> bool:
        """Load company database from SEC or cache."""
        if not force_refresh and self.config.companies_cache.exists():
            try:
                with open(self.config.companies_cache, 'r') as f:
                    cached = json.load(f)
                    self.companies = cached.get('companies', {})
                    if len(self.companies) > 100:  # Only use cache if it has SEC data
                        self._build_lookup_tables()
                        self.logger.info(f"Loaded {len(self.companies)} companies from cache")
                        return True
            except:
                pass
        
        # Try to load from SEC
        if self._load_from_sec():
            return True
        
        # Fall back to built-in list
        self._load_fallback()
        return True
    
    def _load_from_sec(self) -> bool:
        """Load companies from SEC EDGAR."""
        try:
            self.logger.info("Loading companies from SEC EDGAR...")
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {'User-Agent': 'ContractMonitor/1.0 (contact@example.com)'}
            
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                self.logger.warning(f"SEC API returned {response.status_code}")
                return False
            
            data = response.json()
            self.companies = {}
            
            for key, company in data.items():
                ticker = company.get('ticker', '')
                name = company.get('title', '')
                
                if ticker and name:
                    # Clean up ticker
                    ticker = ticker.upper().strip()
                    
                    # Add company
                    self.companies[ticker] = {
                        'name': name,
                        'ticker': ticker,
                        'aliases': []
                    }
            
            # Add aliases from our known contractors list
            for ticker, info in self.MAJOR_GOVT_CONTRACTORS.items():
                if ticker in self.companies:
                    self.companies[ticker]['aliases'] = info.get('aliases', [])
                else:
                    self.companies[ticker] = {
                        'name': info['name'],
                        'ticker': ticker,
                        'aliases': info.get('aliases', [])
                    }
            
            self._build_lookup_tables()
            self._save_cache()
            self.logger.info(f"Loaded {len(self.companies)} companies from SEC")
            return True
            
        except Exception as e:
            self.logger.warning(f"Failed to load from SEC: {e}")
            return False
    
    def _load_fallback(self):
        """Load fallback list of major government contractors."""
        self.companies = {}
        for ticker, info in self.MAJOR_GOVT_CONTRACTORS.items():
            self.companies[ticker] = {'name': info['name'], 'ticker': ticker, 'aliases': info.get('aliases', [])}
        
        self._build_lookup_tables()
        self._save_cache()
        self.logger.info(f"Loaded {len(self.companies)} companies (fallback)")
        return True
    
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
        
        # Skip very short names (likely abbreviations that cause false matches)
        if len(normalized) < 4:
            return None
        
        # Exact match
        if normalized in self.name_to_ticker:
            ticker = self.name_to_ticker[normalized]
            return {'ticker': ticker, 'matched_name': self.companies[ticker]['name'], 'match_score': 100, 'match_type': 'exact'}
        
        # Fuzzy match with stricter validation
        if not self.all_names:
            return None
        
        best_score = 0
        best_match = None
        
        # Use standard ratio first (stricter than token_set_ratio)
        for match_name, score, _ in process.extract(normalized, self.all_names, scorer=fuzz.ratio, limit=5):
            if score > best_score:
                best_score = score
                best_match = match_name
        
        # Also try token_sort_ratio for reordered words
        for match_name, score, _ in process.extract(normalized, self.all_names, scorer=fuzz.token_sort_ratio, limit=5):
            if score > best_score:
                best_score = score
                best_match = match_name
        
        if best_match and best_score >= self.config.fuzzy_match_threshold:
            ticker = self.name_to_ticker[best_match]
            
            # Additional validation: check that first words are similar
            # This prevents "WJM PROFESSIONAL" matching "PROFESSIONAL DIVERSITY"
            input_words = normalized.split()
            match_words = best_match.split()
            
            if input_words and match_words:
                first_word_score = fuzz.ratio(input_words[0], match_words[0])
                # Require first word to be at least 70% similar, or be contained in each other
                first_word_contained = (input_words[0] in match_words[0]) or (match_words[0] in input_words[0])
                
                if first_word_score < 70 and not first_word_contained:
                    # First words don't match well - reject this match
                    return None
            
            # Check length similarity - reject if lengths are very different
            len_ratio = min(len(normalized), len(best_match)) / max(len(normalized), len(best_match))
            if len_ratio < 0.5:
                # Names are too different in length
                return None
            
            return {'ticker': ticker, 'matched_name': self.companies[ticker]['name'], 'match_score': best_score, 'match_type': 'fuzzy'}
        
        return None


# =============================================================================
# MARKET CAP SERVICE
# =============================================================================

class MarketCapService:
    FALLBACK = {
        'LMT': 130e9, 'RTX': 155e9, 'BA': 115e9, 'GD': 82e9, 'NOC': 72e9, 'LHX': 45e9,
        'HII': 12e9, 'LDOS': 20e9, 'BAH': 18e9, 'SAIC': 7e9, 'CACI': 10e9, 'PLTR': 55e9,
        'MSFT': 3100e9, 'AMZN': 2300e9, 'GOOGL': 2100e9, 'ORCL': 450e9, 'IBM': 200e9,
        'ACN': 220e9, 'DELL': 95e9, 'HPE': 25e9, 'CSCO': 230e9, 'PANW': 115e9,
        'CRWD': 85e9, 'NOW': 200e9, 'ATNI': 250e6, 'BWXT': 12e9, 'KTOS': 4.5e9,
        'PSN': 9e9, 'KBR': 9.5e9, 'J': 18e9, 'AAPL': 3400e9, 'NVDA': 3000e9, 'RKLB': 10e9,
        'TXT': 15e9, 'FLR': 8e9, 'TDG': 75e9, 'TDY': 23e9, 'AXON': 35e9, 'AVAV': 6.5e9,
        'MRCY': 2.5e9, 'HPQ': 35e9, 'CRM': 300e9, 'FTNT': 70e9, 'HWM': 40e9,
    }
    
    def __init__(self, config: Config, logger: logging.Logger):
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
        
        market_cap = self._fetch_yfinance(ticker) or self._fetch_yahoo(ticker) or self.FALLBACK.get(ticker)
        
        self.cache[ticker] = {'market_cap': market_cap, 'updated': datetime.now().timestamp()}
        self._save_cache()
        return market_cap
    
    def _fetch_yfinance(self, ticker):
        try:
            import yfinance as yf
            return float(yf.Ticker(ticker).info.get('marketCap', 0)) or None
        except:
            return None
    
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


# =============================================================================
# ALERT SYSTEM
# =============================================================================

class AlertSystem:
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def alert(self, award: dict, match: dict, materiality: dict = None):
        ticker = match['ticker']
        company = match['matched_name']
        amount = award.get('award_amount', 0)
        description = award.get('description', 'Contract Award')[:100]
        agency = award.get('awarding_agency', 'Unknown')
        
        amount_str = f"${amount/1e9:.2f}B" if amount >= 1e9 else f"${amount/1e6:.2f}M" if amount >= 1e6 else f"${amount/1e3:.2f}K" if amount >= 1e3 else f"${amount:,.0f}"
        
        if self.config.enable_console_alerts:
            self._console_alert(ticker, company, amount_str, description, agency, award, match, materiality)
        
        if self.config.enable_discord_alerts:
            self._discord_alert(ticker, company, amount_str, description, agency, award, match, materiality)
        
        if self.config.enable_sound_alerts:
            try:
                if sys.platform == 'win32':
                    import winsound
                    winsound.MessageBeep(winsound.MB_ICONEXCLAMATION)
            except:
                pass
    
    def _console_alert(self, ticker, company, amount, description, agency, award, match, materiality):
        print("\n" + "=" * 80)
        print("🚨 PUBLIC COMPANY CONTRACT AWARD DETECTED! 🚨")
        print("=" * 80)
        print(f"  TICKER:       ${ticker}")
        print(f"  COMPANY:      {company}")
        print(f"  RECIPIENT:    {award.get('recipient_name', 'N/A')}")
        print(f"  AMOUNT:       {amount}")
        if materiality:
            print(f"  MARKET CAP:   {materiality.get('market_cap_formatted', 'Unknown')}")
            if materiality.get('percent_of_market_cap'):
                print(f"  % OF MCAP:    {materiality['percent_of_market_cap']:.4f}%")
            print(f"  MATERIALITY:  {materiality.get('materiality_rating', 'Unknown')}")
        print(f"  AGENCY:       {agency}")
        print(f"  DESCRIPTION:  {description}")
        print(f"  DATE SIGNED:  {award.get('date_signed', 'N/A')}")
        internal_id = award.get('internal_id', '')
        if internal_id:
            print(f"  LINK:         https://www.usaspending.gov/award/{internal_id}")
        print(f"  MATCH:        {match['match_score']}% ({match['match_type']})")
        print("=" * 80 + "\n")
    
    def _discord_alert(self, ticker, company, amount, description, agency, award, match, materiality):
        try:
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
            
            award_id = award.get('award_id', '')
            internal_id = award.get('internal_id', '')
            usa_url = f"https://www.usaspending.gov/award/{internal_id}" if internal_id else None
            
            embed = {
                "title": f"🚨 ${ticker} Government Contract Award",
                "color": color,
                "fields": [
                    {"name": "Company", "value": company, "inline": True},
                    {"name": "Amount", "value": amount, "inline": True},
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
            
            for webhook in self.config.discord_webhook_urls:
                try:
                    requests.post(webhook, json={"embeds": [embed]}, timeout=10)
                except:
                    pass
        except Exception as e:
            self.logger.error(f"Discord failed: {e}")


# =============================================================================
# MATCH TRACKER
# =============================================================================

class MatchTracker:
    def __init__(self, config: Config, logger: logging.Logger):
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
                json.dump({'seen_ids': list(self.seen_ids)[-10000:]}, f)  # Keep last 10k
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


# =============================================================================
# MAIN MONITOR
# =============================================================================

class USASpendingMonitor:
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging(config)
        self.client = USASpendingClient(config, self.logger)
        self.company_db = PublicCompanyDatabase(config, self.logger)
        self.market_cap = MarketCapService(config, self.logger)
        self.alerts = AlertSystem(config, self.logger)
        self.tracker = MatchTracker(config, self.logger)
    
    def initialize(self) -> bool:
        self.logger.info("Initializing USAspending.gov Contract Monitor...")
        self.logger.info(f"  Min contract: ${self.config.min_contract_value:,.0f}")
        self.logger.info(f"  Min materiality: {self.config.min_materiality_percent}% of market cap")
        return self.company_db.load()
    
    def check_once(self) -> list:
        self.logger.info("Checking for new contract awards...")
        
        results = self.client.get_recent_awards(self.config.lookback_days)
        
        new_matches = []
        filtered_low_value = 0
        filtered_low_materiality = 0
        filtered_old_contracts = 0
        
        # Calculate cutoff date for "new" contracts
        cutoff_date = datetime.now() - timedelta(days=self.config.lookback_days)
        
        for result in results:
            award = self.client.parse_award(result)
            
            if not award.get('recipient_name'):
                continue
            
            if self.tracker.is_seen(award):
                continue
            
            self.tracker.mark_seen(award)
            
            # Filter out old contracts - only alert on recently signed ones
            date_signed = award.get('date_signed') or award.get('start_date')
            if date_signed:
                try:
                    # Parse date (format: YYYY-MM-DD)
                    signed_dt = datetime.strptime(date_signed[:10], "%Y-%m-%d")
                    if signed_dt < cutoff_date:
                        filtered_old_contracts += 1
                        continue
                except:
                    pass  # If we can't parse date, let it through
            
            amount = award.get('award_amount', 0)
            if amount < self.config.min_contract_value:
                filtered_low_value += 1
                continue
            
            match = self.company_db.find_match(award['recipient_name'])
            
            if match:
                materiality = self.market_cap.calculate_materiality(amount, self.market_cap.get_market_cap(match['ticker']))
                
                # Filter by minimum materiality (% of market cap)
                pct = materiality.get('percent_of_market_cap')
                if pct is not None and pct < self.config.min_materiality_percent:
                    filtered_low_materiality += 1
                    self.logger.debug(f"Filtered {match['ticker']} - {pct:.4f}% below {self.config.min_materiality_percent}% threshold")
                    continue
                
                self.logger.info(f"MATCH: {award['recipient_name']} -> ${match['ticker']} ({match['match_score']}%) - ${amount/1e6:.1f}M - {pct:.4f}% of mcap")
                
                new_matches.append((award, match, materiality))
                self.alerts.alert(award, match, materiality)
        
        summary = f"Check complete. Found {len(new_matches)} material awards."
        if filtered_old_contracts > 0:
            summary += f" (Filtered {filtered_old_contracts} old/in-progress)"
        if filtered_low_value > 0:
            summary += f" (Filtered {filtered_low_value} below ${self.config.min_contract_value/1000:.0f}K)"
        if filtered_low_materiality > 0:
            summary += f" (Filtered {filtered_low_materiality} below {self.config.min_materiality_percent}% materiality)"
        self.logger.info(summary)
        return new_matches
    
    def is_market_hours(self):
        try:
            now = datetime.now(ZoneInfo(self.config.timezone))
        except:
            now = datetime.utcnow() - timedelta(hours=5)
        
        if now.weekday() >= 5:
            return False, 0, 0
        
        market_open = now.replace(hour=self.config.market_open_hour, minute=self.config.market_open_minute, second=0)
        market_close = now.replace(hour=self.config.market_close_hour, minute=self.config.market_close_minute, second=0)
        
        if now < market_open:
            return False, (market_open - now).total_seconds(), 0
        elif now >= market_close:
            return False, 0, 0
        return True, 0, (market_close - now).total_seconds()
    
    def run_continuous(self):
        self.logger.info(f"Starting monitor (interval: {self.config.check_interval_seconds}s)...")
        print("\n" + "=" * 60)
        print("  USAspending.gov Contract Monitor - Running")
        print(f"  Interval: {self.config.check_interval_seconds}s")
        print(f"  Lookback: {self.config.lookback_days} days")
        print(f"  Min Contract: ${self.config.min_contract_value:,.0f}")
        print(f"  Min Materiality: {self.config.min_materiality_percent}% of market cap")
        if self.config.market_hours_only:
            print("  Mode: Market hours only (9:30 AM - 4:00 PM ET)")
        else:
            print("  Mode: 24/7")
        print("  Press Ctrl+C to stop")
        print("=" * 60 + "\n")
        
        try:
            while True:
                if self.config.market_hours_only:
                    is_open, until_open, _ = self.is_market_hours()
                    if not is_open:
                        mins = int(until_open / 60) if until_open else 0
                        if mins > 0:
                            print(f"⏸️  Market closed. Open in {mins}m")
                        time.sleep(min(300, until_open) if until_open > 0 else 60)
                        continue
                
                try:
                    self.check_once()
                except Exception as e:
                    self.logger.error(f"Check error: {e}")
                
                time.sleep(self.config.check_interval_seconds)
                
        except KeyboardInterrupt:
            print("\nStopped.")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="USAspending.gov Contract Monitor")
    parser.add_argument('-i', '--interval', type=int, default=60, help="Check interval in seconds (default: 60)")
    parser.add_argument('-l', '--lookback', type=int, default=7, help="Days to look back (default: 7)")
    parser.add_argument('-m', '--min-value', type=float, default=500000, help="Min contract value (default: 500000)")
    parser.add_argument('--min-materiality', type=float, default=1.0, help="Min materiality %% of market cap (default: 1.0)")
    parser.add_argument('--all-hours', action='store_true', help="Run 24/7 instead of market hours only")
    parser.add_argument('--once', action='store_true', help="Run once and exit")
    parser.add_argument('--no-discord', action='store_true', help="Disable Discord alerts")
    args = parser.parse_args()
    
    config = Config()
    config.check_interval_seconds = args.interval
    config.lookback_days = args.lookback
    config.min_contract_value = args.min_value
    config.min_materiality_percent = args.min_materiality
    config.market_hours_only = not args.all_hours  # Default: market hours only, --all-hours disables
    config.enable_discord_alerts = not args.no_discord
    
    monitor = USASpendingMonitor(config)
    if not monitor.initialize():
        sys.exit(1)
    
    if args.once:
        monitor.check_once()
    else:
        monitor.run_continuous()


if __name__ == "__main__":
    main()
