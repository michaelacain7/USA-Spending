#!/usr/bin/env python3
"""
Combined Government Contract Award Monitor
Monitors SAM.gov and USAspending.gov for new contract awards to public companies.

Features:
- Dual source: SAM.gov (fastest) + USAspending.gov (backup/verification)
- Cross-check deduplication to avoid duplicate alerts
- Full SEC company database (~10,000 companies)
- Configurable materiality threshold (default 0.5% of market cap)
- Internal validation and self-test
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
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass, field

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import requests
from rapidfuzz import fuzz, process

# =============================================================================
# CONFIGURATION
# =============================================================================

@dataclass
class Config:
    """Configuration settings."""
    
    # API Settings
    usaspending_api: str = "https://api.usaspending.gov/api/v2"
    sam_api: str = "https://api.sam.gov/opportunities/v2/search"
    sam_api_key: str = ""  # Optional - works without key but with rate limits
    
    # Monitoring Settings
    check_interval_seconds: int = 60
    lookback_days: int = 7
    
    # Market Hours Settings (9:30 AM - 4:00 PM ET)
    market_hours_only: bool = True
    market_open_hour: int = 9
    market_open_minute: int = 30
    market_close_hour: int = 16
    market_close_minute: int = 0
    timezone: str = "America/New_York"
    
    # Matching Settings
    fuzzy_match_threshold: int = 92
    
    # Contract Filtering
    min_contract_value: float = 500_000
    
    # Materiality Settings (0.5% threshold)
    materiality_thresholds: dict = field(default_factory=lambda: {
        'very_high': 5.0, 'high': 2.0, 'medium': 1.0, 'low': 0.5,
    })
    min_materiality_percent: float = 0.5
    
    # Alert Settings
    enable_console_alerts: bool = True
    enable_discord_alerts: bool = True
    discord_webhook_urls: list = field(default_factory=lambda: [
        "https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo",
        "https://discordapp.com/api/webhooks/1464048870295076984/_ldSwGExzYM2ZRAKPXy1T1XCx9LE5WGomsmae3eTOnOw_7_7Kz73x6Lmw2UIi2XheyNZ"
    ])
    
    # File Paths
    data_dir: Path = field(default_factory=lambda: Path.home() / ".contract_monitor")
    log_file: Path = field(default_factory=lambda: Path.home() / ".contract_monitor" / "monitor.log")
    companies_cache: Path = field(default_factory=lambda: Path.home() / ".contract_monitor" / "companies.json")
    market_cap_cache: Path = field(default_factory=lambda: Path.home() / ".contract_monitor" / "market_caps.json")
    seen_awards_file: Path = field(default_factory=lambda: Path.home() / ".contract_monitor" / "seen_awards.json")
    
    def __post_init__(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)


def setup_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger("ContractMonitor")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []  # Clear existing handlers
    
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
            'User-Agent': 'ContractMonitor/2.0'
        })
    
    def get_recent_awards(self, days_back: int = 7) -> List[Dict]:
        """Fetch recent contract awards from USAspending.gov."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        url = f"{self.config.usaspending_api}/search/spending_by_award/"
        
        payload = {
            "filters": {
                "time_period": [
                    {
                        "start_date": start_date.strftime("%Y-%m-%d"),
                        "end_date": end_date.strftime("%Y-%m-%d")
                    }
                ],
                "award_type_codes": ["A", "B", "C", "D"],
            },
            "fields": [
                "Award ID", "Recipient Name", "Award Amount", "Description",
                "Start Date", "Date Signed", "Awarding Agency", "Awarding Sub Agency",
                "Contract Award Type", "generated_internal_id"
            ],
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
                    self.logger.error(f"USAspending API error: {response.status_code}")
                    break
                
                data = response.json()
                results = data.get("results", [])
                
                if not results:
                    break
                
                for result in results:
                    award = self._parse_award(result)
                    if award:
                        all_awards.append(award)
                
                if len(results) < 100:
                    break
                
                time.sleep(0.3)
            
            self.logger.debug(f"USAspending: Fetched {len(all_awards)} awards")
            
        except Exception as e:
            self.logger.error(f"USAspending error: {e}")
        
        return all_awards
    
    def _parse_award(self, result: Dict) -> Optional[Dict]:
        """Parse USAspending result."""
        try:
            recipient = result.get("Recipient Name", "")
            if not recipient:
                return None
            
            amount = result.get("Award Amount") or 0
            
            return {
                'source': 'usaspending',
                'award_id': result.get("Award ID", "") or result.get("generated_internal_id", ""),
                'internal_id': result.get("generated_internal_id", ""),
                'recipient_name': recipient,
                'award_amount': float(amount),
                'description': (result.get("Description") or "")[:200],
                'start_date': result.get("Start Date", ""),
                'date_signed': result.get("Date Signed", ""),
                'agency': result.get("Awarding Agency", "") or result.get("Awarding Sub Agency", ""),
                'award_type': result.get("Contract Award Type", ""),
            }
        except Exception as e:
            self.logger.debug(f"Parse error: {e}")
            return None


# =============================================================================
# SAM.GOV API CLIENT
# =============================================================================

class SAMGovClient:
    """Client for SAM.gov API."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ContractMonitor/2.0',
            'Accept': 'application/json'
        })
    
    def get_recent_awards(self, days_back: int = 7) -> List[Dict]:
        """Fetch recent award notices from SAM.gov."""
        all_awards = []
        
        # Try the public API first
        awards = self._fetch_from_api(days_back)
        if awards:
            all_awards.extend(awards)
        
        # Also try the opportunities search
        opps = self._fetch_opportunities(days_back)
        if opps:
            all_awards.extend(opps)
        
        self.logger.debug(f"SAM.gov: Fetched {len(all_awards)} awards")
        return all_awards
    
    def _fetch_from_api(self, days_back: int) -> List[Dict]:
        """Fetch from SAM.gov public API."""
        awards = []
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # SAM.gov Opportunities API
            params = {
                'postedFrom': start_date.strftime("%m/%d/%Y"),
                'postedTo': end_date.strftime("%m/%d/%Y"),
                'ptype': 'a',  # Award notices
                'limit': 100,
            }
            
            if self.config.sam_api_key:
                params['api_key'] = self.config.sam_api_key
            
            response = self.session.get(self.config.sam_api, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                opportunities = data.get('opportunitiesData', []) or data.get('opportunities', [])
                
                for opp in opportunities:
                    award = self._parse_opportunity(opp)
                    if award:
                        awards.append(award)
            else:
                self.logger.debug(f"SAM.gov API returned {response.status_code}")
                
        except Exception as e:
            self.logger.debug(f"SAM.gov API error: {e}")
        
        return awards
    
    def _fetch_opportunities(self, days_back: int) -> List[Dict]:
        """Fetch from SAM.gov opportunities endpoint."""
        awards = []
        
        try:
            # Try alternative endpoint
            url = "https://sam.gov/api/prod/sgs/v1/search/"
            
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            params = {
                'index': 'opp',
                'q': '',
                'page': 0,
                'size': 100,
                'sort': '-modifiedDate',
                'mode': 'search',
                'is_active': 'true',
            }
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                results = data.get('_embedded', {}).get('results', [])
                
                for result in results:
                    # Check if it's an award notice
                    notice_type = result.get('type', {}).get('value', '')
                    if 'award' in notice_type.lower():
                        award = self._parse_search_result(result)
                        if award:
                            awards.append(award)
                            
        except Exception as e:
            self.logger.debug(f"SAM.gov search error: {e}")
        
        return awards
    
    def _parse_opportunity(self, opp: Dict) -> Optional[Dict]:
        """Parse SAM.gov opportunity."""
        try:
            # Get awardee info
            awardee = opp.get('awardee', {}) or {}
            awardee_name = awardee.get('name', '') or opp.get('awardeeList', [{}])[0].get('name', '') if opp.get('awardeeList') else ''
            
            if not awardee_name:
                return None
            
            award_info = opp.get('award', {}) or {}
            amount = award_info.get('amount', 0) or opp.get('baseAndAllOptionsValue', 0) or 0
            
            return {
                'source': 'sam.gov',
                'award_id': opp.get('noticeId', '') or opp.get('solicitationNumber', ''),
                'internal_id': opp.get('noticeId', ''),
                'recipient_name': awardee_name,
                'award_amount': float(amount),
                'description': (opp.get('title', '') or opp.get('description', ''))[:200],
                'start_date': opp.get('responseDeadLine', '') or opp.get('postedDate', ''),
                'date_signed': opp.get('awardDate', '') or opp.get('postedDate', ''),
                'agency': opp.get('department', '') or opp.get('fullParentPathName', ''),
                'award_type': opp.get('type', ''),
            }
        except Exception as e:
            self.logger.debug(f"SAM parse error: {e}")
            return None
    
    def _parse_search_result(self, result: Dict) -> Optional[Dict]:
        """Parse SAM.gov search result."""
        try:
            awardee_name = result.get('awardee', {}).get('name', '')
            if not awardee_name:
                return None
            
            return {
                'source': 'sam.gov',
                'award_id': result.get('_id', ''),
                'internal_id': result.get('_id', ''),
                'recipient_name': awardee_name,
                'award_amount': float(result.get('award', {}).get('amount', 0) or 0),
                'description': result.get('title', '')[:200],
                'start_date': result.get('postedDate', ''),
                'date_signed': result.get('modifiedDate', ''),
                'agency': result.get('organizationHierarchy', [{}])[0].get('name', '') if result.get('organizationHierarchy') else '',
                'award_type': result.get('type', {}).get('value', ''),
            }
        except:
            return None


# =============================================================================
# PUBLIC COMPANY DATABASE
# =============================================================================

class PublicCompanyDatabase:
    """Database of all publicly traded companies."""
    
    # Major government contractors with aliases for better matching
    KNOWN_CONTRACTORS = {
        'LMT': {'name': 'Lockheed Martin Corporation', 'aliases': ['LOCKHEED MARTIN', 'LOCKHEED-MARTIN', 'LOCKHEED MARTIN CORP']},
        'RTX': {'name': 'RTX Corporation', 'aliases': ['RAYTHEON', 'RAYTHEON TECHNOLOGIES', 'RTX CORPORATION', 'RAYTHEON COMPANY']},
        'BA': {'name': 'The Boeing Company', 'aliases': ['BOEING', 'BOEING COMPANY', 'THE BOEING COMPANY']},
        'GD': {'name': 'General Dynamics Corporation', 'aliases': ['GENERAL DYNAMICS', 'GENERAL DYNAMICS CORP', 'GENERAL DYNAMICS INFORMATION TECHNOLOGY']},
        'NOC': {'name': 'Northrop Grumman Corporation', 'aliases': ['NORTHROP GRUMMAN', 'NORTHROP', 'NORTHROP GRUMMAN CORP']},
        'LHX': {'name': 'L3Harris Technologies', 'aliases': ['L3HARRIS', 'L3 HARRIS', 'HARRIS CORPORATION', 'L3 TECHNOLOGIES']},
        'HII': {'name': 'Huntington Ingalls Industries', 'aliases': ['HUNTINGTON INGALLS', 'HII']},
        'LDOS': {'name': 'Leidos Holdings', 'aliases': ['LEIDOS', 'LEIDOS HOLDINGS', 'LEIDOS INC']},
        'BAH': {'name': 'Booz Allen Hamilton', 'aliases': ['BOOZ ALLEN', 'BOOZ ALLEN HAMILTON', 'BOOZ ALLEN HAMILTON INC']},
        'SAIC': {'name': 'Science Applications International', 'aliases': ['SAIC', 'SCIENCE APPLICATIONS INTERNATIONAL CORPORATION']},
        'CACI': {'name': 'CACI International', 'aliases': ['CACI', 'CACI INTERNATIONAL', 'CACI INC']},
        'PLTR': {'name': 'Palantir Technologies', 'aliases': ['PALANTIR', 'PALANTIR TECHNOLOGIES', 'PALANTIR USG']},
        'MSFT': {'name': 'Microsoft Corporation', 'aliases': ['MICROSOFT', 'MICROSOFT CORP', 'MICROSOFT CORPORATION']},
        'AMZN': {'name': 'Amazon.com Inc', 'aliases': ['AMAZON', 'AMAZON WEB SERVICES', 'AWS', 'AMAZON.COM']},
        'GOOGL': {'name': 'Alphabet Inc', 'aliases': ['GOOGLE', 'ALPHABET', 'GOOGLE LLC', 'GOOGLE INC']},
        'ORCL': {'name': 'Oracle Corporation', 'aliases': ['ORACLE', 'ORACLE CORP', 'ORACLE AMERICA', 'ORACLE HEALTH']},
        'IBM': {'name': 'International Business Machines', 'aliases': ['IBM', 'IBM CORP', 'IBM CORPORATION']},
        'ACN': {'name': 'Accenture', 'aliases': ['ACCENTURE FEDERAL', 'ACCENTURE', 'ACCENTURE FEDERAL SERVICES']},
        'DELL': {'name': 'Dell Technologies', 'aliases': ['DELL', 'DELL TECHNOLOGIES', 'DELL INC', 'DELL FEDERAL']},
        'HPE': {'name': 'Hewlett Packard Enterprise', 'aliases': ['HPE', 'HEWLETT PACKARD ENTERPRISE']},
        'HPQ': {'name': 'HP Inc', 'aliases': ['HP', 'HP INC', 'HEWLETT PACKARD', 'HEWLETT-PACKARD']},
        'CSCO': {'name': 'Cisco Systems', 'aliases': ['CISCO', 'CISCO SYSTEMS']},
        'PANW': {'name': 'Palo Alto Networks', 'aliases': ['PALO ALTO NETWORKS', 'PALO ALTO']},
        'CRWD': {'name': 'CrowdStrike', 'aliases': ['CROWDSTRIKE', 'CROWDSTRIKE INC']},
        'FTNT': {'name': 'Fortinet', 'aliases': ['FORTINET', 'FORTINET INC']},
        'NOW': {'name': 'ServiceNow', 'aliases': ['SERVICENOW', 'SERVICE NOW']},
        'CRM': {'name': 'Salesforce', 'aliases': ['SALESFORCE', 'SALESFORCE.COM']},
        'BWXT': {'name': 'BWX Technologies', 'aliases': ['BWX', 'BWXT', 'BWX TECHNOLOGIES']},
        'KTOS': {'name': 'Kratos Defense', 'aliases': ['KRATOS', 'KRATOS DEFENSE']},
        'PSN': {'name': 'Parsons Corporation', 'aliases': ['PARSONS', 'PARSONS CORP']},
        'KBR': {'name': 'KBR Inc', 'aliases': ['KBR', 'KBR INC', 'KBR WYLE']},
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
        'SSYS': {'name': 'Stratasys Ltd', 'aliases': ['STRATASYS', 'STRATASYS LTD', 'STRATASYS INC', 'STRATASYS DIRECT']},
        'DDD': {'name': '3D Systems Corporation', 'aliases': ['3D SYSTEMS', '3D SYSTEMS CORP']},
        'SPCE': {'name': 'Virgin Galactic', 'aliases': ['VIRGIN GALACTIC']},
        'ASTR': {'name': 'Astra Space', 'aliases': ['ASTRA', 'ASTRA SPACE']},
        'RDW': {'name': 'Redwire Corporation', 'aliases': ['REDWIRE', 'REDWIRE CORP']},
        'ASTS': {'name': 'AST SpaceMobile', 'aliases': ['AST SPACEMOBILE', 'AST SPACE']},
        'MNTS': {'name': 'Momentus', 'aliases': ['MOMENTUS', 'MOMENTUS INC']},
        'PL': {'name': 'Planet Labs', 'aliases': ['PLANET LABS', 'PLANET']},
        'BKSY': {'name': 'BlackSky Technology', 'aliases': ['BLACKSKY', 'BLACKSKY TECHNOLOGY']},
    }
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.companies = {}
        self.name_to_ticker = {}
        self.all_names = []
    
    def load(self, force_refresh: bool = False) -> bool:
        """Load all publicly traded companies from SEC."""
        
        # Try cache first
        if not force_refresh and self.config.companies_cache.exists():
            try:
                cache_age = time.time() - self.config.companies_cache.stat().st_mtime
                if cache_age < 86400 * 7:  # 7 days
                    with open(self.config.companies_cache, 'r') as f:
                        cached = json.load(f)
                        self.companies = cached.get('companies', {})
                        if len(self.companies) > 1000:
                            self._build_lookup_tables()
                            self.logger.info(f"Loaded {len(self.companies)} companies from cache")
                            return True
            except Exception as e:
                self.logger.debug(f"Cache load error: {e}")
        
        # Load from SEC
        if self._load_from_sec():
            return True
        
        # Fallback to known contractors
        self._load_fallback()
        return True
    
    def _load_from_sec(self) -> bool:
        """Load all companies from SEC EDGAR."""
        try:
            self.logger.info("Loading companies from SEC EDGAR...")
            
            url = "https://www.sec.gov/files/company_tickers.json"
            headers = {'User-Agent': 'ContractMonitor/2.0 (contact@example.com)'}
            
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                self.logger.warning(f"SEC API returned {response.status_code}")
                return False
            
            data = response.json()
            self.companies = {}
            
            for key, company in data.items():
                ticker = company.get('ticker', '').upper().strip()
                name = company.get('title', '')
                
                if ticker and name and len(ticker) <= 5:
                    self.companies[ticker] = {
                        'name': name,
                        'ticker': ticker,
                        'aliases': []
                    }
            
            # Add aliases from known contractors
            for ticker, info in self.KNOWN_CONTRACTORS.items():
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
            self.logger.warning(f"SEC load failed: {e}")
            return False
    
    def _load_fallback(self):
        """Load fallback list."""
        self.companies = {}
        for ticker, info in self.KNOWN_CONTRACTORS.items():
            self.companies[ticker] = {
                'name': info['name'],
                'ticker': ticker,
                'aliases': info.get('aliases', [])
            }
        self._build_lookup_tables()
        self._save_cache()
        self.logger.info(f"Loaded {len(self.companies)} companies (fallback)")
    
    def _build_lookup_tables(self):
        """Build name-to-ticker lookup tables."""
        self.name_to_ticker = {}
        self.all_names = []
        
        for ticker, info in self.companies.items():
            normalized = self._normalize_name(info['name'])
            self.name_to_ticker[normalized] = ticker
            self.all_names.append(normalized)
            
            for alias in info.get('aliases', []):
                norm_alias = self._normalize_name(alias)
                self.name_to_ticker[norm_alias] = ticker
                if norm_alias not in self.all_names:
                    self.all_names.append(norm_alias)
    
    def _save_cache(self):
        try:
            with open(self.config.companies_cache, 'w') as f:
                json.dump({'companies': self.companies, 'updated': time.time()}, f)
        except:
            pass
    
    def _normalize_name(self, name: str) -> str:
        if not name:
            return ""
        name = name.upper().strip()
        
        # Remove common suffixes
        suffixes = [' INC', ' INC.', ' INCORPORATED', ' CORP', ' CORP.', ' CORPORATION',
                   ' LLC', ' L.L.C.', ' LTD', ' LTD.', ' LIMITED', ' CO', ' CO.',
                   ' COMPANY', ' HOLDINGS', ' GROUP', ' LP', ' L.P.', ' PLC',
                   ' TECHNOLOGIES', ' TECHNOLOGY', ' INTERNATIONAL', ' SERVICES',
                   ' SOLUTIONS', ' SYSTEMS', ' ENTERPRISES']
        
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)].strip()
        
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name
    
    def find_match(self, recipient_name: str) -> Optional[Dict]:
        """Find matching public company."""
        if not recipient_name:
            return None
        
        normalized = self._normalize_name(recipient_name)
        
        # Skip very short names
        if len(normalized) < 4:
            return None
        
        # Exact match
        if normalized in self.name_to_ticker:
            ticker = self.name_to_ticker[normalized]
            return {
                'ticker': ticker,
                'matched_name': self.companies[ticker]['name'],
                'match_score': 100,
                'match_type': 'exact'
            }
        
        # Fuzzy match with strict validation
        if not self.all_names:
            return None
        
        best_score = 0
        best_match = None
        
        # Use standard ratio (stricter)
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
            
            # Validation: check first words are similar
            input_words = normalized.split()
            match_words = best_match.split()
            
            if input_words and match_words:
                first_word_score = fuzz.ratio(input_words[0], match_words[0])
                first_word_contained = (input_words[0] in match_words[0]) or (match_words[0] in input_words[0])
                
                if first_word_score < 70 and not first_word_contained:
                    return None
            
            # Check length similarity
            len_ratio = min(len(normalized), len(best_match)) / max(len(normalized), len(best_match))
            if len_ratio < 0.5:
                return None
            
            return {
                'ticker': ticker,
                'matched_name': self.companies[ticker]['name'],
                'match_score': best_score,
                'match_type': 'fuzzy'
            }
        
        return None


# =============================================================================
# MARKET CAP SERVICE
# =============================================================================

class MarketCapService:
    """Service to fetch and cache market caps."""
    
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
        """Get market cap for ticker."""
        ticker = ticker.upper()
        
        # Check cache (valid for 24 hours)
        if ticker in self.cache:
            cached = self.cache[ticker]
            if time.time() - cached.get('updated', 0) < 86400 and cached.get('market_cap'):
                return cached['market_cap']
        
        # Fetch from Yahoo Finance
        market_cap = self._fetch_yahoo(ticker)
        
        if market_cap:
            self.cache[ticker] = {'market_cap': market_cap, 'updated': time.time()}
            self._save_cache()
        
        return market_cap
    
    def _fetch_yahoo(self, ticker: str) -> Optional[float]:
        """Fetch market cap from Yahoo Finance."""
        try:
            url = "https://query1.finance.yahoo.com/v7/finance/quote"
            params = {'symbols': ticker}
            headers = {'User-Agent': 'Mozilla/5.0'}
            
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                result = data.get('quoteResponse', {}).get('result', [])
                if result:
                    mcap = result[0].get('marketCap')
                    if mcap:
                        return float(mcap)
        except:
            pass
        
        # Try yfinance as backup
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            mcap = info.get('marketCap')
            if mcap:
                return float(mcap)
        except:
            pass
        
        return None
    
    def calculate_materiality(self, contract_value: float, market_cap: Optional[float]) -> Dict:
        """Calculate materiality of contract relative to market cap."""
        if not market_cap:
            return {
                'market_cap': None,
                'market_cap_formatted': 'Unknown',
                'percent_of_market_cap': None,
                'materiality_rating': 'UNKNOWN',
                'materiality_score': 0
            }
        
        pct = (contract_value / market_cap) * 100
        
        if market_cap >= 1e12:
            mcap_str = f"${market_cap/1e12:.2f}T"
        elif market_cap >= 1e9:
            mcap_str = f"${market_cap/1e9:.2f}B"
        else:
            mcap_str = f"${market_cap/1e6:.2f}M"
        
        t = self.config.materiality_thresholds
        if pct >= t['very_high']:
            rating, score = "VERY HIGH 🔥🔥🔥", 4
        elif pct >= t['high']:
            rating, score = "HIGH 🔥🔥", 3
        elif pct >= t['medium']:
            rating, score = "MEDIUM 🔥", 2
        elif pct >= t['low']:
            rating, score = "LOW", 1
        else:
            rating, score = "MINIMAL", 0
        
        return {
            'market_cap': market_cap,
            'market_cap_formatted': mcap_str,
            'percent_of_market_cap': pct,
            'materiality_rating': rating,
            'materiality_score': score
        }


# =============================================================================
# ALERT SYSTEM
# =============================================================================

class AlertSystem:
    """System for sending alerts."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
    
    def alert(self, award: Dict, match: Dict, materiality: Dict):
        """Send alert for matching award."""
        ticker = match['ticker']
        company = match['matched_name']
        amount = award.get('award_amount', 0)
        
        if amount >= 1e9:
            amount_str = f"${amount/1e9:.2f}B"
        elif amount >= 1e6:
            amount_str = f"${amount/1e6:.2f}M"
        elif amount >= 1e3:
            amount_str = f"${amount/1e3:.2f}K"
        else:
            amount_str = f"${amount:,.0f}"
        
        if self.config.enable_console_alerts:
            self._console_alert(ticker, company, amount_str, award, match, materiality)
        
        if self.config.enable_discord_alerts:
            self._discord_alert(ticker, company, amount_str, award, match, materiality)
    
    def _console_alert(self, ticker, company, amount, award, match, materiality):
        print("\n" + "=" * 80)
        print(f"🚨 PUBLIC COMPANY CONTRACT AWARD - ${ticker} 🚨")
        print("=" * 80)
        print(f"  SOURCE:       {award.get('source', 'unknown').upper()}")
        print(f"  TICKER:       ${ticker}")
        print(f"  COMPANY:      {company}")
        print(f"  RECIPIENT:    {award.get('recipient_name', 'N/A')}")
        print(f"  AMOUNT:       {amount}")
        if materiality.get('percent_of_market_cap'):
            print(f"  MARKET CAP:   {materiality.get('market_cap_formatted', 'Unknown')}")
            print(f"  % OF MCAP:    {materiality['percent_of_market_cap']:.4f}%")
            print(f"  MATERIALITY:  {materiality.get('materiality_rating', 'Unknown')}")
        print(f"  AGENCY:       {award.get('agency', 'N/A')}")
        print(f"  DESCRIPTION:  {award.get('description', 'N/A')[:60]}")
        print(f"  DATE SIGNED:  {award.get('date_signed', 'N/A')}")
        
        # Show link based on source
        if award.get('source') == 'usaspending' and award.get('internal_id'):
            print(f"  LINK:         https://www.usaspending.gov/award/{award['internal_id']}")
        elif award.get('source') == 'sam.gov' and award.get('award_id'):
            print(f"  LINK:         https://sam.gov/opp/{award['award_id']}/view")
        
        print(f"  MATCH:        {match['match_score']}% ({match['match_type']})")
        print("=" * 80 + "\n")
    
    def _discord_alert(self, ticker, company, amount, award, match, materiality):
        try:
            # Color based on materiality
            score = materiality.get('materiality_score', 0)
            colors = [0x808080, 0x00cc00, 0xffcc00, 0xff6600, 0xff0000]
            color = colors[min(score, 4)]
            
            # Build materiality text
            mat_text = ""
            if materiality.get('percent_of_market_cap'):
                mat_text = f"**Market Cap:** {materiality.get('market_cap_formatted', 'Unknown')}\n"
                mat_text += f"**% of Market Cap:** {materiality['percent_of_market_cap']:.4f}%\n"
                mat_text += f"**Materiality:** {materiality.get('materiality_rating', 'Unknown')}"
            
            # Get link
            if award.get('source') == 'usaspending' and award.get('internal_id'):
                link = f"https://www.usaspending.gov/award/{award['internal_id']}"
            elif award.get('source') == 'sam.gov' and award.get('award_id'):
                link = f"https://sam.gov/opp/{award['award_id']}/view"
            else:
                link = None
            
            embed = {
                "title": f"🚨 ${ticker} Government Contract Award",
                "color": color,
                "fields": [
                    {"name": "Company", "value": company, "inline": True},
                    {"name": "Amount", "value": amount, "inline": True},
                    {"name": "Source", "value": award.get('source', 'unknown').upper(), "inline": True},
                    {"name": "Agency", "value": str(award.get('agency', 'N/A'))[:100], "inline": False},
                    {"name": "Recipient Name", "value": award.get('recipient_name', 'N/A')[:100], "inline": False},
                    {"name": "Description", "value": award.get('description', 'N/A')[:200], "inline": False},
                ],
                "footer": {"text": f"Date Signed: {award.get('date_signed', 'N/A')} | Match: {match['match_score']}%"},
                "timestamp": datetime.utcnow().isoformat()
            }
            
            if mat_text:
                embed["fields"].insert(3, {"name": "Financial Impact", "value": mat_text, "inline": False})
            
            if link:
                embed["url"] = link
                embed["fields"].append({"name": "🔗 View Details", "value": f"[Open]({link})", "inline": False})
            
            for webhook in self.config.discord_webhook_urls:
                try:
                    requests.post(webhook, json={"embeds": [embed]}, timeout=10)
                except:
                    pass
                    
        except Exception as e:
            self.logger.error(f"Discord alert failed: {e}")


# =============================================================================
# AWARD TRACKER (Deduplication)
# =============================================================================

class AwardTracker:
    """Track seen awards to avoid duplicates across sources."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.seen_ids = set()
        self.seen_hashes = set()
        self._load()
    
    def _load(self):
        if self.config.seen_awards_file.exists():
            try:
                with open(self.config.seen_awards_file, 'r') as f:
                    data = json.load(f)
                    self.seen_ids = set(data.get('seen_ids', []))
                    self.seen_hashes = set(data.get('seen_hashes', []))
            except:
                pass
    
    def _save(self):
        try:
            # Keep last 10000 entries
            with open(self.config.seen_awards_file, 'w') as f:
                json.dump({
                    'seen_ids': list(self.seen_ids)[-10000:],
                    'seen_hashes': list(self.seen_hashes)[-10000:]
                }, f)
        except:
            pass
    
    def _get_hash(self, award: Dict) -> str:
        """Generate hash for award deduplication."""
        # Hash based on recipient + amount + agency to catch duplicates across sources
        key = f"{award.get('recipient_name', '')}_{award.get('award_amount', '')}_{award.get('agency', '')}"
        return hashlib.md5(key.lower().encode()).hexdigest()[:16]
    
    def is_seen(self, award: Dict) -> bool:
        """Check if award has been seen before."""
        award_id = award.get('award_id', '')
        award_hash = self._get_hash(award)
        
        return award_id in self.seen_ids or award_hash in self.seen_hashes
    
    def mark_seen(self, award: Dict):
        """Mark award as seen."""
        award_id = award.get('award_id', '')
        award_hash = self._get_hash(award)
        
        if award_id:
            self.seen_ids.add(award_id)
        self.seen_hashes.add(award_hash)
        self._save()


# =============================================================================
# MAIN MONITOR
# =============================================================================

class CombinedContractMonitor:
    """Combined monitor for SAM.gov and USAspending.gov."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging(config)
        
        self.usaspending = USASpendingClient(config, self.logger)
        self.samgov = SAMGovClient(config, self.logger)
        self.company_db = PublicCompanyDatabase(config, self.logger)
        self.market_cap = MarketCapService(config, self.logger)
        self.alerts = AlertSystem(config, self.logger)
        self.tracker = AwardTracker(config, self.logger)
    
    def initialize(self) -> bool:
        """Initialize the monitor."""
        self.logger.info("=" * 60)
        self.logger.info("Combined Government Contract Monitor")
        self.logger.info("=" * 60)
        self.logger.info(f"  Min contract value: ${self.config.min_contract_value:,.0f}")
        self.logger.info(f"  Min materiality: {self.config.min_materiality_percent}% of market cap")
        self.logger.info(f"  Lookback: {self.config.lookback_days} days")
        self.logger.info(f"  Sources: SAM.gov + USAspending.gov")
        
        return self.company_db.load()
    
    def check_once(self) -> List[Tuple[Dict, Dict, Dict]]:
        """Check both sources for new awards."""
        self.logger.info("Checking for new contract awards...")
        
        all_awards = []
        
        # Fetch from both sources
        try:
            usaspending_awards = self.usaspending.get_recent_awards(self.config.lookback_days)
            all_awards.extend(usaspending_awards)
            self.logger.info(f"  USAspending: {len(usaspending_awards)} awards")
        except Exception as e:
            self.logger.error(f"  USAspending error: {e}")
        
        try:
            sam_awards = self.samgov.get_recent_awards(self.config.lookback_days)
            all_awards.extend(sam_awards)
            self.logger.info(f"  SAM.gov: {len(sam_awards)} awards")
        except Exception as e:
            self.logger.error(f"  SAM.gov error: {e}")
        
        # Process awards
        new_matches = []
        stats = {
            'total': len(all_awards),
            'seen': 0,
            'old': 0,
            'low_value': 0,
            'no_match': 0,
            'low_materiality': 0,
            'alerts': 0
        }
        
        cutoff_date = datetime.now() - timedelta(days=self.config.lookback_days)
        
        for award in all_awards:
            if not award.get('recipient_name'):
                continue
            
            # Skip if seen
            if self.tracker.is_seen(award):
                stats['seen'] += 1
                continue
            
            self.tracker.mark_seen(award)
            
            # Check date
            date_str = award.get('date_signed') or award.get('start_date')
            if date_str:
                try:
                    signed_dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
                    if signed_dt < cutoff_date:
                        stats['old'] += 1
                        continue
                except:
                    pass
            
            # Check value
            amount = award.get('award_amount', 0)
            if amount < self.config.min_contract_value:
                stats['low_value'] += 1
                continue
            
            # Find company match
            match = self.company_db.find_match(award['recipient_name'])
            if not match:
                stats['no_match'] += 1
                continue
            
            # Calculate materiality
            market_cap = self.market_cap.get_market_cap(match['ticker'])
            materiality = self.market_cap.calculate_materiality(amount, market_cap)
            
            # Check materiality threshold
            pct = materiality.get('percent_of_market_cap')
            if pct is not None and pct < self.config.min_materiality_percent:
                stats['low_materiality'] += 1
                self.logger.debug(f"  Filtered {match['ticker']}: {pct:.4f}% < {self.config.min_materiality_percent}%")
                continue
            
            # Alert!
            stats['alerts'] += 1
            self.logger.info(f"  MATCH: {award['recipient_name']} -> ${match['ticker']} ({match['match_score']}%) - ${amount/1e6:.1f}M - {pct:.4f}% of mcap")
            
            new_matches.append((award, match, materiality))
            self.alerts.alert(award, match, materiality)
        
        # Log summary
        self.logger.info(f"Check complete: {stats['alerts']} alerts sent")
        self.logger.debug(f"  Stats: {stats}")
        
        return new_matches
    
    def is_market_hours(self) -> Tuple[bool, float, float]:
        """Check if market is open."""
        try:
            now = datetime.now(ZoneInfo(self.config.timezone))
        except:
            now = datetime.utcnow().replace(tzinfo=None) - timedelta(hours=5)
        
        if now.weekday() >= 5:
            return False, 0, 0
        
        market_open = now.replace(
            hour=self.config.market_open_hour,
            minute=self.config.market_open_minute,
            second=0, microsecond=0
        )
        market_close = now.replace(
            hour=self.config.market_close_hour,
            minute=self.config.market_close_minute,
            second=0, microsecond=0
        )
        
        if now < market_open:
            return False, (market_open - now).total_seconds(), 0
        elif now >= market_close:
            return False, 0, 0
        
        return True, 0, (market_close - now).total_seconds()
    
    def run_continuous(self):
        """Run continuously."""
        print("\n" + "=" * 60)
        print("  Combined Government Contract Monitor - Running")
        print(f"  Interval: {self.config.check_interval_seconds}s")
        print(f"  Lookback: {self.config.lookback_days} days")
        print(f"  Min Contract: ${self.config.min_contract_value:,.0f}")
        print(f"  Min Materiality: {self.config.min_materiality_percent}% of market cap")
        print(f"  Sources: SAM.gov + USAspending.gov")
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
                        if until_open > 0:
                            mins = int(until_open / 60)
                            self.logger.info(f"Market closed. Opens in {mins} minutes.")
                            time.sleep(min(300, until_open))
                        else:
                            self.logger.info("Market closed for the day.")
                            time.sleep(60)
                        continue
                
                try:
                    self.check_once()
                except Exception as e:
                    self.logger.error(f"Check error: {e}")
                
                time.sleep(self.config.check_interval_seconds)
                
        except KeyboardInterrupt:
            print("\nStopped.")
    
    def self_test(self) -> bool:
        """Run internal validation tests."""
        print("\n" + "=" * 60)
        print("  SELF-TEST: Combined Contract Monitor")
        print("=" * 60)
        
        all_passed = True
        
        # Test 1: Company database
        print("\n[Test 1] Company Database")
        if len(self.company_db.companies) > 1000:
            print(f"  ✓ Loaded {len(self.company_db.companies)} companies from SEC")
        else:
            print(f"  ✗ Only {len(self.company_db.companies)} companies loaded (expected >1000)")
            all_passed = False
        
        # Test 2: Known company matching
        print("\n[Test 2] Company Matching")
        test_cases = [
            ("LOCKHEED MARTIN CORPORATION", "LMT"),
            ("BOEING COMPANY", "BA"),
            ("RAYTHEON COMPANY", "RTX"),
            ("STRATASYS LTD", "SSYS"),
            ("PALANTIR TECHNOLOGIES INC", "PLTR"),
            ("BOOZ ALLEN HAMILTON INC", "BAH"),
        ]
        
        for name, expected_ticker in test_cases:
            match = self.company_db.find_match(name)
            if match and match['ticker'] == expected_ticker:
                print(f"  ✓ '{name}' -> ${expected_ticker}")
            else:
                got = match['ticker'] if match else 'None'
                print(f"  ✗ '{name}' -> ${got} (expected ${expected_ticker})")
                all_passed = False
        
        # Test 3: False positive prevention
        print("\n[Test 3] False Positive Prevention")
        false_positives = [
            "WJM PROFESSIONAL SERVICES LLC",
            "ACME CONSULTING GROUP",
            "RANDOM TECH SOLUTIONS",
        ]
        
        for name in false_positives:
            match = self.company_db.find_match(name)
            if match is None:
                print(f"  ✓ '{name}' -> No match (correct)")
            else:
                print(f"  ✗ '{name}' -> ${match['ticker']} (should be no match)")
                all_passed = False
        
        # Test 4: USAspending API
        print("\n[Test 4] USAspending API")
        try:
            awards = self.usaspending.get_recent_awards(1)
            if len(awards) > 0:
                print(f"  ✓ Fetched {len(awards)} awards from USAspending")
            else:
                print("  ⚠ No awards returned (might be normal on weekends)")
        except Exception as e:
            print(f"  ✗ API error: {e}")
            all_passed = False
        
        # Test 5: SAM.gov API
        print("\n[Test 5] SAM.gov API")
        try:
            awards = self.samgov.get_recent_awards(1)
            print(f"  ✓ SAM.gov returned {len(awards)} awards")
        except Exception as e:
            print(f"  ⚠ SAM.gov error (might be rate limited): {e}")
        
        # Test 6: Market cap service
        print("\n[Test 6] Market Cap Service")
        test_tickers = ["LMT", "BA", "PLTR"]
        for ticker in test_tickers:
            mcap = self.market_cap.get_market_cap(ticker)
            if mcap and mcap > 0:
                print(f"  ✓ ${ticker} market cap: ${mcap/1e9:.1f}B")
            else:
                print(f"  ✗ Failed to get market cap for ${ticker}")
                all_passed = False
        
        # Test 7: Materiality calculation
        print("\n[Test 7] Materiality Calculation")
        mat = self.market_cap.calculate_materiality(100_000_000, 10_000_000_000)  # $100M on $10B
        if mat['percent_of_market_cap'] == 1.0:
            print(f"  ✓ $100M / $10B = {mat['percent_of_market_cap']}% ({mat['materiality_rating']})")
        else:
            print(f"  ✗ Calculation error: got {mat['percent_of_market_cap']}%")
            all_passed = False
        
        # Summary
        print("\n" + "=" * 60)
        if all_passed:
            print("  ✓ ALL TESTS PASSED")
        else:
            print("  ✗ SOME TESTS FAILED")
        print("=" * 60 + "\n")
        
        return all_passed


# =============================================================================
# MAIN
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="Combined Government Contract Monitor")
    parser.add_argument('-i', '--interval', type=int, default=60, help="Check interval in seconds")
    parser.add_argument('-l', '--lookback', type=int, default=7, help="Days to look back")
    parser.add_argument('-m', '--min-value', type=float, default=500000, help="Min contract value")
    parser.add_argument('--min-materiality', type=float, default=0.5, help="Min materiality %% of market cap")
    parser.add_argument('--all-hours', action='store_true', help="Run 24/7 instead of market hours only")
    parser.add_argument('--once', action='store_true', help="Run once and exit")
    parser.add_argument('--test', action='store_true', help="Run self-test")
    parser.add_argument('--no-discord', action='store_true', help="Disable Discord alerts")
    args = parser.parse_args()
    
    config = Config()
    config.check_interval_seconds = args.interval
    config.lookback_days = args.lookback
    config.min_contract_value = args.min_value
    config.min_materiality_percent = args.min_materiality
    config.market_hours_only = not args.all_hours
    config.enable_discord_alerts = not args.no_discord
    
    monitor = CombinedContractMonitor(config)
    
    if not monitor.initialize():
        print("Failed to initialize")
        sys.exit(1)
    
    if args.test:
        success = monitor.self_test()
        sys.exit(0 if success else 1)
    elif args.once:
        monitor.check_once()
    else:
        monitor.run_continuous()


if __name__ == "__main__":
    main()
