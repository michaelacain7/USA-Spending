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
import gc
import sys
import json
import time
import logging
import hashlib
import re
from datetime import datetime, timedelta, timezone
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
    sam_api_key: str = "SAM-0dabddd8-7907-4fe2-ba00-238f62fbe331"
    
    # Monitoring Settings
    # 1000 requests/day limit - using 10 min interval to be very safe
    # 7 hours (9 AM - 4 PM) * 6 req/hr = 42 requests/day
    check_interval_seconds: int = 600  # 10 minutes
    lookback_days: int = 7  # Fetch 7 days of awards (handles weekends)
    
    # Operating Hours Settings (9:00 AM - 4:00 PM ET on weekdays)
    market_hours_only: bool = True  # Only run during operating hours
    market_open_hour: int = 9
    market_open_minute: int = 0
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
    
    # Debug mode
    debug_mode: bool = False  # Set to True for verbose logging
    
    def __post_init__(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)


def setup_logging(config: Config) -> logging.Logger:
    logger = logging.getLogger("ContractMonitor")
    logger.setLevel(logging.DEBUG)
    logger.handlers = []  # Clear existing handlers
    
    # Console handler - show DEBUG when debug_mode is True
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if config.debug_mode else logging.INFO)
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
    """Client for SAM.gov API - fetches contract award notices."""
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'ContractMonitor/2.0 (Government Contract Monitoring)',
            'Accept': 'application/json'
        })
    
    def get_recent_awards(self, days_back: int = 7) -> List[Dict]:
        """Fetch recent award notices from SAM.gov using multiple methods."""
        all_awards = []
        errors = []
        
        # Method 1: Public Opportunities API (award notices) - primary method
        try:
            awards1 = self._fetch_award_notices(days_back)
            all_awards.extend(awards1)
            if awards1:
                self.logger.debug(f"  SAM.gov Opportunities API: {len(awards1)} awards")
        except Exception as e:
            errors.append(f"Opportunities API: {e}")
            self.logger.warning(f"  SAM.gov Opportunities API error: {e}")
        
        # Only try backup methods if primary failed
        if not all_awards:
            # Method 2: Alternative search endpoint
            try:
                awards2 = self._fetch_from_search(days_back)
                all_awards.extend(awards2)
                if awards2:
                    self.logger.debug(f"  SAM.gov Search API: {len(awards2)} awards")
            except Exception as e:
                errors.append(f"Search API: {e}")
                self.logger.debug(f"  SAM.gov Search API error: {e}")
            
            # Method 3: Direct opportunities endpoint
            try:
                awards3 = self._fetch_opportunities_direct(days_back)
                all_awards.extend(awards3)
                if awards3:
                    self.logger.debug(f"  SAM.gov Direct API: {len(awards3)} awards")
            except Exception as e:
                errors.append(f"Direct API: {e}")
                self.logger.debug(f"  SAM.gov Direct API error: {e}")
        
        if errors and not all_awards:
            self.logger.warning(f"  SAM.gov all methods failed: {errors}")
        
        self.logger.info(f"SAM.gov total: {len(all_awards)} award notices")
        return all_awards
    
    def _fetch_award_notices(self, days_back: int) -> List[Dict]:
        """Fetch award notices from SAM.gov Opportunities API."""
        awards = []
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # SAM.gov Opportunities API v2
            url = "https://api.sam.gov/opportunities/v2/search"
            
            params = {
                'postedFrom': start_date.strftime("%m/%d/%Y"),
                'postedTo': end_date.strftime("%m/%d/%Y"),
                'ptype': 'a',  # Award Notice type
                'limit': 1000,
                'offset': 0,
            }
            
            if self.config.sam_api_key:
                params['api_key'] = self.config.sam_api_key
            
            self.logger.debug(f"  SAM.gov API: postedFrom={params.get('postedFrom')}, postedTo={params.get('postedTo')}, ptype={params.get('ptype')}")
            response = self.session.get(url, params=params, timeout=60)
            
            if response.status_code != 200:
                self.logger.warning(f"  SAM.gov API returned {response.status_code}: {response.text[:200]}")
                return awards
            
            data = response.json()
            
            # Try different response structures
            opportunities = (
                data.get('opportunitiesData', []) or 
                data.get('opportunities', []) or
                data.get('_embedded', {}).get('opportunities', []) or
                data.get('results', []) or
                []
            )
            
            self.logger.debug(f"  SAM.gov API returned {len(opportunities)} opportunities")
            
            for opp in opportunities:
                award = self._parse_sam_opportunity(opp)
                if award and award.get('recipient_name'):
                    awards.append(award)
                
        except Exception as e:
            self.logger.warning(f"SAM.gov Opportunities API error: {e}")
        
        return awards
    
    def _fetch_from_search(self, days_back: int) -> List[Dict]:
        """Fetch from SAM.gov search endpoint."""
        awards = []
        
        try:
            # Alternative search endpoint
            url = "https://sam.gov/api/prod/sgs/v1/search/"
            
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
                    notice_type = str(result.get('type', {}).get('value', '') or result.get('noticeType', ''))
                    if 'award' in notice_type.lower():
                        award = self._parse_search_result(result)
                        if award and award.get('recipient_name'):
                            awards.append(award)
                            
        except Exception as e:
            self.logger.debug(f"SAM.gov search error: {e}")
        
        return awards
    
    def _fetch_opportunities_direct(self, days_back: int) -> List[Dict]:
        """Fetch directly from opportunities endpoint."""
        awards = []
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            # Try the public opportunities feed
            url = "https://sam.gov/api/prod/opps/v3/opportunities"
            
            params = {
                'postedFrom': start_date.strftime("%m/%d/%Y"),
                'postedTo': end_date.strftime("%m/%d/%Y"),
                'noticeType': 'Award Notice',
                'limit': 100,
            }
            
            response = self.session.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                opportunities = data if isinstance(data, list) else data.get('opportunities', [])
                
                for opp in opportunities:
                    award = self._parse_sam_opportunity(opp)
                    if award and award.get('recipient_name'):
                        awards.append(award)
                        
        except Exception as e:
            self.logger.debug(f"SAM.gov direct API error: {e}")
        
        return awards
    
    def _parse_sam_opportunity(self, opp: Dict) -> Optional[Dict]:
        """Parse SAM.gov opportunity/award notice."""
        try:
            # Try to get awardee info from various possible fields
            awardee_name = None
            
            # Check direct awardee field
            if opp.get('awardee'):
                awardee = opp['awardee']
                if isinstance(awardee, dict):
                    awardee_name = awardee.get('name', '') or awardee.get('awardee', '')
                elif isinstance(awardee, str):
                    awardee_name = awardee
            
            # Check awardeeList
            if not awardee_name and opp.get('awardeeList'):
                awardee_list = opp['awardeeList']
                if isinstance(awardee_list, list) and len(awardee_list) > 0:
                    first = awardee_list[0]
                    if isinstance(first, dict):
                        awardee_name = first.get('name', '')
                    else:
                        awardee_name = str(first)
            
            # Check award object
            if not awardee_name and opp.get('award'):
                award_obj = opp['award']
                if isinstance(award_obj, dict):
                    awardee_val = award_obj.get('awardee', '') or award_obj.get('contractor', '')
                    if isinstance(awardee_val, dict):
                        awardee_name = awardee_val.get('name', '')
                    else:
                        awardee_name = str(awardee_val) if awardee_val else ''
            
            # Check direct contractor field
            if not awardee_name:
                contractor = opp.get('contractor', '') or opp.get('contractorName', '') or opp.get('vendorName', '')
                if isinstance(contractor, dict):
                    awardee_name = contractor.get('name', '')
                else:
                    awardee_name = str(contractor) if contractor else ''
            
            # Final validation - make sure we have a proper string name
            if not awardee_name or not isinstance(awardee_name, str):
                return None
            
            awardee_name = awardee_name.strip()
            if not awardee_name or awardee_name.startswith('{'):
                # Still a dict representation, skip it
                return None
            
            # Get award amount from various possible fields
            amount = 0
            award_obj = opp.get('award', {}) or {}
            if isinstance(award_obj, dict):
                amount = award_obj.get('amount', 0) or award_obj.get('value', 0)
            
            if not amount:
                amount = (
                    opp.get('baseAndAllOptionsValue', 0) or 
                    opp.get('totalValue', 0) or 
                    opp.get('awardAmount', 0) or
                    opp.get('contractValue', 0) or
                    0
                )
            
            # Get notice ID
            notice_id = (
                opp.get('noticeId', '') or 
                opp.get('solicitationNumber', '') or
                opp.get('opportunityId', '') or
                opp.get('id', '')
            )
            
            return {
                'source': 'sam.gov',
                'award_id': notice_id,
                'internal_id': notice_id,
                'recipient_name': awardee_name,
                'award_amount': float(amount) if amount else 0,
                'description': str(opp.get('title', '') or opp.get('description', '') or '')[:200],
                'posted_date': opp.get('postedDate', '') or opp.get('publishDate', ''),
                'start_date': opp.get('postedDate', '') or opp.get('publishDate', ''),
                'date_signed': opp.get('awardDate', '') or opp.get('postedDate', ''),
                'agency': opp.get('department', '') or opp.get('fullParentPathName', '') or opp.get('agency', ''),
                'award_type': opp.get('type', '') or opp.get('noticeType', 'Award Notice'),
            }
        except Exception as e:
            self.logger.debug(f"SAM parse error: {e}")
            return None
    
    def _parse_search_result(self, result: Dict) -> Optional[Dict]:
        """Parse SAM.gov search result."""
        try:
            awardee_name = ''
            
            if result.get('awardee'):
                awardee = result['awardee']
                if isinstance(awardee, dict):
                    awardee_name = awardee.get('name', '')
                else:
                    awardee_name = str(awardee)
            
            if not awardee_name:
                return None
            
            return {
                'source': 'sam.gov',
                'award_id': result.get('_id', ''),
                'internal_id': result.get('_id', ''),
                'recipient_name': awardee_name,
                'award_amount': float(result.get('award', {}).get('amount', 0) or 0),
                'description': str(result.get('title', ''))[:200],
                'start_date': result.get('postedDate', ''),
                'date_signed': result.get('modifiedDate', ''),
                'agency': result.get('organizationHierarchy', [{}])[0].get('name', '') if result.get('organizationHierarchy') else '',
                'award_type': result.get('type', {}).get('value', '') if isinstance(result.get('type'), dict) else str(result.get('type', '')),
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
        # Aviation / Air Charter / Travel
        'UP': {'name': 'Wheels Up Experience', 'aliases': ['WHEELS UP', 'WHEELS UP PARTNERS', 'WHEELS UP PARTNERS LLC', 'WHEELS UP EXPERIENCE']},
        'SKYW': {'name': 'SkyWest Inc', 'aliases': ['SKYWEST', 'SKYWEST INC', 'SKYWEST AIRLINES']},
        'AAL': {'name': 'American Airlines', 'aliases': ['AMERICAN AIRLINES', 'AMERICAN AIRLINES GROUP']},
        'DAL': {'name': 'Delta Air Lines', 'aliases': ['DELTA', 'DELTA AIR LINES', 'DELTA AIRLINES']},
        'UAL': {'name': 'United Airlines', 'aliases': ['UNITED', 'UNITED AIRLINES', 'UNITED AIR LINES']},
        'LUV': {'name': 'Southwest Airlines', 'aliases': ['SOUTHWEST', 'SOUTHWEST AIRLINES']},
        'JBLU': {'name': 'JetBlue Airways', 'aliases': ['JETBLUE', 'JETBLUE AIRWAYS']},
        'ALK': {'name': 'Alaska Air Group', 'aliases': ['ALASKA AIR', 'ALASKA AIRLINES', 'ALASKA AIR GROUP']},
        'HA': {'name': 'Hawaiian Holdings', 'aliases': ['HAWAIIAN', 'HAWAIIAN AIRLINES', 'HAWAIIAN HOLDINGS']},
        'SAVE': {'name': 'Spirit Airlines', 'aliases': ['SPIRIT', 'SPIRIT AIRLINES']},
        'ALGT': {'name': 'Allegiant Travel', 'aliases': ['ALLEGIANT', 'ALLEGIANT AIR', 'ALLEGIANT TRAVEL']},
        'MESA': {'name': 'Mesa Air Group', 'aliases': ['MESA AIR', 'MESA AIRLINES']},
        'ATSG': {'name': 'Air Transport Services', 'aliases': ['ATSG', 'AIR TRANSPORT SERVICES', 'AIR TRANSPORT SERVICES GROUP']},
        'AAWW': {'name': 'Atlas Air Worldwide', 'aliases': ['ATLAS AIR', 'ATLAS AIR WORLDWIDE']},
        'WULF': {'name': 'TeraWulf', 'aliases': ['TERAWULF']},
        # Logistics / Supply Chain
        'XPO': {'name': 'XPO Inc', 'aliases': ['XPO', 'XPO LOGISTICS', 'XPO INC']},
        'JBHT': {'name': 'J.B. Hunt Transport', 'aliases': ['JB HUNT', 'J.B. HUNT', 'JB HUNT TRANSPORT']},
        'CHRW': {'name': 'C.H. Robinson', 'aliases': ['CH ROBINSON', 'C.H. ROBINSON']},
        'EXPD': {'name': 'Expeditors International', 'aliases': ['EXPEDITORS', 'EXPEDITORS INTERNATIONAL']},
        'ODFL': {'name': 'Old Dominion Freight', 'aliases': ['OLD DOMINION', 'OLD DOMINION FREIGHT LINE']},
        'SAIA': {'name': 'Saia Inc', 'aliases': ['SAIA', 'SAIA INC']},
        'WERN': {'name': 'Werner Enterprises', 'aliases': ['WERNER', 'WERNER ENTERPRISES']},
        'KNX': {'name': 'Knight-Swift Transportation', 'aliases': ['KNIGHT SWIFT', 'KNIGHT-SWIFT', 'KNIGHT TRANSPORTATION']},
        'MATX': {'name': 'Matson Inc', 'aliases': ['MATSON', 'MATSON INC']},
        # More defense / govt services
        'MANT': {'name': 'ManTech International', 'aliases': ['MANTECH', 'MANTECH INTERNATIONAL']},
        'ICFI': {'name': 'ICF International', 'aliases': ['ICF', 'ICF INTERNATIONAL']},
        'TTEK': {'name': 'Tetra Tech', 'aliases': ['TETRA TECH', 'TETRATECH']},
        'MAXN': {'name': 'Maxeon Solar', 'aliases': ['MAXEON', 'MAXEON SOLAR']},
        'APPS': {'name': 'Digital Turbine', 'aliases': ['DIGITAL TURBINE']},
        'CIEN': {'name': 'Ciena Corporation', 'aliases': ['CIENA', 'CIENA CORP']},
        'VIAV': {'name': 'Viavi Solutions', 'aliases': ['VIAVI', 'VIAVI SOLUTIONS']},
        'CLVT': {'name': 'Clarivate', 'aliases': ['CLARIVATE']},
        'PRGS': {'name': 'Progress Software', 'aliases': ['PROGRESS', 'PROGRESS SOFTWARE']},
        'VRNT': {'name': 'Verint Systems', 'aliases': ['VERINT', 'VERINT SYSTEMS']},
        'NLOK': {'name': 'Gen Digital', 'aliases': ['GEN DIGITAL', 'NORTONLIFELOCK', 'NORTON']},
        'TENB': {'name': 'Tenable Holdings', 'aliases': ['TENABLE', 'TENABLE HOLDINGS']},
        'RPD': {'name': 'Rapid7', 'aliases': ['RAPID7', 'RAPID 7']},
        'CYBR': {'name': 'CyberArk Software', 'aliases': ['CYBERARK', 'CYBERARK SOFTWARE']},
        'SAIL': {'name': 'SailPoint Technologies', 'aliases': ['SAILPOINT', 'SAILPOINT TECHNOLOGIES']},
        'QLYS': {'name': 'Qualys', 'aliases': ['QUALYS', 'QUALYS INC']},
        'S': {'name': 'SentinelOne', 'aliases': ['SENTINELONE', 'SENTINEL ONE']},
        # Small-cap govt contractors
        'DLHC': {'name': 'DLH Holdings', 'aliases': ['DLH', 'DLH HOLDINGS', 'DLH LLC', 'DLH CORP']},
        'VSH': {'name': 'Vishay Intertechnology', 'aliases': ['VISHAY', 'VISHAY INTERTECHNOLOGY']},
        'ASGN': {'name': 'ASGN Incorporated', 'aliases': ['ASGN', 'APEX GROUP', 'APEX SYSTEMS']},
        'EXP': {'name': 'Eagle Materials', 'aliases': ['EAGLE MATERIALS']},
        'PRIM': {'name': 'Primoris Services', 'aliases': ['PRIMORIS', 'PRIMORIS SERVICES']},
        'VCTR': {'name': 'Victory Capital', 'aliases': ['VICTORY CAPITAL']},
        'NSIT': {'name': 'Insight Enterprises', 'aliases': ['INSIGHT', 'INSIGHT ENTERPRISES']},
        'OSIS': {'name': 'OSI Systems', 'aliases': ['OSI SYSTEMS', 'OSI']},
        'KELYA': {'name': 'Kelly Services', 'aliases': ['KELLY', 'KELLY SERVICES', 'KELLY SERVICES INC']},
        'MMS': {'name': 'Maximus Inc', 'aliases': ['MAXIMUS', 'MAXIMUS INC']},
        'TTEC': {'name': 'TTEC Holdings', 'aliases': ['TTEC', 'TTEC HOLDINGS', 'TELETECH']},
        'HURN': {'name': 'Huron Consulting', 'aliases': ['HURON', 'HURON CONSULTING']},
        'ICFI': {'name': 'ICF International', 'aliases': ['ICF', 'ICF INTERNATIONAL']},
        # Infrastructure/Construction contractors
        'GLDD': {'name': 'Great Lakes Dredge & Dock', 'aliases': ['GREAT LAKES DREDGE', 'GREAT LAKES DREDGE & DOCK', 'GREAT LAKES DREDGE AND DOCK']},
        'BAER': {'name': 'Bridger Aerospace', 'aliases': ['BRIDGER', 'BRIDGER AEROSPACE', 'BRIDGER AEROSPACE GROUP']},
        'GVA': {'name': 'Granite Construction', 'aliases': ['GRANITE', 'GRANITE CONSTRUCTION']},
        'PRIM': {'name': 'Primoris Services', 'aliases': ['PRIMORIS', 'PRIMORIS SERVICES']},
        'MTZ': {'name': 'MasTec Inc', 'aliases': ['MASTEC', 'MASTEC INC']},
        'PWR': {'name': 'Quanta Services', 'aliases': ['QUANTA', 'QUANTA SERVICES']},
        'FLR': {'name': 'Fluor Corporation', 'aliases': ['FLUOR', 'FLUOR CORPORATION']},
        'J': {'name': 'Jacobs Solutions', 'aliases': ['JACOBS', 'JACOBS ENGINEERING', 'JACOBS SOLUTIONS']},
        'ACM': {'name': 'AECOM', 'aliases': ['AECOM']},
    }
    
    # Tickers to NEVER match - ETFs, funds, banks, and tickers that cause false positives
    TICKER_BLACKLIST = {
        # Oil/Gas ETFs (match on "US" or "USA" in names)
        'USO', 'USL', 'UCO', 'SCO', 'BNO', 'UNG', 'UGAZ', 'DGAZ',
        # Other commodity ETFs
        'GLD', 'SLV', 'IAU', 'PPLT', 'PALL',
        # Index ETFs that might match common words
        'SPY', 'QQQ', 'IWM', 'DIA', 'VTI', 'VOO',
        # Leveraged ETFs
        'TQQQ', 'SQQQ', 'UPRO', 'SPXU', 'TNA', 'TZA',
        # Bond ETFs
        'TLT', 'IEF', 'SHY', 'BND', 'AGG', 'LQD', 'HYG', 'JNK',
        # International ETFs
        'EEM', 'VWO', 'EFA', 'VEA', 'IEMG',
        # Sector ETFs
        'XLF', 'XLK', 'XLE', 'XLV', 'XLI', 'XLU', 'XLP', 'XLY', 'XLB', 'XLRE',
        
        # Regional banks (unlikely to win govt tech/service contracts)
        'RRBI',  # Red River Bancshares - matches "RED RIVER TECHNOLOGY"
        'FRBK', 'FRBA', 'FRBM',  # First Republic variants
        'BANR', 'BANC', 'BRKL', 'BHLB', 'BMRC', 'BUSE', 'CADE', 'CBSH',
        'CVBF', 'EWBC', 'FFBC', 'FMBI', 'GBCI', 'HOMB', 'IBOC', 'INDB',
        'NWBI', 'OSBC', 'PACW', 'PPBI', 'SBCF', 'SFBS', 'STBA', 'TCBI',
        'TBBK', 'UBSI', 'UMBF', 'WABC', 'WAFD', 'WSFS',
        
        # Tickers that match common contractor name patterns (false positives)
        'III',   # Information Services Group - matches "INFORMATION" in names
        'ATLN',  # Atlantic International - matches "ATLANTIC" in names  
        'GLBL',  # Cartesian - matches "GLOBAL"
        'GOVT',  # Treasury ETF - matches "GOVERNMENT"
        'TECH',  # Bio-Techne - matches "TECHNOLOGY" 
        'SERV',  # ServiceMaster - matches "SERVICES"
        'NATL',  # matches "NATIONAL"
        'GV',    # Visionary Holdings - matches "VISIONARY"
        'IMMR',  # Immersion Corp - matches "IMMERSION"
        'GTLL',  # Global Technologies - matches "GLOBAL"
        'VISL',  # Vislink Technologies - matches "VISION"
        'CLNE',  # Clean Energy - matches "CLEAN"
        'PRTG',  # Portage - matches "PORT"
        'PRTA',  # Prothena - matches various
        'MTNB',  # Matinas BioPharma - matches "MOUNTAIN"
        
        # Royalty trusts (match geographic names, won't win govt contracts)
        'GULTU', # Gulf Coast Ultra Deep - matches "GULF COAST"
        'GULFTU', # Cross Timbers Royalty Trust
        'PERMTU', # Permian Basin Royalty Trust
        'SBRTU', # Sabine Royalty Trust
        'HGTXU', # Hugoton Royalty Trust
        'SBR',   # Sabine Royalty Trust
        'PBT',   # Permian Basin Royalty Trust
        'CRT',   # Cross Timbers Royalty Trust
        'VOC',   # VOC Energy Trust
        'ROYT',  # Pacific Coast Oil Trust
        
        # Geographic name tickers that cause false positives
        'GULF',  # WideOpenWest - matches "GULF" in names
        'LAKE',  # Lakeland Industries - matches "LAKE"
        'PINE',  # Alpine Income Property - matches "PINE"
        
        # Tickers that are common English words/abbreviations causing false positives
        # NOTE: Don't add tickers of real govt contractors here (e.g., UP is Wheels Up)
        'IT', 'A', 'C', 'F', 'K', 'M', 'T', 'V', 'X', 'Y', 'Z',
        'GO', 'ON', 'SO', 'AN', 'AI', 'AS', 'AT', 'BE', 'BY', 'DO', 'HE', 'IF',
        'IN', 'IS', 'ME', 'MY', 'NO', 'OF', 'OK', 'OR', 'TO', 'WE',
        'ALL', 'ARE', 'BIG', 'CAN', 'DAY', 'FOR', 'GET', 'HAS', 'HER', 'HIM',
        'HIS', 'HOW', 'ITS', 'LET', 'MAY', 'NEW', 'NOW', 'OLD', 'ONE', 'OUR',
        'OUT', 'OWN', 'SAY', 'SEE', 'SHE', 'THE', 'TOO', 'TWO', 'USE', 'WAY',
        'WHO', 'WIN', 'YOU',
        
        # Other problematic tickers
        'USA', 'US',
        # News/Media that might match article text
        'NYT', 'NYTM',
    }
    
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.companies = {}
        self.name_to_ticker = {}
        self.all_names = []
        self.core_names = {}  # Maps core name -> ticker for better matching
    
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
        """Build comprehensive name-to-ticker lookup tables."""
        self.name_to_ticker = {}
        self.all_names = []
        self.core_names = {}
        
        for ticker, info in self.companies.items():
            # Skip blacklisted tickers entirely
            if ticker in self.TICKER_BLACKLIST:
                continue
            
            # Full normalized name
            normalized = self._normalize_name(info['name'])
            self.name_to_ticker[normalized] = ticker
            self.all_names.append(normalized)
            
            # Extract and store core name (the distinctive part)
            core = self._extract_core_name(info['name'])
            if core and len(core) >= 3:
                if core not in self.core_names:
                    self.core_names[core] = ticker
            
            # Add aliases
            for alias in info.get('aliases', []):
                norm_alias = self._normalize_name(alias)
                self.name_to_ticker[norm_alias] = ticker
                if norm_alias not in self.all_names:
                    self.all_names.append(norm_alias)
                
                # Also add core of alias
                alias_core = self._extract_core_name(alias)
                if alias_core and len(alias_core) >= 3:
                    if alias_core not in self.core_names:
                        self.core_names[alias_core] = ticker
            
            # Auto-generate common variations
            self._add_auto_variations(ticker, info['name'])
    
    def _add_auto_variations(self, ticker: str, name: str):
        """Auto-generate common name variations."""
        # Skip blacklisted tickers
        if ticker in self.TICKER_BLACKLIST:
            return
        
        normalized = self._normalize_name(name)
        words = normalized.split()
        
        # Common words that should NEVER be used as single-word matches
        COMMON_WORDS = {
            # Generic business terms
            'PROFESSIONAL', 'TECHNOLOGY', 'TECHNOLOGIES', 'SERVICES', 'SOLUTIONS',
            'SYSTEMS', 'GLOBAL', 'INTERNATIONAL', 'NATIONAL', 'AMERICAN', 'UNITED',
            'FEDERAL', 'GENERAL', 'ADVANCED', 'DIGITAL', 'NETWORK', 'NETWORKS',
            'STRATEGIC', 'PREMIER', 'FIRST', 'CAPITAL', 'PARTNERS', 'GROUP',
            'HOLDINGS', 'MANAGEMENT', 'CONSULTING', 'ASSOCIATES', 'ENTERPRISES',
            'INDUSTRIES', 'RESOURCES', 'COMMUNICATIONS', 'HEALTHCARE', 'HEALTH',
            'MEDICAL', 'FINANCIAL', 'SECURITY', 'DEFENSE', 'ENERGY', 'POWER',
            'ENGINEERING', 'CONSTRUCTION', 'LOGISTICS', 'TRANSPORT', 'AVIATION',
            'AEROSPACE', 'MARINE', 'ENVIRONMENTAL', 'SCIENTIFIC', 'RESEARCH',
            'DEVELOPMENT', 'INNOVATION', 'CREATIVE', 'DYNAMIC', 'INTEGRATED',
            'STANDARD', 'QUALITY', 'PRECISION', 'TECHNICAL', 'INDUSTRIAL',
            'COMMERCIAL', 'CORPORATE', 'BUSINESS', 'ENTERPRISE', 'DIVERSITY',
            'STATES', 'STATE', 'USA', 'US',
            
            # Words that cause false positives with unrelated companies
            'INFORMATION', 'SCIENCES', 'SCIENCE', 'DATA', 'ANALYTICS',
            'CONSULTANTS', 'ADVISORS', 'ADVISORY',
            'RED', 'BLUE', 'GREEN', 'BLACK', 'WHITE', 'GOLD', 'SILVER',
            'RIVER', 'MOUNTAIN', 'VALLEY', 'HILL', 'LAKE', 'OCEAN', 'PACIFIC',
            'ATLANTIC', 'WESTERN', 'EASTERN', 'NORTHERN', 'SOUTHERN', 'CENTRAL',
            'NORTH', 'SOUTH', 'EAST', 'WEST', 'GULF', 'COAST', 'BAY', 'HARBOR',
            'APPLIED', 'CONTINENTAL', 'UNIVERSAL', 'MODERN', 'CLASSIC',
            'VANGUARD', 'PIONEER', 'FRONTIER', 'SUMMIT', 'APEX', 'PEAK',
            'PINNACLE', 'HORIZON', 'CENTURY', 'MILLENNIUM',
            'INSPECTION', 'TESTING', 'SUPPLY', 'DIVING', 'ELECTRICAL',
            'PHOENIX', 'EAGLE', 'FALCON', 'LIBERTY', 'FREEDOM', 'PATRIOT',
            'CROWN', 'ROYAL', 'EMPIRE', 'DOMINION', 'SOVEREIGN',
            'SPECTRUM', 'VISION', 'INSIGHT', 'FOCUS', 'TARGET',
            'SUPPORT', 'ASSIST', 'MISSION', 'PROGRAM', 'PROJECT',
            'VISIONARY', 'IMMERSION', 'MIRACLE', 'SOLUTIONS', 'LEGACY',
            'PREMIER', 'ELITE', 'PRIME', 'SUPERIOR', 'OPTIMAL', 'SYNERGY',
        }
        
        if len(words) >= 2:
            # First two words (e.g., "WHEELS UP" from "WHEELS UP EXPERIENCE")
            two_word = ' '.join(words[:2])
            
            # Only add if BOTH words are NOT common words (need at least one distinctive word)
            has_distinctive = any(w not in COMMON_WORDS for w in words[:2])
            if has_distinctive:
                if two_word not in self.name_to_ticker and len(two_word) >= 5:
                    self.name_to_ticker[two_word] = ticker
                    self.all_names.append(two_word)
                    # Also add to core_names for contains matching
                    if two_word not in self.core_names:
                        self.core_names[two_word] = ticker
    
    def _save_cache(self):
        try:
            with open(self.config.companies_cache, 'w') as f:
                json.dump({'companies': self.companies, 'updated': time.time()}, f)
        except:
            pass
    
    def _normalize_name(self, name: str) -> str:
        """Normalize company name by removing suffixes and cleaning."""
        if not name:
            return ""
        name = name.upper().strip()
        
        # Remove common entity suffixes (order matters - longer first)
        suffixes = [
            ' INCORPORATED', ' CORPORATION', ' INTERNATIONAL', ' TECHNOLOGIES', 
            ' TECHNOLOGY', ' ENTERPRISES', ' HOLDINGS', ' PARTNERS', ' SOLUTIONS',
            ' SERVICES', ' SYSTEMS', ' EXPERIENCE', ' WORLDWIDE', ' GLOBAL',
            ' LIMITED', ' COMPANY', ' GROUP', ' L.L.C.', ' L.P.', ' INC.',
            ' CORP.', ' LTD.', ' INC', ' CORP', ' LLC', ' LTD', ' CO.', ' CO',
            ' LP', ' PLC', ' SA', ' NV', ' AG', ' SE',
        ]
        
        # Apply multiple passes to catch nested suffixes
        for _ in range(3):
            for suffix in suffixes:
                if name.endswith(suffix):
                    name = name[:-len(suffix)].strip()
        
        # Remove punctuation and extra spaces
        name = re.sub(r'[^\w\s]', ' ', name)
        name = re.sub(r'\s+', ' ', name).strip()
        return name
    
    def _extract_core_name(self, name: str) -> str:
        """Extract the core distinctive part of a company name."""
        normalized = self._normalize_name(name)
        words = normalized.split()
        
        # Skip common prefixes
        skip_prefixes = {'THE', 'A', 'AN'}
        while words and words[0] in skip_prefixes:
            words = words[1:]
        
        # Skip common generic words at the end
        skip_suffixes = {'CORP', 'INC', 'LLC', 'LTD', 'CO', 'COMPANY', 'GROUP', 
                        'HOLDINGS', 'PARTNERS', 'INTERNATIONAL', 'GLOBAL', 
                        'TECHNOLOGIES', 'TECHNOLOGY', 'SERVICES', 'SOLUTIONS',
                        'SYSTEMS', 'ENTERPRISES', 'WORLDWIDE', 'EXPERIENCE'}
        
        while words and words[-1] in skip_suffixes:
            words = words[:-1]
        
        # Return first 1-3 distinctive words
        if len(words) >= 2:
            return ' '.join(words[:2])
        elif len(words) == 1:
            return words[0]
        return ""
    
    def find_match(self, recipient_name: str) -> Optional[Dict]:
        """Find matching public company using multiple strategies."""
        if not recipient_name:
            return None
        
        normalized = self._normalize_name(recipient_name)
        
        # Skip very short names
        if len(normalized) < 3:
            return None
        
        # Helper to validate a match (check blacklist)
        def _validate_match(ticker: str, match_name: str, score: int, match_type: str) -> Optional[Dict]:
            # Check if ticker is blacklisted
            if ticker in self.TICKER_BLACKLIST:
                return None
            # Check if ticker exists in companies dict
            if ticker not in self.companies:
                return None
            return {
                'ticker': ticker,
                'matched_name': self.companies[ticker]['name'],
                'match_score': score,
                'match_type': match_type
            }
        
        # Strategy 1: Exact match on normalized name
        if normalized in self.name_to_ticker:
            ticker = self.name_to_ticker[normalized]
            result = _validate_match(ticker, normalized, 100, 'exact')
            if result:
                return result
        
        # Strategy 2: Core name match
        core = self._extract_core_name(recipient_name)
        if core and core in self.core_names:
            ticker = self.core_names[core]
            result = _validate_match(ticker, core, 98, 'core')
            if result:
                return result
        
        # Strategy 3: Check if recipient contains any known core name
        # E.g., "WHEELS UP PARTNERS LLC" contains core "WHEELS UP"
        # IMPORTANT: Only match on distinctive multi-word cores, not single common words
        for core_name, ticker in self.core_names.items():
            # Skip blacklisted tickers early
            if ticker in self.TICKER_BLACKLIST:
                continue
            
            # Must be multi-word OR a very distinctive single word (8+ chars, not common)
            words_in_core = core_name.split()
            is_single_word = len(words_in_core) == 1
            
            # Skip single common words that cause false positives
            COMMON_WORDS = {
                # Generic business terms
                'PROFESSIONAL', 'TECHNOLOGY', 'TECHNOLOGIES', 'SERVICES', 'SOLUTIONS',
                'SYSTEMS', 'GLOBAL', 'INTERNATIONAL', 'NATIONAL', 'AMERICAN', 'UNITED',
                'FEDERAL', 'GENERAL', 'ADVANCED', 'DIGITAL', 'NETWORK', 'NETWORKS',
                'STRATEGIC', 'PREMIER', 'FIRST', 'CAPITAL', 'PARTNERS', 'GROUP',
                'HOLDINGS', 'MANAGEMENT', 'CONSULTING', 'ASSOCIATES', 'ENTERPRISES',
                'INDUSTRIES', 'RESOURCES', 'COMMUNICATIONS', 'HEALTHCARE', 'HEALTH',
                'MEDICAL', 'FINANCIAL', 'SECURITY', 'DEFENSE', 'ENERGY', 'POWER',
                'ENGINEERING', 'CONSTRUCTION', 'LOGISTICS', 'TRANSPORT', 'AVIATION',
                'AEROSPACE', 'MARINE', 'ENVIRONMENTAL', 'SCIENTIFIC', 'RESEARCH',
                'DEVELOPMENT', 'INNOVATION', 'CREATIVE', 'DYNAMIC', 'INTEGRATED',
                'STANDARD', 'QUALITY', 'PRECISION', 'TECHNICAL', 'INDUSTRIAL',
                'COMMERCIAL', 'CORPORATE', 'BUSINESS', 'ENTERPRISE', 'DIVERSITY',
                'STATES', 'STATE', 'USA', 'US',
                
                # Words that cause false positives with unrelated companies
                'INFORMATION', 'SCIENCES', 'SCIENCE', 'DATA', 'ANALYTICS',
                'CONSULTING', 'CONSULTANTS', 'ADVISORS', 'ADVISORY',
                'RED', 'BLUE', 'GREEN', 'BLACK', 'WHITE', 'GOLD', 'SILVER',
                'RIVER', 'MOUNTAIN', 'VALLEY', 'HILL', 'LAKE', 'OCEAN', 'PACIFIC',
                'ATLANTIC', 'WESTERN', 'EASTERN', 'NORTHERN', 'SOUTHERN', 'CENTRAL',
                'NORTH', 'SOUTH', 'EAST', 'WEST', 'GULF', 'COAST', 'BAY', 'HARBOR',
                'VANGUARD', 'PIONEER', 'FRONTIER', 'SUMMIT', 'APEX', 'PEAK',
                'INSPECTION', 'TESTING', 'SUPPLY', 'DIVING', 'ELECTRICAL',
                'PHOENIX', 'EAGLE', 'FALCON', 'LIBERTY', 'FREEDOM', 'PATRIOT',
                'CROWN', 'ROYAL', 'EMPIRE', 'DOMINION', 'SOVEREIGN',
                'SPECTRUM', 'VISION', 'INSIGHT', 'FOCUS', 'TARGET',
                'SUPPORT', 'ASSIST', 'MISSION', 'PROGRAM', 'PROJECT',
                'VISIONARY', 'IMMERSION', 'MIRACLE', 'SOLUTIONS', 'LEGACY',
                'PREMIER', 'ELITE', 'PRIME', 'SUPERIOR', 'OPTIMAL', 'SYNERGY',
            }
            
            if is_single_word:
                # Single word must be 8+ chars AND not in common words list
                if len(core_name) < 8 or core_name in COMMON_WORDS:
                    continue
            
            # Multi-word cores: check none of the words are in common words
            if not is_single_word:
                # Skip if it's just two common words together
                if all(w in COMMON_WORDS for w in words_in_core):
                    continue
            
            # Now check if this core appears in the recipient name
            if len(core_name) >= 6 and core_name in normalized:
                # Validate it's whole words, not partial
                pattern = r'\b' + re.escape(core_name) + r'\b'
                if re.search(pattern, normalized):
                    result = _validate_match(ticker, core_name, 95, 'contains')
                    if result:
                        return result
        
        # Strategy 4: Fuzzy match with validation
        if not self.all_names:
            return None
        
        best_score = 0
        best_match = None
        
        # Try multiple fuzzy matching algorithms
        for scorer in [fuzz.ratio, fuzz.token_sort_ratio, fuzz.token_set_ratio]:
            for match_name, score, _ in process.extract(normalized, self.all_names, scorer=scorer, limit=5):
                if score > best_score:
                    best_score = score
                    best_match = match_name
        
        if best_match and best_score >= self.config.fuzzy_match_threshold:
            ticker = self.name_to_ticker[best_match]
            
            # Skip blacklisted tickers
            if ticker in self.TICKER_BLACKLIST:
                return None
            
            # Validation: check that core names overlap
            input_core = self._extract_core_name(recipient_name)
            match_core = self._extract_core_name(best_match)
            
            if input_core and match_core:
                # Check if cores are similar
                core_score = fuzz.ratio(input_core, match_core)
                if core_score < 60:
                    # Cores are too different - likely false positive
                    return None
            
            # Additional validation: first word check
            input_words = normalized.split()
            match_words = best_match.split()
            
            if input_words and match_words:
                first_word_score = fuzz.ratio(input_words[0], match_words[0])
                first_word_contained = (input_words[0] in match_words[0]) or (match_words[0] in input_words[0])
                
                if first_word_score < 70 and not first_word_contained and len(input_words[0]) > 3:
                    # First words don't match and aren't related
                    return None
            
            # Check length similarity
            len_ratio = min(len(normalized), len(best_match)) / max(len(normalized), len(best_match))
            if len_ratio < 0.4:
                return None
            
            return _validate_match(ticker, best_match, best_score, 'fuzzy')
        
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
                "timestamp": datetime.now(timezone.utc).isoformat()
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
    """Monitor for SAM.gov contract award notices."""
    
    def __init__(self, config: Config):
        self.config = config
        self.logger = setup_logging(config)
        
        self.samgov = SAMGovClient(config, self.logger)
        self.company_db = PublicCompanyDatabase(config, self.logger)
        self.market_cap = MarketCapService(config, self.logger)
        self.alerts = AlertSystem(config, self.logger)
        self.tracker = AwardTracker(config, self.logger)
    
    def initialize(self) -> bool:
        """Initialize the monitor."""
        self.logger.info("=" * 60)
        self.logger.info("SAM.gov Contract Award Monitor")
        self.logger.info("=" * 60)
        self.logger.info(f"  Min contract value: ${self.config.min_contract_value:,.0f}")
        self.logger.info(f"  Min materiality: {self.config.min_materiality_percent}% of market cap")
        self.logger.info(f"  Lookback: {self.config.lookback_days} days")
        self.logger.info(f"  Source: SAM.gov (new awards only)")
        
        return self.company_db.load()
    
    def check_once(self) -> List[Tuple[Dict, Dict, Dict]]:
        """Check SAM.gov for new award notices."""
        self.logger.info("Checking for new contract awards...")
        
        all_awards = []
        
        # Fetch from SAM.gov only
        try:
            sam_awards = self.samgov.get_recent_awards(self.config.lookback_days)
            all_awards.extend(sam_awards)
            self.logger.info(f"  SAM.gov: {len(sam_awards)} award notices")
        except Exception as e:
            self.logger.error(f"  SAM.gov error: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
        
        # Process awards
        new_matches = []
        stats = {
            'total': len(all_awards),
            'no_recipient': 0,
            'seen': 0,
            'old_award': 0,
            'low_value': 0,
            'no_match': 0,
            'low_materiality': 0,
            'no_market_cap': 0,
            'alerts': 0
        }
        
        # Track sample data for debugging
        sample_recipients = []
        sample_high_value_unmatched = []
        sample_matches_filtered = []
        
        # Only alert on awards posted in last 2 days (including today)
        today = datetime.now().date()
        recent_cutoff = today - timedelta(days=2)  # e.g., if today is Feb 23, cutoff is Feb 21
        
        for award in all_awards:
            recipient = award.get('recipient_name', '')
            amount = award.get('award_amount', 0)
            
            if not recipient:
                stats['no_recipient'] += 1
                continue
            
            # Collect samples for debugging
            if len(sample_recipients) < 5:
                sample_recipients.append(f"{recipient[:40]} (${amount/1e6:.1f}M)")
            
            # Skip if seen
            if self.tracker.is_seen(award):
                stats['seen'] += 1
                continue
            
            self.tracker.mark_seen(award)
            
            # Check if award was posted recently (last 2 days)
            # Priority: posted_date (when posted to SAM.gov) > date_signed > start_date
            date_str = award.get('posted_date') or award.get('date_signed') or award.get('start_date')
            if date_str:
                try:
                    award_date = datetime.strptime(str(date_str)[:10], "%Y-%m-%d").date()
                    if award_date < recent_cutoff:
                        stats['old_award'] += 1
                        continue
                except:
                    pass  # If we can't parse date, proceed anyway
            
            # Check value
            if amount < self.config.min_contract_value:
                stats['low_value'] += 1
                continue
            
            # Track high-value contracts for debugging
            # Track high-value contracts that didn't match for debugging
            if amount >= 1_000_000 and len(sample_high_value_unmatched) < 10:
                # Will be removed from list if we find a match below
                pass
            
            # Find company match
            match = self.company_db.find_match(recipient)
            if not match:
                stats['no_match'] += 1
                # Track high-value unmatched contracts
                if amount >= 1_000_000 and len(sample_high_value_unmatched) < 10:
                    sample_high_value_unmatched.append(f"{recipient[:35]} -> ${amount/1e6:.1f}M")
                continue
            
            # Calculate materiality
            market_cap = self.market_cap.get_market_cap(match['ticker'])
            
            if not market_cap:
                stats['no_market_cap'] += 1
                self.logger.debug(f"  No market cap for ${match['ticker']} - skipping")
                continue
            
            materiality = self.market_cap.calculate_materiality(amount, market_cap)
            
            # Check materiality threshold
            pct = materiality.get('percent_of_market_cap')
            if pct is not None and pct < self.config.min_materiality_percent:
                stats['low_materiality'] += 1
                sample_matches_filtered.append(f"${match['ticker']}: ${amount/1e6:.1f}M = {pct:.3f}% (min {self.config.min_materiality_percent}%)")
                self.logger.debug(f"  Filtered ${match['ticker']}: {pct:.4f}% < {self.config.min_materiality_percent}%")
                continue
            
            # Alert!
            stats['alerts'] += 1
            self.logger.info(f"  🚨 MATCH: {recipient} -> ${match['ticker']} ({match['match_score']}%) - ${amount/1e6:.1f}M - {pct:.2f}% of mcap")
            
            new_matches.append((award, match, materiality))
            self.alerts.alert(award, match, materiality)
        
        # Log detailed summary
        self.logger.info(f"Check complete: {stats['alerts']} alerts sent")
        self.logger.info(f"  Stats: total={stats['total']}, seen={stats['seen']}, old={stats['old_award']}, low_value={stats['low_value']}, no_match={stats['no_match']}, low_mat={stats['low_materiality']}, no_mcap={stats['no_market_cap']}")
        
        # Debug: show sample recipients
        if sample_recipients:
            self.logger.debug(f"  Sample recipients: {sample_recipients[:3]}")
        
        # Debug: show high-value contracts that weren't matched
        if sample_high_value_unmatched and stats['no_match'] > 0:
            self.logger.debug(f"  High-value unmatched: {sample_high_value_unmatched[:5]}")
        
        # Debug: show matches filtered by materiality
        if sample_matches_filtered:
            self.logger.debug(f"  Filtered by materiality: {sample_matches_filtered[:5]}")
        
        return new_matches
    
    def is_market_hours(self) -> Tuple[bool, float, float]:
        """Check if within operating hours (6 AM - 8 PM ET, weekdays)."""
        try:
            tz = ZoneInfo(self.config.timezone)
            now = datetime.now(tz)
        except:
            # Fallback: approximate EST
            utc_now = datetime.now(timezone.utc)
            now = (utc_now - timedelta(hours=5)).replace(tzinfo=None)
        
        # Weekend check (Sat=5, Sun=6)
        if now.weekday() >= 5:
            # Calculate seconds until Monday 6 AM
            days_until_monday = 7 - now.weekday()  # Sat=2, Sun=1
            next_open = now.replace(
                hour=self.config.market_open_hour,
                minute=self.config.market_open_minute,
                second=0, microsecond=0
            ) + timedelta(days=days_until_monday)
            until_open = (next_open - now).total_seconds()
            return False, until_open, 0
        
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
            # Before market open - wait until open
            until_open = (market_open - now).total_seconds()
            return False, until_open, 0
        elif now >= market_close:
            # After market close - wait until tomorrow 6 AM
            next_open = market_open + timedelta(days=1)
            until_open = (next_open - now).total_seconds()
            return False, until_open, 0
        
        # Market is open
        until_close = (market_close - now).total_seconds()
        return True, 0, until_close
    
    def run_continuous(self):
        """Run continuously."""
        print("\n" + "=" * 60)
        print("  SAM.gov Contract Award Monitor - Running")
        print(f"  Interval: {self.config.check_interval_seconds}s")
        print(f"  Lookback: {self.config.lookback_days} days")
        print(f"  Min Contract: ${self.config.min_contract_value:,.0f}")
        print(f"  Min Materiality: {self.config.min_materiality_percent}% of market cap")
        print(f"  Source: SAM.gov (new awards only)")
        if self.config.market_hours_only:
            print("  Mode: Weekdays 9:00 AM - 4:00 PM ET")
        else:
            print("  Mode: 24/7")
        print("  Press Ctrl+C to stop")
        if self.config.debug_mode:
            print("  DEBUG MODE: Verbose logging enabled")
        print("=" * 60 + "\n")
        
        try:
            while True:
                if self.config.market_hours_only:
                    is_open, until_open, _ = self.is_market_hours()
                    if not is_open:
                        if until_open > 0:
                            hours = int(until_open / 3600)
                            mins = int((until_open % 3600) / 60)
                            self.logger.info(f"Outside operating hours. Sleeping {hours}h {mins}m until market opens.")
                            # Sleep in chunks of max 1 hour to handle daylight saving transitions
                            sleep_time = min(3600, until_open)
                            time.sleep(sleep_time)
                        else:
                            # Shouldn't happen but just in case
                            self.logger.info("Outside operating hours. Checking again in 5 minutes.")
                            time.sleep(300)
                        continue
                
                try:
                    self.check_once()
                except Exception as e:
                    self.logger.error(f"Check error: {e}")
                    import traceback
                    self.logger.error(traceback.format_exc())
                
                time.sleep(self.config.check_interval_seconds)
                gc.collect()
                
        except KeyboardInterrupt:
            print("\nStopped.")
    
    def self_test(self) -> bool:
        """Run internal validation tests."""
        print("\n" + "=" * 60)
        print("  SELF-TEST: SAM.gov Contract Monitor")
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
    
    parser = argparse.ArgumentParser(description="SAM.gov Contract Award Monitor")
    parser.add_argument('-i', '--interval', type=int, default=600, help="Check interval in seconds")
    parser.add_argument('-l', '--lookback', type=int, default=7, help="Days to look back (default: 7)")
    parser.add_argument('-m', '--min-value', type=float, default=500000, help="Min contract value")
    parser.add_argument('--min-materiality', type=float, default=0.5, help="Min materiality %% of market cap")
    parser.add_argument('--all-hours', action='store_true', help="Run 24/7 instead of market hours only")
    parser.add_argument('--once', action='store_true', help="Run once and exit")
    parser.add_argument('--test', action='store_true', help="Run self-test")
    parser.add_argument('--no-discord', action='store_true', help="Disable Discord alerts")
    parser.add_argument('--debug', action='store_true', help="Enable debug logging")
    parser.add_argument('--clear-seen', action='store_true', help="Clear seen awards cache at startup")
    args = parser.parse_args()
    
    config = Config()
    config.check_interval_seconds = args.interval
    config.lookback_days = args.lookback
    config.min_contract_value = args.min_value
    config.min_materiality_percent = args.min_materiality
    if args.all_hours:
        config.market_hours_only = False  # Enable 24/7 mode
    config.enable_discord_alerts = not args.no_discord
    config.debug_mode = args.debug  # Default is False (debug off)
    
    # Clear seen awards cache if requested
    if args.clear_seen:
        if config.seen_awards_file.exists():
            config.seen_awards_file.unlink()
            print(f"Cleared seen awards cache: {config.seen_awards_file}")
    
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