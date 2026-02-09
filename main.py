#!/usr/bin/env python3
"""
Combined Government Activity Monitor for Railway
- USASpending.gov Contract Awards Monitor
- Congress Financial Disclosure Monitor (House + Senate)

Runs both monitors concurrently with Discord webhook alerts.

KEY FIX: House probing now uses much higher consecutive miss tolerance (200)
and larger probe window (500) to handle sparse doc ID sequences. Also adds
XML ZIP download as a backup detection method to catch anything probing misses.
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

DISCORD_WEBHOOK_URLS = [
    url.strip() for url in os.environ.get(
        'DISCORD_WEBHOOK_URLS',
        'https://discordapp.com/api/webhooks/919672540237017138/Zga2QHBVwPUKXbCMNQ6hRXSsJaW8d136pOZNheRz1SK0YS5GIRnpjsGdN7trPul-zeXo,https://discordapp.com/api/webhooks/1464048870295076984/_ldSwGExzYM2ZRAKPXy1T1XCx9LE5WGomsmae3eTOnOw_7_7Kz73x6Lmw2UIi2XheyNZ,https://discordapp.com/api/webhooks/1466210412910346422/qVVnM5ulkUwy17I6zJlYNNleqDX8CS9ivuayd3HRMIyDPOCl4P0rijneuJI9DueqEosi'
    ).split(',') if url.strip()
]

DATA_DIR = Path(os.environ.get('RAILWAY_VOLUME_MOUNT_PATH', os.environ.get('DATA_DIR', '/data')))
if not DATA_DIR.exists():
    DATA_DIR = Path.home() / '.gov_monitor'
DATA_DIR.mkdir(parents=True, exist_ok=True)

USASPENDING_CHECK_INTERVAL = int(os.environ.get('USASPENDING_INTERVAL', '60'))
CONGRESS_CHECK_INTERVAL = int(os.environ.get('CONGRESS_INTERVAL', '30'))

DEBUG = os.environ.get('DEBUG', 'false').lower() == 'true'

CURRENT_YEAR = datetime.now().year
FILING_YEARS = [CURRENT_YEAR, CURRENT_YEAR - 1]

# =============================================================================
# LOGGING
# =============================================================================

def setup_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG if DEBUG else logging.INFO)
    console.setFormatter(logging.Formatter(
        f'%(asctime)s | {name[:8]:8s} | %(levelname)-7s | %(message)s', datefmt='%H:%M:%S'
    ))
    logger.addHandler(console)
    return logger


# =============================================================================
# DISCORD WEBHOOK HELPER
# =============================================================================

def send_discord_webhook(embed: dict, logger: logging.Logger = None):
    """Send a Discord webhook with the given embed to all configured URLs."""
    payload = {"embeds": [embed]}
    for url in DISCORD_WEBHOOK_URLS:
        try:
            resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code == 429:
                retry_after = resp.json().get('retry_after', 5)
                if logger:
                    logger.warning(f"Rate limited, waiting {retry_after}s")
                time.sleep(retry_after)
                requests.post(url, json=payload, timeout=10)
        except Exception as e:
            if logger:
                logger.error(f"Webhook error: {e}")


# =============================================================================
# CURRENT MEMBERS LIST
# =============================================================================

MEMBERS_JSON_URL = "https://unitedstates.github.io/congress-legislators/legislators-current.json"


def load_current_members(data_dir: Path, logger: logging.Logger) -> List[Dict]:
    """Load current members of Congress, caching for 24 hours."""
    cache_file = data_dir / "current_members.json"
    
    # Check cache
    if cache_file.exists():
        try:
            age = time.time() - cache_file.stat().st_mtime
            if age < 86400:  # 24 hours
                with open(cache_file) as f:
                    members = json.load(f)
                logger.info(f"Loaded {len(members)} members from cache")
                return members
        except Exception:
            pass
    
    # Fetch fresh
    try:
        resp = requests.get(MEMBERS_JSON_URL, timeout=30)
        resp.raise_for_status()
        raw = resp.json()
        
        members = []
        for m in raw:
            name = m.get('name', {})
            term = m.get('terms', [{}])[-1] if m.get('terms') else {}
            members.append({
                'first': name.get('first', ''),
                'last': name.get('last', ''),
                'official_full': name.get('official_full', ''),
                'nickname': name.get('nickname', ''),
                'type': term.get('type', ''),  # 'rep' or 'sen'
                'state': term.get('state', ''),
                'district': term.get('district', ''),
            })
        
        with open(cache_file, 'w') as f:
            json.dump(members, f)
        
        logger.info(f"Fetched {len(members)} current members")
        return members
    except Exception as e:
        logger.error(f"Failed to fetch members list: {e}")
        # Try stale cache
        if cache_file.exists():
            with open(cache_file) as f:
                return json.load(f)
        return []


def is_current_member(full_name: str, last_name: str, members: List[Dict]) -> bool:
    """Check if a name matches a current member of Congress using fuzzy matching."""
    if not members or not last_name:
        return True  # If we can't check, assume yes
    
    full_name_clean = full_name.replace("Hon. ", "").replace("Hon ", "").strip()
    last_lower = last_name.lower().strip()
    
    for m in members:
        m_last = m['last'].lower()
        if m_last != last_lower:
            continue
        
        # Last name matches — check first name loosely
        m_first = m['first'].lower()
        m_nick = m.get('nickname', '').lower()
        m_official = m.get('official_full', '').lower()
        
        name_lower = full_name_clean.lower()
        
        if m_first in name_lower or name_lower in m_official:
            return True
        if m_nick and m_nick in name_lower:
            return True
        
        # Fuzzy match on full name
        if m_official:
            score = fuzz.token_sort_ratio(name_lower, m_official)
            if score > 70:
                return True
        
        # If last name matches and first initial matches, likely same person
        name_parts = full_name_clean.split()
        if name_parts and m_first and name_parts[0][0].lower() == m_first[0]:
            return True
    
    return False


# =============================================================================
# USASPENDING MONITOR
# =============================================================================

class USASpendingMonitor:
    """Monitors USASpending.gov for new contract awards to publicly traded companies."""
    
    def __init__(self):
        self.logger = setup_logging("USASPEND")
        self.data_file = DATA_DIR / "usaspending_seen.json"
        self.seen_awards: Set[str] = set()
        self.running = False
        self.thread = None
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; GovMonitor/1.0)',
            'Accept': 'application/json',
        })
        
        self._load_seen()
    
    def _load_seen(self):
        if self.data_file.exists():
            try:
                with open(self.data_file) as f:
                    data = json.load(f)
                self.seen_awards = set(data.get('awards', []))
                self.logger.info(f"Loaded {len(self.seen_awards)} seen awards")
            except Exception as e:
                self.logger.warning(f"Could not load seen awards: {e}")
    
    def _save_seen(self):
        try:
            with open(self.data_file, 'w') as f:
                json.dump({'awards': list(self.seen_awards)}, f)
        except Exception as e:
            self.logger.error(f"Could not save seen awards: {e}")
    
    def check_awards(self):
        """Check for new contract awards."""
        try:
            min_value = float(os.environ.get('MIN_CONTRACT_VALUE', '500000'))
            
            url = "https://api.usaspending.gov/api/v2/search/spending_by_award/"
            payload = {
                "filters": {
                    "time_period": [
                        {
                            "start_date": (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d"),
                            "end_date": datetime.now().strftime("%Y-%m-%d")
                        }
                    ],
                    "award_type_codes": ["A", "B", "C", "D"],
                },
                "fields": [
                    "Award ID", "Recipient Name", "Award Amount",
                    "Awarding Agency", "Description", "Start Date",
                    "generated_internal_id"
                ],
                "page": 1,
                "limit": 100,
                "sort": "Award Amount",
                "order": "desc"
            }
            
            resp = self.session.post(url, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            
            results = data.get('results', [])
            new_count = 0
            
            for award in results:
                award_id = award.get('generated_internal_id') or award.get('Award ID', '')
                if not award_id or award_id in self.seen_awards:
                    continue
                
                amount = award.get('Award Amount', 0)
                if amount and amount < min_value:
                    continue
                
                self.seen_awards.add(award_id)
                new_count += 1
                
                recipient = award.get('Recipient Name', 'Unknown')
                agency = award.get('Awarding Agency', 'Unknown')
                description = award.get('Description', 'N/A')
                start_date = award.get('Start Date', 'N/A')
                
                amount_str = f"${amount:,.0f}" if amount else "N/A"
                
                embed = {
                    "title": f"🏛️ New Contract Award",
                    "description": f"**{recipient}**",
                    "color": 0x1E90FF,
                    "fields": [
                        {"name": "Amount", "value": amount_str, "inline": True},
                        {"name": "Agency", "value": agency[:100], "inline": True},
                        {"name": "Start Date", "value": start_date, "inline": True},
                        {"name": "Description", "value": description[:200] if description else "N/A", "inline": False},
                    ],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "footer": {"text": "USASpending.gov Monitor"}
                }
                
                send_discord_webhook(embed, self.logger)
                self.logger.info(f"New award: {recipient} - {amount_str}")
                time.sleep(1)
            
            if new_count > 0:
                self._save_seen()
                self.logger.info(f"Found {new_count} new awards")
            
        except Exception as e:
            self.logger.error(f"Check error: {e}")
    
    def run(self):
        self.running = True
        self.logger.info("USASpending monitor started")
        
        while self.running:
            try:
                self.check_awards()
            except Exception as e:
                self.logger.error(f"Loop error: {e}")
            
            for _ in range(USASPENDING_CHECK_INTERVAL * 10):
                if not self.running:
                    break
                time.sleep(0.1)
    
    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
    
    def stop(self):
        self.running = False


# =============================================================================
# CONGRESS DISCLOSURE MONITOR
# =============================================================================

class CongressDisclosureMonitor:
    """Monitors House and Senate financial disclosures for current members."""
    
    VALID_STATES = {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC', 'PR', 'GU', 'VI', 'AS', 'MP'
    }
    
    def __init__(self):
        self.logger = setup_logging("CONGRESS")
        self.data_file = DATA_DIR / "congress_seen.json"
        self.seen_filings: Set[str] = set()
        self.highest_house_doc_id: int = 20033000
        self.running = False
        self.thread = None
        self.members: List[Dict] = []
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        
        self._load_seen()
    
    def _load_seen(self):
        if self.data_file.exists():
            try:
                with open(self.data_file) as f:
                    data = json.load(f)
                self.seen_filings = set(data.get('filings', []))
                self.highest_house_doc_id = data.get('highest_house_doc_id', 20033000)
                self.logger.info(f"Loaded {len(self.seen_filings)} seen filings, highest doc ID: {self.highest_house_doc_id}")
            except Exception as e:
                self.logger.warning(f"Could not load seen filings: {e}")
    
    def _save_seen(self):
        try:
            with open(self.data_file, 'w') as f:
                json.dump({
                    'filings': list(self.seen_filings),
                    'highest_house_doc_id': self.highest_house_doc_id,
                }, f)
        except Exception as e:
            self.logger.error(f"Could not save seen filings: {e}")
    
    # ========== HOUSE: PDF PROBING ==========
    
    def _fetch_and_validate_house_pdf(self, doc_id: int) -> Optional[Dict]:
        """Fetch a House PDF and validate it contains real disclosure data.
        Returns filing dict if valid, None otherwise."""
        
        for year in FILING_YEARS:
            for file_type, path in [('PTR', 'ptr-pdfs'), ('FD', 'financial-pdfs')]:
                url = f"https://disclosures-clerk.house.gov/public_disc/{path}/{year}/{doc_id}.pdf"
                
                try:
                    response = self.session.get(url, timeout=15)
                    
                    if response.status_code != 200:
                        continue
                    
                    content = response.content
                    
                    # Must be a real PDF (not an error page)
                    if len(content) < 10000:
                        continue
                    if not content.startswith(b'%PDF'):
                        continue
                    
                    # Extract text from PDF binary
                    text = content.decode('latin-1', errors='ignore')
                    
                    # Extract name
                    name_match = re.search(
                        r'Name:\s*(Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-zA-Z\'\-]+)',
                        text
                    )
                    if not name_match:
                        # Try broader pattern for names with middle names/initials
                        name_match = re.search(
                            r'Name:\s*(Hon\.?\s+)?([A-Z][a-z]+(?:\s+[A-Z](?:\.|[a-z]+))?\s+(?:[A-Z]\.?\s+)?[A-Z][a-zA-Z\'\-]+)',
                            text
                        )
                    if not name_match:
                        continue
                    
                    name_part = name_match.group(2).strip()
                    if not name_part or len(name_part) < 5:
                        continue
                    
                    # Extract state/district
                    state_match = re.search(r'State/District:\s*([A-Z]{2}\d{2})', text)
                    if not state_match:
                        continue
                    
                    state_dst = state_match.group(1)
                    if state_dst[:2] not in self.VALID_STATES:
                        continue
                    
                    # Extract filing date
                    filing_date = ''
                    date_match = re.search(r'Digitally Signed:.*?,\s*(\d{2}/\d{2}/\d{4})', text)
                    if date_match:
                        filing_date = date_match.group(1)
                    
                    full_name = f"Hon. {name_part}"
                    name_parts = name_part.split()
                    first_name = name_parts[0] if name_parts else ''
                    last_name = name_parts[-1] if len(name_parts) >= 2 else ''
                    
                    return {
                        'doc_id': doc_id,
                        'full_name': full_name,
                        'first_name': first_name,
                        'last_name': last_name,
                        'state_dst': state_dst,
                        'file_type': file_type,
                        'filing_date': filing_date,
                        'url': url,
                        'year': year,
                        'chamber': 'House',
                    }
                    
                except requests.exceptions.Timeout:
                    continue
                except Exception as e:
                    if DEBUG:
                        self.logger.debug(f"PDF fetch error for {doc_id}: {e}")
                    continue
        
        return None
    
    def _probe_house_filings(self) -> List[Dict]:
        """Find new House filings by probing sequential PDF doc IDs.
        
        CRITICAL FIX: Uses much higher consecutive miss tolerance (200) and
        larger probe window (500) because doc IDs are sparse — there can be
        100+ consecutive IDs with no public PDF between filings.
        """
        filings = []
        
        try:
            probe_start = self.highest_house_doc_id + 1
            max_probe_ahead = 500  # Check up to 500 IDs ahead
            consecutive_misses = 0
            max_consecutive_misses = 200  # INCREASED from 20-40 to 200
            
            self.logger.info(f"House probe: starting from doc ID {probe_start}")
            
            for doc_id in range(probe_start, probe_start + max_probe_ahead):
                # Skip already-seen filings
                if f"house_{doc_id}" in self.seen_filings:
                    consecutive_misses = 0
                    continue
                
                filing = self._fetch_and_validate_house_pdf(doc_id)
                
                if filing:
                    full_name = filing['full_name']
                    last_name = filing['last_name']
                    
                    # Check if current member
                    if is_current_member(full_name, last_name, self.members):
                        filings.append(filing)
                        self.logger.info(f"House probe: found {doc_id} -> {full_name} ({filing['file_type']})")
                    else:
                        self.logger.debug(f"House probe: {doc_id} -> {full_name} (not current member, skipping)")
                    
                    # Always update highest ID and reset misses (even for non-members)
                    if doc_id > self.highest_house_doc_id:
                        self.highest_house_doc_id = doc_id
                    consecutive_misses = 0
                else:
                    consecutive_misses += 1
                
                if consecutive_misses >= max_consecutive_misses:
                    self.logger.info(f"House probe: stopping after {max_consecutive_misses} consecutive misses at ID {doc_id}")
                    break
                
                # Rate limiting — light delay between requests
                time.sleep(0.1)
            
            self.logger.info(f"House probe: found {len(filings)} new filings (highest ID: {self.highest_house_doc_id})")
            
        except Exception as e:
            self.logger.error(f"House probe error: {e}")
        
        return filings
    
    # ========== HOUSE: XML ZIP BACKUP ==========
    
    def _get_house_zip_filings(self) -> List[Dict]:
        """Backup method: Download House XML ZIP files and parse for new filings.
        This catches anything the probing approach misses (ZIP updates less frequently
        but contains ALL filings when it does update)."""
        filings = []
        
        for year in FILING_YEARS:
            for file_type, zip_name in [('PTR', f'{year}PTR.zip'), ('FD', f'{year}FD.zip')]:
                url = f"https://disclosures-clerk.house.gov/public_disc/{zip_name}"
                
                try:
                    resp = self.session.get(url, timeout=60)
                    if resp.status_code != 200:
                        continue
                    
                    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                        for name in zf.namelist():
                            if name.endswith('.xml'):
                                with zf.open(name) as xf:
                                    tree = ET.parse(xf)
                                    root = tree.getroot()
                                    
                                    for member in root.findall('.//Member'):
                                        doc_id_str = member.findtext('DocID', '').strip()
                                        if not doc_id_str:
                                            continue
                                        
                                        filing_key = f"house_{doc_id_str}"
                                        if filing_key in self.seen_filings:
                                            continue
                                        
                                        # Extract fields
                                        last = member.findtext('Last', '').strip()
                                        first = member.findtext('First', '').strip()
                                        state_dst = member.findtext('StateDst', '').strip()
                                        filing_type = member.findtext('FilingType', '').strip()
                                        filing_date = member.findtext('FilingDate', '').strip()
                                        
                                        if not last or not state_dst:
                                            continue
                                        
                                        # Must have valid state code
                                        state_code = state_dst[:2] if len(state_dst) >= 2 else ''
                                        if state_code not in self.VALID_STATES:
                                            continue
                                        
                                        full_name = f"Hon. {first} {last}".strip()
                                        
                                        # Determine PDF URL
                                        if file_type == 'PTR':
                                            pdf_path = 'ptr-pdfs'
                                        else:
                                            pdf_path = 'financial-pdfs'
                                        pdf_url = f"https://disclosures-clerk.house.gov/public_disc/{pdf_path}/{year}/{doc_id_str}.pdf"
                                        
                                        try:
                                            doc_id_int = int(doc_id_str)
                                        except ValueError:
                                            doc_id_int = 0
                                        
                                        filings.append({
                                            'doc_id': doc_id_int,
                                            'full_name': full_name,
                                            'first_name': first,
                                            'last_name': last,
                                            'state_dst': state_dst,
                                            'file_type': file_type,
                                            'filing_type': filing_type,
                                            'filing_date': filing_date,
                                            'url': pdf_url,
                                            'year': year,
                                            'chamber': 'House',
                                        })
                
                except Exception as e:
                    self.logger.warning(f"ZIP download error for {zip_name}: {e}")
        
        self.logger.info(f"House ZIP: found {len(filings)} unseen filings")
        return filings
    
    # ========== SENATE ==========
    
    def _get_senate_filings(self) -> List[Dict]:
        """Get Senate filings from efdsearch.senate.gov."""
        filings = []
        
        try:
            # Get CSRF token
            landing = self.session.get('https://efdsearch.senate.gov/search/', timeout=15)
            if landing.status_code != 200:
                self.logger.warning(f"Senate landing page returned {landing.status_code}")
                return filings
            
            csrf_token = None
            soup = BeautifulSoup(landing.text, 'html.parser')
            csrf_input = soup.find('input', {'name': 'csrfmiddlewaretoken'})
            if csrf_input:
                csrf_token = csrf_input.get('value', '')
            
            if not csrf_token:
                # Try cookie
                csrf_token = self.session.cookies.get('csrftoken', '')
            
            if not csrf_token:
                self.logger.warning("Could not get Senate CSRF token")
                return filings
            
            # Search for recent PTR filings
            search_url = 'https://efdsearch.senate.gov/search/report/data/'
            headers = {
                'X-CSRFToken': csrf_token,
                'Referer': 'https://efdsearch.senate.gov/search/',
                'Content-Type': 'application/x-www-form-urlencoded',
            }
            
            payload = {
                'start': '0',
                'length': '100',
                'report_types': '[11]',  # PTR Original
                'filer_types': '[1]',    # Senator only
                'submitted_start_date': (datetime.now() - timedelta(days=30)).strftime('%m/%d/%Y'),
                'submitted_end_date': datetime.now().strftime('%m/%d/%Y'),
            }
            
            resp = self.session.post(search_url, data=payload, headers=headers, timeout=15)
            if resp.status_code != 200:
                self.logger.warning(f"Senate search returned {resp.status_code}")
                return filings
            
            data = resp.json()
            records = data.get('data', [])
            
            for record in records:
                try:
                    # Parse record: [first, last, filer_type, report_html, filing_date]
                    if len(record) < 5:
                        continue
                    
                    first_name = record[0].strip() if record[0] else ''
                    last_name = record[1].strip() if record[1] else ''
                    filer_type = record[2].strip() if record[2] else ''
                    report_html = record[3] if record[3] else ''
                    filing_date = record[4].strip() if record[4] else ''
                    
                    # Only Senators
                    if 'Senator' not in filer_type:
                        continue
                    
                    # Extract doc ID from URL
                    doc_id_match = re.search(r'/(?:ptr|annual|paper)/([a-f0-9-]+)/', report_html)
                    doc_id = doc_id_match.group(1) if doc_id_match else ''
                    
                    if not doc_id:
                        continue
                    
                    filing_key = f"senate_{doc_id}"
                    if filing_key in self.seen_filings:
                        continue
                    
                    # Determine type from URL
                    if '/ptr/' in report_html:
                        file_type = 'PTR'
                        doc_url = f"https://efdsearch.senate.gov/search/view/ptr/{doc_id}/"
                    elif '/annual/' in report_html:
                        file_type = 'Annual'
                        doc_url = f"https://efdsearch.senate.gov/search/view/annual/{doc_id}/"
                    else:
                        file_type = 'Other'
                        doc_url = ''
                    
                    full_name = f"{first_name} {last_name}".strip()
                    
                    filings.append({
                        'doc_id': doc_id,
                        'full_name': full_name,
                        'first_name': first_name,
                        'last_name': last_name,
                        'state_dst': 'Senate',
                        'file_type': file_type,
                        'filing_date': filing_date,
                        'url': doc_url,
                        'year': CURRENT_YEAR,
                        'chamber': 'Senate',
                    })
                    
                except Exception as e:
                    if DEBUG:
                        self.logger.debug(f"Senate record parse error: {e}")
                    continue
            
            self.logger.info(f"Senate: found {len(filings)} unseen filings")
            
        except Exception as e:
            self.logger.error(f"Senate check error: {e}")
        
        return filings
    
    # ========== ALERT & CHECK ==========
    
    def _send_filing_alert(self, filing: Dict):
        """Send a Discord alert for a new filing."""
        chamber_emoji = "🏠" if filing['chamber'] == 'House' else "🏛️"
        
        # Color by type
        if filing['file_type'] == 'PTR':
            color = 0x00FF00  # Green for stock trades
            type_label = "📈 Periodic Transaction Report (Stock Trade)"
        elif filing['file_type'] == 'FD':
            color = 0xFFAA00  # Orange for financial disclosure
            type_label = "📋 Financial Disclosure"
        else:
            color = 0x0099FF
            type_label = f"📄 {filing['file_type']}"
        
        fields = [
            {"name": "Member", "value": filing['full_name'], "inline": True},
            {"name": "Chamber", "value": f"{chamber_emoji} {filing['chamber']}", "inline": True},
            {"name": "Type", "value": type_label, "inline": False},
        ]
        
        if filing.get('state_dst') and filing['state_dst'] != 'Senate':
            fields.append({"name": "State/District", "value": filing['state_dst'], "inline": True})
        
        if filing.get('filing_date'):
            fields.append({"name": "Filed", "value": filing['filing_date'], "inline": True})
        
        if filing.get('url'):
            fields.append({"name": "Document", "value": f"[View Filing]({filing['url']})", "inline": False})
        
        embed = {
            "title": f"{chamber_emoji} New Congressional Disclosure",
            "color": color,
            "fields": fields,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "Congress Disclosure Monitor"}
        }
        
        send_discord_webhook(embed, self.logger)
    
    def check_filings(self):
        """Main check cycle: probe House, check ZIP backup, check Senate."""
        try:
            # Load members if needed
            if not self.members:
                self.members = load_current_members(DATA_DIR, self.logger)
            
            new_count = 0
            
            # === HOUSE: Primary method - PDF probing ===
            house_probe_filings = self._probe_house_filings()
            for filing in house_probe_filings:
                filing_key = f"house_{filing['doc_id']}"
                if filing_key not in self.seen_filings:
                    self.seen_filings.add(filing_key)
                    self._send_filing_alert(filing)
                    new_count += 1
                    time.sleep(1)
            
            # === HOUSE: Backup method - XML ZIP (runs every 10th cycle) ===
            cycle_count = getattr(self, '_cycle_count', 0) + 1
            self._cycle_count = cycle_count
            
            if cycle_count % 10 == 0:
                self.logger.info("Running House ZIP backup check...")
                house_zip_filings = self._get_house_zip_filings()
                for filing in house_zip_filings:
                    filing_key = f"house_{filing['doc_id']}"
                    if filing_key not in self.seen_filings:
                        # Verify it's a current member
                        if is_current_member(filing['full_name'], filing['last_name'], self.members):
                            self.seen_filings.add(filing_key)
                            self._send_filing_alert(filing)
                            new_count += 1
                            time.sleep(1)
                        else:
                            self.seen_filings.add(filing_key)  # Still mark as seen
                
                # Update highest doc ID from ZIP data
                for filing in house_zip_filings:
                    doc_id = filing.get('doc_id', 0)
                    if isinstance(doc_id, int) and doc_id > self.highest_house_doc_id:
                        self.highest_house_doc_id = doc_id
            
            # === SENATE ===
            senate_filings = self._get_senate_filings()
            for filing in senate_filings:
                filing_key = f"senate_{filing['doc_id']}"
                if filing_key not in self.seen_filings:
                    if is_current_member(filing['full_name'], filing['last_name'], self.members):
                        self.seen_filings.add(filing_key)
                        self._send_filing_alert(filing)
                        new_count += 1
                        time.sleep(1)
                    else:
                        self.seen_filings.add(filing_key)
            
            if new_count > 0:
                self.logger.info(f"Total new filings this cycle: {new_count}")
            
            self._save_seen()
            
        except Exception as e:
            self.logger.error(f"Check cycle error: {e}")
            import traceback
            traceback.print_exc()
    
    def run(self):
        self.running = True
        self.logger.info("Congress disclosure monitor started")
        
        # Load members on startup
        self.members = load_current_members(DATA_DIR, self.logger)
        
        while self.running:
            try:
                self.check_filings()
            except Exception as e:
                self.logger.error(f"Loop error: {e}")
            
            for _ in range(CONGRESS_CHECK_INTERVAL * 10):
                if not self.running:
                    break
                time.sleep(0.1)
    
    def start(self):
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()
    
    def stop(self):
        self.running = False


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 60)
    print("  Combined Government Activity Monitor")
    print("=" * 60)
    print(f"  Data dir: {DATA_DIR}")
    print(f"  Webhooks: {len(DISCORD_WEBHOOK_URLS)} configured")
    print(f"  USASpending interval: {USASPENDING_CHECK_INTERVAL}s")
    print(f"  Congress interval: {CONGRESS_CHECK_INTERVAL}s")
    print(f"  Debug: {DEBUG}")
    print("=" * 60)
    
    # Send startup notification
    startup_embed = {
        "title": "🟢 Government Activity Monitor Started",
        "description": "Monitoring USASpending.gov and Congressional disclosures.",
        "color": 0x00FF00,
        "fields": [
            {"name": "USASpending", "value": f"Every {USASPENDING_CHECK_INTERVAL}s", "inline": True},
            {"name": "Congress", "value": f"Every {CONGRESS_CHECK_INTERVAL}s", "inline": True},
        ],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "footer": {"text": "Government Activity Monitor"}
    }
    send_discord_webhook(startup_embed)
    
    usaspending_monitor = USASpendingMonitor()
    congress_monitor = CongressDisclosureMonitor()
    
    usaspending_monitor.start()
    congress_monitor.start()
    
    print("\n[INFO] All monitors running. Press Ctrl+C to stop.\n")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[INFO] Shutting down...")
        usaspending_monitor.stop()
        congress_monitor.stop()
        
        shutdown_embed = {
            "title": "🛑 Government Activity Monitor Stopped",
            "description": "Monitor has been stopped.",
            "color": 0xFF0000,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "footer": {"text": "Government Activity Monitor"}
        }
        send_discord_webhook(shutdown_embed)
        
        print("[INFO] Shutdown complete.")


if __name__ == "__main__":
    main()
