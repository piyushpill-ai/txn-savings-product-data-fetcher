"""
Transaction and Savings Account Product Data Fetcher
Fetches Transaction & Savings account products from the Consumer Data Right APIs.
Each product is expanded into one row per balance tier.

Max Rate logic:
  - If an INTRODUCTORY rate exists for the tier → Max Rate = INTRODUCTORY
    (INTRODUCTORY is a standalone total rate, not additive to base)
  - Otherwise → Max Rate = VARIABLE (base) + BONUS
"""

import re
import requests
import sqlite3
import os
from datetime import datetime, timezone

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "products.db")

# Banks to track - add more entries here to expand coverage
BANKS = {
    "Bankwest": {
        "base_url": "https://open-api.bankwest.com.au/bwpublic",
    },
    "CommBank": {
        "base_url": "https://api.commbank.com.au/public",
    },
    "ANZ": {
        "base_url": "https://api.anz",
    },
    "ANZ Plus": {
        "base_url": "https://cdr.apix.anz",
    },
    "NAB": {
        "base_url": "https://openbank.api.nab.com.au",
    },
    "Westpac": {
        "base_url": "https://digital-api.westpac.com.au",
    },
    "Macquarie Bank": {
        "base_url": "https://api.macquariebank.io",
    },
    "ING": {
        "base_url": "https://id.ob.ing.com.au",
    },
    "Rabobank": {
        "base_url": "https://openbanking.api.rabobank.com.au/public",
    },
    "MyState Bank": {
        "base_url": "https://public.cdr.mystate.com.au",
    },
    "IMB Bank": {
        "base_url": "https://openbank.openbanking.imb.com.au",
    },
    "P&N Bank": {
        "base_url": "https://public.cdr-api.pnbank.com.au",
    },
    "BCU Bank": {
        "base_url": "https://public.cdr-api.bcu.com.au",
    },
    "ubank": {
        "base_url": "https://public.cdr-api.86400.com.au",
    },
    "Judo Bank": {
        "base_url": "https://public.open.judo.bank",
    },
    "Bendigo Bank": {
        "base_url": "https://api.cdr.bendigobank.com.au",
    },
    "Bank of Queensland": {
        "base_url": "https://api.cds.boq.com.au",
    },
    "Up Bank": {
        "base_url": "https://api.up.com.au",
    },
    "MOVE Bank": {
        "base_url": "https://openbanking.movebank.com.au/OpenBanking",
    },
    "First Option Bank": {
        "base_url": "https://internetbanking.firstoption.com.au/OpenBanking",
    },
    "Australian Mutual Bank": {
        "base_url": "https://internetbanking.australianmutual.bank/openbanking",
    },
    "Great Southern Bank": {
        "base_url": "https://api.open-banking.greatsouthernbank.com.au",
    },
    "Newcastle Permanent": {
        "base_url": "https://openbank.newcastlepermanent.com.au",
    },
    "Greater Bank": {
        "base_url": "https://public.cdr.greater.com.au",
    },
    "HSBC": {
        "base_url": "https://public.ob.hsbc.com.au",
    },
    "ME Bank": {
        "base_url": "https://public.openbank.mebank.com.au",
    },
    "Virgin Money": {
        "base_url": "https://api.cds.virginmoney.com.au",
    },
    "St.George Bank": {
        "base_url": "https://digital-api.stgeorge.com.au",
    },
    "Bank of Melbourne": {
        "base_url": "https://digital-api.bankofmelbourne.com.au",
    },
    "BankSA": {
        "base_url": "https://digital-api.banksa.com.au",
    },
    "RACQ Bank": {
        "base_url": "https://cdrbank.racq.com.au",
    },
}

PRODUCT_CATEGORY = "TRANS_AND_SAVINGS_ACCOUNTS"

# CDR API version headers — some banks only support newer versions for
# the Get Product Detail endpoint, so we try multiple versions on 406.
BASE_HEADERS = {"User-Agent": "TxnSavingsProductFetcher/1.0"}
PRODUCT_LIST_HEADERS = {**BASE_HEADERS, "x-v": "4", "x-min-v": "1"}
PRODUCT_DETAIL_VERSIONS = ["4", "5", "6", "3"]


# ---------------------------------------------------------------------------
# Bonus condition parser
# ---------------------------------------------------------------------------

def parse_bonus_conditions(text):
    """Parse free-text bonus conditions into structured fields.

    Returns dict with:
      deposit_condition   – dollar amount required (e.g. "$10") or "$" if generic, else ""
      withdrawal_condition – "YES" if no withdrawals allowed, else "NO"
      transaction_condition – number of transactions (e.g. "5") or "Yes" if generic, else ""
      other_conditions    – leftover text that doesn't fit above categories
    """
    empty = {"deposit_condition": "", "withdrawal_condition": "NO",
             "transaction_condition": "", "other_conditions": ""}
    if not text:
        return empty

    lower = text.lower()

    # Skip pure intro-period descriptions — the Term column already covers these
    if re.match(r'^(?:p\d+m|\d+\s*month)', lower):
        return empty
    if re.match(r'^(?:available\s+)?(?:for\s+)?(?:new\s+)?(?:customers?\s+)?for\s+\d+\s+month', lower):
        return {**empty, "other_conditions": text}
    if re.match(r'^(?:fixed\s+)?(?:bonus\s+)?(?:margin|rate)\s+for\s+the\s+first', lower):
        return {**empty, "other_conditions": text}
    if re.match(r'^(?:in\s+addition|introductory)', lower):
        return {**empty, "other_conditions": text}
    if re.match(r'^(?:kick\s*start|first\s+\d+)', lower):
        return {**empty, "other_conditions": text}
    if re.match(r'^for\s+the\s+first\s+(?:six|four|five|\d+)\s+month', lower):
        return {**empty, "other_conditions": text}

    deposit = ""
    withdrawal = "NO"
    transactions = ""

    # --- Deposit condition ---
    # Specific dollar amounts in various patterns
    dep_match = re.search(
        r'(?:deposit|grow\b.*?savings\b.*?by)\s+(?:of\s+)?'
        r'(?:at\s+least\s+)?(?:a\s+)?(?:minimum\s+)?(?:an?\s+)?(?:eligible\s+)?'
        r'\$(\d[\d,]*)',
        lower
    )
    if not dep_match:
        dep_match = re.search(r'minimum\s+\$(\d[\d,]*)\s*(?:deposit|deposited)', lower)
    if not dep_match:
        dep_match = re.search(r'(?:eligible\s+)?deposit\s+of\s+\$(\d[\d,]*)', lower)
    if dep_match:
        deposit = f"${dep_match.group(1).replace(',', '')}"
    elif re.search(r'make\s+(?:a\s+)?(?:single\s+)?deposit|make\s+at\s+least\s+one\s+deposit', lower):
        deposit = "$"
    elif re.search(r'deposits?\s*\(excluding\s+interest\)\s*exceeds?', lower):
        deposit = "$"

    # --- Withdrawal condition ---
    if re.search(r'no\s+withdrawal', lower):
        withdrawal = "YES"
    elif re.search(r'balance\s+is\s+higher\s+at\s+the\s+end', lower):
        withdrawal = "YES"
    elif re.search(r'any\s+withdrawals?\s.*?(?:closed|forfeit)', lower):
        withdrawal = "YES"

    # --- Transaction condition ---
    # Search backwards from "transaction(s)" / "purchase(s)" to find a count
    txn_kw = re.search(r'(transactions?|purchases?)', lower)
    if txn_kw:
        before_txn = lower[:txn_kw.start()]
        # Look for "make 5 or more", "5 or more eligible", etc. near the keyword
        num_match = re.search(r'(?<!\$)(\d+)\s+(?:or\s+more\s+)?(?:eligible\s+)?[^.;]{0,60}$', before_txn)
        if num_match:
            transactions = num_match.group(1)
    if not transactions and re.search(r'(?:eligible|debit\s+card|visa|eftpos)\s+.*?(?:transactions?|purchases?)', lower):
        transactions = "Yes"

    # --- Other conditions ---
    has_structured = deposit or withdrawal == "YES" or transactions
    if has_structured:
        # Only include text that adds genuinely new info beyond what we parsed
        other = text
        # Strip deposit clause
        other = re.sub(r'(?i)(?:make\s+)?(?:a\s+)?(?:single\s+)?(?:minimum\s+)?(?:eligible\s+)?'
                       r'deposit\s+(?:of\s+)?(?:at\s+least\s+)?(?:a\s+)?(?:minimum\s+)?'
                       r'\$[\d,]+(?:\s+or\s+more)?[^.;]*[.;]?\s*', '', other).strip()
        other = re.sub(r'(?i)minimum\s+\$[\d,]+\s*deposit(?:ed)?[^.;]*[.;]?\s*', '', other).strip()
        # Strip withdrawal clause
        other = re.sub(r'(?i)(?:and\s+)?no\s+withdrawals?[^.;]*[.;]?\s*', '', other).strip()
        # Strip transaction clause
        other = re.sub(r'(?i)(?:and\s+)?(?:make\s+)?\d+\s+(?:or\s+more\s+)?(?:eligible\s+)?'
                       r'(?:[\w\s]*?)?(?:transactions?|purchases?)[^.;]*[.;]?\s*', '', other).strip()
        # Strip generic deposit mentions
        other = re.sub(r'(?i)(?:make\s+)?(?:a\s+)?(?:single\s+)?deposit\s+(?:to|into)\s+[^.;]*[.;]?\s*', '', other).strip()
        other = re.sub(r'(?i)(?:the\s+)?total\s+amount\s+of\s+deposits?[^.;]*[.;]?\s*', '', other).strip()
        other = re.sub(r'(?i)ensure\s+.*?balance\s+is\s+higher[^.;]*[.;]?\s*', '', other).strip()
        other = re.sub(r'(?i)keep\s+.*?balance\s+above[^.;]*[.;]?\s*', '', other).strip()
        # Cleanup
        other = re.sub(r'^[\s,;:and]+', '', other).strip()
        other = re.sub(r'[\s,;:.]+$', '', other).strip()
        # If what remains is very short or just filler, drop it
        if len(other) < 20 or re.match(
                r'^[\s\W]*(and|when|if|you|that|the|bonus|interest|per|each|is|payable|'
                r'month|for|to|in|on|of|at|with|your|are|be|will|it|a|an)[\s\W]*$',
                other, re.I):
            other = ""
    else:
        other = text

    return {
        "deposit_condition": deposit,
        "withdrawal_condition": withdrawal,
        "transaction_condition": transactions,
        "other_conditions": other,
    }


# ---------------------------------------------------------------------------
# Term cleanup
# ---------------------------------------------------------------------------

def clean_term(raw_term):
    """Extract a clean numeric month value from term strings.

    'P5M' → '5', '4 month variable introductory rate' → '4',
    'P1M' → '1', 'Variable' → '', 'Introductory' → ''
    """
    if not raw_term:
        return ""

    raw = raw_term.strip()

    # ISO 8601 duration: P5M, P12M, P1M etc.
    iso_match = re.match(r'^P(\d+)M$', raw, re.IGNORECASE)
    if iso_match:
        return iso_match.group(1)

    # "4 month..." or "four months..." — extract leading number
    num_match = re.match(r'^(\d+)\s*month', raw, re.IGNORECASE)
    if num_match:
        return num_match.group(1)

    # Written-out numbers
    word_nums = {
        "one": "1", "two": "2", "three": "3", "four": "4", "five": "5",
        "six": "6", "seven": "7", "eight": "8", "nine": "9", "ten": "10",
        "eleven": "11", "twelve": "12",
    }
    word_match = re.match(r'^(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*month',
                          raw, re.IGNORECASE)
    if word_match:
        return word_nums[word_match.group(1).lower()]

    # "first 4 months" or "first four months" buried in text
    mid_match = re.search(r'(?:first|initial)\s+(\d+)\s*month', raw, re.IGNORECASE)
    if mid_match:
        return mid_match.group(1)
    mid_word = re.search(
        r'(?:first|initial)\s+(one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s*month',
        raw, re.IGNORECASE)
    if mid_word:
        return word_nums[mid_word.group(1).lower()]

    # If it's just "Variable" or "Introductory" with no month info
    if raw.lower() in ("variable", "introductory"):
        return ""

    return ""


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS product_tiers (
            id TEXT PRIMARY KEY,
            product_id TEXT NOT NULL,
            bank_name TEXT NOT NULL,
            product_name TEXT NOT NULL,
            description TEXT,
            min_balance REAL,
            max_balance REAL,
            max_interest_rate REAL,
            max_interest_rate_term TEXT,
            base_rate REAL,
            bonus_rate REAL,
            bonus_deposit_condition TEXT DEFAULT '',
            bonus_withdrawal_condition TEXT DEFAULT 'NO',
            bonus_transaction_condition TEXT DEFAULT '',
            bonus_other_conditions TEXT DEFAULT '',
            product_category TEXT,
            last_updated TEXT,
            fetched_at TEXT,
            prev_base_rate REAL,
            prev_bonus_rate REAL,
            prev_max_interest_rate REAL,
            change_log TEXT DEFAULT '',
            change_detected_at TEXT
        )
    """)
    # Migrate: add new columns if they don't exist (safe for re-runs)
    for col_def in [
        ("bonus_deposit_condition", "TEXT DEFAULT ''"),
        ("bonus_withdrawal_condition", "TEXT DEFAULT 'NO'"),
        ("bonus_transaction_condition", "TEXT DEFAULT ''"),
        ("bonus_other_conditions", "TEXT DEFAULT ''"),
        ("prev_base_rate", "REAL"),
        ("prev_bonus_rate", "REAL"),
        ("prev_max_interest_rate", "REAL"),
        ("change_log", "TEXT DEFAULT ''"),
        ("change_detected_at", "TEXT"),
    ]:
        col, spec = col_def
        try:
            c.execute(f"ALTER TABLE product_tiers ADD COLUMN {col} {spec}")
        except sqlite3.OperationalError:
            pass  # column already exists
    c.execute("""
        CREATE TABLE IF NOT EXISTS fetch_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            bank_name TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            products_count INTEGER,
            status TEXT,
            error TEXT
        )
    """)
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# CDR API fetching
# ---------------------------------------------------------------------------

def fetch_product_list(base_url):
    url = f"{base_url}/cds-au/v1/banking/products?product-category={PRODUCT_CATEGORY}&page-size=25"
    all_products = []

    while url:
        resp = requests.get(url, headers=PRODUCT_LIST_HEADERS, timeout=30)
        if resp.status_code == 406 or resp.status_code == 403:
            for v in PRODUCT_DETAIL_VERSIONS:
                resp = requests.get(url, headers={**BASE_HEADERS, "x-v": v}, timeout=30)
                if resp.status_code == 200:
                    break
        resp.raise_for_status()
        data = resp.json()
        products = data.get("data", {}).get("products", [])
        all_products.extend(products)

        next_url = data.get("links", {}).get("next")
        url = next_url if next_url and next_url != url else None

    return all_products


def fetch_product_detail(base_url, product_id):
    url = f"{base_url}/cds-au/v1/banking/products/{product_id}"
    for v in PRODUCT_DETAIL_VERSIONS:
        resp = requests.get(url, headers={**BASE_HEADERS, "x-v": v}, timeout=30)
        if resp.status_code != 406:
            break
    resp.raise_for_status()
    return resp.json().get("data", {})


# ---------------------------------------------------------------------------
# Tier helpers
# ---------------------------------------------------------------------------

def _is_intro_additive(intro_info, intro_rate, variable_rate):
    """Decide whether an INTRODUCTORY rate is an additive bonus margin
    or a standalone total rate.

    Most banks publish INTRODUCTORY as a total rate (Bankwest, Rabobank,
    Macquarie, MyState), but some publish it as a bonus margin that adds on
    top of the base variable rate (CommBank, ING, IMB).

    Heuristics:
      1. If the info text contains margin/bonus keywords → additive
      2. If intro_rate < variable_rate → additive (intro wouldn't be lower)
      3. Otherwise → standalone
    """
    info_lower = (intro_info or "").lower()

    additive_keywords = [
        "bonus margin",
        "fixed bonus",
        "kick starter", "kick start",
        "on top of",
        "additional bonus",
        "in addition",
    ]
    for kw in additive_keywords:
        if kw in info_lower:
            return True

    # Standalone "margin" as a whole word
    if re.search(r"\bmargin\b", info_lower):
        return True

    # If intro is lower than base, it has to be additive
    if variable_rate > 0 and intro_rate < variable_rate:
        return True

    return False


def _distribute_additive_intros(tier_data):
    """Split additive INTRODUCTORY tiers across variable tier boundaries.

    When an additive intro (bonus margin) applies to a broad balance range
    that spans multiple VARIABLE rate tiers, the intro should be distributed
    across each variable tier it covers.

    Example — ING Savings Accelerator:
      VARIABLE: 2.75% ($0-$50k), 3.65% ($50k-$150k), 4.60% ($150k+)
      INTRODUCTORY: 1.05% ($0-$500k, additive "Kick starter offer")

    Produces sub-tiers:
      $0-$50k:       base 2.75% + intro 1.05% = 3.80%
      $50k-$150k:    base 3.65% + intro 1.05% = 4.70%
      $150k-$500k:   base 4.60% + intro 1.05% = 5.65%
      $500k+:        base 4.60%  (no intro)
    """
    if not tier_data:
        return

    INF = float("inf")

    # Find additive intro-only tiers (no variable set yet)
    additive_intros = []
    for key, t in list(tier_data.items()):
        if t["intro_rate"] is None or t["variable_rate"] > 0:
            continue
        if _is_intro_additive(t["intro_info"], t["intro_rate"], 0):
            additive_intros.append((key, t))

    for intro_key, intro in additive_intros:
        i_min = intro["min_balance"] or 0
        i_max = intro["max_balance"]
        i_max_eff = INF if i_max is None else i_max

        # Collect variable-only tiers (snapshot — we mutate tier_data below)
        variable_tiers = [(k, dict(t)) for k, t in tier_data.items()
                          if t["variable_rate"] > 0 and t["intro_rate"] is None]

        overlapping = []
        for v_key, v_tier in variable_tiers:
            v_min = v_tier["min_balance"] or 0
            v_max = v_tier["max_balance"]
            v_max_eff = INF if v_max is None else v_max
            if v_min < i_max_eff and v_max_eff > i_min:
                overlapping.append((v_key, v_tier))

        if not overlapping:
            continue  # leave intro-only tier; base inheritance will handle it

        # Remove the original intro-only tier
        del tier_data[intro_key]

        for v_key, v_tier in overlapping:
            v_min = v_tier["min_balance"] or 0
            v_max = v_tier["max_balance"]
            v_max_eff = INF if v_max is None else v_max

            # Intersection of the intro range and this variable tier
            overlap_min = max(v_min, i_min)
            overlap_max_eff = min(v_max_eff, i_max_eff)
            overlap_max = None if overlap_max_eff == INF else overlap_max_eff

            # Piece covered by intro: existing variable tier gets intro applied
            if overlap_min == v_min and overlap_max == v_max:
                # Full coverage — just add intro to the existing variable tier
                tier_data[v_key]["intro_rate"] = intro["intro_rate"]
                tier_data[v_key]["intro_info"] = intro["intro_info"]
                tier_data[v_key]["intro_term"] = intro["intro_term"]
            else:
                # Partial coverage — split the variable tier
                del tier_data[v_key]

                # Covered piece (with intro)
                covered_key = (overlap_min, overlap_max)
                tier_data[covered_key] = {
                    "min_balance": overlap_min,
                    "max_balance": overlap_max,
                    "variable_rate": v_tier["variable_rate"],
                    "bonus_rate": v_tier["bonus_rate"],
                    "bonus_conditions": v_tier["bonus_conditions"],
                    "intro_rate": intro["intro_rate"],
                    "intro_info": intro["intro_info"],
                    "intro_term": intro["intro_term"],
                }

                # Piece below the intro range (pure variable, no intro)
                if v_min < overlap_min:
                    below_key = (v_min, overlap_min)
                    tier_data[below_key] = {
                        "min_balance": v_min,
                        "max_balance": overlap_min,
                        "variable_rate": v_tier["variable_rate"],
                        "bonus_rate": v_tier["bonus_rate"],
                        "bonus_conditions": v_tier["bonus_conditions"],
                        "intro_rate": None,
                        "intro_info": "",
                        "intro_term": "",
                    }

                # Piece above the intro range (pure variable, no intro)
                if v_max_eff > overlap_max_eff:
                    above_key = (overlap_max_eff, v_max)
                    tier_data[above_key] = {
                        "min_balance": overlap_max_eff,
                        "max_balance": v_max,
                        "variable_rate": v_tier["variable_rate"],
                        "bonus_rate": v_tier["bonus_rate"],
                        "bonus_conditions": v_tier["bonus_conditions"],
                        "intro_rate": None,
                        "intro_info": "",
                        "intro_term": "",
                    }


def _inherit_base_rates(tier_data):
    """Fill in missing VARIABLE (base) rates from broader tiers.

    Banks often publish a single VARIABLE rate with a wide or catch-all tier
    (e.g. $0-$5M or no tier at all) and separate BONUS rates with narrower
    tiers (e.g. $0-$250k).  Because the tier keys differ, the bonus tiers
    end up with variable_rate=0.  This function finds those gaps and copies
    the base rate from the covering tier.
    """
    if not tier_data:
        return

    # Collect tiers that have a base rate (potential donors)
    base_tiers = [(k, t) for k, t in tier_data.items()
                  if t["variable_rate"] > 0]

    # For each tier missing a base rate, find a donor whose range covers it
    for key, t in tier_data.items():
        if t["variable_rate"] > 0:
            continue  # already has a base rate
        if t["bonus_rate"] == 0 and t["intro_rate"] is None:
            continue  # nothing to combine with

        t_min = t["min_balance"] or 0
        t_max = t["max_balance"]  # None means unlimited

        for _bk, bt in base_tiers:
            b_min = bt["min_balance"] or 0
            b_max = bt["max_balance"]

            # Check if the base tier covers this tier's range
            if b_min <= t_min and (b_max is None or (t_max is not None and b_max >= t_max)):
                t["variable_rate"] = bt["variable_rate"]
                break
        else:
            # Fallback: if no tier strictly covers it, use the base tier
            # whose min is closest to 0 (the broadest catch-all)
            if base_tiers:
                best = min(base_tiers, key=lambda x: x[1]["min_balance"] or 0)
                t["variable_rate"] = best[1]["variable_rate"]


def _collapse_tiers(tier_data):
    """Collapse adjacent tiers that have identical rates into a single tier."""
    if not tier_data:
        return tier_data

    sorted_keys = sorted(tier_data.keys(), key=lambda k: k[0])
    collapsed = {}
    prev_key = None

    for key in sorted_keys:
        t = tier_data[key]
        rates_signature = (t["variable_rate"], t["bonus_rate"], t["intro_rate"])

        if prev_key is not None:
            prev_t = collapsed[prev_key]
            prev_sig = (prev_t["variable_rate"], prev_t["bonus_rate"], prev_t["intro_rate"])

            if rates_signature == prev_sig:
                prev_t["max_balance"] = t["max_balance"]
                continue

        collapsed[key] = dict(t)
        prev_key = key

    return collapsed


# ---------------------------------------------------------------------------
# Product parsing
# ---------------------------------------------------------------------------

def _to_float(val, default=None):
    """Coerce a possibly-string numeric field to float; return default on failure."""
    if val is None or val == "":
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def parse_product_tiers(bank_name, detail):
    """Parse a product detail into one row per balance tier."""
    # Some banks return depositRates=null rather than omitting the field
    deposit_rates = detail.get("depositRates") or []
    product_id = detail.get("productId", "")
    product_name = detail.get("name", "")
    description = detail.get("description", "")
    product_category = detail.get("productCategory", "")
    last_updated = detail.get("lastUpdated", "")

    tier_data = {}

    for rate_entry in deposit_rates:
        rate_type = rate_entry.get("depositRateType", "")
        rate_value = _to_float(rate_entry.get("rate"), 0) or 0
        tiers = rate_entry.get("tiers") or []
        additional_info = rate_entry.get("additionalInfo", "") or ""
        additional_value = rate_entry.get("additionalValue", "") or ""

        if not tiers:
            tiers = [{"minimumValue": 0, "maximumValue": None}]

        for tier in tiers:
            # CDR spec types these as numbers, but some banks return strings
            min_bal = _to_float(tier.get("minimumValue"), 0) or 0
            max_bal = _to_float(tier.get("maximumValue"), None)
            tier_key = (min_bal, max_bal)

            if tier_key not in tier_data:
                tier_data[tier_key] = {
                    "min_balance": min_bal,
                    "max_balance": max_bal,
                    "variable_rate": 0,
                    "bonus_rate": 0,
                    "bonus_conditions": "",
                    "intro_rate": None,
                    "intro_term": "",
                    "intro_info": "",
                }

            entry = tier_data[tier_key]

            if rate_type in ("VARIABLE", "FIXED", "FLOATING", "MARKET_LINKED"):
                entry["variable_rate"] = rate_value
            elif rate_type == "BONUS":
                entry["bonus_rate"] = rate_value
                entry["bonus_conditions"] = additional_value or additional_info
            elif rate_type == "INTRODUCTORY":
                entry["intro_rate"] = rate_value
                entry["intro_info"] = additional_info
                entry["intro_term"] = additional_value or additional_info

    # Split additive INTRO rates across variable tier boundaries first.
    # (e.g. ING "Kick starter offer" 1.05% covering $0-$500k, which spans
    # three different variable rate tiers 2.75%/3.65%/4.60%.)
    _distribute_additive_intros(tier_data)

    # Inherit base rates into tiers that have BONUS/INTRO but no VARIABLE.
    # Banks often publish the VARIABLE rate with a broad/catch-all tier and
    # BONUS rates with narrower tiers, so they land in different buckets.
    _inherit_base_rates(tier_data)

    collapsed = _collapse_tiers(tier_data)

    rows = []
    for tier_key, t in collapsed.items():
        base_rate = t["variable_rate"]
        bonus_rate = t["bonus_rate"]
        intro_rate = t["intro_rate"]

        if intro_rate is not None:
            if _is_intro_additive(t["intro_info"], intro_rate, base_rate):
                # INTRODUCTORY is a bonus margin added on top of the base
                # (e.g. CommBank NetBank Saver "Fixed bonus margin",
                #  ING Savings Accelerator "Kick starter offer",
                #  IMB "Introductory Fixed Bonus Interest Rate")
                max_rate = base_rate + intro_rate
                display_bonus = intro_rate
            else:
                # INTRODUCTORY is a standalone total rate
                # (e.g. Bankwest Easy Saver, Rabobank HISA, Macquarie Savings)
                max_rate = intro_rate
                display_bonus = intro_rate - base_rate
            raw_term = t["intro_term"] or "Introductory"
            raw_conditions = t["intro_info"] or t["bonus_conditions"]
        else:
            max_rate = base_rate + bonus_rate
            raw_term = "Variable"
            display_bonus = bonus_rate
            raw_conditions = t["bonus_conditions"]

        # Clean up term to just a number
        cleaned_term = clean_term(raw_term)

        # Parse bonus conditions into structured columns
        parsed_cond = parse_bonus_conditions(raw_conditions)

        tier_id = f"{product_id}_{t['min_balance']}_{t['max_balance']}"

        rows.append({
            "id": tier_id,
            "product_id": product_id,
            "bank_name": bank_name,
            "product_name": product_name,
            "description": description,
            "min_balance": t["min_balance"],
            "max_balance": t["max_balance"],
            "max_interest_rate": max_rate,
            "max_interest_rate_term": cleaned_term,
            "base_rate": base_rate,
            "bonus_rate": display_bonus,
            "bonus_deposit_condition": parsed_cond["deposit_condition"],
            "bonus_withdrawal_condition": parsed_cond["withdrawal_condition"],
            "bonus_transaction_condition": parsed_cond["transaction_condition"],
            "bonus_other_conditions": parsed_cond["other_conditions"],
            "product_category": product_category,
            "last_updated": last_updated,
        })

    if not rows:
        rows.append({
            "id": f"{product_id}_0_None",
            "product_id": product_id,
            "bank_name": bank_name,
            "product_name": product_name,
            "description": description,
            "min_balance": 0,
            "max_balance": None,
            "max_interest_rate": 0,
            "max_interest_rate_term": "",
            "base_rate": 0,
            "bonus_rate": 0,
            "bonus_deposit_condition": "",
            "bonus_withdrawal_condition": "NO",
            "bonus_transaction_condition": "",
            "bonus_other_conditions": "",
            "product_category": product_category,
            "last_updated": last_updated,
        })

    return rows


# ---------------------------------------------------------------------------
# Database persistence
# ---------------------------------------------------------------------------

def _format_rate_change(label, old, new):
    """Return 'Base rate +0.10%' style string, or None if no meaningful change."""
    if old is None:
        return None
    # Rates are stored as fractions (0.0485 = 4.85%). Compare to 0.01% granularity.
    if abs((new or 0) - old) < 0.0001:
        return None
    delta = (new or 0) - old
    sign = "+" if delta > 0 else ""
    return f"{label} {sign}{delta * 100:.2f}% ({old * 100:.2f}% → {(new or 0) * 100:.2f}%)"


def save_product_tiers(conn, rows):
    c = conn.cursor()
    now = datetime.now(timezone.utc).isoformat()
    for row in rows:
        # Compare with previous values for this tier to produce a change log
        prev = c.execute(
            "SELECT base_rate, bonus_rate, max_interest_rate, change_log, change_detected_at "
            "FROM product_tiers WHERE id = ?",
            (row["id"],)
        ).fetchone()

        change_parts = []
        if prev is not None:
            p_base, p_bonus, p_max, old_change_log, old_change_at = prev
            for label, old, new in [
                ("Max rate", p_max, row["max_interest_rate"]),
                ("Base rate", p_base, row["base_rate"]),
                ("Bonus rate", p_bonus, row["bonus_rate"]),
            ]:
                msg = _format_rate_change(label, old, new)
                if msg:
                    change_parts.append(msg)

        if change_parts:
            change_log = "; ".join(change_parts)
            change_detected_at = now
            prev_base = prev[0] if prev else None
            prev_bonus = prev[1] if prev else None
            prev_max = prev[2] if prev else None
        elif prev is not None:
            # No new changes — keep the previous change log (last known change)
            change_log = prev[3] or ""
            change_detected_at = prev[4]
            prev_base = prev[0]
            prev_bonus = prev[1]
            prev_max = prev[2]
        else:
            # First time seeing this tier — no change log yet
            change_log = ""
            change_detected_at = None
            prev_base = None
            prev_bonus = None
            prev_max = None

        c.execute("""
            INSERT OR REPLACE INTO product_tiers
            (id, product_id, bank_name, product_name, description,
             min_balance, max_balance, max_interest_rate, max_interest_rate_term,
             base_rate, bonus_rate,
             bonus_deposit_condition, bonus_withdrawal_condition,
             bonus_transaction_condition, bonus_other_conditions,
             product_category, last_updated, fetched_at,
             prev_base_rate, prev_bonus_rate, prev_max_interest_rate,
             change_log, change_detected_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["id"], row["product_id"], row["bank_name"], row["product_name"],
            row["description"], row["min_balance"], row["max_balance"],
            row["max_interest_rate"], row["max_interest_rate_term"],
            row["base_rate"], row["bonus_rate"],
            row["bonus_deposit_condition"], row["bonus_withdrawal_condition"],
            row["bonus_transaction_condition"], row["bonus_other_conditions"],
            row["product_category"], row["last_updated"], now,
            prev_base, prev_bonus, prev_max,
            change_log, change_detected_at,
        ))


def fetch_bank(bank_name, bank_config):
    base_url = bank_config["base_url"]
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] Fetching {bank_name}...")

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    try:
        product_list = fetch_product_list(base_url)
        tier_count = 0
        skipped = 0
        for prod_summary in product_list:
            product_id = prod_summary.get("productId")
            try:
                detail = fetch_product_detail(base_url, product_id)
                rows = parse_product_tiers(bank_name, detail)
                save_product_tiers(conn, rows)
                tier_count += len(rows)
            except Exception as prod_err:
                # Don't let a single bad product abort the whole bank
                skipped += 1
                print(f"  ! Skipping {product_id} ({prod_summary.get('name', '?')}): {prod_err}")
                continue

            for r in rows:
                bal_range = f"${r['min_balance']:,.0f}" if r['min_balance'] else "$0"
                if r['max_balance']:
                    bal_range += f" - ${r['max_balance']:,.0f}"
                else:
                    bal_range += "+"
                print(f"  - {r['product_name']} ({bal_range}): "
                      f"max={r['max_interest_rate']*100:.2f}% "
                      f"base={r['base_rate']*100:.2f}% "
                      f"bonus={r['bonus_rate']*100:.2f}%")

        conn.cursor().execute(
            "INSERT INTO fetch_log (bank_name, fetched_at, products_count, status) VALUES (?, ?, ?, ?)",
            (bank_name, datetime.now(timezone.utc).isoformat(), tier_count, "success"),
        )
        conn.commit()
        print(f"  Saved {tier_count} tier rows for {bank_name}")
        return tier_count

    except Exception as e:
        conn.cursor().execute(
            "INSERT INTO fetch_log (bank_name, fetched_at, products_count, status, error) VALUES (?, ?, ?, ?, ?)",
            (bank_name, datetime.now(timezone.utc).isoformat(), 0, "error", str(e)),
        )
        conn.commit()
        print(f"  ERROR fetching {bank_name}: {e}")
        return 0

    finally:
        conn.close()


def fetch_all():
    total = 0
    for bank_name, config in BANKS.items():
        total += fetch_bank(bank_name, config)
    print(f"\nTotal tier rows fetched: {total}")
    return total


if __name__ == "__main__":
    init_db()
    fetch_all()
