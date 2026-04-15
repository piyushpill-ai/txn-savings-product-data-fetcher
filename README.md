# Transaction and Savings Account Product Data Fetcher

A live dashboard that fetches Transaction & Savings account product data from
Australian banks via the [Consumer Data Right (CDR)](https://consumerdatastandards.gov.au/)
APIs. Tracks 31 banks, refreshes every 2 hours, and detects rate changes between
fetches.

## What it shows

For each balance tier of every product:

- Bank name and product name
- Min / max balance for the tier
- **Max interest rate** — combined headline rate (base + bonus, or introductory)
- Term in months (for introductory rates)
- Base rate (VARIABLE)
- Bonus rate (BONUS or introductory bonus margin above base)
- Bonus conditions broken into structured fields:
  - Deposit requirement (e.g. `$100`, or `$` if generic)
  - No-withdrawals required (YES / NO)
  - Transaction count requirement (e.g. `5`, or `Yes`)
  - Other conditions (free text)
- Change log — diffs between fetches (e.g. `Base rate +0.10% (4.75% → 4.85%)`)

## Banks tracked

Bankwest, CommBank, ANZ, ANZ Plus, NAB, Westpac, Macquarie Bank, ING, Rabobank,
MyState Bank, IMB Bank, P&N Bank, BCU Bank, ubank, Judo Bank, Bendigo Bank,
Bank of Queensland, Up Bank, MOVE Bank, First Option Bank, Australian Mutual
Bank, Great Southern Bank, Newcastle Permanent, Greater Bank, HSBC, ME Bank,
Virgin Money, St.George Bank, Bank of Melbourne, BankSA, RACQ Bank.

## Setup

```bash
pip install -r requirements.txt
python3 app.py
```

Open <http://localhost:5050>.

The first fetch runs immediately on startup, then every 2 hours via APScheduler.
A `Refresh Now` button on the dashboard triggers a manual refresh.

## How rates are computed

The CDR API publishes deposit rates as separate entries by type (`VARIABLE`,
`BONUS`, `INTRODUCTORY`) with their own balance tiers. The fetcher merges them:

| Scenario | Max Rate |
|---|---|
| `VARIABLE` only | base rate |
| `VARIABLE` + `BONUS` (additive bonus condition) | base + bonus |
| `INTRODUCTORY` published as standalone total rate | intro rate |
| `INTRODUCTORY` published as additive bonus margin | base + intro |

Detection of additive vs standalone introductory rates uses the bank's own
description text (`"bonus margin"`, `"fixed bonus"`, `"kick starter"`, etc.)
plus the rule that an intro rate lower than the base rate must be additive.

For products where an additive intro spans multiple variable rate tiers (e.g.
ING Savings Accelerator), the intro is split across each variable tier so the
correct combined rate is shown per balance range.

For tier mismatches where banks publish the base `VARIABLE` rate at a broad or
catch-all tier and the `BONUS` at a narrower tier, the fetcher inherits the
base rate into the bonus tier.

## Architecture

```
cdr_fetcher.py   – CDR API client + parsing, change detection, SQLite persistence
app.py           – Flask server, APScheduler 2-hour job, JSON API endpoints
templates/
  dashboard.html – Single-page dashboard (filtering, sorting, change tracking)
products.db      – SQLite database (generated, gitignored)
```

### API endpoints

- `GET /` — dashboard
- `GET /api/products` — all tier rows as JSON
- `GET /api/status` — fetch log (most recent 20 entries) + next scheduled fetch
- `POST /api/refresh` — trigger an immediate refresh

## Adding more banks

Look up the bank's `publicBaseUri` in the
[CDR Register](https://api.cdr.gov.au/cdr-register/v1/all/data-holders/brands/summary)
(send `x-v: 1`), then add an entry to the `BANKS` dict in `cdr_fetcher.py`:

```python
BANKS = {
    ...
    "Some Other Bank": {
        "base_url": "https://public.cdr-api.example.com.au",
    },
}
```

The fetcher handles version negotiation across CDR API versions 3-6 and falls
back to `User-Agent` overrides for banks that block the default Python agent.
