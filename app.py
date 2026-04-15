"""
Transaction and Savings Account Product Data Fetcher
Flask app serving a real-time dashboard of bank product rates.
"""

import sqlite3
import os
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template
from apscheduler.schedulers.background import BackgroundScheduler

from cdr_fetcher import fetch_all, init_db, DB_PATH

app = Flask(__name__)

# Initialize database and do first fetch on startup
init_db()


def scheduled_fetch():
    print(f"\n{'='*60}")
    print(f"Scheduled fetch at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"{'='*60}")
    fetch_all()


# Set up scheduler for 2-hour refresh
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_fetch, "interval", hours=2, id="cdr_fetch",
                  next_run_time=datetime.now(timezone.utc))
scheduler.start()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/products")
def api_products():
    conn = get_db()
    rows = conn.execute("""
        SELECT id, product_id, bank_name, product_name, description,
               min_balance, max_balance, max_interest_rate, max_interest_rate_term,
               base_rate, bonus_rate,
               bonus_deposit_condition, bonus_withdrawal_condition,
               bonus_transaction_condition, bonus_other_conditions,
               product_category, last_updated, fetched_at,
               change_log, change_detected_at
        FROM product_tiers
        ORDER BY bank_name, product_name, min_balance
    """).fetchall()
    conn.close()

    products = []
    for row in rows:
        products.append({
            "id": row["id"],
            "product_id": row["product_id"],
            "bank_name": row["bank_name"],
            "product_name": row["product_name"],
            "description": row["description"],
            "min_balance": row["min_balance"],
            "max_balance": row["max_balance"],
            "max_interest_rate": row["max_interest_rate"],
            "max_interest_rate_term": row["max_interest_rate_term"],
            "base_rate": row["base_rate"],
            "bonus_rate": row["bonus_rate"],
            "bonus_deposit_condition": row["bonus_deposit_condition"] or "",
            "bonus_withdrawal_condition": row["bonus_withdrawal_condition"] or "NO",
            "bonus_transaction_condition": row["bonus_transaction_condition"] or "",
            "bonus_other_conditions": row["bonus_other_conditions"] or "",
            "product_category": row["product_category"],
            "last_updated": row["last_updated"],
            "fetched_at": row["fetched_at"],
            "change_log": row["change_log"] or "",
            "change_detected_at": row["change_detected_at"],
        })

    return jsonify(products)


@app.route("/api/status")
def api_status():
    conn = get_db()
    rows = conn.execute("""
        SELECT bank_name, fetched_at, products_count, status, error
        FROM fetch_log
        ORDER BY fetched_at DESC
        LIMIT 20
    """).fetchall()
    conn.close()

    log = []
    for row in rows:
        log.append({
            "bank_name": row["bank_name"],
            "fetched_at": row["fetched_at"],
            "products_count": row["products_count"],
            "status": row["status"],
            "error": row["error"],
        })

    next_run = scheduler.get_job("cdr_fetch").next_run_time
    return jsonify({
        "log": log,
        "next_fetch": next_run.isoformat() if next_run else None,
    })


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    fetch_all()
    return jsonify({"status": "ok", "fetched_at": datetime.now(timezone.utc).isoformat()})


if __name__ == "__main__":
    app.run(debug=True, port=5050, use_reloader=False)
