from __future__ import annotations

import asyncio
import json
import os
import re
from collections import Counter, defaultdict
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

BUSINESS_TZ = ZoneInfo("Asia/Kolkata")
DISCOVERY_URL = os.getenv("LEDGER_DISCOVERY_URL", "").strip()
HTTP_TIMEOUT = float(os.getenv("LEDGER_HTTP_TIMEOUT", "45"))

DATA: dict[str, Any] = {
    "ready": False,
    "error": None,
    "orders": [],
    "rates": {"USD": 1.0},
    "products": [],
    "regions": [],
}
_LOAD_LOCK = asyncio.Lock()


class AnswerRequest(BaseModel):
    question: str = Field(min_length=1, max_length=2000)


def parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        raw = str(value).strip()
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=BUSINESS_TZ)
        return parsed.astimezone(BUSINESS_TZ)
    except (TypeError, ValueError):
        return None


async def request_json(client: httpx.AsyncClient, url: str) -> Any:
    response = await client.get(url, headers={"Accept": "application/json", "User-Agent": "LedgerAgent/1.0"})
    response.raise_for_status()
    return response.json()


async def load_ledger(force: bool = False) -> None:
    if DATA["ready"] and not force:
        return
    async with _LOAD_LOCK:
        if DATA["ready"] and not force:
            return
        if not DISCOVERY_URL:
            DATA["error"] = "LEDGER_DISCOVERY_URL is not configured"
            return
        try:
            async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, follow_redirects=True) as client:
                discovery = await request_json(client, DISCOVERY_URL)
                links = discovery.get("links", {}) if isinstance(discovery, dict) else {}
                export_url = links.get("export")
                rates_url = links.get("rates")
                if not export_url or not rates_url:
                    raise ValueError("Discovery response must provide export and rates links")

                export_response, rates = await asyncio.gather(
                    client.get(export_url, headers={"Accept": "application/x-ndjson", "User-Agent": "LedgerAgent/1.0"}),
                    request_json(client, rates_url),
                )
                export_response.raise_for_status()
                raw = export_response.text
                rows: list[dict[str, Any]] = []
                # The export endpoint returns newline-delimited JSON (one order per line).
                for line_number, line in enumerate(raw.splitlines(), start=1):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        # If a provider wraps NDJSON in a JSON document, handle that too.
                        if line_number == 1:
                            try:
                                obj = json.loads(raw)
                                if isinstance(obj, list):
                                    rows = [x for x in obj if isinstance(x, dict)]
                                    break
                            except json.JSONDecodeError:
                                pass
                        raise ValueError(f"Could not parse export line {line_number}")
                    if isinstance(item, dict):
                        rows.append(item)

                # Keep the latest version of each order ID, based on updated_at.
                latest: dict[str, dict[str, Any]] = {}
                for row in rows:
                    order_id = str(row.get("id", "")).strip()
                    if not order_id:
                        continue
                    current = latest.get(order_id)
                    if current is None:
                        latest[order_id] = row
                        continue
                    old_dt = parse_dt(current.get("updated_at"))
                    new_dt = parse_dt(row.get("updated_at"))
                    if new_dt and (not old_dt or new_dt > old_dt):
                        latest[order_id] = row

                rate_block = rates.get("usd_per_unit", {}) if isinstance(rates, dict) else {}
                clean_rates: dict[str, float] = {}
                for currency, amount in rate_block.items():
                    try:
                        clean_rates[str(currency).upper()] = float(amount)
                    except (ValueError, TypeError):
                        continue
                clean_rates.setdefault("USD", 1.0)

                orders = list(latest.values())
                DATA.update({
                    "ready": True,
                    "error": None,
                    "orders": orders,
                    "rates": clean_rates,
                    "products": sorted({str(x.get("product", "")).strip() for x in orders if x.get("product")}),
                    "regions": sorted({str(x.get("region", "")).strip() for x in orders if x.get("region")}),
                    "raw_count": len(rows),
                    "deduplicated_count": len(orders),
                })
                print(f"Ledger loaded: {len(rows)} exported rows, {len(orders)} current orders, rates={list(clean_rates)}")
        except Exception as exc:
            DATA["error"] = f"{type(exc).__name__}: {exc}"
            print(f"Ledger load failed: {DATA['error']}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Load data before requests arrive, so /answer can respond quickly.
    await load_ledger()
    yield


app = FastAPI(title="Acme Appliances Ledger Agent", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, Any]:
    return {
        "ok": True,
        "ledger_loaded": bool(DATA["ready"]),
        "orders": len(DATA["orders"]),
        "error": DATA["error"],
    }


def usd_amount(order: dict[str, Any]) -> float:
    try:
        amount = float(order.get("amount", 0) or 0)
        currency = str(order.get("currency", "USD")).upper()
        rate = float(DATA["rates"].get(currency, 1.0 if currency == "USD" else 0.0))
        return amount * rate
    except (TypeError, ValueError):
        return 0.0


def business_dt(order: dict[str, Any]) -> datetime | None:
    return parse_dt(order.get("created_at"))


def build_date_filter(question: str):
    q = question.lower()
    start: datetime | None = None
    end: datetime | None = None

    month_names = {
        "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
        "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
        "august": 8, "aug": 8, "september": 9, "sep": 9, "sept": 9, "october": 10,
        "oct": 10, "november": 11, "nov": 11, "december": 12, "dec": 12,
    }
    # Explicit quarter, e.g. Q1 2026.
    quarter_match = re.search(r"\bq([1-4])\s*(20\d{2})\b", q)
    if quarter_match:
        quarter, year = int(quarter_match.group(1)), int(quarter_match.group(2))
        month = (quarter - 1) * 3 + 1
        start = datetime(year, month, 1, tzinfo=BUSINESS_TZ)
        if month == 10:
            end = datetime(year + 1, 1, 1, tzinfo=BUSINESS_TZ)
        else:
            end = datetime(year, month + 3, 1, tzinfo=BUSINESS_TZ)
    else:
        # Month and year: March 2026, Mar-2026, 2026 March.
        month_match = re.search(r"\b(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sept|sep|october|oct|november|nov|december|dec)[\s,./-]+(20\d{2})\b", q)
        if month_match:
            month = month_names[month_match.group(1)]
            year = int(month_match.group(2))
            start = datetime(year, month, 1, tzinfo=BUSINESS_TZ)
            end = datetime(year + 1, 1, 1, tzinfo=BUSINESS_TZ) if month == 12 else datetime(year, month + 1, 1, tzinfo=BUSINESS_TZ)
        else:
            reverse_match = re.search(r"\b(20\d{2})[\s,./-]+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sept|sep|october|oct|november|nov|december|dec)\b", q)
            if reverse_match:
                year = int(reverse_match.group(1))
                month = month_names[reverse_match.group(2)]
                start = datetime(year, month, 1, tzinfo=BUSINESS_TZ)
                end = datetime(year + 1, 1, 1, tzinfo=BUSINESS_TZ) if month == 12 else datetime(year, month + 1, 1, tzinfo=BUSINESS_TZ)
            else:
                year_match = re.search(r"\b(20\d{2})\b", q)
                if year_match:
                    year = int(year_match.group(1))
                    start = datetime(year, 1, 1, tzinfo=BUSINESS_TZ)
                    end = datetime(year + 1, 1, 1, tzinfo=BUSINESS_TZ)

    # Explicit day/month/year range, for example 2026-03-01 to 2026-03-31.
    iso_dates = re.findall(r"\b(20\d{2}-\d{1,2}-\d{1,2})\b", q)
    if len(iso_dates) >= 2:
        try:
            d1, d2 = date.fromisoformat(iso_dates[0]), date.fromisoformat(iso_dates[1])
            start = datetime.combine(min(d1, d2), time.min, BUSINESS_TZ)
            end = datetime.combine(max(d1, d2) + timedelta(days=1), time.min, BUSINESS_TZ)
        except ValueError:
            pass

    if "last month" in q:
        today = datetime.now(BUSINESS_TZ).date().replace(day=1)
        last_end = today
        last_start_date = (today - timedelta(days=1)).replace(day=1)
        start = datetime.combine(last_start_date, time.min, BUSINESS_TZ)
        end = datetime.combine(last_end, time.min, BUSINESS_TZ)
    if "this month" in q:
        today = datetime.now(BUSINESS_TZ).date()
        start = datetime(today.year, today.month, 1, tzinfo=BUSINESS_TZ)
        end = datetime(today.year + (today.month == 12), (today.month % 12) + 1, 1, tzinfo=BUSINESS_TZ)
    if "last year" in q:
        year = datetime.now(BUSINESS_TZ).year - 1
        start, end = datetime(year, 1, 1, tzinfo=BUSINESS_TZ), datetime(year + 1, 1, 1, tzinfo=BUSINESS_TZ)

    if start is None:
        return None
    return lambda row: (business_dt(row) is not None and business_dt(row) >= start and (end is None or business_dt(row) < end))


def filter_by_question(question: str, base: list[dict[str, Any]]) -> list[dict[str, Any]]:
    q = question.lower()
    result = list(base)

    # Region match, using literal names seen in the API.
    for region in sorted(DATA["regions"], key=len, reverse=True):
        if region and re.search(r"\b" + re.escape(region.lower()) + r"\b", q):
            result = [row for row in result if str(row.get("region", "")).lower() == region.lower()]
            break

    # Match full API product names, preferring longest names first.
    for product in sorted(DATA["products"], key=len, reverse=True):
        if product and product.lower() in q:
            result = [row for row in result if str(row.get("product", "")).lower() == product.lower()]
            break

    # Customer identifiers are present in the order data (e.g. C1231).
    customer_match = re.search(r"\bc\d{3,6}\b", q, re.I)
    if customer_match:
        customer = customer_match.group(0).upper()
        result = [row for row in result if str(row.get("customer", "")).upper() == customer]

    date_filter = build_date_filter(question)
    if date_filter:
        result = [row for row in result if date_filter(row)]

    return result


def fmt_money(amount: float) -> str:
    # Amounts are normalized to USD and rounded to cents for the response.
    return f"{amount:,.2f} USD"


def rank_group(rows: list[dict[str, Any]], field: str, metric: str) -> tuple[str | None, float]:
    totals: dict[str, float] = defaultdict(float)
    for row in rows:
        key = str(row.get(field, "")).strip()
        if not key:
            continue
        if metric == "amount":
            value = usd_amount(row)
        elif metric == "qty":
            try:
                value = float(row.get("qty", 0) or 0)
            except (TypeError, ValueError):
                value = 0.0
        else:
            value = 1.0
        totals[key] += value
    if not totals:
        return None, 0.0
    # Stable tie-breaker for deterministic answers.
    best_key = sorted(totals, key=lambda k: (-totals[k], k))[0]
    return best_key, totals[best_key]


def answer_question(question: str) -> str:
    q = question.lower().strip()
    all_orders: list[dict[str, Any]] = DATA["orders"]
    asks_refund = any(term in q for term in ("refund", "refunded", "refunds"))
    asks_revenue = any(term in q for term in ("revenue", "sales", "earned", "earnings", "spent", "spend", "turnover"))
    asks_count = any(term in q for term in ("how many", "number of", "count of", "count", "quantity of"))
    asks_customer = "customer" in q or "customers" in q
    asks_product = "product" in q or "products" in q
    asks_order = "order" in q or "orders" in q
    asks_rank = any(term in q for term in ("most", "highest", "top", "best", "largest", "least", "lowest", "smallest", "popular"))
    asks_avg = any(term in q for term in ("average", "avg", "mean"))
    asks_rate = "rate" in q or "percentage" in q or "percent" in q or "%" in q
    asks_unit = any(term in q for term in ("units", "unit sold", "quantity sold", "how many items"))

    if asks_refund:
        cohort = [o for o in all_orders if str(o.get("status", "")).lower() == "refunded"]
    elif asks_revenue or "paid" in q:
        cohort = [o for o in all_orders if str(o.get("status", "")).lower() == "paid"]
    else:
        cohort = all_orders

    filtered = filter_by_question(question, cohort)

    # Refund rate/count/amount.
    if asks_refund:
        if asks_rate:
            all_filtered = filter_by_question(question, all_orders)
            refunded_count = len([o for o in all_filtered if str(o.get("status", "")).lower() == "refunded"])
            denominator = len(all_filtered)
            percentage = (100.0 * refunded_count / denominator) if denominator else 0.0
            return f"{percentage:.2f}%"
        if asks_count or "how much" not in q and "total" not in q and "amount" not in q and not asks_rank:
            if asks_rank and asks_product:
                product, value = rank_group(filtered, "product", "count")
                return product or "No matching records"
            if asks_rank and asks_customer:
                customer, value = rank_group(filtered, "customer", "count")
                return customer or "No matching records"
            return str(len(filtered))
        if asks_rank and asks_product:
            metric = "amount" if any(x in q for x in ("amount", "value", "cost", "dollar", "usd")) else "count"
            product, value = rank_group(filtered, "product", metric)
            return f"{product} ({fmt_money(value)})" if product and metric == "amount" else (product or "No matching records")
        if asks_rank and asks_customer:
            customer, value = rank_group(filtered, "customer", "amount")
            return f"{customer} ({fmt_money(value)})" if customer else "No matching records"
        return fmt_money(sum(usd_amount(o) for o in filtered))

    # Comparative product/customer questions.
    if asks_rank and asks_product:
        if asks_unit or "sold" in q or "quantity" in q or "units" in q:
            name, value = rank_group(filtered, "product", "qty")
            return f"{name} ({int(value)} units)" if name else "No matching records"
        metric = "count" if asks_count or "orders" in q else "amount"
        name, value = rank_group(filtered, "product", metric)
        if not name:
            return "No matching records"
        if metric == "amount":
            return f"{name} ({fmt_money(value)})"
        return f"{name} ({int(value)} orders)"

    if asks_rank and asks_customer:
        metric = "count" if asks_count or "orders" in q else "amount"
        name, value = rank_group(filtered, "customer", metric)
        if not name:
            return "No matching records"
        return f"{name} ({int(value)} orders)" if metric == "count" else f"{name} ({fmt_money(value)})"

    # Distinct customer questions.
    if asks_customer and asks_count and any(x in q for x in ("unique", "distinct", "how many", "number of", "customers")):
        return str(len({str(o.get("customer", "")) for o in filtered if o.get("customer")}))

    # Unit and order-count questions.
    if asks_unit:
        try:
            return str(sum(int(float(o.get("qty", 0) or 0)) for o in filtered))
        except (ValueError, TypeError):
            return str(sum(o.get("qty", 0) or 0 for o in filtered))
    if asks_count or (asks_order and not asks_revenue):
        if "customer" in q:
            return str(len({str(o.get("customer", "")) for o in filtered if o.get("customer")}))
        if "product" in q and ("unique" in q or "distinct" in q):
            return str(len({str(o.get("product", "")) for o in filtered if o.get("product")}))
        return str(len(filtered))

    # Revenue amounts are always USD-converted and only paid orders qualify.
    if asks_revenue or "total" in q or "amount" in q or "how much" in q:
        total = sum(usd_amount(o) for o in filtered)
        if asks_avg:
            total = total / len(filtered) if filtered else 0.0
        return fmt_money(total)

    # If a product is named but the question is vague, return its paid revenue.
    if asks_product:
        paid_filtered = filter_by_question(question, [o for o in all_orders if str(o.get("status", "")).lower() == "paid"])
        return fmt_money(sum(usd_amount(o) for o in paid_filtered))

    return "I could not determine the requested metric from the question."


@app.post("/answer")
async def answer(request: AnswerRequest) -> dict[str, str]:
    if not DATA["ready"]:
        await load_ledger()
    if not DATA["ready"]:
        raise HTTPException(status_code=503, detail=f"Ledger data unavailable: {DATA['error'] or 'load failed'}")
    try:
        return {"answer": answer_question(request.question)}
    except Exception as exc:
        # Keep the endpoint responsive and return a structured JSON response.
        print(f"Question processing error: {type(exc).__name__}: {exc}")
        raise HTTPException(status_code=500, detail="Could not process the question") from exc
