"""
ETL data-quality rules (pandas). The ELT pipeline implements the same rules in
SQL (include/sql/elt/staging/*.sql) so both pipelines are comparable.

Each check marks rows that fail it. Outcomes per row:

  PASSED            no check failed
  WARNING           only Warning checks failed -> row loads, warnings logged
  FAILED            a Critical check that rejects the row failed -> quarantined,
                    never loaded to the warehouse
  FAILED_ATTRIBUTE  dimension rows only: a Critical *attribute* check failed
                    (e.g. negative mrp). The row is quarantined for the data
                    owner, and the dimension keeps the member with that attribute
                    set to NULL / 'Unknown', so no order is dropped because of a
                    master-data defect and no impossible value reaches a report.

Every check also produces one dq_results row (rows_checked / rows_failed over
the population described in `population`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd

from .config import (
    ALLOWED_ORDER_STATUS, ALLOWED_PAYMENT_STATUS, ALLOWED_TIERS, ALLOWED_ZONES,
    AMOUNT_TOLERANCE, DEFAULT_ORDERS_BASELINE, DELIVERY_MAX_MINUTES, DELIVERY_NORMAL,
    GROSS_PRICE_RATIO_BOUNDS, PAYMENT_MODE_MAP, PRICE_RATIO_BOUNDS, STALE_AFTER_HOURS, STORE_PRICE_DRIFT_RATIO,
    VOLUME_DROP_THRESHOLD,
)
from .warehouse import check_status

TS_FMT = "%Y-%m-%d %H:%M:%S"
DATE_FMT = "%Y-%m-%d"
EMAIL_RE = r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$"
PHONE_RE = r"^[6-9][0-9]{9}$"
PINCODE_RE = r"^56[0-9]{4}$"
INT_RE = r"^-?[0-9]+$"
NUM_RE = r"^-?[0-9]+(\.[0-9]+)?$"


# ----------------------------------------------------------------- helpers
def s(df: pd.DataFrame, col: str) -> pd.Series:
    return df[col].astype(str).str.strip()


def blank(series: pd.Series) -> pd.Series:
    return series.str.strip().eq("")


def num(series: pd.Series) -> pd.Series:
    t = series.str.strip()
    return pd.to_numeric(t.where(t.str.match(NUM_RE)), errors="coerce")


def ts(series: pd.Series, fmt: str = TS_FMT) -> pd.Series:
    return pd.to_datetime(series.str.strip(), format=fmt, errors="coerce")


def dec(x: str) -> Decimal | None:
    try:
        return Decimal(str(x).strip())
    except (InvalidOperation, ValueError):
        return None


def money(d: Decimal) -> Decimal:
    return d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def expected_amount(q: str, price: str, disc: str) -> Decimal | None:
    qd, pd_, dd = dec(q), dec(price), dec(disc)
    if None in (qd, pd_, dd):
        return None
    return money(qd * pd_ * (1 - dd / Decimal(100)))


def map_payment_mode(series: pd.Series) -> pd.Series:
    return series.str.strip().str.lower().map(PAYMENT_MODE_MAP)


@dataclass
class Check:
    name: str
    dimension: str
    severity: str           # Critical | Warning
    action: str = "reject"  # reject | nullify | warn
    nullify: str | None = None


@dataclass
class Validator:
    df: pd.DataFrame
    source_file: str
    run_id: str
    dag_id: str
    results: list[dict] = field(default_factory=list)

    def __post_init__(self):
        self.df = self.df.reset_index(drop=True)
        n = len(self.df)
        self._crit: list[list[str]] = [[] for _ in range(n)]
        self._reason: list[list[str]] = [[] for _ in range(n)]
        self._warn: list[list[str]] = [[] for _ in range(n)]
        self._reject = [False] * n
        self._nullify: list[set] = [set() for _ in range(n)]

    def check(self, chk: Check, failed: pd.Series, detail, population: pd.Series | None = None,
              population_desc: str | None = None) -> int:
        population = pd.Series(True, index=self.df.index) if population is None else population.fillna(False)
        failed = failed.fillna(False).astype(bool) & population.astype(bool)
        for i in failed[failed].index:
            msg = detail(self.df.loc[i]) if callable(detail) else str(detail)
            if chk.severity == "Critical":
                self._crit[i].append(chk.name)
                self._reason[i].append(f"{chk.name}: {msg}")
                if chk.action == "reject":
                    self._reject[i] = True
                elif chk.action == "nullify":
                    self._nullify[i].add(chk.nullify)
            else:
                self._warn[i].append(f"{chk.name}: {msg}")
        rows_failed = int(failed.sum())
        self.results.append(self._result(chk, int(population.sum()), rows_failed,
                                         population_desc or f"source rows of {self.source_file}"))
        return rows_failed

    def dataset_check(self, chk: Check, failed: bool, details: str, rows_checked: int = 1,
                      rows_failed: int | None = None, population_desc: str = "dataset (1 = whole file)",
                      status: str | None = None) -> None:
        rf = (1 if failed else 0) if rows_failed is None else rows_failed
        r = self._result(chk, rows_checked, rf, population_desc, details)
        if status:
            r["status"] = status
        self.results.append(r)

    def _result(self, chk, rows_checked, rows_failed, population, details=""):
        return dict(run_id=self.run_id, dag_id=self.dag_id, check_name=chk.name,
                    dimension=chk.dimension, severity=chk.severity, rows_checked=rows_checked,
                    rows_failed=rows_failed, status=check_status(chk.severity, rows_failed),
                    checked_at=datetime.now().isoformat(sep=" ", timespec="seconds"),
                    source_file=self.source_file, population=population, details=details)

    def finish(self) -> pd.DataFrame:
        out = self.df.copy()
        status = []
        for i in range(len(out)):
            if self._reject[i]:
                status.append("FAILED")
            elif self._crit[i]:
                status.append("FAILED_ATTRIBUTE")
            elif self._warn[i]:
                status.append("WARNING")
            else:
                status.append("PASSED")
        out["dq_status"] = status
        out["failed_checks"] = [";".join(sorted(set(c))) for c in self._crit]
        out["failure_reason"] = [" | ".join(r) for r in self._reason]
        out["warning_reason"] = [" | ".join(w) for w in self._warn]
        out["nullify_columns"] = [";".join(sorted(n)) for n in self._nullify]
        return out


def duplicate_masks(df: pd.DataFrame, key: str, value_cols: list[str],
                    keep_first_order: pd.Series | None = None):
    """Returns (failed_mask, detail_by_index) for the uniqueness rule.

    Exact duplicate rows: the first copy (by keep_first_order, default file
    order) passes, the extra copies fail. Conflicting copies: everything after
    the preferred copy fails, with the conflicting columns named.
    """
    k = s(df, key)
    valid = ~blank(k)
    order = df["_row_num"] if keep_first_order is None else keep_first_order
    failed = pd.Series(False, index=df.index)
    detail: dict[int, str] = {}
    dup_keys = k[valid & k.duplicated(keep=False)].unique()
    for kv in dup_keys:
        idx = df.index[k == kv]
        grp = df.loc[idx]
        keep = order.loc[idx].sort_values(kind="stable").index[0]
        diff = [c for c in value_cols if grp[c].nunique() > 1]
        for i in idx:
            if i == keep:
                continue
            failed.loc[i] = True
            kept_row = int(df.loc[keep, "_row_num"])
            detail[i] = (f"exact duplicate of row {kept_row}" if not diff
                         else f"duplicate {key}; differs from kept row {kept_row} in {', '.join(diff)}")
    return failed, detail


# ------------------------------------------------------------------ orders
def validate_orders(df: pd.DataFrame, *, source_file: str, run_id: str, dag_id: str,
                    cutoff: datetime, customer_ids: set, product_ids: set, store_ids: set,
                    product_mrp: dict, store_capacity: dict, baseline: int | None,
                    is_increment: bool = False, expect_reduced_volume: bool = False) -> tuple[pd.DataFrame, list[dict]]:
    v = Validator(df, source_file, run_id, dag_id)
    d = v.df
    oid, cid = s(d, "order_id"), s(d, "customer_id")
    status = s(d, "order_status").str.upper()
    mode = map_payment_mode(d["payment_mode"])
    q, up, dp, amt = num(d["quantity"]), num(d["unit_price"]), num(d["discount_pct"]), num(d["order_amount"])
    dm_raw = s(d, "delivery_minutes")
    dm = num(d["delivery_minutes"])
    ots = ts(d["order_ts"])
    delivered = status.eq("DELIVERED")

    # Completeness
    v.check(Check("orders_order_id_not_null", "Completeness", "Critical"), blank(oid), "order_id is blank")
    v.check(Check("orders_customer_id_not_null", "Completeness", "Critical"), blank(cid), "customer_id is blank")
    v.check(Check("orders_delivered_has_delivery_minutes", "Completeness", "Critical"),
            delivered & blank(dm_raw), "DELIVERED order has no delivery_minutes",
            population=delivered, population_desc="DELIVERED rows")
    # Uniqueness
    dup, ddetail = duplicate_masks(d, "order_id", [c for c in d.columns if not c.startswith("_")])
    v.check(Check("orders_order_id_unique", "Uniqueness", "Critical"), dup, lambda r: ddetail[r.name],
            population=~blank(oid), population_desc="rows with non-blank order_id")
    # Validity
    v.check(Check("orders_order_ts_valid", "Validity", "Critical"), ots.isna(),
            lambda r: f"order_ts {r['order_ts']!r} is not YYYY-MM-DD HH:MM:SS")
    v.check(Check("orders_status_allowed", "Validity", "Critical"), ~status.isin(ALLOWED_ORDER_STATUS),
            lambda r: f"order_status {r['order_status']!r} not in {'/'.join(ALLOWED_ORDER_STATUS)}")
    v.check(Check("orders_payment_mode_allowed", "Validity", "Critical"), mode.isna(),
            lambda r: f"payment_mode {r['payment_mode']!r} not mappable to UPI/Card/Cash on Delivery/Wallet")
    bad_num = (~s(d, "quantity").str.match(INT_RE) | up.isna() | dp.isna() | amt.isna()
               | (~blank(dm_raw) & dm.isna()))
    v.check(Check("orders_numeric_fields_valid", "Validity", "Critical"), bad_num,
            "quantity/unit_price/discount_pct/order_amount/delivery_minutes not numeric")
    v.check(Check("orders_discount_pct_range", "Validity", "Critical"), (dp < 0) | (dp > 100),
            lambda r: f"discount_pct {r['discount_pct']} outside 0-100")
    # Accuracy
    v.check(Check("orders_quantity_positive", "Accuracy", "Critical"), q <= 0,
            lambda r: f"quantity {r['quantity']} <= 0")
    v.check(Check("orders_unit_price_positive", "Accuracy", "Critical"), up <= 0,
            lambda r: f"unit_price {r['unit_price']} <= 0")
    exp = pd.Series([expected_amount(a, b, c) for a, b, c in
                     zip(d["quantity"], d["unit_price"], d["discount_pct"])], index=d.index)
    amt_dec = d["order_amount"].map(dec)
    formula_bad = pd.Series([(e is not None and a is not None and abs(e - a) > Decimal(str(AMOUNT_TOLERANCE)))
                             for e, a in zip(exp, amt_dec)], index=d.index)
    v.check(Check("orders_amount_formula", "Accuracy", "Critical"), formula_bad,
            lambda r: f"order_amount {r['order_amount']} != qty*price*(1-disc/100) = {exp[r.name]}")
    v.check(Check("orders_delivery_minutes_lt_120", "Accuracy", "Critical"),
            (dm >= DELIVERY_MAX_MINUTES) | (dm <= 0),
            lambda r: f"delivery_minutes {r['delivery_minutes']} not in 1-{DELIVERY_MAX_MINUTES - 1}")
    lo, hi = DELIVERY_NORMAL
    v.check(Check("orders_delivery_minutes_normal_range", "Accuracy", "Warning", "warn"),
            (dm > 0) & (dm < DELIVERY_MAX_MINUTES) & ((dm < lo) | (dm > hi)),
            lambda r: f"delivery_minutes {r['delivery_minutes']} outside {lo}-{hi}",
            population=dm.notna(), population_desc="rows with delivery_minutes")
    pid = s(d, "product_id")
    mrp = pid.map(product_mrp)
    ratio = up / mrp.where(mrp > 0)
    plo, phi = PRICE_RATIO_BOUNDS
    v.check(Check("orders_unit_price_vs_mrp", "Accuracy", "Warning", "warn"),
            (ratio < plo) | (ratio > phi),
            lambda r: f"unit_price {r['unit_price']} vs mrp {product_mrp.get(r['product_id'].strip())}",
            population=ratio.notna(), population_desc="rows whose product has a positive mrp")
    # Consistency
    v.check(Check("orders_customer_fk", "Consistency", "Critical"), ~blank(cid) & ~cid.isin(customer_ids),
            lambda r: f"customer_id {r['customer_id']} not in customer master",
            population=~blank(cid), population_desc="rows with non-blank customer_id")
    v.check(Check("orders_product_fk", "Consistency", "Critical"), ~pid.isin(product_ids),
            lambda r: f"product_id {r['product_id']} not in product master")
    v.check(Check("orders_store_fk", "Consistency", "Critical"), ~s(d, "store_id").isin(store_ids),
            lambda r: f"store_id {r['store_id']} not in store master")
    v.check(Check("orders_status_casing", "Consistency", "Warning", "warn"),
            status.isin(ALLOWED_ORDER_STATUS) & (d["order_status"] != status),
            lambda r: f"order_status {r['order_status']!r} standardised to {r['order_status'].strip().upper()}")
    v.check(Check("orders_payment_mode_casing", "Consistency", "Warning", "warn"),
            mode.notna() & (d["payment_mode"] != mode),
            lambda r: f"payment_mode {r['payment_mode']!r} standardised")
    v.check(Check("orders_cancelled_without_delivery", "Consistency", "Warning", "warn"),
            status.eq("CANCELLED") & ~blank(dm_raw), "CANCELLED order has delivery_minutes",
            population=status.eq("CANCELLED"), population_desc="CANCELLED rows")
    # Timeliness
    v.check(Check("orders_order_ts_not_future", "Timeliness", "Critical"), ots > cutoff,
            lambda r: f"order_ts {r['order_ts']} after batch cut-off {cutoff}")
    # Added after experiment 8b: a price 10x away from the catalogue mrp is a unit/scale error,
    # not a pricing decision; it must not reach finance even when order_amount is consistent.
    glo, ghi = GROSS_PRICE_RATIO_BOUNDS
    v.check(Check("orders_unit_price_gross_outlier", "Accuracy", "Critical"), (ratio < glo) | (ratio > ghi),
            lambda r: f"unit_price {r['unit_price']} is outside {glo}-{ghi}x of mrp {product_mrp.get(r['product_id'].strip())}",
            population=ratio.notna(), population_desc="rows whose product has a positive mrp")

    # Dataset-level checks (population = whole file / store-days)
    ok_ts = ots[(ots <= cutoff)]
    latest = ok_ts.max()
    age_h = (cutoff - latest).total_seconds() / 3600 if pd.notna(latest) else None
    v.dataset_check(Check("orders_freshness", "Timeliness", "Warning", "warn"),
                    age_h is None or age_h > STALE_AFTER_HOURS,
                    f"latest valid order_ts {latest} is {age_h:.1f}h before cut-off {cutoff} "
                    f"(threshold {STALE_AFTER_HOURS}h)" if age_h is not None else "no valid order_ts")
    if not is_increment:
        base = baseline or DEFAULT_ORDERS_BASELINE
        drop = 1 - len(d) / base
        anomalous = drop > VOLUME_DROP_THRESHOLD
        note = (f"rows={len(d)} baseline={base} "
                f"({'history median' if baseline else 'documented default'}) drop={drop:.1%} "
                f"threshold={VOLUME_DROP_THRESHOLD:.0%}")
        if expect_reduced_volume:
            v.dataset_check(Check("orders_volume_vs_baseline", "Completeness", "Warning", "warn"), False,
                            note + " | suppressed: run flagged expect_reduced_volume",
                            rows_checked=len(d), rows_failed=0, status="SKIPPED",
                            population_desc="orders file row count vs baseline")
        else:
            v.dataset_check(Check("orders_volume_vs_baseline", "Completeness", "Warning", "warn"),
                            anomalous, note, rows_checked=len(d), rows_failed=1 if anomalous else 0,
                            population_desc="orders file row count vs baseline")
    # Distribution drift: a store whose average price/mrp ratio moved far from 1.
    sd = pd.DataFrame({"store": s(d, "store_id"), "ratio": ratio}).dropna()
    by_store = sd.groupby("store")["ratio"].mean()
    drifted = by_store[(by_store > STORE_PRICE_DRIFT_RATIO) | (by_store < 1 / STORE_PRICE_DRIFT_RATIO)]
    v.dataset_check(Check("orders_store_price_distribution", "Accuracy", "Warning", "warn"),
                    len(drifted) > 0,
                    "stores with mean unit_price/mrp outside "
                    f"[{1 / STORE_PRICE_DRIFT_RATIO:.2f}, {STORE_PRICE_DRIFT_RATIO}]: "
                    + (", ".join(f"{k}={v_:.2f}" for k, v_ in drifted.items()) or "none"),
                    rows_checked=len(by_store), rows_failed=len(drifted),
                    population_desc="stores (mean unit_price/mrp per store)")
    # Store capacity: daily orders per store vs capacity_orders_per_day.
    cap = pd.DataFrame({"store": s(d, "store_id"), "day": ots.dt.date}).dropna()
    daily = cap.groupby(["store", "day"]).size().reset_index(name="n")
    daily["cap"] = daily["store"].map(store_capacity)
    over = daily[daily["cap"].notna() & (daily["n"] > daily["cap"])]
    v.dataset_check(Check("orders_store_daily_capacity", "Consistency", "Warning", "warn"),
                    len(over) > 0,
                    f"store-days over capacity: {len(over)}; max orders/store/day={daily['n'].max() if len(daily) else 0}",
                    rows_checked=int(daily["cap"].notna().sum()), rows_failed=len(over),
                    population_desc="store-days with a known positive capacity")
    out = v.finish()
    out["_cutoff"] = cutoff.isoformat(sep=" ")
    return out, v.results


# ---------------------------------------------------------------- payments
def validate_payments(df: pd.DataFrame, *, source_file: str, run_id: str, dag_id: str,
                      cutoff: datetime, orders_src: pd.DataFrame) -> tuple[pd.DataFrame, list[dict]]:
    """orders_src: latest source version of every order_id (base + increment)."""
    v = Validator(df, source_file, run_id, dag_id)
    d = v.df
    pid, oid = s(d, "payment_id"), s(d, "order_id")
    pst = s(d, "payment_status").str.upper()
    mode = map_payment_mode(d["payment_mode"])
    paid = num(d["amount_paid"])
    pts = ts(d["payment_ts"])
    omap = orders_src.set_index("order_id")
    known = oid.isin(omap.index)
    o_amt = oid.map(omap["order_amount"]).map(lambda x: dec(x) if isinstance(x, str) else None)
    o_mode = map_payment_mode(oid.map(omap["payment_mode"]).fillna(""))
    o_ts = ts(oid.map(omap["order_ts"]).fillna(""))
    success = pst.eq("SUCCESS")

    v.check(Check("payments_payment_id_not_null", "Completeness", "Critical"), blank(pid), "payment_id is blank")
    v.check(Check("payments_order_id_not_null", "Completeness", "Critical"), blank(oid), "order_id is blank")
    non_cod = mode.notna() & mode.ne("Cash on Delivery")
    v.check(Check("payments_gateway_ref_present", "Completeness", "Warning", "warn"),
            non_cod & blank(d["gateway_ref"]), "non-COD payment without gateway_ref",
            population=non_cod, population_desc="non-COD payments")
    dup, ddetail = duplicate_masks(d, "payment_id", [c for c in d.columns if not c.startswith("_")])
    v.check(Check("payments_payment_id_unique", "Uniqueness", "Critical"), dup, lambda r: ddetail[r.name],
            population=~blank(pid), population_desc="rows with non-blank payment_id")
    v.check(Check("payments_payment_ts_valid", "Validity", "Critical"), pts.isna(),
            lambda r: f"payment_ts {r['payment_ts']!r} is not YYYY-MM-DD HH:MM:SS")
    v.check(Check("payments_amount_numeric", "Validity", "Critical"), paid.isna(),
            lambda r: f"amount_paid {r['amount_paid']!r} not numeric")
    v.check(Check("payments_status_allowed", "Validity", "Critical"), ~pst.isin(ALLOWED_PAYMENT_STATUS),
            lambda r: f"payment_status {r['payment_status']!r} not allowed")
    v.check(Check("payments_payment_mode_allowed", "Validity", "Critical"), mode.isna(),
            lambda r: f"payment_mode {r['payment_mode']!r} not mappable")
    v.check(Check("payments_success_amount_positive", "Accuracy", "Critical"), success & (paid <= 0),
            lambda r: f"SUCCESS payment amount {r['amount_paid']} <= 0",
            population=success, population_desc="SUCCESS payments")
    v.check(Check("payments_order_fk", "Consistency", "Critical"), ~blank(oid) & ~known,
            lambda r: f"order_id {r['order_id']} not in orders", population=~blank(oid),
            population_desc="rows with non-blank order_id")
    mism = pd.Series([(o is not None and not pd.isna(p) and abs(Decimal(str(p)) - o) > Decimal(str(AMOUNT_TOLERANCE)))
                      for p, o in zip(paid, o_amt)], index=d.index)
    v.check(Check("payments_success_amount_matches_order", "Consistency", "Critical"),
            success & known & mism,
            lambda r: f"amount_paid {r['amount_paid']} != order_amount {o_amt[r.name]}",
            population=success & known, population_desc="SUCCESS payments with a known order")
    v.check(Check("payments_mode_matches_order", "Consistency", "Warning", "warn"),
            known & mode.notna() & o_mode.notna() & (mode != o_mode),
            lambda r: f"payment_mode {mode[r.name]} != order payment_mode {o_mode[r.name]}",
            population=known, population_desc="payments with a known order")
    v.check(Check("payments_payment_mode_casing", "Consistency", "Warning", "warn"),
            mode.notna() & (d["payment_mode"] != mode), lambda r: f"payment_mode {r['payment_mode']!r} standardised")
    v.check(Check("payments_payment_ts_not_future", "Timeliness", "Critical"), pts > cutoff,
            lambda r: f"payment_ts {r['payment_ts']} after batch cut-off {cutoff}")
    v.check(Check("payments_paid_after_order", "Timeliness", "Warning", "warn"),
            pts.notna() & o_ts.notna() & (pts < o_ts), "payment_ts earlier than order_ts",
            population=pts.notna() & o_ts.notna(), population_desc="payments with valid payment_ts and order_ts")
    return v.finish(), v.results


# --------------------------------------------------------------- customers
def validate_customers(df, *, source_file, run_id, dag_id, cutoff):
    v = Validator(df, source_file, run_id, dag_id)
    d = v.df
    cid = s(d, "customer_id")
    tier = s(d, "loyalty_tier").str.title()
    sd = ts(d["signup_date"], DATE_FMT)
    v.check(Check("customers_customer_id_not_null", "Completeness", "Critical"), blank(cid), "customer_id is blank")
    v.check(Check("customers_city_present", "Completeness", "Warning", "warn"), blank(d["city"]), "city is blank")
    dup, ddetail = duplicate_masks(d, "customer_id", [c for c in d.columns if not c.startswith("_")])
    v.check(Check("customers_customer_id_unique", "Uniqueness", "Critical"), dup, lambda r: ddetail[r.name],
            population=~blank(cid), population_desc="rows with non-blank customer_id")
    v.check(Check("customers_email_format", "Validity", "Warning", "warn"), ~s(d, "email").str.match(EMAIL_RE),
            "email is not name@domain.tld (value withheld: PII)")
    v.check(Check("customers_phone_format", "Validity", "Warning", "warn"), ~s(d, "phone").str.match(PHONE_RE),
            lambda r: f"phone has {len(r['phone'].strip())} digits, expected 10 starting 6-9 (value withheld: PII)")
    v.check(Check("customers_pincode_format", "Validity", "Warning", "warn"), ~s(d, "pincode").str.match(PINCODE_RE),
            lambda r: f"pincode {r['pincode']!r} is not a 6-digit Bengaluru pincode")
    v.check(Check("customers_signup_date_valid", "Validity", "Critical", "nullify", "signup_date"), sd.isna(),
            lambda r: f"signup_date {r['signup_date']!r} not YYYY-MM-DD")
    v.check(Check("customers_loyalty_tier_allowed", "Validity", "Critical", "nullify", "loyalty_tier"),
            ~tier.isin(ALLOWED_TIERS), lambda r: f"loyalty_tier {r['loyalty_tier']!r} not allowed")
    v.check(Check("customers_is_active_flag", "Validity", "Warning", "warn"), ~s(d, "is_active").isin(["Y", "N"]),
            lambda r: f"is_active {r['is_active']!r} not Y/N")
    v.check(Check("customers_loyalty_tier_casing", "Consistency", "Warning", "warn"),
            tier.isin(ALLOWED_TIERS) & (d["loyalty_tier"] != tier),
            lambda r: f"loyalty_tier {r['loyalty_tier']!r} standardised to {r['loyalty_tier'].strip().title()}")
    v.check(Check("customers_signup_not_future", "Timeliness", "Critical", "nullify", "signup_date"), sd > cutoff,
            lambda r: f"signup_date {r['signup_date']} after batch cut-off {cutoff}")
    return v.finish(), v.results


# ---------------------------------------------------------------- products
def product_keep_order(d: pd.DataFrame) -> pd.Series:
    """Prefer the current catalogue row over a superseded '(old)' row, then file order."""
    old = d["product_name"].str.strip().str.endswith("(old)").astype(int)
    return old * 1_000_000 + d["_row_num"].astype(int)


def validate_products(df, *, source_file, run_id, dag_id, cutoff):
    v = Validator(df, source_file, run_id, dag_id)
    d = v.df
    pid = s(d, "product_id")
    mrp, cp = num(d["mrp"]), num(d["cost_price"])
    ld = ts(d["launch_date"], DATE_FMT)
    v.check(Check("products_product_id_not_null", "Completeness", "Critical"), blank(pid), "product_id is blank")
    v.check(Check("products_category_present", "Completeness", "Critical", "nullify", "category"),
            blank(d["category"]), lambda r: f"category blank (sub_category {r['sub_category']})")
    dup, ddetail = duplicate_masks(d, "product_id", [c for c in d.columns if not c.startswith("_")],
                                   keep_first_order=product_keep_order(d))
    v.check(Check("products_product_id_unique", "Uniqueness", "Critical"), dup, lambda r: ddetail[r.name],
            population=~blank(pid), population_desc="rows with non-blank product_id")
    v.check(Check("products_prices_numeric", "Validity", "Critical", "nullify", "cost_price"),
            mrp.isna() | cp.isna(), "mrp or cost_price not numeric")
    v.check(Check("products_launch_date_valid", "Validity", "Critical", "nullify", "launch_date"), ld.isna(),
            lambda r: f"launch_date {r['launch_date']!r} not YYYY-MM-DD")
    v.check(Check("products_mrp_positive", "Accuracy", "Critical", "nullify", "mrp"), mrp <= 0,
            lambda r: f"mrp {r['mrp']} <= 0")
    v.check(Check("products_cost_price_positive", "Accuracy", "Critical", "nullify", "cost_price"), cp <= 0,
            lambda r: f"cost_price {r['cost_price']} <= 0")
    v.check(Check("products_cost_le_mrp", "Accuracy", "Critical", "nullify", "cost_price"),
            (mrp > 0) & (cp > mrp), lambda r: f"cost_price {r['cost_price']} > mrp {r['mrp']}",
            population=mrp > 0, population_desc="rows with positive mrp")
    nonblank = ~blank(d["category"])
    cats = pd.DataFrame({"sub": s(d, "sub_category"), "cat": s(d, "category")})[nonblank]
    multi = cats.groupby("sub")["cat"].nunique()
    bad_sub = set(multi[multi > 1].index)
    v.check(Check("products_subcategory_single_category", "Consistency", "Warning", "warn"),
            nonblank & s(d, "sub_category").isin(bad_sub),
            lambda r: f"sub_category {r['sub_category']} maps to several categories",
            population=nonblank, population_desc="rows with category")
    v.check(Check("products_launch_not_future", "Timeliness", "Critical", "nullify", "launch_date"), ld > cutoff,
            lambda r: f"launch_date {r['launch_date']} after batch cut-off {cutoff}")
    return v.finish(), v.results


# ------------------------------------------------------------------ stores
def validate_stores(df, *, source_file, run_id, dag_id, cutoff):
    v = Validator(df, source_file, run_id, dag_id)
    d = v.df
    sid = s(d, "store_id")
    cap = num(d["capacity_orders_per_day"])
    od = ts(d["open_date"], DATE_FMT)
    v.check(Check("stores_store_id_not_null", "Completeness", "Critical"), blank(sid), "store_id is blank")
    v.check(Check("stores_zone_present", "Completeness", "Critical", "nullify", "zone"), blank(d["zone"]),
            "zone is blank")
    dup, ddetail = duplicate_masks(d, "store_id", [c for c in d.columns if not c.startswith("_")])
    v.check(Check("stores_store_id_unique", "Uniqueness", "Critical"), dup, lambda r: ddetail[r.name],
            population=~blank(sid), population_desc="rows with non-blank store_id")
    v.check(Check("stores_zone_allowed", "Validity", "Critical", "nullify", "zone"),
            ~blank(d["zone"]) & ~s(d, "zone").str.title().isin(ALLOWED_ZONES),
            lambda r: f"zone {r['zone']!r} not North/South/East/West", population=~blank(d["zone"]),
            population_desc="rows with zone")
    v.check(Check("stores_pincode_format", "Validity", "Warning", "warn"), ~s(d, "pincode").str.match(PINCODE_RE),
            lambda r: f"pincode {r['pincode']!r} invalid")
    v.check(Check("stores_capacity_positive", "Accuracy", "Critical", "nullify", "capacity_orders_per_day"),
            cap.isna() | (cap <= 0), lambda r: f"capacity_orders_per_day {r['capacity_orders_per_day']!r} <= 0")
    v.check(Check("stores_name_matches_area", "Consistency", "Warning", "warn"),
            ~(s(d, "store_name") == ("NammaMart " + s(d, "area"))), "store_name does not match area")
    v.check(Check("stores_open_date_not_future", "Timeliness", "Critical", "nullify", "open_date"),
            od.isna() | (od > cutoff), lambda r: f"open_date {r['open_date']!r} invalid or after cut-off")
    return v.finish(), v.results


def kept_masters(customers: pd.DataFrame, products: pd.DataFrame, stores: pd.DataFrame) -> dict:
    """Keys and attributes other checks need, from the validated master rows that survive."""
    keep = lambda df: df[df["dq_status"] != "FAILED"]
    c, p, st = keep(customers), keep(products), keep(stores)
    mrp = num(p["mrp"])
    cap = num(st["capacity_orders_per_day"])
    return {
        "customer_ids": set(s(c, "customer_id")),
        "product_ids": set(s(p, "product_id")),
        "store_ids": set(s(st, "store_id")),
        # |mrp|: a sign-flipped mrp still describes the shelf price for the drift check.
        "product_mrp": dict(zip(s(p, "product_id"), mrp.abs())),
        "store_capacity": {k: v_ for k, v_ in zip(s(st, "store_id"), cap) if pd.notna(v_) and v_ > 0},
    }
