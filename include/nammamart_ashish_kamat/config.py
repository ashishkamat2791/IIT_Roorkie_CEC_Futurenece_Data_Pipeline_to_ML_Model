"""
Shared configuration for the NammaMart ETL and ELT pipelines (Ashish Kamat).

Every path is derived from this file's location so the same code runs inside
the Astro containers (/usr/local/airflow/...) and on a laptop for tests.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
INCLUDE_DIR = PROJECT_ROOT / "include"
DATA_DIR = INCLUDE_DIR / "data" / "nammamart"
WAREHOUSE_PATH = INCLUDE_DIR / "warehouse_ashish_kamat.duckdb"
QUARANTINE_DIR = INCLUDE_DIR / "quarantine" / "ashish_kamat"
WORK_DIR = INCLUDE_DIR / "tmp" / "ashish_kamat"
SQL_DIR = INCLUDE_DIR / "sql"

ETL_DAG_ID = "nammamart_etl_ashish_kamat"
ELT_DAG_ID = "nammamart_elt_ashish_kamat"

# Source contract: file name, business key and the exact column list.
# Any added, missing, renamed or re-ordered column fails extraction loudly.
ORDER_COLUMNS = [
    "order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity",
    "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status",
    "channel", "delivery_minutes",
]
SOURCES: dict[str, dict] = {
    "orders": {"file": "orders.csv", "key": "order_id", "columns": ORDER_COLUMNS},
    "payments": {
        "file": "payments.csv", "key": "payment_id",
        "columns": ["payment_id", "order_id", "payment_ts", "payment_mode",
                    "amount_paid", "payment_status", "gateway_ref"],
    },
    "customers": {
        "file": "customers.csv", "key": "customer_id",
        "columns": ["customer_id", "full_name", "email", "phone", "city", "area",
                    "pincode", "signup_date", "loyalty_tier", "is_active"],
    },
    "products": {
        "file": "products.csv", "key": "product_id",
        "columns": ["product_id", "product_name", "category", "sub_category", "brand",
                    "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    },
    "stores": {
        "file": "stores.csv", "key": "store_id",
        "columns": ["store_id", "store_name", "zone", "area", "pincode", "open_date",
                    "manager_name", "capacity_orders_per_day"],
    },
}
INCREMENT_PATTERN = re.compile(r"^orders_(\d{4}-\d{2}-\d{2})_increment\.csv$")
DEFAULT_INCREMENT_FILE = "orders_2026-10-01_increment.csv"

# Business rules agreed with the client (see SUBMISSION.md, Part 0).
ALLOWED_ORDER_STATUS = ("DELIVERED", "CANCELLED", "RETURNED")
ALLOWED_PAYMENT_STATUS = ("SUCCESS", "FAILED", "REFUNDED")
ALLOWED_TIERS = ("Bronze", "Silver", "Gold", "Platinum")
ALLOWED_ZONES = ("North", "South", "East", "West")
# Synonyms seen in profiling. Anything not in this map is invalid (e.g. 'Cashh').
PAYMENT_MODE_MAP = {
    "upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
    "cod": "Cash on Delivery", "wallet": "Wallet",
}
AMOUNT_TOLERANCE = 0.01          # rupees, for order_amount and payment checks
DELIVERY_MAX_MINUTES = 120       # >= this is impossible for a 10-minute promise
DELIVERY_NORMAL = (8, 35)        # outside this range is a warning
WITHIN_SLA_MINUTES = 15          # within_15_min is delivery_minutes <= 15
STALE_AFTER_HOURS = 24           # latest order older than this vs as_of -> warning
VOLUME_DROP_THRESHOLD = 0.90     # >90% drop against the baseline -> warning
# Documented fallback baseline for the first run (September file, profiled).
DEFAULT_ORDERS_BASELINE = 2412
PRICE_RATIO_BOUNDS = (0.5, 1.5)  # unit_price / product mrp outside -> warning
GROSS_PRICE_RATIO_BOUNDS = (0.1, 10.0)  # outside -> Critical (scale/unit error, experiment 8b)
STORE_PRICE_DRIFT_RATIO = 1.5    # store mean(unit_price/mrp) above -> drift warning

# The September snapshot is consumed by the run at 09:00 IST on 1 Oct 2026.
# Pinning as_of keeps timeliness checks reproducible when the run is replayed.
DEFAULT_AS_OF = "2026-10-01 09:00:00"


def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':', '+' and '.', which are unsafe in file names."""
    return re.sub(r"[^A-Za-z0-9_-]+", "_", run_id).strip("_")[:120] or "run"


def _inside_include(path_str: str | None, default: Path) -> Path:
    """Overrides (used by isolated experiments) must stay under include/."""
    if not path_str:
        return default
    p = Path(path_str)
    if not p.is_absolute():
        p = PROJECT_ROOT / p
    p = p.resolve()
    if INCLUDE_DIR.resolve() not in (p, *p.parents):
        raise ValueError(f"path override {p} must be inside {INCLUDE_DIR}")
    return p


def increment_cutoff(file_name: str) -> datetime:
    """An increment for day D is consumed by the 09:00 IST run on D+1."""
    m = INCREMENT_PATTERN.match(file_name)
    if not m:
        raise ValueError(f"increment file name {file_name!r} must match {INCREMENT_PATTERN.pattern}")
    return datetime.strptime(m.group(1), "%Y-%m-%d") + timedelta(days=1, hours=9)


@dataclass
class RunConfig:
    run_id: str
    dag_id: str
    data_dir: Path = DATA_DIR
    warehouse: Path = WAREHOUSE_PATH
    quarantine_dir: Path = QUARANTINE_DIR
    work_dir: Path = WORK_DIR
    as_of: datetime = field(default_factory=lambda: datetime.fromisoformat(DEFAULT_AS_OF))
    apply_increment: bool = False
    increment_file: str = DEFAULT_INCREMENT_FILE
    orders_baseline: int | None = None
    expect_reduced_volume: bool = False

    @property
    def safe_run_id(self) -> str:
        return safe_run_id(self.run_id)

    @property
    def run_work_dir(self) -> Path:
        return self.work_dir / self.dag_id / self.safe_run_id

    def to_dict(self) -> dict:
        d = asdict(self)
        for k, v in d.items():
            if isinstance(v, Path):
                d[k] = str(v)
            elif isinstance(v, datetime):
                d[k] = v.isoformat(sep=" ")
        return d

    @classmethod
    def from_params(cls, run_id: str, dag_id: str, params: dict | None) -> "RunConfig":
        params = params or {}
        data_dir = _inside_include(params.get("data_dir"), DATA_DIR)
        warehouse = _inside_include(params.get("warehouse_path"), WAREHOUSE_PATH)
        quarantine = _inside_include(params.get("quarantine_dir"), QUARANTINE_DIR)
        as_of_raw = params.get("as_of") or DEFAULT_AS_OF
        baseline = params.get("orders_baseline")
        inc = params.get("increment_file") or DEFAULT_INCREMENT_FILE
        increment_cutoff(inc)  # validates the name early
        return cls(
            run_id=run_id, dag_id=dag_id, data_dir=data_dir, warehouse=warehouse,
            quarantine_dir=quarantine,
            as_of=datetime.fromisoformat(str(as_of_raw).replace("T", " ")[:19]),
            apply_increment=bool(params.get("apply_increment", False)),
            increment_file=inc,
            orders_baseline=int(baseline) if baseline not in (None, "") else None,
            expect_reduced_volume=bool(params.get("expect_reduced_volume", False)),
        )

    @classmethod
    def from_dict(cls, d: dict) -> "RunConfig":
        d = dict(d)
        for k in ("data_dir", "warehouse", "quarantine_dir", "work_dir"):
            d[k] = Path(d[k])
        d["as_of"] = datetime.fromisoformat(d["as_of"])
        return cls(**d)
