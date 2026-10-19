"""
PII masking. The ELT staging SQL (include/sql/elt/staging/stg_customers.sql)
implements the same rules so both pipelines publish identical masked values.

  email  test.user001@example.com -> te***@example.com   (invalid email -> NULL)
  phone  9000000012                 -> 90******12          (invalid phone -> NULL)
  name   Test User                -> T*** U***

Invalid values are nulled rather than partially masked, because a malformed
value (e.g. a 7-digit phone) would reveal a larger share of the original.
"""

from __future__ import annotations

import re

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")
PHONE_RE = re.compile(r"^[6-9][0-9]{9}$")


def mask_email(value) -> str | None:
    v = str(value or "").strip()
    if not EMAIL_RE.match(v):
        return None
    local, domain = v.split("@", 1)
    return f"{local[:2]}***@{domain}"


def mask_phone(value) -> str | None:
    v = str(value or "").strip()
    if not PHONE_RE.match(v):
        return None
    return f"{v[:2]}******{v[-2:]}"


def mask_name(value) -> str | None:
    words = str(value or "").split()
    if not words:
        return None
    return " ".join(w[0] + "***" for w in words)
