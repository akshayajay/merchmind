from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ValidationResult:
    clean: pd.DataFrame
    quarantine: pd.DataFrame
    report: dict[str, int | float]


def validate_transactions(
    transactions: pd.DataFrame,
    products: pd.DataFrame,
    customers: pd.DataFrame,
) -> ValidationResult:
    """Validate transaction contracts and preserve rejected rows for auditability."""
    frame = transactions.copy()
    frame["transaction_ts"] = pd.to_datetime(frame["transaction_ts"], errors="coerce", utc=True)

    duplicate_mask = frame.duplicated("transaction_id", keep="first")
    null_id_mask = frame[["transaction_id", "customer_id", "product_id"]].isna().any(axis=1)
    invalid_quantity_mask = frame["quantity"].fillna(0).le(0)
    invalid_price_mask = frame["unit_price"].fillna(-1).lt(0)
    invalid_timestamp_mask = frame["transaction_ts"].isna()
    unknown_customer_mask = ~frame["customer_id"].isin(customers["customer_id"])
    unknown_product_mask = ~frame["product_id"].isin(products["product_id"])

    reason_masks = {
        "duplicate_transaction_id": duplicate_mask,
        "missing_required_id": null_id_mask,
        "invalid_quantity": invalid_quantity_mask,
        "invalid_price": invalid_price_mask,
        "invalid_timestamp": invalid_timestamp_mask,
        "unknown_customer": unknown_customer_mask & ~null_id_mask,
        "unknown_product": unknown_product_mask & ~null_id_mask,
    }

    rejection_reason = pd.Series("", index=frame.index, dtype="string")
    for reason, mask in reason_masks.items():
        rejection_reason.loc[mask] = rejection_reason.loc[mask].apply(
            lambda current, rejection=reason: f"{current}|{rejection}" if current else rejection
        )

    rejected_mask = rejection_reason.ne("")
    quarantine = frame.loc[rejected_mask].copy()
    quarantine["rejection_reason"] = rejection_reason.loc[rejected_mask]
    clean = frame.loc[~rejected_mask].copy().sort_values("transaction_ts").reset_index(drop=True)

    source_rows = len(frame)
    report: dict[str, int | float] = {
        "source_rows": source_rows,
        "clean_rows": len(clean),
        "quarantine_rows": len(quarantine),
        "duplicate_rows": int(duplicate_mask.sum()),
        "missing_required_id_rows": int(null_id_mask.sum()),
        "invalid_quantity_rows": int(invalid_quantity_mask.sum()),
        "invalid_price_rows": int(invalid_price_mask.sum()),
        "invalid_timestamp_rows": int(invalid_timestamp_mask.sum()),
        "referential_integrity_failures": int((unknown_customer_mask | unknown_product_mask).sum()),
        "pass_rate": round(len(clean) / source_rows, 6) if source_rows else 0.0,
    }
    return ValidationResult(clean=clean, quarantine=quarantine, report=report)
