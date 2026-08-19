from __future__ import annotations

from merchmind.quality import validate_transactions
from merchmind.synthetic import SyntheticConfig, generate_all


def test_bad_rows_are_quarantined_with_reasons() -> None:
    datasets = generate_all(
        SyntheticConfig(seed=8, customers=80, products=30, transactions=500, quality_noise=0.03)
    )
    result = validate_transactions(
        datasets["transactions"], datasets["products"], datasets["customers"]
    )

    assert result.report["quarantine_rows"] > 0
    assert (
        result.report["clean_rows"] + result.report["quarantine_rows"]
        == result.report["source_rows"]
    )
    assert result.quarantine["rejection_reason"].str.len().gt(0).all()
    assert result.clean["transaction_id"].is_unique
    assert result.clean["quantity"].gt(0).all()
    assert result.clean["unit_price"].ge(0).all()
