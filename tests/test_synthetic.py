from __future__ import annotations

from merchmind.synthetic import SyntheticConfig, generate_all


def test_synthetic_generation_is_reproducible() -> None:
    config = SyntheticConfig(seed=17, customers=80, products=25, transactions=400)
    first = generate_all(config)
    second = generate_all(config)

    assert first.keys() == second.keys()
    assert first["products"].equals(second["products"])
    assert first["transactions"].equals(second["transactions"])
    assert len(first["products"]) == 25
    assert len(first["customers"]) == 80
    assert len(first["transactions"]) > 400  # duplicate noise is intentionally injected
