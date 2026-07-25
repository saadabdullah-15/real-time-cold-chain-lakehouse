import pytest

from simulator.config import RunConfig


def test_demo_profile_defaults_and_overrides() -> None:
    config = RunConfig.from_profile("demo", event_count=25, seed=7)
    assert config.event_count == 25
    assert config.rate_per_second == 500
    assert config.seed == 7


@pytest.mark.parametrize(
    "field", ["duplicate_rate", "invalid_rate", "out_of_order_rate", "too_late_rate"]
)
def test_rejects_invalid_rates(field: str) -> None:
    with pytest.raises(ValueError):
        RunConfig.from_profile("demo", **{field: 1.1})


@pytest.mark.parametrize("run_id", ["", "has spaces", "bad/slash", "x" * 81])
def test_rejects_invalid_run_ids(run_id: str) -> None:
    with pytest.raises(ValueError):
        RunConfig.from_profile("demo", run_id=run_id)
