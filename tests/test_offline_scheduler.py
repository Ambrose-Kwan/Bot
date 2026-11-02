from __future__ import annotations

import json
from pathlib import Path

import pytest

from core.state_machine import CheckoutState
from main import OfflineScheduler, SimulationProfile, load_profiles


@pytest.mark.asyncio
async def test_scheduler_respects_concurrency_and_reports_fsm_outcomes() -> None:
    profiles = [
        SimulationProfile(profile_id=f"SIM-{index}", step_delay_ms=5)
        for index in range(6)
    ]
    profiles[1] = SimulationProfile(
        profile_id="SIM-1",
        stock_available=False,
        step_delay_ms=5,
    )
    scheduler = OfflineScheduler(concurrency_limit=2)

    results = await scheduler.run(profiles)

    assert len(results) == 6
    assert scheduler.max_active_tasks == 2
    assert all(result.status == "SUCCESS" for result in results if result.profile_id != "SIM-1")
    no_stock_result = next(result for result in results if result.profile_id == "SIM-1")
    assert no_stock_result.status == "FAILED"
    assert no_stock_result.final_state is CheckoutState.FAILED
    assert no_stock_result.states_visited == [
        CheckoutState.INIT,
        CheckoutState.MONITOR,
        CheckoutState.FAILED,
    ]


def test_load_profiles_validates_unique_synthetic_ids(tmp_path: Path) -> None:
    profiles_path = tmp_path / "profiles.json"
    profiles_path.write_text(
        json.dumps(
            [
                {"profile_id": "SIM-1", "step_delay_ms": 0},
                {"profile_id": "SIM-2", "fail_at": "PAYMENT", "step_delay_ms": 0},
            ]
        ),
        encoding="utf-8",
    )

    profiles = load_profiles(profiles_path)

    assert [profile.profile_id for profile in profiles] == ["SIM-1", "SIM-2"]


def test_load_profiles_rejects_duplicate_ids(tmp_path: Path) -> None:
    profiles_path = tmp_path / "profiles.json"
    profiles_path.write_text(
        json.dumps([{"profile_id": "SIM-1"}, {"profile_id": "SIM-1"}]),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unique"):
        load_profiles(profiles_path)


def test_scheduler_rejects_non_positive_concurrency() -> None:
    with pytest.raises(ValueError, match="at least 1"):
        OfflineScheduler(0)
