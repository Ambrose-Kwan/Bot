"""Offline concurrency scheduler for exercising checkout orchestration safely.

This entry point deliberately uses a local simulator instead of VansSite or an
HTTP session. Profiles contain only synthetic test IDs and simulation settings.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from core.state_machine import AsyncStateMachine, CheckoutState

logger = logging.getLogger("nexus.offline_scheduler")

SimulationFailurePoint = Literal["INIT", "MONITOR", "ATC", "SHIPPING", "PAYMENT"]


class SimulationProfile(BaseModel):
    """Non-sensitive inputs for an offline simulation task."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    profile_id: str = Field(min_length=1)
    profile_name: str = "Offline test profile"
    stock_available: bool = True
    fail_at: SimulationFailurePoint | None = None
    step_delay_ms: int = Field(default=10, ge=0, le=10_000)


class SimulationResult(BaseModel):
    """Result and FSM metrics for one simulated task."""

    task_id: str
    profile_id: str
    status: Literal["SUCCESS", "FAILED"]
    final_state: CheckoutState
    elapsed_seconds: float = Field(ge=0)
    states_visited: list[CheckoutState]
    error_message: str | None = None


class OfflineVansSimulation:
    """A deterministic workflow simulator; it has no HTTP or proxy support."""

    def __init__(self, task_id: str, profile: SimulationProfile) -> None:
        self.task_id = task_id
        self.profile = profile
        self.fsm = AsyncStateMachine(task_id=task_id)
        self.states_visited = [CheckoutState.INIT]

    async def run(self) -> SimulationResult:
        started_at = time.monotonic()
        failure_message: str | None = None

        try:
            while not self.fsm.is_terminal:
                current = self.fsm.current_state
                await self._simulate_step(current)
                if current == CheckoutState.INIT:
                    next_state = CheckoutState.MONITOR
                elif current == CheckoutState.MONITOR:
                    if not self.profile.stock_available:
                        raise RuntimeError("Simulated inventory is unavailable")
                    next_state = CheckoutState.ATC
                elif current == CheckoutState.ATC:
                    next_state = CheckoutState.SHIPPING
                elif current == CheckoutState.SHIPPING:
                    next_state = CheckoutState.PAYMENT
                elif current == CheckoutState.PAYMENT:
                    next_state = CheckoutState.SUCCESS
                else:
                    raise RuntimeError(f"Unexpected simulation state: {current.value}")
                await self._transition(next_state)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Simulation task failed")
            failure_message = str(exc)
            if not self.fsm.is_terminal:
                await self._transition(CheckoutState.FAILED)

        return SimulationResult(
            task_id=self.task_id,
            profile_id=self.profile.profile_id,
            status="SUCCESS" if self.fsm.current_state == CheckoutState.SUCCESS else "FAILED",
            final_state=self.fsm.current_state,
            elapsed_seconds=round(time.monotonic() - started_at, 4),
            states_visited=self.states_visited,
            error_message=failure_message,
        )

    async def _simulate_step(self, state: CheckoutState) -> None:
        delay = self.profile.step_delay_ms / 1000
        if delay:
            await asyncio.sleep(delay)
        if self.profile.fail_at == state.value:
            raise RuntimeError(f"Simulated failure at {state.value}")

    async def _transition(self, state: CheckoutState) -> None:
        await self.fsm.transition_to(state)
        self.states_visited.append(state)


class OfflineScheduler:
    """Runs synthetic tasks under a bounded concurrency semaphore."""

    def __init__(self, concurrency_limit: int) -> None:
        if concurrency_limit < 1:
            raise ValueError("concurrency_limit must be at least 1")
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.active_tasks = 0
        self.max_active_tasks = 0

    async def worker(self, task_id: str, profile: SimulationProfile) -> SimulationResult:
        async with self.semaphore:
            self.active_tasks += 1
            self.max_active_tasks = max(self.max_active_tasks, self.active_tasks)
            try:
                logger.info("[%s] starting offline simulation for profile %s", task_id, profile.profile_id)
                return await OfflineVansSimulation(task_id, profile).run()
            finally:
                self.active_tasks -= 1

    async def run(self, profiles: list[SimulationProfile]) -> list[SimulationResult]:
        jobs = [
            self.worker(f"TASK-{index + 1:02d}", profile)
            for index, profile in enumerate(profiles)
        ]
        results = await asyncio.gather(*jobs, return_exceptions=True)

        normalized: list[SimulationResult] = []
        for index, result in enumerate(results):
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, BaseException):
                profile = profiles[index]
                logger.error(
                    "Task TASK-%02d failed outside the simulator: %s",
                    index + 1,
                    result,
                )
                normalized.append(
                    SimulationResult(
                        task_id=f"TASK-{index + 1:02d}",
                        profile_id=profile.profile_id,
                        status="FAILED",
                        final_state=CheckoutState.FAILED,
                        elapsed_seconds=0,
                        states_visited=[CheckoutState.INIT, CheckoutState.FAILED],
                        error_message=str(result),
                    )
                )
            else:
                normalized.append(result)
        return normalized


def load_profiles(path: Path) -> list[SimulationProfile]:
    """Load and validate synthetic profiles from a JSON array."""
    try:
        with path.open("r", encoding="utf-8") as profile_file:
            raw_profiles = json.load(profile_file)
    except OSError as exc:
        raise OSError(f"Unable to read simulation profiles from {path}: {exc}") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in simulation profile file {path}: {exc}") from exc

    if not isinstance(raw_profiles, list):
        raise TypeError("Simulation profiles file must contain a JSON array")
    profiles = [SimulationProfile.model_validate(item) for item in raw_profiles]
    profile_ids = [profile.profile_id for profile in profiles]
    if len(profile_ids) != len(set(profile_ids)):
        raise ValueError("Simulation profile IDs must be unique")
    return profiles


def report_results(
    results: list[SimulationResult],
    max_active_tasks: int,
    concurrency_limit: int,
) -> None:
    """Print per-task duration and aggregate FSM-state visit counts."""
    state_distribution = {state.value: 0 for state in CheckoutState}
    for result in results:
        for state in result.states_visited:
            state_distribution[state.value] += 1
        logger.info(
            "%s | %s | final=%s | elapsed=%.4fs%s",
            result.task_id,
            result.status,
            result.final_state.value,
            result.elapsed_seconds,
            f" | {result.error_message}" if result.error_message else "",
        )

    succeeded = sum(result.status == "SUCCESS" for result in results)
    logger.info(
        "Summary: total=%d succeeded=%d failed=%d concurrency_limit=%d observed_max=%d",
        len(results),
        succeeded,
        len(results) - succeeded,
        concurrency_limit,
        max_active_tasks,
    )
    logger.info(
        "FSM state visits: %s",
        ", ".join(f"{state}: {count}" for state, count in state_distribution.items()),
    )


async def main(
    profiles_path: Path = Path("config/profiles.json"),
    concurrency_limit: int = 5,
) -> list[SimulationResult]:
    """Load synthetic inputs, execute offline tasks, and report the run."""
    profiles = load_profiles(profiles_path)
    if not profiles:
        logger.warning("No simulation profiles found in %s", profiles_path)
        return []

    scheduler = OfflineScheduler(concurrency_limit)
    results = await scheduler.run(profiles)
    report_results(results, scheduler.max_active_tasks, concurrency_limit)
    return results


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    mode_parser = argparse.ArgumentParser(add_help=False)
    mode_parser.add_argument("--mode", choices=("offline", "live"), default="offline")
    mode_args, remaining = mode_parser.parse_known_args(argv)
    if mode_args.mode == "live":
        return argparse.Namespace(mode="live", live_args=remaining)

    parser = argparse.ArgumentParser(
        description="Run the offline checkout workflow simulator",
        epilog="Use --mode live to explicitly dispatch Vans tasks through main_live.py.",
    )
    parser.add_argument(
        "--mode",
        choices=("offline", "live"),
        default="offline",
        help="Select offline simulation (default) or explicit live dispatch",
    )
    parser.add_argument(
        "--profiles",
        type=Path,
        default=Path("config/profiles.json"),
        help="JSON file containing synthetic simulation profiles",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum number of simultaneous offline tasks",
    )
    arguments = parser.parse_args(remaining)
    arguments.mode = "offline"
    return arguments


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    arguments = _parse_args()
    if arguments.mode == "live":
        try:
            from .main_live import async_main as live_main
        except ImportError:
            from main_live import async_main as live_main
        raise SystemExit(asyncio.run(live_main(arguments.live_args)))
    asyncio.run(main(arguments.profiles, arguments.concurrency))