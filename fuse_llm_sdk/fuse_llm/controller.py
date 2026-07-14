"""TrafficAwareController: traffic-driven model switching for FuseModelDeployment.

.. warning::
   **Pending rewrite.** This controller targets the pre-combination,
   single-active-model API (``switch_model`` / ``active_model``) and its
   automatic triggers rely on requests *queueing* for a sleeping model.  The
   deployment now uses combinations (``switch_combination`` / ``active_models``)
   and **rejects** requests for non-awake models, so nothing queues and these
   triggers never fire.  ``deploy()`` no longer starts it by default
   (``start_controller=False``).  It is kept for reference until a
   combination-aware controller replaces it; use ``switch_combination`` for
   explicit switching in the meantime.

The controller periodically polls the :class:`FuseModelDeployment` for
runtime statistics (QPS, queue depth, in-flight requests) and decides
when to switch the active model.

Switching policy (in priority order)
-------------------------------------
1. **Manual override** — ``force_switch()`` sets a target model that
   takes priority over all automatic decisions.
2. **Queue threshold** — if a non-active model's queued requests exceed
   ``queue_threshold``, switch to it.
3. **Traffic ratio** — if a non-active model's observed QPS exceeds
   its expected share (``model_weights * total_qps * 1.5``), switch.
4. **Cooldown** — enforce a minimum interval between switches.
"""

import asyncio
import logging
import time
from typing import Any, Dict, List, Optional

import ray

logger = logging.getLogger(__name__)


@ray.remote(num_cpus=0)
class TrafficAwareController:
    """Traffic-aware controller for multi-model GPU time-sharing.

    Parameters
    ----------
    fuse_deployment_handle:
        A Ray Serve handle (or Ray Actor handle) to the
        :class:`FuseModelDeployment`.  Must expose ``get_stats()`` and
        ``switch_model()``.
    model_weights:
        Expected traffic share per model, e.g.
        ``{"llama-8b": 0.7, "qwen-7b": 0.3}``.
    poll_interval_s:
        Seconds between polls of :meth:`get_stats`.
    min_interval_s:
        Minimum seconds between two consecutive model switches.
    queue_threshold:
        If a non-active model's ``queued_requests`` exceeds this
        number, switch to it immediately.
    drain_timeout_s:
        Passed to ``switch_model()`` as ``drain_timeout``.
    qps_window_s:
        Sliding window (seconds) for QPS calculation.
    default_sleep_level:
        Default vLLM sleep level (1 or 2) used when sleeping models
        during automatic switching.
    """

    def __init__(
        self,
        fuse_deployment_handle: Any,
        model_weights: Dict[str, float],
        poll_interval_s: float = 10.0,
        min_interval_s: float = 60.0,
        queue_threshold: int = 10,
        drain_timeout_s: float = 30.0,
        qps_window_s: float = 60.0,
        default_sleep_level: int = 1,
    ) -> None:
        self._handle = fuse_deployment_handle
        self._model_weights = model_weights
        self._poll_interval = poll_interval_s
        self._min_interval = min_interval_s
        self._queue_threshold = queue_threshold
        self._drain_timeout = drain_timeout_s
        self._qps_window = qps_window_s
        self._default_sleep_level = default_sleep_level

        self._last_switch_time: float = 0.0
        self._running: bool = False

        self._override_model: Optional[str] = None

        self._request_history: Dict[str, List[tuple]] = {
            m: [] for m in model_weights
        }

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background control loop (runs indefinitely)."""
        self._running = True
        logger.info(
            "TrafficAwareController started (poll=%.1fs, min_interval=%.1fs, "
            "sleep_level=%d)",
            self._poll_interval,
            self._min_interval,
            self._default_sleep_level,
        )
        while self._running:
            try:
                await self._poll_and_decide()
            except Exception as e:
                logger.error("Controller poll error: %s", e, exc_info=True)
            await asyncio.sleep(self._poll_interval)

    async def stop(self) -> None:
        """Stop the background control loop."""
        self._running = False
        logger.info("TrafficAwareController stopped.")

    async def force_switch(
        self, target_model: str, sleep_level: Optional[int] = None
    ) -> bool:
        """Manually force a switch to *target_model*.

        Parameters
        ----------
        target_model:
            The model to switch to.
        sleep_level:
            vLLM sleep level to use when sleeping the current model.
            If ``None``, uses the controller's ``default_sleep_level``.
        """
        level = sleep_level if sleep_level is not None else self._default_sleep_level
        logger.info("Force switch to '%s' (sleep_level=%d).", target_model, level)
        self._override_model = target_model
        result = await self._handle.switch_model.remote(
            target_model,
            drain_timeout=self._drain_timeout,
            sleep_level=level,
        )
        self._last_switch_time = time.time()
        return result

    async def clear_override(self) -> None:
        """Clear the manual override and resume automatic mode."""
        logger.info("Manual override cleared, resuming automatic mode.")
        self._override_model = None

    async def get_status(self) -> Dict[str, Any]:
        """Return the controller's view of the world."""
        stats = await self._handle.get_stats.remote()
        qps = self._calculate_qps()
        return {
            "override_model": self._override_model,
            "last_switch_time": self._last_switch_time,
            "can_switch": self._can_switch(),
            "qps": qps,
            "deployment_stats": stats,
        }

    # ------------------------------------------------------------------
    # Internal logic
    # ------------------------------------------------------------------

    async def _poll_and_decide(self) -> None:
        """Poll deployment stats and make a switching decision."""
        stats = await self._handle.get_stats.remote()
        active_model = stats["active_model"]
        models = stats["models"]

        now = time.time()
        for model_id, m_stats in models.items():
            history = self._request_history.setdefault(model_id, [])
            history.append((now, m_stats["total_requests"]))
            cutoff = now - max(self._qps_window * 5, 300)
            self._request_history[model_id] = [
                (t, r) for t, r in history if t > cutoff
            ]

        qps = self._calculate_qps()

        # 1. Manual override
        if self._override_model:
            if self._override_model != active_model and self._can_switch():
                logger.info("Override: switching to '%s'", self._override_model)
                await self._handle.switch_model.remote(
                    self._override_model,
                    drain_timeout=self._drain_timeout,
                    sleep_level=self._default_sleep_level,
                )
                self._last_switch_time = now
            return

        # 2. Queue threshold trigger
        for model_id, m_stats in models.items():
            if (
                model_id != active_model
                and m_stats["queued_requests"] >= self._queue_threshold
            ):
                if self._can_switch():
                    logger.info(
                        "Queue threshold triggered: switching to '%s' "
                        "(queued=%d >= threshold=%d)",
                        model_id,
                        m_stats["queued_requests"],
                        self._queue_threshold,
                    )
                    await self._handle.switch_model.remote(
                        model_id,
                        drain_timeout=self._drain_timeout,
                        sleep_level=self._default_sleep_level,
                    )
                    self._last_switch_time = now
                    return

        # 3. Traffic-ratio trigger
        total_qps = sum(qps.values())
        if total_qps > 0:
            for model_id, weight in self._model_weights.items():
                expected_qps = total_qps * weight
                actual_qps = qps.get(model_id, 0.0)

                if (
                    model_id != active_model
                    and actual_qps > 0
                    and actual_qps > expected_qps * 1.5
                    and self._can_switch()
                ):
                    logger.info(
                        "Traffic ratio triggered: switching to '%s' "
                        "(qps=%.1f > expected=%.1f)",
                        model_id,
                        actual_qps,
                        expected_qps,
                    )
                    await self._handle.switch_model.remote(
                        model_id,
                        drain_timeout=self._drain_timeout,
                        sleep_level=self._default_sleep_level,
                    )
                    self._last_switch_time = now
                    return

    def _calculate_qps(self) -> Dict[str, float]:
        """Estimate per-model QPS over the sliding window."""
        now = time.time()
        qps: Dict[str, float] = {}

        for model_id, history in self._request_history.items():
            if len(history) < 2:
                qps[model_id] = 0.0
                continue

            recent = [(t, r) for t, r in history if now - t <= self._qps_window]
            if len(recent) < 2:
                qps[model_id] = 0.0
                continue

            delta_requests = recent[-1][1] - recent[0][1]
            delta_time = recent[-1][0] - recent[0][0]
            qps[model_id] = delta_requests / delta_time if delta_time > 0 else 0.0

        return qps

    def _can_switch(self) -> bool:
        """Check whether the cooldown period has elapsed."""
        return time.time() - self._last_switch_time >= self._min_interval
