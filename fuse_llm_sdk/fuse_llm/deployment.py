"""FuseModelDeployment: single-actor multi-model GPU time-sharing.

This module implements a Ray Serve deployment that loads multiple vLLM
engines inside a single actor, sharing one GPU (or one set of GPUs at
``tensor_parallel_size > 1``).  A **combination** — an arbitrary subset of
the loaded models — is *awake* (holding GPU memory) at any given time; the
rest are put to sleep via vLLM's ``sleep`` / ``wakeup`` mechanism.  Switching
from one combination to another sleeps the models leaving the awake set and
wakes the ones entering it, keeping the overlap awake.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, AsyncGenerator, Dict, List, Optional, Set

from ray.serve.llm import LLMConfig

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# VLLMEngine resolution
# ---------------------------------------------------------------------------

_VLLM_ENGINE_CLS = None


def set_vllm_engine_class(cls):
    """Override the VLLMEngine class used by FuseModelDeployment.

    By default, the engine class is imported from
    ``ray.llm._internal.serve.engines.vllm.vllm_engine.VLLMEngine``.
    If you want to use a custom engine (e.g. for testing or a different
    vLLM version), call this function before initialising the deployment.

    Example::

        from fuse_llm import set_vllm_engine_class
        from my_custom_engine import MyVLLMEngine
        set_vllm_engine_class(MyVLLMEngine)
    """
    global _VLLM_ENGINE_CLS
    _VLLM_ENGINE_CLS = cls


def _get_vllm_engine_class():
    """Resolve the VLLMEngine class to use.

    Priority:
    1. Class set via :func:`set_vllm_engine_class`
    2. Environment variable ``FUSE_LLM_VLLM_ENGINE_CLS``
    3. Default: ``ray.llm._internal.serve.engines.vllm.vllm_engine.VLLMEngine``
    """
    global _VLLM_ENGINE_CLS

    if _VLLM_ENGINE_CLS is not None:
        return _VLLM_ENGINE_CLS

    import os

    custom_path = os.environ.get("FUSE_LLM_VLLM_ENGINE_CLS")
    if custom_path:
        from importlib import import_module

        module_path, class_name = custom_path.rsplit(".", 1)
        module = import_module(module_path)
        _VLLM_ENGINE_CLS = getattr(module, class_name)
        return _VLLM_ENGINE_CLS

    # Default: the in-process AsyncLLM engine using FusedRayExecutor + the
    # deployment's shared placement group.  (The stock ray.serve.llm VLLMEngine
    # forces distributed_executor_backend="ray" and reserves whole GPUs per
    # engine, so it cannot time-share GPUs across engines.)
    from fuse_llm.engines import InProcessVLLMEngine

    _VLLM_ENGINE_CLS = InProcessVLLMEngine
    return _VLLM_ENGINE_CLS


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ModelStats:
    """Runtime statistics for a single model inside the fuse deployment."""

    model_id: str
    state: str = "uninitialized"
    """Lifecycle state: uninitialized | awake | sleeping | draining | switching | error"""

    in_flight_requests: int = 0
    """Number of requests currently being processed by this model's engine."""

    total_requests: int = 0
    """Total number of requests processed since deployment start."""

    queued_requests: int = 0
    """Deprecated: always 0.  Requests for a non-awake model are now rejected
    immediately rather than queued, so nothing accumulates here.  Retained for
    stats-shape compatibility."""

    last_active_time: float = 0.0
    """Epoch timestamp of the last completed request."""

    request_rate_per_min: float = 0.0
    """Recent QPS estimate (updated by the controller)."""

    sleep_level: Optional[int] = None
    """vLLM sleep level used when this model was put to sleep.

    - ``None``: model is awake or was never slept.
    - ``1``: Level 1 sleep — weights offloaded to CPU RAM, KV cache
      discarded.  Wake-up must reallocate both weights and KV cache
      (``wakeup()`` with no tags wakes every component).
    - ``2``: Level 2 sleep — both weights and KV cache discarded.
      Wake-up requires three steps:
      1. ``wakeup(tags=["weights"])``  — reallocate weight memory
      2. ``collective_rpc("reload_weights")`` — load weights from disk
      3. ``wakeup(tags=["kv_cache"])``  — reallocate KV cache memory
    """


# ---------------------------------------------------------------------------
# FuseModelDeployment
# ---------------------------------------------------------------------------


class FuseModelDeployment:
    """Single-actor multi-model GPU time-sharing deployment.

    Loads multiple vLLM engines inside one Ray Serve actor.  All engines
    share the same GPU(s) (same ``CUDA_VISIBLE_DEVICES`` / placement group).
    At any point in time a **combination** — an arbitrary subset of the
    loaded models — is *awake*; the rest are sleeping via ``sleep(level=1)``
    which offloads model weights to CPU RAM and discards the KV cache.  As
    many models can be awake together as fit in GPU memory (set
    ``gpu_memory_utilization`` low enough per model), and callers switch
    combinations explicitly via :meth:`switch_combination`.

    Core responsibilities
    ---------------------
    1.  **Sequential initialisation** — start engines one at a time,
        immediately sleeping the ones outside the default combination to
        keep peak GPU usage bounded during startup.
    2.  **Request routing** — forward incoming requests to the engine
        identified by ``request.model``.  Requests for a model that is not
        in the currently-awake combination are **rejected** — the caller
        must :meth:`switch_combination` first.
    3.  **In-flight tracking** — maintain a per-model counter of
        in-flight requests, used to drain gracefully on a switch.
    4.  **Graceful switching** — :meth:`switch_combination` drains
        in-flight requests on the models leaving the awake set (with a
        timeout), sleeps them, then wakes the models entering it.

    Parameters
    ----------
    llm_configs:
        List of :class:`~ray.serve.llm.LLMConfig` objects, one per model.
    default_models:
        List of ``model_id`` values forming the combination that should be
        awake after initialisation.  Defaults to ``[first config]``.  Every
        entry must appear in ``llm_configs``, and the combination must fit
        in GPU memory simultaneously (the caller's responsibility).
    drain_timeout_s:
        Default timeout (seconds) for draining in-flight requests during
        a combination switch.
    """

    def __init__(
        self,
        llm_configs: List[LLMConfig],
        default_models: Optional[List[str]] = None,
        drain_timeout_s: float = 30.0,
    ) -> None:
        self._llm_configs: Dict[str, LLMConfig] = {
            c.model_loading_config.model_id: c for c in llm_configs
        }
        if not self._llm_configs:
            raise ValueError("At least one LLMConfig must be provided")

        # Restriction check — done HERE, before any engine is loaded, so a bad
        # config fails immediately instead of deep inside vLLM after minutes of
        # model loading.  All models share ONE placement group sized to their
        # tensor_parallel_size, so every model must use the same TP.
        tp_by_model = {
            m: int((c.engine_kwargs or {}).get("tensor_parallel_size", 1))
            for m, c in self._llm_configs.items()
        }
        if len(set(tp_by_model.values())) > 1:
            raise ValueError(
                "All models in a FuseModelDeployment must use the same "
                "tensor_parallel_size — they share a single placement group "
                f"sized to it. Got per-model tensor_parallel_size: {tp_by_model}."
            )
        self._tensor_parallel_size = next(iter(tp_by_model.values()))

        # VLLMEngine instances — populated during _initialize()
        self._engines: Dict[str, Any] = {}

        # Per-model sleep level tracking.
        # Keyed by model_id; value is 1 or 2 (or None if awake).
        self._sleep_levels: Dict[str, Optional[int]] = {
            m: None for m in self._llm_configs
        }

        # Per-model statistics
        self._stats: Dict[str, ModelStats] = {}
        for model_id in self._llm_configs:
            self._stats[model_id] = ModelStats(model_id=model_id)

        # The currently awake combination (set of model_ids).  Routing
        # accepts a request iff its model is in this set.
        self._active_models: Set[str] = set()

        # Lock to serialise concurrent switch_combination calls
        self._switch_lock = asyncio.Lock()

        # Resolve the default combination
        if default_models is None:
            default_models = [next(iter(self._llm_configs))]
        if not default_models:
            raise ValueError("default_models must contain at least one model")
        unknown = [m for m in default_models if m not in self._llm_configs]
        if unknown:
            raise ValueError(
                f"default_models {unknown} not found in llm_configs"
            )
        # Preserve order, drop duplicates.
        self._default_models: List[str] = list(dict.fromkeys(default_models))
        self._drain_timeout_s = drain_timeout_s

        self._initialized = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def _initialize(self) -> None:
        """Sequentially start every engine, sleeping all but the default combo.

        Engines are started one at a time with the **default combination
        loaded last**, so that models outside it sleep and free their GPU
        memory before the awake set is built up.  This keeps peak GPU usage
        during startup to ``max(1, len(default_models))`` engines' worth,
        which is required at high ``gpu_memory_utilization``.  (The default
        combination itself must fit in memory simultaneously — the caller's
        responsibility.)

        This method is **idempotent** — calling it again after the
        first successful run is a no-op.
        """
        if self._initialized:
            return

        engine_cls = _get_vllm_engine_class()

        # Load the default-combination members LAST so that during startup
        # each non-default engine is put to sleep (freeing its GPU memory)
        # before the awake set is assembled.  Loading a default member earlier
        # would keep it awake while unrelated engines start, inflating peak
        # GPU memory beyond the intended combination.
        default_set = set(self._default_models)
        ordered_ids = [m for m in self._llm_configs if m not in default_set]
        ordered_ids.extend(self._default_models)

        for model_id in ordered_ids:
            llm_config = self._llm_configs[model_id]
            logger.info("Initialising engine for model '%s' ...", model_id)

            # 1. Create the engine
            engine = engine_cls(llm_config)

            # 2. Start (allocates GPU memory, loads model weights)
            await engine.start()
            self._engines[model_id] = engine
            self._stats[model_id].state = "awake"
            logger.info("Engine for '%s' started.", model_id)

            # 3. Sleep immediately unless this model is in the default combo.
            #    Use Level 1 by default during initialisation (weights
            #    kept in CPU RAM for fast wake-up).
            if model_id not in default_set:
                await engine.sleep(level=1)
                self._sleep_levels[model_id] = 1
                self._stats[model_id].state = "sleeping"
                self._stats[model_id].sleep_level = 1
                logger.info("Engine for '%s' put to sleep (level=1).", model_id)
            else:
                # Default-combination members are loaded last and left awake.
                self._active_models.add(model_id)
                self._stats[model_id].last_active_time = time.time()
                logger.info(
                    "Engine for '%s' is awake (default combination).", model_id
                )

        self._initialized = True
        logger.info(
            "FuseModelDeployment initialised with %d models, active=%s",
            len(self._engines),
            sorted(self._active_models),
        )

    # ------------------------------------------------------------------
    # Request handling
    # ------------------------------------------------------------------

    async def chat(
        self,
        request: Any,
        raw_request_info: Optional[Any] = None,
    ) -> AsyncGenerator[Any, None]:
        """Handle a chat-completion request.

        Routes to the engine identified by ``request.model``.  If that
        model is not in the currently-awake combination, the request is
        **rejected** — call :meth:`switch_combination` to wake it first.

        Yields response chunks compatible with the OpenAI streaming format.
        """
        model_id = self._require_awake(request)

        self._stats[model_id].in_flight_requests += 1
        self._stats[model_id].total_requests += 1

        try:
            engine = self._engines[model_id]
            async for response in engine.chat(request, raw_request_info):
                yield response
        finally:
            self._stats[model_id].in_flight_requests -= 1
            self._stats[model_id].last_active_time = time.time()

    async def completions(
        self,
        request: Any,
        raw_request_info: Optional[Any] = None,
    ) -> AsyncGenerator[Any, None]:
        """Handle a completion request (same routing logic as :meth:`chat`)."""
        model_id = self._require_awake(request)

        self._stats[model_id].in_flight_requests += 1
        self._stats[model_id].total_requests += 1

        try:
            engine = self._engines[model_id]
            async for response in engine.completions(request, raw_request_info):
                yield response
        finally:
            self._stats[model_id].in_flight_requests -= 1
            self._stats[model_id].last_active_time = time.time()

    def _require_awake(self, request: Any) -> str:
        """Resolve ``request.model`` and ensure it is in the awake combination.

        Returns the validated ``model_id``.  Raises ``ValueError`` for an
        unknown model, or ``RuntimeError`` if the model is loaded but asleep
        (the caller must :meth:`switch_combination` to wake it first).
        """
        model_id = getattr(request, "model", None)
        if model_id is None or model_id not in self._engines:
            raise ValueError(
                f"Unknown model '{model_id}'. Available: {list(self._engines)}"
            )
        if model_id not in self._active_models:
            raise RuntimeError(
                f"Model '{model_id}' is not awake. Active combination: "
                f"{sorted(self._active_models)}. Call switch_combination() to "
                f"wake it before sending requests."
            )
        return model_id

    # ------------------------------------------------------------------
    # Combination switching
    # ------------------------------------------------------------------

    async def switch_combination(
        self,
        models: List[str],
        drain_timeout: Optional[float] = None,
        sleep_level: int = 1,
    ) -> bool:
        """Switch the awake combination to exactly ``models``.

        Models currently awake but not in ``models`` are drained and slept;
        models in ``models`` not currently awake are woken; the overlap is
        left untouched.  The outgoing models are slept **before** the incoming
        ones are woken, so peak GPU memory never exceeds
        ``max(len(current), len(models))`` engines' worth (never the union).

        Parameters
        ----------
        models:
            The exact set of ``model_id`` values to be awake afterwards.  May
            be empty (sleeps every model).  Every entry must be a loaded model.
        drain_timeout:
            Override the default drain timeout.  ``None`` uses the
            deployment-level default.
        sleep_level:
            vLLM sleep level used when sleeping the *outgoing* models.
            - ``1`` (default): offload weights to CPU RAM, discard KV cache.
            - ``2``: discard both weights and KV cache.  Slower wake-up
              (weights must be reloaded from disk), but frees CPU RAM.

        Returns
        -------
        bool
            ``True`` if the switch succeeded.
        """
        if sleep_level not in (1, 2):
            raise ValueError(f"sleep_level must be 1 or 2, got {sleep_level}")

        target = set(models)
        unknown = target - set(self._engines)
        if unknown:
            raise ValueError(
                f"Unknown model(s): {sorted(unknown)}. "
                f"Available: {list(self._engines)}"
            )

        async with self._switch_lock:
            current = set(self._active_models)
            if target == current:
                return True

            to_sleep = current - target
            to_wake = target - current
            timeout = (
                drain_timeout if drain_timeout is not None else self._drain_timeout_s
            )

            logger.info(
                "Switching combination: %s -> %s "
                "(sleep=%s, wake=%s, drain_timeout=%.1fs, sleep_level=%d)",
                sorted(current),
                sorted(target),
                sorted(to_sleep),
                sorted(to_wake),
                timeout,
                sleep_level,
            )

            # 1. Stop accepting new requests for the outgoing models by
            #    removing them from the awake set immediately (routing rejects
            #    them from here on) and marking them draining.
            for model_id in to_sleep:
                self._active_models.discard(model_id)
                self._stats[model_id].state = "draining"

            # 2. Wait for in-flight requests on the outgoing models to drain.
            in_flight = sum(
                self._stats[m].in_flight_requests for m in to_sleep
            )
            if in_flight > 0:
                logger.info(
                    "Draining %d in-flight request(s) on %s ...",
                    in_flight,
                    sorted(to_sleep),
                )
                deadline = time.time() + timeout
                while time.time() < deadline:
                    if all(
                        self._stats[m].in_flight_requests == 0 for m in to_sleep
                    ):
                        break
                    await asyncio.sleep(0.5)

                remaining = sum(
                    self._stats[m].in_flight_requests for m in to_sleep
                )
                if remaining > 0:
                    logger.warning(
                        "Drain timeout: %d request(s) still in-flight on %s, "
                        "force sleeping (these requests will be interrupted)",
                        remaining,
                        sorted(to_sleep),
                    )

            # 3. Sleep the outgoing models FIRST (frees GPU memory) and record
            #    the sleep level, so the incoming models can wake into it.
            for model_id in to_sleep:
                self._stats[model_id].state = "switching"
                await self._engines[model_id].sleep(level=sleep_level)
                self._sleep_levels[model_id] = sleep_level
                self._stats[model_id].sleep_level = sleep_level
                self._stats[model_id].state = "sleeping"
                logger.info(
                    "Model '%s' put to sleep (level=%d).", model_id, sleep_level
                )

            # 4. Wake the incoming models using the correct wake-up sequence.
            for model_id in to_wake:
                self._stats[model_id].state = "switching"
                await self._wakeup_engine(model_id)
                self._active_models.add(model_id)
                self._stats[model_id].state = "awake"
                self._stats[model_id].last_active_time = time.time()

            logger.info(
                "Switch complete: awake combination is now %s",
                sorted(self._active_models),
            )
            return True

    async def _wakeup_engine(self, model_id: str) -> None:
        """Wake up a model using the correct sequence for its sleep level.

        - **Level 1** (weights in CPU RAM): wake **all** components.  Level-1
          sleep offloads weights to CPU RAM *and discards the KV cache*, so
          both must be reallocated on wake.  Waking only ``["weights"]``
          leaves the KV cache asleep — the engine then reports
          ``is_sleeping() == True`` and hangs on the next request.  Passing
          no tags wakes every component.
        - **Level 2** (weights discarded): three-step sequence:
          1. ``wakeup(tags=["weights"])`` — reallocate weight GPU memory
          2. ``collective_rpc("reload_weights")`` — load weights from disk
          3. ``wakeup(tags=["kv_cache"])`` — reallocate KV cache memory
        """
        engine = self._engines[model_id]
        level = self._sleep_levels.get(model_id)

        if level == 2:
            logger.info(
                "Waking up '%s' from level-2 sleep "
                "(3-step: weights -> reload -> kv_cache) ...",
                model_id,
            )
            await engine.wakeup(tags=["weights"])
            await engine.collective_rpc(method="reload_weights")
            await engine.wakeup(tags=["kv_cache"])
        else:
            logger.info(
                "Waking up '%s' from level-1 sleep "
                "(weights + kv_cache) ...",
                model_id,
            )
            await engine.wakeup()

        self._sleep_levels[model_id] = None
        self._stats[model_id].sleep_level = None

    # ------------------------------------------------------------------
    # Stats & control-plane helpers
    # ------------------------------------------------------------------

    async def get_stats(self) -> Dict[str, Any]:
        """Return runtime statistics for all models."""
        return {
            "active_models": sorted(self._active_models),
            "models": {
                model_id: {
                    "state": s.state,
                    "sleep_level": s.sleep_level,
                    "in_flight_requests": s.in_flight_requests,
                    "total_requests": s.total_requests,
                    "queued_requests": s.queued_requests,
                    "last_active_time": s.last_active_time,
                    "request_rate_per_min": s.request_rate_per_min,
                }
                for model_id, s in self._stats.items()
            },
        }

    async def sleep_model(self, model_id: str, level: int = 1) -> None:
        """Manually sleep a specific model (control-plane helper).

        Parameters
        ----------
        model_id:
            The model to sleep.
        level:
            vLLM sleep level:
            - ``1`` (default): offload weights to CPU RAM, discard KV cache.
            - ``2``: discard both weights and KV cache (frees CPU RAM,
              but wake-up requires reloading weights from disk).
        """
        if model_id not in self._engines:
            raise ValueError(f"Unknown model: {model_id}")
        if level not in (1, 2):
            raise ValueError(f"level must be 1 or 2, got {level}")
        await self._engines[model_id].sleep(level=level)
        self._active_models.discard(model_id)
        self._sleep_levels[model_id] = level
        self._stats[model_id].sleep_level = level
        self._stats[model_id].state = "sleeping"
        logger.info(
            "Model '%s' manually put to sleep (level=%d).", model_id, level
        )

    async def wakeup_model(self, model_id: str) -> None:
        """Manually wake a specific model (control-plane helper).

        Adds the model to the awake combination and uses the correct
        wake-up sequence based on the model's recorded sleep level.
        """
        if model_id not in self._engines:
            raise ValueError(f"Unknown model: {model_id}")
        await self._wakeup_engine(model_id)
        self._active_models.add(model_id)
        self._stats[model_id].state = "awake"
        self._stats[model_id].last_active_time = time.time()

    async def is_model_sleeping(self, model_id: str) -> bool:
        """Check whether a specific model is sleeping."""
        if model_id not in self._engines:
            raise ValueError(f"Unknown model: {model_id}")
        return await self._engines[model_id].is_sleeping()

    async def check_health(self) -> None:
        """Health check — verify every awake engine is responsive."""
        for model_id in sorted(self._active_models):
            if model_id in self._engines:
                await self._engines[model_id].check_health()

    @property
    def active_models(self) -> List[str]:
        """The currently awake combination (sorted ``model_id`` list)."""
        return sorted(self._active_models)

    @property
    def model_ids(self) -> List[str]:
        """All model IDs managed by this deployment."""
        return list(self._llm_configs.keys())
