"""Pod e2e validation of the RDT level-2 wake path (single node, NIXL).

Validates the genuinely-new GPU/NIXL code:
  - WeightCacheServer serving weights over RDT/NIXL from host RAM
  - RDTReloadWorkerExtension pulling them into a woken vLLM worker + load_weights
  - the exact wake sequence deployment._wakeup_engine uses for weight_source="rdt"

Runs the engine inside a Ray actor so vLLM's ray-executor workers have a Ray
context (required for RDT + to avoid the RAY_ADDRESS EngineCore hang).

Usage (on the pod, from a non-/vllm cwd):
    cd /root && python /root/fuse_llm_sdk/examples/rdt_pod_validation.py
"""

import asyncio
import time

import ray

MODEL_ID = "Qwen3-0.6B"
MODEL_DIR = "/var/model/Qwen3-0.6B"
PROMPT = "The capital of France is"


@ray.remote(num_gpus=1)
class Validator:
    async def run(self, cache):
        import os

        # RDT needs the cache ACTOR HANDLE passed directly (a name resolved via
        # ray.get_actor loses the enable_tensor_transport metadata).  vLLM's
        # collective_rpc msgpack encoder can't serialize an ActorHandle, so allow
        # its pickle fallback.
        os.environ["VLLM_ALLOW_INSECURE_SERIALIZATION"] = "1"

        from vllm import AsyncEngineArgs, SamplingParams
        from vllm.v1.engine.async_llm import AsyncLLM

        args = AsyncEngineArgs(
            model=MODEL_DIR,
            served_model_name=MODEL_ID,
            tensor_parallel_size=1,
            distributed_executor_backend="ray",  # workers are Ray actors -> RDT endpoints
            enforce_eager=True,
            gpu_memory_utilization=0.4,
            max_model_len=2048,
            max_num_seqs=4,
            enable_sleep_mode=True,
            worker_extension_cls="fuse_llm.rdt_worker.RDTReloadWorkerExtension",
        )
        eng = AsyncLLM.from_engine_args(args)
        sp = SamplingParams(max_tokens=16, temperature=0.0)

        async def gen():
            text = ""
            async for o in eng.generate(PROMPT, sp, request_id=f"r{time.time_ns()}"):
                text = o.outputs[0].text
            return text

        report = {}
        report["baseline"] = await gen()

        # ---- Level-2 sleep (frees weights + KV) ----
        await eng.sleep(level=2)
        report["is_sleeping_after_sleep2"] = await eng.is_sleeping()

        # ---- RDT wake sequence (mirrors deployment._wakeup_engine rdt branch) ----
        await eng.wake_up(tags=["weights"])
        t0 = time.time()
        rpc_results = await eng.collective_rpc(
            method="rdt_reload_weights", args=(cache, MODEL_ID)
        )
        report["rdt_reload_seconds"] = round(time.time() - t0, 3)
        report["rpc_results"] = rpc_results  # each worker: {"ok","loaded","source"}
        await eng.wake_up(tags=["kv_cache"])

        report["after_rdt_wake"] = await gen()
        report["output_matches"] = report["after_rdt_wake"] == report["baseline"]
        report["source"] = (rpc_results[0] or {}).get("source") if rpc_results else None

        return report


async def main():
    from fuse_llm.weight_cache import WeightCacheServer

    # Cache on CPU-only (no GPU) to prove host-RAM source; pin_memory guard handles
    # the CUDA-less case.
    cache = WeightCacheServer.options(num_cpus=4, num_gpus=0).remote(
        {MODEL_ID: MODEL_DIR}
    )
    n = len(ray.get(cache.get_weight_names.remote(MODEL_ID)))  # force __init__ load
    print(f"[cache] loaded {n} tensors (handle passed directly for RDT)")

    v = Validator.remote()
    report = await v.run.remote(cache)  # pass the handle directly (RDT needs it)
    report = ray.get(report) if not isinstance(report, dict) else report

    print("\n===== RDT POD VALIDATION REPORT =====")
    for k, val in report.items():
        print(f"{k}: {val!r}")
    ok = report.get("output_matches") and report.get("source") == "rdt"
    print(f"\nRESULT: {'PASS' if ok else 'FAIL'} "
          f"(output_matches={report.get('output_matches')}, source={report.get('source')})")


if __name__ == "__main__":
    ray.init(ignore_reinit_error=True)  # fresh single-node cluster on this pod
    asyncio.run(main())
