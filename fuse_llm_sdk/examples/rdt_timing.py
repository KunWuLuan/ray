"""Measure RDT vs disk level-2 wake latency on the pod (single node, NIXL).

Same engine, same model: alternately reload weights via the built-in disk path
(collective_rpc "reload_weights") and via RDT (collective_rpc
"rdt_reload_weights", cache handle). Warmup then N timed iterations each; reports
the reload-step time (the differentiator) and the full 3-step wake time.

Usage: cd /root && python -u rdt_timing.py [MODEL_ID] [N]
"""

import asyncio
import os
import statistics
import sys
import time

import ray

MODEL_ID = sys.argv[1] if len(sys.argv) > 1 else "Qwen3-0.6B"
# Override the model directory with FUSE_MODEL_DIR (e.g. a local-SSD copy) to
# avoid slow OSS-fuse cold loads; steady-state warm reload timing is unaffected.
MODEL_DIR = os.environ.get("FUSE_MODEL_DIR", f"/var/model/{MODEL_ID}")
N = int(sys.argv[2]) if len(sys.argv) > 2 else 5
GPU_MEM_UTIL = float(sys.argv[3]) if len(sys.argv) > 3 else 0.4
PROMPT = "The capital of France is"


@ray.remote(num_gpus=1)
class Timer:
    async def run(self, cache):
        import os

        os.environ["VLLM_ALLOW_INSECURE_SERIALIZATION"] = "1"
        from vllm import AsyncEngineArgs, SamplingParams
        from vllm.v1.engine.async_llm import AsyncLLM

        args = AsyncEngineArgs(
            model=MODEL_DIR,
            served_model_name=MODEL_ID,
            tensor_parallel_size=1,
            distributed_executor_backend="ray",
            enforce_eager=True,
            gpu_memory_utilization=GPU_MEM_UTIL,
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

        baseline = await gen()

        async def wake(kind):
            """One level-2 wake; returns (reload_seconds, total_wake_seconds, source)."""
            await eng.sleep(level=2)
            t_total = time.time()
            await eng.wake_up(tags=["weights"])
            t_reload = time.time()
            if kind == "disk":
                res = await eng.collective_rpc(method="reload_weights")
                source = "disk"
            else:
                res = await eng.collective_rpc(
                    method="rdt_reload_weights", args=(cache, MODEL_ID)
                )
                source = (res[0] or {}).get("source")
            reload_s = time.time() - t_reload
            await eng.wake_up(tags=["kv_cache"])
            total_s = time.time() - t_total
            return reload_s, total_s, source

        out = {"model": MODEL_ID, "baseline_ok": None, "n": N}

        # Warmup one of each (page cache, NIXL agent, CUDA ctx).
        await wake("disk")
        await wake("rdt")

        results = {"disk": [], "rdt": []}
        sources = {}
        correct = True
        for kind in ("disk", "rdt"):
            for _ in range(N):
                reload_s, total_s, source = await wake(kind)
                results[kind].append((reload_s, total_s))
                sources[kind] = source
                # correctness: output must still match baseline
                if (await gen()) != baseline:
                    correct = False

        def summ(pairs):
            rel = [p[0] for p in pairs]
            tot = [p[1] for p in pairs]
            return {
                "reload_ms_median": round(statistics.median(rel) * 1000, 1),
                "reload_ms_min": round(min(rel) * 1000, 1),
                "total_ms_median": round(statistics.median(tot) * 1000, 1),
            }

        out["disk"] = summ(results["disk"])
        out["rdt"] = summ(results["rdt"])
        out["rdt_source"] = sources.get("rdt")
        out["output_correct"] = correct
        dm = out["disk"]["reload_ms_median"]
        rm = out["rdt"]["reload_ms_median"]
        out["reload_speedup"] = round(dm / rm, 2) if rm else None
        return out


async def main():
    from fuse_llm.weight_cache import WeightCacheServer

    cache = WeightCacheServer.options(num_cpus=4, num_gpus=0).remote(
        {MODEL_ID: MODEL_DIR}
    )
    n = len(ray.get(cache.get_weight_names.remote(MODEL_ID)))
    print(f"[cache] {MODEL_ID}: {n} tensors; N={N} iterations each")
    out = await Timer.remote().run.remote(cache)
    print("\n===== RDT vs DISK WAKE TIMING =====")
    for k, v in out.items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    ray.init(ignore_reinit_error=True)
    asyncio.run(main())
