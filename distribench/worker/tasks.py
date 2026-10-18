import random
import time
import math
from typing import Dict, Any

def compute_matrix(payload: Dict[str, Any]) -> Dict[str, Any]:    
    """Simulates CPU-bound numerical matrix computation."""
    size = payload.get("size", 150)  # 150x150 is ~0.5s-1.5s in pure Python
    seed = payload.get("seed", 42)
    rng = random.Random(seed)
    print(f"[Task] Generating two {size}x{size} matrices...")
    A = [[rng.random() for _ in range(size)] for _ in range(size)]
    B = [[rng.random() for _ in range(size)] for _ in range(size)]
    print(f"[Task] Computing matrix multiplication ({size}x{size})...")
    start = time.time()
    # C[i][j] = sum(A[i][k] * B[k][j] for k in range(size))
    C = [[0.0] * size for _ in range(size)]
    for i in range(size):
        for k in range(size):
            a_ik = A[i][k]
            for j in range(size):
                C[i][j] += a_ik * B[k][j]
    elapsed = time.time() - start
    
    trace = sum(C[i][i] for i in range(size))
    print(f"[Task] Done! Computed {size}x{size} multiplication in {elapsed:.3f}s. Matrix Trace: {trace:.4f}")
    return {
        "status": "ok",
        "matrix_size": size,
        "elapsed_seconds": round(elapsed, 4),
        "trace_checksum": round(trace, 4)
    }

def eval_dataset(payload:Dict[str, Any]) -> Dict[str, Any]:
    """Simulates batch evaluation of test cases or model inferences."""
    batch_size = payload.get("batch_size", 50)
    print(f"[Task] Evaluating dataset batch of {batch_size} samples...")
    time.sleep(1.0)
    accuracy = 0.94
    print(f"[Task] Evaluation finished. Benchmark accuracy: {accuracy * 100:.1f}%")
    return {"accuracy": accuracy, "samples_evaluated": batch_size}

def sim_oom(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates memory exhaustion (Out of Memory)."""
    print("[Task] Allocating large memory buffers for image caching...")
    time.sleep(0.5)
    print("ERROR: Fatal error encountered in worker process memory allocator!")
    raise MemoryError("Process terminated: Requested allocation of 8589934592 bytes exceeds node memory cgroup limit (OOMKilled)")

def sim_assertion(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates logic or assertion failure during validation."""
    print("[Task] Running model output sanity assertions...")
    time.sleep(0.5)
    expected = payload.get("expected", 100)
    actual = 42
    print(f"[Task] Validating metric: expected={expected}, actual={actual}")
    assert actual == expected, f"ValidationAssertionError: Expected {expected} items processed, but received {actual}"

def sim_timeout(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Simulates a hung process or infinite loop."""
    duration = payload.get("sleep_seconds", 30)
    print(f"[Task] Starting operation that will hang for {duration} seconds...")
    time.sleep(duration)
    return {"completed": True}

TASK_REGISTRY = {
    "compute_matrix": compute_matrix,
    "eval_dataset": eval_dataset,
    "sim_oom": sim_oom,
    "sim_assertion": sim_assertion,
    "sim_timeout": sim_timeout,
}