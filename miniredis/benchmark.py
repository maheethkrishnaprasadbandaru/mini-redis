"""Reproducible, dependency-free Mini Redis microbenchmarks."""

import argparse
import concurrent.futures
import os
import platform
import statistics
import sys
import time
from tempfile import TemporaryDirectory

from miniredis import persistence
from miniredis.store import MiniRedisStore


def measure(name, operation, iterations):
    latencies = []
    started = time.perf_counter()
    for i in range(iterations):
        one = time.perf_counter()
        operation(i)
        latencies.append((time.perf_counter() - one) * 1000)
    elapsed = time.perf_counter() - started
    ordered = sorted(latencies)
    percentile = lambda fraction: ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]
    print(f"{name:20} {iterations / elapsed:10.0f} ops/s  avg={statistics.mean(latencies):.4f}ms  p50={percentile(.50):.4f}ms  p95={percentile(.95):.4f}ms")


def run(iterations=10000, clients=4):
    store = MiniRedisStore()
    print(f"Mini Redis benchmark | Python {sys.version.split()[0]} | {platform.platform()} | iterations={iterations} clients={clients}")
    measure("SET", lambda i: store.set(f"k:{i}", str(i)), iterations)
    measure("GET", lambda i: store.get(f"k:{i % iterations}"), iterations)
    measure("MSET", lambda i: store.mset([(f"m:{i}", str(i)), (f"m:{i}:b", str(i))]), iterations)
    measure("MGET", lambda i: store.mget([f"m:{i % iterations}", f"m:{i % iterations}:b"]), iterations)
    measure("list push/pop", lambda i: (store.list_push("bench:list", [str(i)], False), store.list_pop("bench:list", False)), iterations)
    measure("set add/remove", lambda i: (store.set_add("bench:set", [str(i)]), store.set_remove("bench:set", [str(i)])), iterations)
    measure("hash set/get", lambda i: (store.hash_set("bench:hash", [(str(i), str(i))]), store.hash_get("bench:hash", str(i))), iterations)

    per_client = max(1, iterations // max(1, clients))
    started = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
        list(pool.map(lambda offset: [store.set(f"c:{offset}:{i}", "v") for i in range(per_client)], range(clients)))
    elapsed = time.perf_counter() - started
    print(f"concurrent SET        {clients * per_client / elapsed:10.0f} ops/s  clients={clients}")
    with TemporaryDirectory(prefix="miniredis-bench-") as directory:
        path = os.path.join(directory, "snapshot.json")
        measure("persistence save", lambda _i: persistence.save(store, path), max(1, min(iterations, 100)))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--iterations", type=int, default=10000)
    parser.add_argument("--clients", type=int, default=4)
    args = parser.parse_args(argv)
    if args.iterations < 1 or args.clients < 1:
        parser.error("iterations and clients must be positive")
    run(args.iterations, args.clients)


if __name__ == "__main__":
    main()
