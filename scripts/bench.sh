#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo " Running blazesse Benchmark Suite"
echo "=========================================================="
echo "CPU Information:"
lscpu | grep "Model name\|Socket\|Thread\|NUMA\|CPU(s):" || true
echo ""

echo "--- 1. High-Fanout Broadcast Benchmarks (10k & 50k subscribers) ---"
go test -bench=BenchmarkFanout -benchmem -benchtime=3s ./pkg/broadcaster

echo ""
echo "--- 2. Sharded Multi-Topic Publish Benchmarks ---"
go test -bench=BenchmarkPublish_MultiTopicShards -benchmem -benchtime=3s ./pkg/broadcaster

echo ""
echo "--- 3. Pre-serialized Wire Frame Format Benchmarks ---"
go test -bench=BenchmarkFrame -benchmem -benchtime=3s ./pkg/frame

echo ""
echo "=========================================================="
echo " Benchmark Suite Complete"
echo "=========================================================="
