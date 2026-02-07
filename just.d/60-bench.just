bench-pipeline *args:
  ./just.d/scripts/run-python.sh ./just.d/scripts/benchmark_pipeline.py {{args}}

bench-threshold *args:
  ./just.d/scripts/run-python.sh ./just.d/scripts/benchmark_threshold.py {{args}}
