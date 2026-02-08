bench-pipeline *args:
  {{repo_root}}/just.d/scripts/run-python.sh {{repo_root}}/just.d/scripts/benchmark_pipeline.py {{args}}

bench-threshold *args:
  {{repo_root}}/just.d/scripts/run-python.sh {{repo_root}}/just.d/scripts/benchmark_threshold.py {{args}}
