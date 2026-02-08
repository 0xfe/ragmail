release-check:
  {{repo_root}}/just.d/scripts/release-check.sh

release-tag:
  {{repo_root}}/just.d/scripts/release-tag.sh

release-artifacts platform='host':
  {{repo_root}}/just.d/scripts/build-release-artifacts.sh --platform {{platform}}

release-smoke:
  {{repo_root}}/just.d/scripts/release-smoke.sh

release-ci-dry-run:
  {{repo_root}}/just.d/scripts/release-ci-dry-run.sh

release platform='host':
  just release-artifacts {{platform}}

release-cut platform='host':
  just release-check
  just release-artifacts {{platform}}
  just release-tag
