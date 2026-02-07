release-check:
  ./just.d/scripts/release-check.sh

release-tag:
  ./just.d/scripts/release-tag.sh

release-artifacts:
  ./just.d/scripts/build-release-artifacts.sh

release-smoke:
  ./just.d/scripts/release-smoke.sh

release-ci-dry-run:
  ./just.d/scripts/release-ci-dry-run.sh

release:
  just release-check
  just release-artifacts
  just release-tag
