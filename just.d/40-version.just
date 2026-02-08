version:
  @cat {{repo_root}}/VERSION

sync-version:
  {{repo_root}}/just.d/scripts/sync-version.sh

bump-patch:
  {{repo_root}}/just.d/scripts/bump-patch.sh
