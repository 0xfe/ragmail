from __future__ import annotations

import subprocess
import tarfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_release_workflow_has_expected_distribution_matrix() -> None:
    workflow = (REPO_ROOT / ".github/workflows/release.yml").read_text(encoding="utf-8")
    assert "name: Release" in workflow
    assert '- "v*"' in workflow
    assert "os: ubuntu-latest" in workflow
    assert "suffix: linux-amd64" in workflow
    assert "target: x86_64-unknown-linux-gnu" in workflow
    assert "suffix: linux-arm64" in workflow
    assert "target: aarch64-unknown-linux-gnu" in workflow
    assert "os: ubuntu-24.04-arm" in workflow
    assert "os: macos-13" in workflow
    assert "suffix: macos-amd64" in workflow
    assert "os: macos-14" in workflow
    assert "suffix: macos-arm64" in workflow
    assert "build-python-bridge.sh" in workflow
    assert "release-publish-assets.sh" in workflow
    assert "publish-homebrew-tap.sh" in workflow


def test_local_release_recipes_support_platform_targets() -> None:
    release_recipes = (REPO_ROOT / "just.d/50-release.just").read_text(encoding="utf-8")
    assert "release-artifacts platform='host':" in release_recipes
    assert "release platform='host':" in release_recipes
    assert "just release-artifacts {{platform}}" in release_recipes
    assert "release-cut platform='host':" in release_recipes


def test_build_release_artifacts_script_accepts_platform_argument() -> None:
    script = (REPO_ROOT / "just.d/scripts/build-release-artifacts.sh").read_text(
        encoding="utf-8"
    )
    assert "--platform" in script
    assert "canonicalize_platform" in script
    assert "cross-platform build requested" in script


def test_ci_workflow_has_benchmark_smoke_gate() -> None:
    workflow = (REPO_ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    assert "benchmark-smoke:" in workflow
    assert "benchmark_threshold.py" in workflow
    assert "--min-msg-per-s 1" in workflow


def test_generate_homebrew_formula_script_emits_expected_fields(tmp_path: Path) -> None:
    script = REPO_ROOT / "just.d/scripts/generate-homebrew-formula.sh"
    output = tmp_path / "ragmail.rb"
    version = "0.1.0"
    repo = "example/ragmail"
    sha_amd64 = "1" * 64
    sha_arm64 = "2" * 64

    subprocess.run(
        [
            str(script),
            "--version",
            version,
            "--repo",
            repo,
            "--macos-amd64-sha",
            sha_amd64,
            "--macos-arm64-sha",
            sha_arm64,
            "--output",
            str(output),
        ],
        check=True,
        cwd=REPO_ROOT,
    )

    formula = output.read_text(encoding="utf-8")
    assert "class Ragmail < Formula" in formula
    assert f'version "{version}"' in formula
    assert f"https://github.com/{repo}/releases/download/v{version}/ragmail-v{version}-macos-amd64.tar.gz" in formula
    assert f"https://github.com/{repo}/releases/download/v{version}/ragmail-v{version}-macos-arm64.tar.gz" in formula
    assert sha_amd64 in formula
    assert sha_arm64 in formula


def test_release_publish_assets_script_collects_checksums_and_formula(tmp_path: Path) -> None:
    script = REPO_ROOT / "just.d/scripts/release-publish-assets.sh"
    artifacts = tmp_path / "artifacts"
    artifacts.mkdir()
    version = "0.1.0"

    # Create expected tarballs with dummy ragmail + ragmail-py payloads.
    for suffix in ("macos-amd64", "macos-arm64", "linux-amd64", "linux-arm64"):
        tar_path = artifacts / f"ragmail-v{version}-{suffix}.tar.gz"
        payload = tmp_path / f"ragmail-{suffix}"
        bridge_payload = tmp_path / f"ragmail-py-{suffix}"
        payload.write_text("dummy", encoding="utf-8")
        bridge_payload.write_text("bridge", encoding="utf-8")
        with tarfile.open(tar_path, mode="w:gz") as tf:
            tf.add(payload, arcname="ragmail")
            tf.add(bridge_payload, arcname="ragmail-py")

    (artifacts / f"ragmail_{version}_amd64.deb").write_bytes(b"deb-amd64")
    (artifacts / f"ragmail_{version}_arm64.deb").write_bytes(b"deb-arm64")

    output = tmp_path / "publish"
    subprocess.run(
        [
            str(script),
            "--version",
            version,
            "--repo",
            "example/ragmail",
            "--artifacts-dir",
            str(artifacts),
            "--output-dir",
            str(output),
        ],
        check=True,
        cwd=REPO_ROOT,
    )

    checksums = (output / "flat" / "SHA256SUMS").read_text(encoding="utf-8")
    assert f"ragmail-v{version}-macos-amd64.tar.gz" in checksums
    assert f"ragmail-v{version}-macos-arm64.tar.gz" in checksums
    assert f"ragmail-v{version}-linux-amd64.tar.gz" in checksums
    assert f"ragmail-v{version}-linux-arm64.tar.gz" in checksums
    assert f"ragmail_{version}_amd64.deb" in checksums
    assert f"ragmail_{version}_arm64.deb" in checksums

    formula = (output / "homebrew" / "ragmail.rb").read_text(encoding="utf-8")
    assert "class Ragmail < Formula" in formula
    assert f'version "{version}"' in formula


def test_publish_homebrew_tap_script_updates_local_tap_repo(tmp_path: Path) -> None:
    script = REPO_ROOT / "just.d/scripts/publish-homebrew-tap.sh"
    formula_path = tmp_path / "ragmail.rb"
    formula_path.write_text(
        """class Ragmail < Formula
  desc "test"
  homepage "https://example.com"
  url "https://example.com/ragmail-v0.1.0-macos-amd64.tar.gz"
  sha256 "1111111111111111111111111111111111111111111111111111111111111111"
  version "0.1.0"
end
""",
        encoding="utf-8",
    )

    tap_bare = tmp_path / "homebrew-ragmail.git"
    subprocess.run(["git", "init", "--bare", str(tap_bare)], check=True, cwd=REPO_ROOT)

    tap_seed = tmp_path / "tap-seed"
    subprocess.run(["git", "clone", str(tap_bare), str(tap_seed)], check=True, cwd=REPO_ROOT)
    (tap_seed / "Formula").mkdir(parents=True, exist_ok=True)
    (tap_seed / "Formula" / "ragmail.rb").write_text(
        """class Ragmail < Formula
  desc "seed"
  homepage "https://example.com"
  url "https://example.com/seed.tar.gz"
  sha256 "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  version "0.0.0"
end
""",
        encoding="utf-8",
    )
    subprocess.run(["git", "config", "user.name", "seed"], check=True, cwd=tap_seed)
    subprocess.run(["git", "config", "user.email", "seed@example.com"], check=True, cwd=tap_seed)
    subprocess.run(["git", "add", "Formula/ragmail.rb"], check=True, cwd=tap_seed)
    subprocess.run(["git", "commit", "-m", "seed"], check=True, cwd=tap_seed)
    subprocess.run(["git", "push", "origin", "HEAD"], check=True, cwd=tap_seed)

    subprocess.run(
        [
            str(script),
            "--formula",
            str(formula_path),
            "--tap-repo",
            f"file://{tap_bare}",
            "--version",
            "0.1.0",
        ],
        check=True,
        cwd=REPO_ROOT,
    )

    tap_verify = tmp_path / "tap-verify"
    subprocess.run(["git", "clone", str(tap_bare), str(tap_verify)], check=True, cwd=REPO_ROOT)
    published_formula = (tap_verify / "Formula" / "ragmail.rb").read_text(encoding="utf-8")
    assert "version \"0.1.0\"" in published_formula
    assert "seed.tar.gz" not in published_formula
