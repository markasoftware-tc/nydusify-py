from __future__ import annotations

import json
import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import Any

@dataclass
class NydusImageConfig:
    """Configuration and metadata for a nydus image."""
    architecture: str = "amd64"
    os: str = "linux"
    config: dict[str, Any] | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'NydusImageConfig':
        """Create NydusImageConfig from a dictionary (e.g., from JSON)."""
        return cls(
            architecture=data.get("architecture", "amd64"),
            os=data.get("os", "linux"),
            config=data.get("config")
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        result: dict[str, Any] = {
            "architecture": self.architecture,
            "os": self.os
        }
        if self.config is not None:
            result["config"] = self.config
        return result


@dataclass
class NydusImageWithBackends:
    """Represents a nydus image with information about exactly where each blob is"""
    bootstrap_local_path: Path
    layers: list[NydusBlobLocation]
    image_config: NydusImageConfig

@dataclass
class NydusBlobLocation:
    # as a hex string
    digest: str
    backend: NydusBackend


@dataclass
class NydusS3Backend:
    endpoint: str
    bucket: str
    region: str
    access_key: str
    secret_key: str
    # there's also meta_prefix, but IDK what it's for so we don't support it yet.
    object_prefix: str = "nydus/"

@dataclass
class NydusLocalFilesystemBackend:
    # should end in a slash
    blob_dir: Path


def get_blob_digests(bootstrap_path: Path) -> list[str]:
    """Get list of blob digests from a bootstrap file using nydus-image inspect."""
    inspect_result = subprocess.run([
        "nydus-image", "inspect", str(bootstrap_path), "--request", "blobs"
    ], capture_output=True, text=True, check=True)

    blob_data = json.loads(inspect_result.stdout)
    blob_digests = [blob["blob_id"] for blob in blob_data]

    # Assert that all blob IDs look like hashes (hexadecimal strings)
    for digest in blob_digests:
        assert isinstance(digest, str), f"Blob ID must be a string, got {type(digest)}: {digest}"
        assert len(digest) > 0, "Blob ID cannot be empty"
        assert all(c in '0123456789abcdefABCDEF' for c in digest), f"Blob ID must be hexadecimal: {digest}"
        assert len(digest) >= 8, f"Blob ID too short (expected at least 8 chars): {digest}"

    return blob_digests


def change_backends(image: NydusImageWithBackends, new_backend: NydusBackend) -> NydusImageWithBackends:
    """Create a new NydusImageWithBackends with all layers using the specified backend."""
    new_layers = []
    for layer in image.layers:
        new_layers.append(NydusBlobLocation(
            digest=layer.digest,
            backend=new_backend
        ))

    return NydusImageWithBackends(
        bootstrap_local_path=image.bootstrap_local_path,
        layers=new_layers,
        image_config=image.image_config
    )


def read_from_dir(directory: Path, backend: NydusBackend) -> NydusImageWithBackends:
    """Read a nydus image from a local directory containing bootstrap and config.json."""
    directory = Path(directory)

    # Check for bootstrap file
    bootstrap_path = directory / "bootstrap"
    if not bootstrap_path.exists():
        raise FileNotFoundError(f"Bootstrap file not found: {bootstrap_path}")

    # Read config.json if it exists, otherwise use defaults
    config_path = directory / "config.json"
    if config_path.exists():
        with open(config_path, 'r') as f:
            config_data = json.load(f)
            image_config = NydusImageConfig.from_dict(config_data)
    else:
        image_config = NydusImageConfig()

    # Get the blob list using nydus-image inspect
    blob_digests = get_blob_digests(bootstrap_path)

    # Create NydusBlobLocation objects for each blob
    blob_locations = []
    for digest in blob_digests:
        blob_locations.append(NydusBlobLocation(
            digest=digest,
            backend=backend
        ))

    return NydusImageWithBackends(
        bootstrap_local_path=bootstrap_path,
        layers=blob_locations,
        image_config=image_config
    )


def write_to_dir(image: NydusImageWithBackends, directory: Path) -> None:
    """Write a nydus image to a local directory with bootstrap and config.json."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    # Copy bootstrap to target directory
    target_bootstrap_path = directory / "bootstrap"
    if image.bootstrap_local_path != target_bootstrap_path:
        import shutil
        shutil.copy2(image.bootstrap_local_path, target_bootstrap_path)

    # Write config.json using the centralized config handling
    config_path = directory / "config.json"
    with open(config_path, 'w') as f:
        json.dump(image.image_config.to_dict(), f, indent=2)


NydusBackend = NydusS3Backend | NydusLocalFilesystemBackend
