import json
import logging
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import Any

from nydus_image import NydusImageWithBackends, NydusBackend, NydusBlobLocation, get_blob_digests, NydusImageConfig

_LOGGER = logging.getLogger(__name__)


def upload_bootstrap_and_manifest(nydus_image: NydusImageWithBackends, reference: str) -> None:
    """
    Upload a Nydus bootstrap and OCI manifest to a registry using regctl.

    Args:
        nydus_image: The NydusImageWithBackends containing bootstrap and layer info
        reference: The OCI reference to upload to (e.g., "registry.example.com/repo:tag")
    """
    _LOGGER.info(f"Starting upload of Nydus image to {reference}")
    _LOGGER.debug(f"Bootstrap path: {nydus_image.bootstrap_local_path}")

    # Create a temporary tarball with the bootstrap
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as temp_tar:
        temp_tar_path = Path(temp_tar.name)
        _LOGGER.debug(f"Creating bootstrap tarball at {temp_tar_path}")

        # Create tarball with bootstrap at image/image.boot
        with tarfile.open(temp_tar_path, "w:gz") as tar:
            tar.add(nydus_image.bootstrap_local_path, arcname="image/image.boot")

        # Upload the bootstrap tarball as a layer using regctl
        _LOGGER.info("Uploading bootstrap layer")
        result = subprocess.run([
            "regctl", "blob", "put", reference, str(temp_tar_path)
        ], capture_output=True, text=True, check=True)

        # Extract the digest from regctl output
        bootstrap_digest = result.stdout.strip()
        bootstrap_size = temp_tar_path.stat().st_size
        _LOGGER.info(f"Bootstrap layer uploaded: {bootstrap_digest} ({bootstrap_size} bytes)")

        # Create a config blob using the centralized config handling
        config = nydus_image.image_config.to_dict()
        config["rootfs"] = {
            "type": "layers",
            "diff_ids": [bootstrap_digest]
        }

        # Upload config blob
        _LOGGER.debug("Creating and uploading config blob")
        with tempfile.NamedTemporaryFile(mode='w', suffix=".json") as config_file:
            config_path = Path(config_file.name)
            json.dump(config, config_file)
            config_file.flush()

            config_result = subprocess.run([
                "regctl", "blob", "put", reference, str(config_path)
            ], capture_output=True, text=True, check=True)

            config_digest = config_result.stdout.strip()
            config_size = config_path.stat().st_size
            _LOGGER.debug(f"Config blob uploaded: {config_digest} ({config_size} bytes)")

            # Create the OCI manifest
            manifest = create_oci_manifest(
                config_digest=config_digest,
                config_size=config_size,
                bootstrap_digest=bootstrap_digest,
                bootstrap_size=bootstrap_size
            )

            # Upload the manifest
            _LOGGER.info("Uploading OCI manifest")
            with tempfile.NamedTemporaryFile(mode='w', suffix=".json") as manifest_file:
                manifest_path = Path(manifest_file.name)
                json.dump(manifest, manifest_file)
                manifest_file.flush()

                subprocess.run([
                    "regctl", "manifest", "put", reference, str(manifest_path)
                ], capture_output=True, text=True, check=True)

    _LOGGER.info(f"Successfully uploaded Nydus image to {reference}")


def create_oci_manifest(config_digest: str, config_size: int,
                       bootstrap_digest: str, bootstrap_size: int) -> dict[str, Any]:
    """Create an OCI manifest for a Nydus image."""
    return {
        "schemaVersion": 2,
        "mediaType": "application/vnd.docker.distribution.manifest.v2+json",
        "config": {
            "mediaType": "application/vnd.docker.container.image.v1+json",
            "digest": config_digest,
            "size": config_size
        },
        "layers": [
            {
                "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
                "digest": bootstrap_digest,
                "size": bootstrap_size,
                "annotations": {
                    "containerd.io/snapshot/nydus-bootstrap": "true",
                    "containerd.io/snapshot/nydus-fs-version": "6"
                }
            }
        ]
    }


def download_bootstrap_and_manifest(reference: str, backend: NydusBackend, output_dir: Path) -> NydusImageWithBackends:
    """
    Download a Nydus bootstrap and OCI manifest from a registry using regctl.

    Args:
        reference: The OCI reference to download from (e.g., "registry.example.com/repo:tag")
        backend: The backend to use for all blobs
        output_dir: Directory to save the bootstrap file

    Returns:
        NydusImageWithBackends with bootstrap and blob information
    """
    _LOGGER.info(f"Starting download of Nydus image from {reference}")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Download the manifest
    _LOGGER.info("Downloading OCI manifest")
    with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as manifest_file:
        manifest_path = Path(manifest_file.name)

        try:
            result = subprocess.run([
                "regctl", "manifest", "get", "--format", "raw-body", reference
            ], capture_output=True, text=True, check=True)
        except subprocess.CalledProcessError as e:
            _LOGGER.error(f"Failed to download manifest from {reference}")
            _LOGGER.error(f"regctl stdout: {e.stdout}")
            _LOGGER.error(f"regctl stderr: {e.stderr}")
            raise

        _LOGGER.debug(f"regctl stdout: '{result.stdout}'")
        _LOGGER.debug(f"regctl stderr: '{result.stderr}'")

        if not result.stdout.strip():
            raise ValueError(f"regctl returned empty output for manifest {reference}")

        manifest_data = json.loads(result.stdout)
        _LOGGER.debug(f"Downloaded manifest: {manifest_data}")

        # Validate manifest format
        if manifest_data.get("schemaVersion") != 2:
            raise ValueError(f"Unsupported manifest schema version: {manifest_data.get('schemaVersion')}")

        # Accept both Docker and OCI manifest formats
        accepted_manifest_media_types = [
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.oci.image.manifest.v1+json"
        ]
        manifest_media_type = manifest_data.get("mediaType")
        if manifest_media_type not in accepted_manifest_media_types:
            raise ValueError(f"Unexpected manifest media type: {manifest_media_type}, expected one of: {accepted_manifest_media_types}")

        # Validate config section
        config_section = manifest_data.get("config")
        if not config_section:
            raise ValueError("No config section found in manifest")

        expected_config_media_type = "application/vnd.oci.image.config.v1+json"
        if config_section.get("mediaType") != expected_config_media_type:
            raise ValueError(f"Unexpected config media type: {config_section.get('mediaType')}, expected: {expected_config_media_type}")

        # Extract config digest from manifest
        config_digest = config_section["digest"]

        # Download the config blob
        _LOGGER.info("Downloading config blob")
        with tempfile.NamedTemporaryFile(mode='w+', suffix=".json") as config_file:
            config_path = Path(config_file.name)

            config_result = subprocess.run([
                "regctl", "blob", "get", reference, config_digest
            ], capture_output=True, text=True, check=True)

            config_data = json.loads(config_result.stdout)
            _LOGGER.debug(f"Downloaded config: {config_data}")

            # Extract bootstrap layer digest from manifest (should be the first/only layer)
            layers = manifest_data.get("layers", [])
            if not layers:
                raise ValueError("No layers found in manifest")

            if len(layers) != 1:
                raise ValueError(f"Expected exactly 1 layer in manifest, found {len(layers)}")

            bootstrap_layer = layers[0]
            expected_layer_media_type = "application/vnd.oci.image.layer.v1.tar+gzip"
            if bootstrap_layer.get("mediaType") != expected_layer_media_type:
                raise ValueError(f"Unexpected layer media type: {bootstrap_layer.get('mediaType')}, expected: {expected_layer_media_type}")

            # Validate bootstrap layer annotations
            annotations = bootstrap_layer.get("annotations", {})

            bootstrap_annotation = annotations.get("containerd.io/snapshot/nydus-bootstrap")
            if bootstrap_annotation != "true":
                raise ValueError(f"Invalid or missing nydus-bootstrap annotation: expected 'true', got '{bootstrap_annotation}'")

            fs_version_annotation = annotations.get("containerd.io/snapshot/nydus-fs-version")
            if fs_version_annotation != "6":
                raise ValueError(f"Invalid or missing nydus-fs-version annotation: expected '6', got '{fs_version_annotation}'")

            bootstrap_digest = bootstrap_layer["digest"]

            # Download the bootstrap layer
            _LOGGER.info("Downloading bootstrap layer")
            with tempfile.NamedTemporaryFile(suffix=".tar.gz") as temp_tar:
                temp_tar_path = Path(temp_tar.name)

                try:
                    with open(temp_tar_path, 'wb') as f:
                        subprocess.run([
                            "regctl", "blob", "get", reference, bootstrap_digest
                        ], stdout=f, stderr=subprocess.PIPE, check=True)
                except subprocess.CalledProcessError as e:
                    _LOGGER.error(f"Failed to download bootstrap blob {bootstrap_digest}")
                    _LOGGER.error(f"regctl stdout: {e.stdout}")
                    _LOGGER.error(f"regctl stderr: {e.stderr}")
                    raise

                # Extract the bootstrap from the tarball
                bootstrap_path = output_dir / "image.boot"
                _LOGGER.info(f"Extracting bootstrap to {bootstrap_path}")

                with tarfile.open(temp_tar_path, "r:gz") as tar:
                    # Extract image/image.boot to the output directory
                    try:
                        boot_member = tar.getmember("image/image.boot")
                        boot_member.name = "image.boot"  # Rename to avoid subdirectory
                        tar.extract(boot_member, output_dir)
                    except KeyError:
                        raise ValueError("Bootstrap file 'image/image.boot' not found in layer tarball")

                # Get the blob list using nydus-image inspect
                _LOGGER.info("Getting blob list from bootstrap")
                blob_digests = get_blob_digests(bootstrap_path)
                _LOGGER.debug(f"Found {len(blob_digests)} blobs")

                # Create NydusBlobLocation objects for each blob
                blob_locations = []
                for digest in blob_digests:
                    blob_locations.append(NydusBlobLocation(
                        digest=digest,
                        backend=backend
                    ))

                # Create NydusImageWithBackends object using centralized config handling
                image_config = NydusImageConfig.from_dict(config_data)
                nydus_image = NydusImageWithBackends(
                    bootstrap_local_path=bootstrap_path,
                    layers=blob_locations,
                    image_config=image_config
                )

                _LOGGER.info(f"Successfully downloaded Nydus image from {reference}")
                _LOGGER.info(f"Bootstrap saved to: {bootstrap_path}")
                _LOGGER.info(f"Found {len(blob_locations)} blobs")

                return nydus_image