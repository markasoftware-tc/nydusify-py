import json
import logging
import subprocess
import tempfile
from pathlib import Path

from nydus_image import NydusImageWithBackends, NydusBackend, NydusS3Backend, NydusLocalFilesystemBackend, NydusBlobLocation, get_blob_digests

_LOGGER = logging.getLogger(__name__)


def unpack_nydus_image(bootstrap_path: Path, blob_dir: Path, output_tar: Path) -> None:
    """Unpack a RAFS image to a tar file using nydus-image unpack command.

    Assumes blobs are available locally in blob_dir.
    """
    # Build the command - simple local operation only
    cmd = [
        "nydus-image", "unpack", "--output", str(output_tar),
        "--blob-dir", str(blob_dir), str(bootstrap_path)
    ]

    _LOGGER.debug(f"Running nydus-image unpack: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    _LOGGER.debug(f"nydus-image unpack stdout: {result.stdout}")

    if result.returncode != 0:
        _LOGGER.error(f"nydus-image unpack command failed: {' '.join(cmd)}")
        _LOGGER.error(f"stdout: {result.stdout}")
        _LOGGER.error(f"stderr: {result.stderr}")
        raise RuntimeError(f"nydus-image unpack failed: {result.stderr}")


def repack_nydus_image(tar_path: Path, repack_dir: Path, create_opts: str, target_blob_dir: Path | None = None) -> tuple[Path, Path]:
    """Repack a tar file into a RAFS image using nydus-image create command.

    Args:
        target_blob_dir: If provided, create blobs directly in this directory (optimization for local backends)
    """
    bootstrap_path = repack_dir / "bootstrap"

    # Use target blob directory if provided (optimization), otherwise use temp directory
    if target_blob_dir is not None:
        blob_dir = target_blob_dir
        blob_dir.mkdir(parents=True, exist_ok=True)
    else:
        blob_dir = repack_dir / "blobs"
        blob_dir.mkdir(parents=True)

    # Build nydus-image create command
    cmd = [
        "nydus-image", "create", "--type", "tar-rafs", "--bootstrap", str(bootstrap_path),
        "--blob-dir", str(blob_dir), str(tar_path)
    ]

    # Add additional create options if provided
    if create_opts.strip():
        # Split the options string and add them to the command
        # Handle quoted strings properly
        import shlex
        additional_opts = shlex.split(create_opts)
        cmd.extend(additional_opts)

    _LOGGER.debug(f"Running nydus-image create: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    _LOGGER.debug(f"nydus-image create stdout: {result.stdout}")

    if result.returncode != 0:
        raise RuntimeError(f"nydus-image create failed: {result.stderr}")

    return bootstrap_path, blob_dir


def repack_image(source_image: NydusImageWithBackends, target_backend: NydusBackend,
                 create_opts: str, temp_dir: Path) -> tuple[NydusImageWithBackends, NydusImageWithBackends]:
    """
    Complete repack operation: unpack source image to tar, then repack to new RAFS image.

    Args:
        source_image: The source NydusImageWithBackends to repack
        target_backend: The backend to use for the repacked image blobs
        create_opts: Additional options to pass to nydus-image create
        temp_dir: Temporary directory to use for intermediate files

    Returns:
        Tuple of (target_image, temp_source_image) where:
        - target_image: New NydusImageWithBackends with repacked bootstrap and target backend
        - temp_source_image: Temporary image pointing to generated blobs in temp directory
    """
    # Create paths for intermediate files
    unpack_tar_path = temp_dir / "unpacked.tar"
    repack_dir = temp_dir / "repack"
    repack_dir.mkdir(parents=True)

    # Step 1: Ensure source blobs are available locally
    if source_image.layers and isinstance(source_image.layers[0].backend, NydusLocalFilesystemBackend):
        # Optimization: source is already local, use it directly
        _LOGGER.info("Source blobs are already local, using directly")
        local_blob_dir = source_image.layers[0].backend.blob_dir
    else:
        # Download source blobs to local directory first
        _LOGGER.info("Downloading source blobs to local directory")
        local_blob_dir = temp_dir / "local_blobs"
        local_blob_dir.mkdir(parents=True)

        local_source_backend = NydusLocalFilesystemBackend(blob_dir=local_blob_dir)
        local_blob_locations = []
        for layer in source_image.layers:
            local_blob_locations.append(NydusBlobLocation(
                digest=layer.digest,
                backend=local_source_backend
            ))

        local_source_image = NydusImageWithBackends(
            bootstrap_local_path=source_image.bootstrap_local_path,
            layers=local_blob_locations,
            image_config=source_image.image_config
        )

        # Copy blobs from source to local directory
        from copier import copy_blobs
        copy_blobs(source_image, local_source_image)

    # Step 2: Unpack the RAFS image to a tar file (now everything is local)
    _LOGGER.info("Unpacking nydus image to tar")
    unpack_nydus_image(source_image.bootstrap_local_path, local_blob_dir, unpack_tar_path)

    # Step 3: Repack the tar into a new RAFS image
    _LOGGER.info("Repacking tar to nydus image")

    # Optimization: if target backend is local filesystem, create blobs directly there
    if isinstance(target_backend, NydusLocalFilesystemBackend):
        _LOGGER.info("Target backend is local, creating blobs directly in target directory")
        target_blob_dir_for_create = target_backend.blob_dir
    else:
        target_blob_dir_for_create = None

    new_bootstrap_path, actual_blob_dir = repack_nydus_image(
        unpack_tar_path, repack_dir, create_opts, target_blob_dir_for_create
    )

    # Step 4: Create new NydusImageWithBackends with repacked bootstrap
    repacked_image = NydusImageWithBackends(
        bootstrap_local_path=new_bootstrap_path,
        layers=[],  # Will be populated after getting blob digests
        image_config=source_image.image_config
    )

    # Get blob digests from the new bootstrap
    blob_digests = get_blob_digests(new_bootstrap_path)
    blob_locations = []
    for digest in blob_digests:
        blob_locations.append(NydusBlobLocation(
            digest=digest,
            backend=target_backend
        ))
    repacked_image.layers = blob_locations

    # Step 5: Create a temporary source image pointing to the generated blobs
    # copy_blobs will handle the case where source and target are the same (no-op)
    temp_localfs_backend = NydusLocalFilesystemBackend(blob_dir=actual_blob_dir)
    temp_blob_locations = []
    for digest in blob_digests:
        temp_blob_locations.append(NydusBlobLocation(
            digest=digest,
            backend=temp_localfs_backend
        ))

    temp_source_image = NydusImageWithBackends(
        bootstrap_local_path=new_bootstrap_path,
        layers=temp_blob_locations,
        image_config=repacked_image.image_config
    )

    return repacked_image, temp_source_image