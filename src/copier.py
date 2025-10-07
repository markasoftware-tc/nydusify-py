import logging
import subprocess
import shutil
import tempfile
from nydus_image import NydusImageWithBackends, NydusBlobLocation, NydusBackend, NydusS3Backend, NydusLocalFilesystemBackend

_LOGGER = logging.getLogger(__name__)

def copy_blobs(source_image: NydusImageWithBackends, target_image: NydusImageWithBackends):
    """For every blob in the image, copy it from the source backend to the destination backend. Nothing is done with the bootstrap"""
    if source_image.bootstrap_local_path != target_image.bootstrap_local_path:
        raise ValueError("Can only copy_blobs between images with the same bootstrap")
    if len(source_image.layers) != len(target_image.layers):
        raise ValueError("Two images with the same bootstrap had different number of layers!")
    if any(source_blob.digest != target_blob.digest for source_blob, target_blob in zip(source_image.layers, target_image.layers)):
        raise ValueError("Tried to copy between images with blobs having different digests")

    for source_blob, target_blob in zip(source_image.layers, target_image.layers):
        copy_blob(source_blob, target_blob)

def copy_blob(source_blob: NydusBlobLocation, target_blob: NydusBlobLocation) -> None:
    assert source_blob.digest == target_blob.digest
    _LOGGER.info(f"copying blob from {source_blob.backend} to {target_blob.backend}: {source_blob.digest}")
    source_backend = source_blob.backend
    target_backend = target_blob.backend

    if source_backend == target_backend:
        _LOGGER.info("Source and target backend are the same, skipping")
        return

    source_rclone = RcloneConfig(source_backend, "source")
    target_rclone = RcloneConfig(target_backend, "target")

    rclone_copy(source_blob.digest, source_rclone, target_rclone)


class RcloneConfig:
    """Represents an rclone config, can be source or destination"""
    def __init__(self, backend: NydusBackend, name: str) -> None:
        assert name.lower() == name, "RcloneConfig name must be lowercase"
        self.backend = backend
        self.name = name

    def with_digest(self, digest: str) -> str:
        """Given a filename, produce a full string suitable for use as an rclone CLI source or target"""
        match self.backend:
            case NydusS3Backend():
                # Include bucket name in the path for S3
                return f"{self.name}:{self.backend.bucket}/{self.backend.object_prefix}{digest}"
            case NydusLocalFilesystemBackend():
                # For local filesystem, rclone copy expects a directory, not full file path
                return str(self.backend.blob_dir)
            case _:
                raise ValueError(f"Unknown backend type: {type(self.backend)}")

    def env(self) -> dict[str, str]:
        """Return environment variables that should be set when running"""
        match self.backend:
            case NydusS3Backend():
                env_prefix = f"RCLONE_CONFIG_{self.name.upper()}"
                return {
                    f"{env_prefix}_TYPE": "s3",
                    f"{env_prefix}_ACCESS_KEY_ID": self.backend.access_key,
                    f"{env_prefix}_SECRET_ACCESS_KEY": self.backend.secret_key,
                    f"{env_prefix}_ENDPOINT": self.backend.endpoint,
                    f"{env_prefix}_REGION": self.backend.region,
                }
            case NydusLocalFilesystemBackend():
                return {}
            case _:
                raise ValueError(f"Unknown backend type: {type(self.backend)}")
    

def rclone_copy(digest: str, source_config: RcloneConfig, target_config: RcloneConfig) -> None:
    assert source_config.name != target_config.name, "can't copy between rclone configs with the same name"
    import os
    import logging
    _LOGGER = logging.getLogger(__name__)

    env = {**os.environ, **source_config.env(), **target_config.env()}
    source_path = source_config.with_digest(digest)
    target_path = target_config.with_digest(digest)

    _LOGGER.info(f"Running rclone copy from {source_path} to {target_path}")

    try:
        result = subprocess.run([
            "rclone", "copy", source_path, target_path
        ], check=True, env=env, capture_output=True, text=True)
        _LOGGER.info("rclone copy completed successfully")
    except subprocess.CalledProcessError as e:
        _LOGGER.error(f"rclone copy failed with exit code {e.returncode}")
        _LOGGER.error(f"rclone stdout: {e.stdout}")
        _LOGGER.error(f"rclone stderr: {e.stderr}")
        raise
