from argparse import ArgumentParser, Namespace
import json
import logging
import tempfile
import warnings
from pathlib import Path
from nydus_image import NydusBackend, NydusS3Backend, NydusLocalFilesystemBackend, NydusImageWithBackends, NydusBlobLocation, change_backends, read_from_dir, write_to_dir
from manifest import download_bootstrap_and_manifest, upload_bootstrap_and_manifest
from copier import copy_blobs

class BackendArguments:
    def __init__(self, prefix: str) -> None:
        self.prefix = f"{prefix}-" if prefix else ""

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(f"--{self.prefix}backend-type", required=True, help="s3 | localfs")
        parser.add_argument(f"--{self.prefix}backend-config", required=True, help="For s3, path to config file. For localfs, the blob directory.")

    def parse_arguments(self, args: Namespace) -> NydusBackend:
        backend_type_attr = f"{self.prefix}backend_type".replace("-", "_")
        backend_config_attr = f"{self.prefix}backend_config".replace("-", "_")
        backend_type_str = getattr(args, backend_type_attr)
        backend_config_str = getattr(args, backend_config_attr)
        match backend_type_str:
            case "s3":
                with open(backend_config_str, 'r') as f:
                    config = json.load(f)

                # Expected keys for NydusS3Backend
                expected_keys = {'endpoint', 'bucket', 'region', 'object_prefix', 'access_key', 'secret_key'}

                # Handle key aliases
                if 'bucket_name' in config:
                    config['bucket'] = config.pop('bucket_name')
                if 'access_key_id' in config:
                    config['access_key'] = config.pop('access_key_id')
                if 'access_key_secret' in config:
                    config['secret_key'] = config.pop('access_key_secret')

                # Check for excess keys (after alias handling)
                excess_keys = set(config.keys()) - expected_keys
                if excess_keys:
                    warnings.warn(f"Unknown keys in S3 config file: {', '.join(excess_keys)}")

                # Set default for object_prefix if not provided
                if 'object_prefix' not in config:
                    config['object_prefix'] = "nydus/"

                # Remove unexpected elements and construct with **kwargs
                filtered_config = {k: v for k, v in config.items() if k in expected_keys}
                return NydusS3Backend(**filtered_config)
            case "localfs":
                return NydusLocalFilesystemBackend(blob_dir=Path(backend_config_str))
            case other:
                raise ValueError(f"Unknown backend type: {backend_type_str}")

class CopyCommand:
    """Command to copy nydus images between different backends."""

    def __init__(self):
        self.source_backend_args = BackendArguments("source")
        self.target_backend_args = BackendArguments("target")

    def add_arguments(self, parser: ArgumentParser) -> None:
        """Add command-line arguments for the copy command."""
        # Source options (mutually exclusive)
        source_group = parser.add_mutually_exclusive_group(required=True)
        source_group.add_argument("--source-oci-reference",
                                help="Source OCI reference (e.g., registry.example.com/repo:tag)")
        source_group.add_argument("--source-oci-dir",
                                help="Source directory containing bootstrap and config.json")

        # Target options (mutually exclusive)
        target_group = parser.add_mutually_exclusive_group(required=True)
        target_group.add_argument("--target-oci-reference",
                                help="Target OCI reference (e.g., registry.example.com/repo:tag)")
        target_group.add_argument("--target-oci-dir",
                                help="Target directory to write bootstrap and config.json")

        # Add source backend arguments
        self.source_backend_args.add_arguments(parser)

        # Add target backend arguments
        self.target_backend_args.add_arguments(parser)

    def run(self, args: Namespace) -> None:
        """Execute the copy command."""
        logging.basicConfig(level=logging.INFO)
        _LOGGER = logging.getLogger(__name__)

        # Parse source and target backends
        source_backend = self.source_backend_args.parse_arguments(args)
        target_backend = self.target_backend_args.parse_arguments(args)

        # Create temporary directory for bootstrap that persists through the entire operation
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)

            # Load source image
            if args.source_oci_reference:
                _LOGGER.info(f"Downloading from source OCI reference: {args.source_oci_reference}")
                source_image = download_bootstrap_and_manifest(
                    reference=args.source_oci_reference,
                    backend=source_backend,
                    output_dir=temp_path
                )
            elif args.source_oci_dir:
                _LOGGER.info(f"Reading from source directory: {args.source_oci_dir}")
                source_image = read_from_dir(
                    directory=Path(args.source_oci_dir),
                    backend=source_backend
                )
            else:
                raise ValueError("Either --source-oci-reference or --source-oci-dir must be specified")

            # Create target image with different backends
            target_image = change_backends(source_image, target_backend)

            # Copy blobs from source to target backend
            _LOGGER.info("Copying blobs between backends")
            copy_blobs(source_image, target_image)

            # Save target image
            if args.target_oci_reference:
                _LOGGER.info(f"Uploading to target OCI reference: {args.target_oci_reference}")
                upload_bootstrap_and_manifest(target_image, args.target_oci_reference)
            elif args.target_oci_dir:
                _LOGGER.info(f"Writing to target directory: {args.target_oci_dir}")
                write_to_dir(target_image, Path(args.target_oci_dir))
            else:
                raise ValueError("Either --target-oci-reference or --target-oci-dir must be specified")

            _LOGGER.info("Copy operation completed successfully")


if __name__ == "__main__":
    parser = ArgumentParser(description="Nydusify2 - Convert and copy nydus images")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Copy subcommand
    copy_command = CopyCommand()
    copy_parser = subparsers.add_parser("copy", help="Copy nydus image between different backends")
    copy_command.add_arguments(copy_parser)
    copy_parser.set_defaults(func=copy_command.run)

    args = parser.parse_args()

    if hasattr(args, 'func'):
        args.func(args)
    else:
        parser.print_help()
