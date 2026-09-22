import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path

from tqdm import tqdm


def _binary_name():
    return "datamule-archive-downloader.exe" if os.name == "nt" else "datamule-archive-downloader"


def find_binary():
    configured = os.environ.get("DATAMULE_ARCHIVE_DOWNLOADER")
    package_root = Path(__file__).resolve().parents[2]
    repository_root = package_root.parent
    candidates = [
        Path(configured) if configured else None,
        package_root / "bin" / _binary_name(),
        repository_root / "rust" / "archive-downloader" / "target" / "release" / _binary_name(),
        repository_root / "rust" / "archive-downloader" / "target" / "debug" / _binary_name(),
    ]
    for candidate in candidates:
        if candidate is not None and candidate.is_file():
            return candidate
    return None


def available():
    return find_binary() is not None


def run(
    mode,
    jobs,
    output_dir,
    max_workers,
    decomp_workers,
    decompress,
    tar_max_size_mb,
    description,
    logger=None,
):
    logger = logger or logging.getLogger(__name__)
    binary = find_binary()
    if binary is None:
        raise RuntimeError("The datamule Rust archive downloader is not installed.")

    manifest_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            suffix=".jsonl",
            delete=False,
        ) as manifest:
            manifest_path = Path(manifest.name)
            total = 0
            for job in jobs:
                json.dump(job, manifest, separators=(",", ":"))
                manifest.write("\n")
                total += 1

        tar_max_bytes = (
            "none"
            if tar_max_size_mb is None
            else str(int(tar_max_size_mb * 1024 * 1024))
        )
        command = [
            str(binary),
            mode,
            str(manifest_path),
            str(output_dir),
            str(max_workers),
            str(decomp_workers),
            "true" if decompress else "false",
            tar_max_bytes,
        ]
        process = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )

        downloaded = []
        with tqdm(total=total, desc=description, unit="file") as progress:
            for line in process.stdout:
                event = json.loads(line)
                if event["type"] == "complete":
                    downloaded.extend(Path(path) for path in event["paths"])
                else:
                    logger.error("Download failed: %s", event["message"])
                progress.update(1)

        stderr = process.stderr.read()
        return_code = process.wait()
        if return_code:
            raise RuntimeError(
                f"Rust archive downloader exited with code {return_code}: {stderr.strip()}"
            )
        return downloaded
    finally:
        if manifest_path is not None:
            manifest_path.unlink(missing_ok=True)

