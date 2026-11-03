import shutil
from pathlib import Path


def validate_tar_max_size_mb(tar_max_size_mb) -> None:
    if tar_max_size_mb is None:
        return
    if tar_max_size_mb <= 0:
        raise ValueError("tar_max_size_mb must be a positive number or None.")


def prepare_output_dir(output_dir: Path, overwrite: bool = False) -> None:
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        return

    if not output_dir.is_dir():
        raise FileExistsError(f"output_dir exists and is not a directory: {output_dir}")

    if overwrite:
        resolved = output_dir.resolve()
        if resolved == Path(resolved.anchor):
            raise ValueError(f"Refusing to clear filesystem root: {output_dir}")
        if resolved == Path.cwd().resolve():
            raise ValueError(f"Refusing to clear current working directory: {output_dir}")
        shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        return

    if any(output_dir.iterdir()):
        raise FileExistsError(
            f"output_dir must be empty: {output_dir}. "
            "Pass overwrite=True to clear it before downloading."
        )
