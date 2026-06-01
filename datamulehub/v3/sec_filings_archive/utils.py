import io
import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union


TAR_BLOCK_SIZE = 512
TAR_END_BLOCKS_SIZE = TAR_BLOCK_SIZE * 2


@dataclass(frozen=True)
class DownloadItem:
    logical_path: Path
    data: bytes


def decompress_zstd(data: bytes) -> bytes:
    try:
        import zstandard as zstd
    except ImportError as exc:
        raise ImportError("Install zstandard to download SEC filings archives.") from exc

    return zstd.ZstdDecompressor().decompress(data)


def write_file(output_dir: Path, item: DownloadItem) -> Path:
    out_path = output_dir / item.logical_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_bytes(item.data)
    return out_path


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


class TarBatchWriter:
    def __init__(self, output_dir: Path, max_size_mb: Union[int, float]):
        validate_tar_max_size_mb(max_size_mb)
        if max_size_mb is None:
            raise ValueError("TarBatchWriter requires max_size_mb to be set.")

        self.output_dir = output_dir
        self.max_size = int(max_size_mb * 1024 * 1024)
        self.index = 0
        self.current_size = 0
        self.current_path: Optional[Path] = None
        self.current_tar: Optional[tarfile.TarFile] = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def write(self, item: DownloadItem) -> Path:
        entry_size = self._entry_size(len(item.data))
        if (
            self.current_tar is not None
            and self.current_size > 0
            and self.current_size + entry_size + TAR_END_BLOCKS_SIZE > self.max_size
        ):
            self._open_next()

        if self.current_tar is None:
            self._open_next()

        info = tarfile.TarInfo(item.logical_path.as_posix())
        info.size = len(item.data)
        self.current_tar.addfile(info, io.BytesIO(item.data))
        self.current_size += entry_size
        return self.current_path

    def close(self) -> None:
        if self.current_tar is not None:
            self.current_tar.close()
            self.current_tar = None
            self.current_path = None
            self.current_size = 0

    def _open_next(self) -> None:
        self.close()
        self.index += 1
        self.current_path = self.output_dir / f"batch_{self.index:06d}.tar"
        self.current_tar = tarfile.open(self.current_path, "w")
        self.current_size = 0

    @staticmethod
    def _entry_size(data_size: int) -> int:
        padded_data_size = ((data_size + TAR_BLOCK_SIZE - 1) // TAR_BLOCK_SIZE) * TAR_BLOCK_SIZE
        return TAR_BLOCK_SIZE + padded_data_size


def store_item(output_dir: Path, item: DownloadItem, tar_writer: Optional[TarBatchWriter]) -> Path:
    if tar_writer is not None:
        return tar_writer.write(item)
    return write_file(output_dir, item)
