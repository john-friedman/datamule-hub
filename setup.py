import os
import shutil
import subprocess
from pathlib import Path

from setuptools import Distribution, find_packages, setup
from setuptools.command.bdist_wheel import bdist_wheel
from setuptools.command.build_py import build_py


class BuildPyWithRust(build_py):
    def run(self):
        repository_root = Path(__file__).parent.resolve()
        manifest = repository_root / "rust" / "archive-downloader" / "Cargo.toml"
        subprocess.check_call(
            ["cargo", "build", "--release", "--manifest-path", str(manifest)]
        )
        super().run()

        binary_name = (
            "datamule-archive-downloader.exe"
            if os.name == "nt"
            else "datamule-archive-downloader"
        )
        source = manifest.parent / "target" / "release" / binary_name
        destination_dir = Path(self.build_lib) / "datamulehub" / "bin"
        destination_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination_dir / binary_name)


class BinaryDistribution(Distribution):
    def has_ext_modules(self):
        return True


class PlatformWheel(bdist_wheel):
    def get_tag(self):
        _, _, platform_tag = super().get_tag()
        return "py3", "none", platform_tag

setup(
    name="datamule-hub",
    author="John Friedman",
    version="0.2.3",
    description="Access Datamule cloud",
    url="https://github.com/john-friedman/datamule-hub",
    packages=find_packages(),
    cmdclass={
        "bdist_wheel": PlatformWheel,
        "build_py": BuildPyWithRust,
    },
    distclass=BinaryDistribution,
    python_requires=">=3.9",
    install_requires=[
        "tqdm",
        "aiohttp",
        "aioboto3",
        "gcloud-aio-storage",
        "google-auth",
        "google-cloud-storage",
        "pyarrow",
        "websocket-client",
        "zstandard"
    ],
)
