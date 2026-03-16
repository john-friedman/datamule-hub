from setuptools import setup

setup(
    name="datamule_hub",
    author="John Friedman",
    version="0.0.3",
    description="Access Datamule cloud",
    url="https://github.com/john-friedman/datamule-hub",
    install_requires=[
        "tqdm",
        "aiohttp",
        "aioboto3",
        "gcloud-aio-storage",
        "google-auth",
        "google-cloud-storage",
],
)