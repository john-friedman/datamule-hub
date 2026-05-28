from setuptools import find_packages, setup

setup(
    name="datamule-hub",
    author="John Friedman",
    version="0.1.1",
    description="Access Datamule cloud",
    url="https://github.com/john-friedman/datamule-hub",
    packages=find_packages(),
    install_requires=[
        "tqdm",
        "aiohttp",
        "aioboto3",
        "gcloud-aio-storage",
        "google-auth",
        "google-cloud-storage",
        "websocket-client",
    ],
)
