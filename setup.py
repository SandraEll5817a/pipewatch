"""Package setup for pipewatch."""

from setuptools import setup, find_packages


setup(
    name="pipewatch",
    version="0.1.0",
    description="A lightweight CLI tool to monitor and alert on ETL pipeline health.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="pipewatch contributors",
    python_requires=">=3.11",
    packages=find_packages(exclude=["pipewatch/tests*"]),
    install_requires=[
        "click>=8.1",
        "pyyaml>=6.0",
        "requests>=2.31",
        "pydantic>=2.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4",
            "pytest-cov",
            "responses",
        ]
    },
    entry_points={
        "console_scripts": [
            "pipewatch=pipewatch.cli:cli",
        ]
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: System :: Monitoring",
        "Environment :: Console",
    ],
)
