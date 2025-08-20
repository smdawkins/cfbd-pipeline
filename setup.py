from setuptools import setup, find_packages

import src

setup(
  name = "cfbd-pipeline",
  version = "0.0.1",
  author = "Stephen Dawkins",
  author_email = "stephen@dawkinsdigital.com",
  description = "helper functions",
  packages=find_packages(where='./src'),
  package_dir={'': 'src'},
  install_requires=[
    "setuptools","requests>=2.31","pyspark>=3.5"
  ],
  entry_points={
        "console_scripts": [
            "cfbd-ingest=cfbd.cli:ingest",
            "cfbd-silver=cfbd.cli:silver",
        ]
    }
)