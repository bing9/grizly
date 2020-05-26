import setuptools
from sys import platform
import subprocess

with open("README.md", "r") as fh:
    long_description = fh.read()

import sys
sys.path.insert(0, "/home/acoe_workflows/workflows")

# with open("requirements.txt", "r") as f:
#     requires = [line.strip() for line in f]

setuptools.setup(
    name="grizly",
    version="0.3.5",
    author="Alessio Civitillo",
    description="Small package to build SQL with a Pandas api, generate workflows and more.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    # url="https://github.com/pypa/sampleproject",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    # install_requires=requires,
    entry_points={"console_scripts": ["grizly=grizly.cli:cli"]}
    )

# if platform.startswith("linux"):
    # subprocess.run(["cp", "grizly/cli/cli.py", "/usr/local/bin/grizly"])
