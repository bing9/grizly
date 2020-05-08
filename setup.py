import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


# with open("requirements.txt", "r") as f:
#     requires = [line.strip() for line in f]

setuptools.setup(
    name="grizly",
    version="0.3.3",
    author="Alessio Civitillo",
    description="Small package to build SQL with a Pandas api",
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
)
