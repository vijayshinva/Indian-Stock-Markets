import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name="indian-stock-markets",
    version="0.0.1",
    author="Vijayshinva B Karnure",
    author_email="vijayshinva@outlook.com",
    description="A Python package for analyzing Indian Stock Markets",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/vijayshinva/Indian-Stock-Markets",
    packages=setuptools.find_packages(),
    classifiers=["Programming Language :: Python :: 3",
        "License :: OSI Approved :: Apache License 2.0",
        "Operating System :: OS Independent",],
    python_requires='>=3.6',)
