import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(name="indian_stock_markets",
                 version="0.0.6",
                 author="Vijayshinva B Karnure",
                 author_email="vijayshinva@outlook.com",
                 description="A Python package for analyzing Indian Stock Markets",
                 long_description=long_description,
                 long_description_content_type="text/markdown",
                 url="https://github.com/vijayshinva/Indian-Stock-Markets",
                 packages=setuptools.find_packages(),
                 include_package_data=True,
                 classifiers=["Programming Language :: Python :: 3",
                              "License :: OSI Approved :: Apache Software License",
                              "Operating System :: OS Independent", ],
                 python_requires='>=3.6',
                 install_requires=[
                     'requests',
                     'pandas'
                 ])
