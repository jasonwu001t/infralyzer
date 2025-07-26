from setuptools import setup, find_packages

setup(
    name="de_polars",
    version="0.3.0",
    packages=find_packages(),
    install_requires=[
        "polars>=0.18.0",
        "boto3>=1.26.0", 
        "s3fs>=2023.1.0",
        "pyarrow>=10.0.0",
    ],
    python_requires=">=3.8",
    description="Simple SQL interface for AWS Data Exports using Polars - supports CUR 2.0, FOCUS 1.0, Carbon emissions, and Cost Optimization Recommendations",
    # long_description=open("README.md").read(),
    # long_description_content_type="text/markdown",
    keywords=["aws", "data-exports", "cur", "focus", "carbon", "polars", "sql", "analytics"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Topic :: Office/Business :: Financial",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
    ],
    project_urls={
        "Source": "https://github.com/jasonwu001t/de-polars",
        "Documentation": "https://github.com/jasonwu001t/de-polars#readme",
        "Bug Reports": "https://github.com/jasonwu001t/de-polars/issues",
    },
) 


"""
Build the package:
python setup.py sdist bdist_wheel

Install the package:
pip install .

Update the package:
pip install -e . 
or pip install --upgrade .
"""