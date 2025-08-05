from setuptools import setup, find_packages

setup(
    name="infralyzer",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "polars>=0.18.0",  # Optional for Polars engine
        "boto3>=1.26.0", 
        "s3fs>=2023.1.0",
        "pyarrow>=10.0.0",
        "duckdb>=0.8.0",   # Core engine
        "pandas>=1.5.0",   # Core data handling
        "fastapi>=0.100.0",  # API framework
        "uvicorn>=0.23.0",   # ASGI server
    ],
    python_requires=">=3.8",
    description="Infralyzer - Multi-engine FinOps analytics platform for AWS cost optimization with pluggable query engines (DuckDB, Polars, Athena)",
    # long_description=open("README.md").read(),
    # long_description_content_type="text/markdown",
    keywords=["aws", "finops", "cost-optimization", "analytics", "duckdb", "polars", "athena", "infrastructure"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: Financial and Insurance Industry",
        "Intended Audience :: System Administrators",
        "Topic :: Office/Business :: Financial",
        "Topic :: System :: Systems Administration",
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
        "Source": "https://github.com/jasonwu001t/infralyzer",
        "Documentation": "https://github.com/jasonwu001t/infralyzer#readme",
        "Bug Reports": "https://github.com/jasonwu001t/infralyzer/issues",
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