"""
DE Polars Data Management - S3 and local data handling
"""

from .s3_data_manager import S3DataManager
from .local_data_manager import LocalDataManager
from .data_downloader import DataDownloader

__all__ = ["S3DataManager", "LocalDataManager", "DataDownloader"]