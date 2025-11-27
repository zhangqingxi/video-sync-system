"""基础设施层 - 存储模块"""

from src.infrastructure.storage.base import BaseStorageAdapter
from src.infrastructure.storage.s3 import S3Adapter
from src.infrastructure.storage.oss import OSSAdapter
from src.infrastructure.storage.factory import StorageFactory

__all__ = [
    "BaseStorageAdapter",
    "S3Adapter",
    "OSSAdapter",
    "StorageFactory",
]
