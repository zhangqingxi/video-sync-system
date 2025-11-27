"""
基础设施层 - 存储工厂

根据配置创建存储适配器。

Author: Qasim
Version: 3.0
Python: 3.0
"""

from src.core.config import StorageConfig, ConstantsConfig
from src.core.protocols import LoggerProvider, StorageProvider
from src.infrastructure.storage.s3 import S3Adapter
from src.infrastructure.storage.oss import OSSAdapter


class StorageFactory:
    """存储工厂"""

    @staticmethod
    def create_s3(
        config: StorageConfig, constants: ConstantsConfig, logger: LoggerProvider
    ) -> StorageProvider:
        """
        创建S3存储适配器

        Args:
            config: 存储配置
            constants: 常量配置
            logger: 日志提供者

        Returns:
            StorageProvider: S3存储适配器
        """
        return S3Adapter(config=config, constants=constants, logger=logger)

    @staticmethod
    def create_oss(
        config: StorageConfig, constants: ConstantsConfig, logger: LoggerProvider
    ) -> StorageProvider:
        """
        创建OSS存储适配器

        Args:
            config: 存储配置
            constants: 常量配置
            logger: 日志提供者

        Returns:
            StorageProvider: OSS存储适配器
        """
        return OSSAdapter(config=config, constants=constants, logger=logger)
