"""
基础设施层 - S3存储适配器

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import boto3
from botocore.exceptions import ClientError

from src.core.config import StorageConfig, ConstantsConfig
from src.core.exceptions import StorageError
from src.core.protocols import LoggerProvider
from src.infrastructure.storage.base import BaseStorageAdapter

class S3Adapter(BaseStorageAdapter):
    """AWS S3存储适配器"""
    
    def __init__(
        self,
        config: StorageConfig,
        constants: ConstantsConfig,
        logger: LoggerProvider
    ):
        """
        初始化S3适配器
        
        Args:
            config: 存储配置
            constants: 常量配置
            logger: 日志提供者
        """
        super().__init__(config, constants, logger)
        
        try:
            self._client = boto3.client(
                's3',
                aws_access_key_id=config.access_key,
                aws_secret_access_key=config.secret_key,
                region_name=config.region
            )
            self._bucket = config.bucket
        except Exception as e:
            raise StorageError(f"Failed to initialize S3 client: {e}")
    
    def upload_file(self, key: str, content: bytes, content_type: str | None = None) -> bool:
        """
        上传文件到S3
        
        Args:
            key: 对象键名
            content: 文件内容
            content_type: 内容类型
            
        Returns:
            bool: 是否成功
        """
        try:
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type

            self._client.put_object(
                Bucket=self._bucket,
                Key=key,
                Body=content,
                **extra_args
            )
            self.logger.info(f"S3上传成功: {key}")
            return True
        except ClientError as e:
            self.logger.error(f"S3上传失败 ({key}): {e}")
            return False
    
    def check_exists(self, key: str) -> bool:
        """
        检查S3对象是否存在
        
        Args:
            key: 对象键名
            
        Returns:
            bool: 是否存在
        """
        try:
            self._client.head_object(Bucket=self._bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                self.logger.error(f"S3检查失败 ({key}): {e}")
                return False
    
    def download_file(self, key: str) -> bytes | None:
        """
        从S3下载文件
        
        Args:
            key: 对象键名
            
        Returns:
            bytes | None: 文件内容，失败返回None
        """
        try:
            response = self._client.get_object(Bucket=self._bucket, Key=key)
            return response['Body'].read()
        except ClientError as e:
            self.logger.error(f"S3下载失败 ({key}): {e}")
            return None

    
