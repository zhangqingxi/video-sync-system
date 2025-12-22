"""
基础设施层 - S3存储适配器

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import boto3
from typing import cast, Any
from io import BytesIO
from botocore.config import Config
from botocore.exceptions import ClientError
from src.core.config import StorageConfig, ConstantsConfig
from src.core.exceptions import StorageError
from src.core.protocols import LoggerProvider
from src.infrastructure.storage.base import BaseStorageAdapter


class S3Adapter(BaseStorageAdapter):
    """AWS S3存储适配器"""

    def __init__(
        self, config: StorageConfig, constants: ConstantsConfig, logger: LoggerProvider
    ) -> None:
        """
        初始化S3适配器

        Args:
            config: 存储配置
            constants: 常量配置
            logger: 日志提供者
        """
        super().__init__(config, constants, logger)

        try:
            # 创建自定义配置，设置超时参数
            s3_config = Config(
                connect_timeout=config.connect_timeout,    # 连接超时时间（秒）
                read_timeout=config.connect_timeout,      # 读取超时时间（秒）
            )
            
            self._client = boto3.client(
                "s3",
                aws_access_key_id=config.access_key,
                aws_secret_access_key=config.secret_key,
                region_name=config.region,
                config=s3_config
            )
            self._bucket_name = config.bucket
        except Exception as e:
            raise StorageError(f"Failed to initialize S3 client: {e}")

    def upload_file(
        self, key: str, content: bytes, content_type: str | None = None
    ) -> bool:
        """
        上传文件到S3

        Args:
            key: 对象键名
            content: 文件内容
            content_type: 内容类型

        Returns:
            bool: 是否成功
        """
        max_retries: int = 3  # 最大重试次数
        for attempt in range(max_retries):
            try:
                # 创建 BytesIO 对象
                body: BytesIO = BytesIO(content)

                extra_args: dict[str, Any] = {}
                # 设置Content-Type
                if content_type:
                    extra_args["ContentType"] = content_type
                
                # 上传
                result: dict[str, Any] = self._client.put_object(
                    bucket=self._bucket_name, key=key, body=body
                )
                etag = result.get("ETag", "")
                self.logger.info(f"S3上传成功: {key}, ETag={etag}")
            except Exception as e:
                if attempt < max_retries - 1:
                    # 未达到最大重试次数，记录警告并重试
                    self.logger.warning(
                        f"OSS上传失败 ({key}), 第{attempt + 1}/{max_retries}次尝试: {e}"
                    )
                    # 等待一段时间再重试
                    import time
                    time.sleep(2 ** attempt)  # 指数退避: 1s, 2s, 4s
                else:
                    # 最后一次尝试失败，记录错误
                    self.logger.error(f"S3上传失败 ({key}): {e}")
                    return False
        return True

    def check_exists(self, key: str) -> bool:
        """
        检查S3对象是否存在

        Args:
            key: 对象键名

        Returns:
            bool: 是否存在
        """
        try:
            self._client.head_object(Bucket=self._bucket_name, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
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
            response = self._client.get_object(Bucket=self._bucket_name, Key=key)
            return cast(bytes, response["Body"].read())
        except ClientError as e:
            self.logger.error(f"S3下载失败 ({key}): {e}")
            return None
