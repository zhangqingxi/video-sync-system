"""
基础设施层 - OSS存储适配器

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import alibabacloud_oss_v2 as oss
from io import BytesIO
from src.core.config import StorageConfig, ConstantsConfig
from src.core.exceptions import StorageError
from src.core.protocols import LoggerProvider
from src.infrastructure.storage.base import BaseStorageAdapter


class OSSAdapter(BaseStorageAdapter):
    """阿里云OSS存储适配器（SDK v2）"""

    def __init__(
        self, config: StorageConfig, constants: ConstantsConfig, logger: LoggerProvider
    ) -> None:
        """
        初始化OSS适配器

        Args:
            config: 存储配置
            constants: 常量配置
            logger: 日志提供者
        """
        super().__init__(config=config, constants=constants, logger=logger)

        try:
            # 创建凭证提供者（直接传参）
            credentials_provider: oss.credentials.CredentialsProvider = (
                oss.credentials.StaticCredentialsProvider(
                    access_key_id=config.access_key, access_key_secret=config.secret_key
                )
            )

            # 配置客户端
            cfg: oss.config.Config = oss.config.load_default()
            cfg.credentials_provider = credentials_provider
            cfg.region = config.region

            # 创建客户端
            self._client: oss.Client = oss.Client(cfg)
            self._bucket_name: str = config.bucket

            self.logger.info(
                f"OSS客户端初始化成功: region={config.region}, bucket={config.bucket}"
            )
        except Exception as e:
            raise StorageError(f"Failed to initialize OSS client: {e}")

    def upload_file(
        self, key: str, content: bytes, content_type: str | None = None
    ) -> bool:
        """
        上传文件到OSS

        Args:
            key: 对象键名
            content: 文件内容
            content_type: 内容类型

        Returns:
            bool: 是否成功
        """
        try:
            # 构建请求
            request: oss.PutObjectRequest = oss.PutObjectRequest(
                bucket=self._bucket_name, key=key, body=BytesIO(content)
            )

            # 设置Content-Type
            if content_type:
                request.headers = {"Content-Type": content_type}

            # 上传
            result: oss.PutObjectResult = self._client.put_object(request)

            self.logger.info(f"OSS上传成功: {key}, ETag={result.etag}")
            return True
        except Exception as e:
            self.logger.error(f"OSS上传失败 ({key}): {e}")
            return False

    def check_exists(self, key: str) -> bool:
        """
        检查OSS对象是否存在

        Args:
            key: 对象键名

        Returns:
            bool: 是否存在
        """
        try:
            request: oss.HeadObjectRequest = oss.HeadObjectRequest(
                bucket=self._bucket_name, key=key
            )

            self._client.head_object(request=request)
            return True
        except oss.exceptions.OperationError as e:
            # 更通用的OSS异常处理
            if hasattr(e, "status") and getattr(e, "status", None) == 404:
                return False

            # 或者检查错误消息
            if (
                "404" in str(e)
                or "not exist" in str(e).lower()
                or "not found" in str(e).lower()
            ):
                return False

            self.logger.error(f"OSS检查失败 ({key}): {e}")
            return False
        except Exception as e:
            self.logger.error(f"OSS检查失败 ({key}): {e}")
            return False

    def download_file(self, key: str) -> bytes | None:
        """
        从OSS下载文件

        Args:
            key: 对象键名

        Returns:
            bytes | None: 文件内容，失败返回None
        """
        try:
            request: oss.GetObjectRequest = oss.GetObjectRequest(
                bucket=self._bucket_name, key=key
            )

            result: oss.GetObjectResult = self._client.get_object(request)

            if result.body is None:
                raise ValueError("获取的对象内容为空")

            # 一次性读取所有内容
            content: bytes = result.body.read()

            self.logger.info(f"OSS下载成功: {key}, Size={len(content)}")
            return content
        except Exception as e:
            self.logger.error(f"OSS下载失败 ({key}): {e}")
            return None
