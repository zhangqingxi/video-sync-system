"""
核心层 - 协议定义

定义项目核心接口和协议。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Protocol, runtime_checkable, Any


@runtime_checkable
class StorageProvider(Protocol):
    """存储提供者协议"""
    
    def upload_file(self, key: str, content: bytes, content_type: str | None = None) -> bool:
        """
        上传文件
        
        Args:
            key: 对象键名
            content: 文件内容
            content_type: 内容类型
            
        Returns:
            bool: 上传是否成功
        """
        ...
    
    def check_exists(self, key: str) -> bool:
        """
        检查文件是否存在
        
        Args:
            key: 对象键名
            
        Returns:
            bool: 文件是否存在
        """
        ...
    
    def download_file(self, key: str) -> bytes | None:
        """
        下载文件
        
        Args:
            key: 对象键名
            
        Returns:
            bytes | None: 文件内容，不存在则返回None
        """
        ...
    
    def generate_key(
        self,
        resource_id: int,
        resource_origin: str,
        resource_filename: str,
        resource_episode: int | None = None
    ) -> str:
        """
        生成存储键名
        
        Args:
            resource_id: 资源ID
            resource_origin: 资源来源
            resource_filename: 资源名称
            resource_episode: 资源集数（可选）
            
        Returns:
            str: 生成的键名
        """
        ...
    
    def upload_from_url(self, resource_url: str, resource_type: str, resource_key: str) -> bool:
        """
        从URL下载并上传到存储
        
        Args:
            resource_url: 源URL
            resource_type: 资源类型 (index/origin/cover)
            resource_key: 目标对象键名
            
        Returns:
            bool: 是否成功
        """
        ...
    
    def process_single_video_sync(
        self,
        resource_id: int,
        resource_title: str,
        resource_type: str,
        video_list: list[str],
        cover: str
    ) -> bool:
        """
        批量同步单个视频的所有剧集和封面
        
        Args:
            resource_id: 资源ID
            resource_title: 资源标题
            resource_type: 资源类型 (index/origin/cover)
            video_list: m3u8链接列表
            cover: 封面图片链接
            
        Returns:
            bool: 是否全部成功
        """
        ...


@runtime_checkable
class VideoRepository(Protocol):
    """视频数据仓库协议"""
    
    def get_by_id(self, video_id: int) -> dict[str, Any] | None:
        """
        根据ID获取视频
        
        Args:
            video_id: 视频ID
            
        Returns:
            dict[str, Any] | None: 视频数据，不存在则返回None
        """
        ...
    
    def get_videos_after_id(self, last_id: int, limit: int = 100) -> list[dict[str, Any]]:
        """
        获取ID之后的视频列表
        
        Args:
            last_id: 上一个视频ID
            limit: 限制数量
            
        Returns:
            list[dict[str, Any]]: 视频列表
        """
        ...
    
    def insert(self, video_data: dict[str, Any]) -> bool:
        """
        插入视频数据
        
        Args:
            video_data: 视频数据
            
        Returns:
            bool: 是否成功
        """
        ...
    
    def exists(self, video_id: int) -> bool:
        """
        检查视频是否存在
        
        Args:
            video_id: 视频ID
            
        Returns:
            bool: 是否存在
        """
        ...

@runtime_checkable
class LoggerProvider(Protocol):
    """日志提供者协议"""
    
    def debug(self, message: str, **kwargs: Any) -> None:
        """
        记录DEBUG级别日志
        
        Args:
            message: 日志消息
            **kwargs: 其他参数，如 exc_info=True 用于记录异常信息
        """
        ...
    
    def info(self, message: str, **kwargs: Any) -> None:
        """
        记录INFO级别日志
        
        Args:
            message: 日志消息
            **kwargs: 其他参数，如 exc_info=True 用于记录异常信息
        """
        ...
    
    def warning(self, message: str, **kwargs: Any) -> None:
        """
        记录WARNING级别日志
        
        Args:
            message: 日志消息
            **kwargs: 其他参数，如 exc_info=True 用于记录异常信息
        """
        ...
    
    def error(self, message: str, **kwargs: Any) -> None:
        """
        记录ERROR级别日志
        
        Args:
            message: 日志消息
            **kwargs: 其他参数，如 exc_info=True 用于记录异常信息
        """
        ...
    
    def critical(self, message: str, **kwargs: Any) -> None:
        """
        记录CRITICAL级别日志
        
        Args:
            message: 日志消息
            **kwargs: 其他参数，如 exc_info=True 用于记录异常信息
        """
        ...
