"""
核心层 - 异常定义

定义项目范围内的自定义异常类。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

class VideoSyncException(Exception):
    """视频同步系统基础异常"""
    pass


class ConfigurationError(VideoSyncException):
    """配置错误"""
    pass


class ValidationError(VideoSyncException):
    """验证错误"""
    pass


class StorageError(VideoSyncException):
    """存储操作错误"""
    pass


class DatabaseError(VideoSyncException):
    """数据库操作错误"""
    pass


class APIError(VideoSyncException):
    """API请求错误"""
    pass


class TokenExpiredError(APIError):
    """Token过期错误"""
    pass


class ResourceNotFoundError(VideoSyncException):
    """资源不存在错误"""
    pass


class SyncError(VideoSyncException):
    """同步错误"""
    pass
