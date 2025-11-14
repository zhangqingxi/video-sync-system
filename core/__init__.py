"""
Core模块包初始化文件

导出所有核心处理器和工具函数，提供统一的导入接口。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

from core.api_handler import ApiHandler
from core.db_handler import DatabaseHandler
from core.logger_handler import setup_logger
from core.oss_handler import OSSHandler
from core.s3_handler import S3Handler
from core.site_handler import SiteHandler
from core.util_handler import (
    BASE_DIR,
    is_ffmpeg_installed,
    load_config,
    load_state,
    save_state,
)

# ============================================================================
# 公共导出
# ============================================================================

__all__: list[str] = [
    # 处理器类
    'ApiHandler',
    'DatabaseHandler',
    'OSSHandler',
    'S3Handler',
    'SiteHandler',
    # 工具函数
    'setup_logger',
    'load_config',
    'load_state',
    'save_state',
    'is_ffmpeg_installed',
    # 常量
    'BASE_DIR',
]

# ============================================================================
# 版本信息
# ============================================================================

__version__: str = '2.0.0'
__author__: str = 'Qasim'
__license__: str = 'MIT'
__description__: str = '视频数据同步系统核心模块'
