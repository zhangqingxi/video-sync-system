"""
全局日志系统处理器

提供按小时分级的日志管理，支持文件和控制台双输出。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import logging
import sys
from datetime import datetime
from logging import FileHandler, Formatter, StreamHandler
from pathlib import Path

from core.util_handler import BASE_DIR

# ============================================================================
# 常量定义
# ============================================================================

# 日志格式化字符串
LOG_FORMAT: str = (
    '%(asctime)s - %(levelname)s - '
    '[%(name)s:%(funcName)s:%(lineno)d] - %(message)s'
)

# 日志时间格式
LOG_DATE_FORMAT: str = '%Y-%m-%d %H:%M:%S'


# ============================================================================
# 自定义日志处理器
# ============================================================================

class HourlyDirectoryLogHandler(FileHandler):
    """
    按小时分级的日志文件处理器
    
    自动按 YYYYMMDD/HH.log 格式创建日志文件，支持跨日跨小时自动切换。
    
    Directory Structure:
        logs/
        ├── 20250114/
        │   ├── 00.log  # 0点-1点的日志
        │   ├── 01.log  # 1点-2点的日志
        │   └── ...
        └── 20250115/
            └── ...
    
    Features:
        - 自动按小时创建日志文件
        - 跨日自动创建新目录
        - 线程安全的文件切换
        - UTF-8编码支持
    
    Attributes:
        log_dir: 日志基础目录路径
    """
    
    def __init__(self, log_dir: Path, encoding: str = 'utf-8') -> None:
        """
        初始化日志处理器
        
        Args:
            log_dir: 日志基础目录路径
            encoding: 文件编码格式，默认为UTF-8
        """
        # 确保日志基础目录存在
        self.log_dir: Path = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # 计算初始日志文件路径
        initial_log_path: Path = self._get_log_path()
        initial_log_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 调用父类初始化
        super().__init__(filename=str(initial_log_path), encoding=encoding)
    
    def _get_log_path(self) -> Path:
        """
        根据当前时间计算日志文件完整路径
        
        Returns:
            Path: 日志文件的完整路径 (例: logs/20250114/15.log)
        """
        now: datetime = datetime.now()
        date_dir: Path = self.log_dir / now.strftime('%Y%m%d')
        log_filename: str = now.strftime('%H') + '.log'
        return date_dir / log_filename
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        发送日志记录
        
        在每次写入前检查是否需要切换日志文件（跨小时或跨日）。
        
        Args:
            record: 日志记录对象
        
        Note:
            该方法会自动处理文件切换，无需手动干预
        """
        current_log_path: Path = self._get_log_path()
        
        # 检查是否需要切换日志文件
        if self.baseFilename != str(current_log_path):
            # 确保新目录存在
            current_log_path.parent.mkdir(parents=True, exist_ok=True)
            
            # 关闭当前文件流
            if self.stream:
                self.stream.close()
            
            # 更新文件路径并重新打开
            self.baseFilename = str(current_log_path)
            self.mode = 'a'  # 追加模式
            
            # 强制重新打开文件
            self._open()
        
        # 调用父类方法完成日志写入
        super().emit(record)


# ============================================================================
# 日志系统初始化
# ============================================================================

def setup_logger() -> None:
    """
    配置全局日志记录器
    
    设置日志级别、格式和输出目标（文件+控制台双输出）。
    
    Configuration:
        - 日志级别: INFO
        - 输出目标: 文件（按小时分级） + 控制台
        - 日志格式: 时间 + 级别 + 模块 + 函数 + 行号 + 消息
        - 文件编码: UTF-8
    
    Note:
        该函数应在程序启动时调用一次，之后所有模块都可使用 logging
    
    Example:
        >>> from core.logger_handler import setup_logger
        >>> setup_logger()
        >>> import logging
        >>> logging.info("应用程序已启动")
    """
    log_dir: Path = BASE_DIR / 'logs'
    
    # 获取根日志记录器
    root_logger: logging.Logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # 清除已有的处理器（避免重复配置）
    if root_logger.hasHandlers():
        root_logger.handlers.clear()
    
    # 创建日志格式化器
    formatter: Formatter = Formatter(
        fmt=LOG_FORMAT,
        datefmt=LOG_DATE_FORMAT
    )
    
    # 配置文件日志处理器（按小时分级）
    file_handler: HourlyDirectoryLogHandler = HourlyDirectoryLogHandler(log_dir=log_dir)
    file_handler.setFormatter(fmt=formatter)
    file_handler.setLevel(level=logging.INFO)
    root_logger.addHandler(hdlr=file_handler)
    
    # 配置控制台日志处理器
    console_handler: StreamHandler = StreamHandler(stream=sys.stdout)
    console_handler.setFormatter(fmt=formatter)
    console_handler.setLevel(level=logging.INFO)
    root_logger.addHandler(hdlr=console_handler)
    
    # 记录日志系统初始化成功
    root_logger.info("日志系统初始化完成")
