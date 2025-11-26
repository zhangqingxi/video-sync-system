"""
核心层 - 日志管理器

支持按命令名称隔离日志文件。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from src.core.protocols import LoggerProvider


class PathFilter(logging.Filter):
    """路径过滤器 - 将绝对路径转换为相对模块路径"""
    
    def __init__(self, project_root: Path):
        super().__init__()
        self.project_root = project_root
    
    def filter(self, record: logging.LogRecord) -> bool:
        """格式化路径为模块路径格式"""
        try:
            # 获取相对路径
            rel_path: Path = Path(record.pathname).relative_to(self.project_root)
            # 转换为模块路径格式: src/core/oss_handler.py -> core.oss_handler
            parts: tuple[str, ...] = rel_path.parts
            if parts and parts[0] == 'src':
                # 去掉 'src' 前缀和 '.py' 后缀
                module_parts: list[str] = list(parts[1:])
                if module_parts:
                    module_parts[-1] = module_parts[-1].replace('.py', '')
                    record.module_path = '.'.join(module_parts)
                else:
                    record.module_path = record.module
            else:
                # 如果不在 src 目录下，使用原始模块名
                record.module_path = record.module
        except (ValueError, AttributeError):
            # 如果无法获取相对路径，使用原始模块名
            record.module_path = record.module
        
        return True


class CommandLogger:
    """命令专用日志器
    
    使用 stack level 参数确保日志记录显示调用者的位置，而不是 wrapper 的位置
    """
    
    def __init__(self, logger: logging.Logger):
        self._logger: logging.Logger = logger
    
    def debug(self, message: str, **kwargs) -> None:
        # stacklevel=2 表示跳过当前方法，显示调用 logger.debug() 的位置
        self._logger.debug(message, stacklevel=2, **kwargs)
    
    def info(self, message: str, **kwargs) -> None:
        self._logger.info(message, stacklevel=2, **kwargs)
    
    def warning(self, message: str, **kwargs) -> None:
        self._logger.warning(message, stacklevel=2, **kwargs)
    
    def error(self, message: str, exc_info: bool = False, **kwargs) -> None:
        self._logger.error(message, exc_info=exc_info, stacklevel=2, **kwargs)
    
    def critical(self, message: str, exc_info: bool = False, **kwargs) -> None:
        self._logger.critical(message, exc_info=exc_info, stacklevel=2, **kwargs)


class LoggerManager:
    """日志管理器"""
    
    def __init__(self, base_dir: Path):
        """
        初始化日志管理器
        
        Args:
            base_dir: 日志基础目录
        """
        self.base_dir: Path = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._loggers: dict[str, logging.Logger] = {}
        
        # 获取项目根目录
        self.project_root = Path(__file__).parent.parent.parent
    
    def get_logger(self, command_name: str, module_name: str = '__main__') -> 'CommandLogger':
        """
        获取命令专用日志器
        
        Args:
            command_name: 命令名称
            module_name: 模块名称
            
        Returns:
            CommandLogger: 命令专用日志器
        """
        key: str = f"{command_name}:{module_name}"
        
        if key not in self._loggers:
            logger: logging.Logger = logging.getLogger(key)
            logger.setLevel(logging.DEBUG)
            
            # 创建命令专用日志目录
            now: datetime = datetime.now()
            log_dir: Path = self.base_dir / command_name / now.strftime('%Y%m%d')
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # 按小时分割日志文件
            log_file: Path = log_dir / f"{now.strftime('%H')}.log"
            
            # 创建路径过滤器
            path_filter: PathFilter = PathFilter(self.project_root)
            
            # 文件处理器
            file_handler: logging.FileHandler = logging.FileHandler(log_file, encoding='utf-8')
            file_handler.setLevel(logging.DEBUG)
            file_handler.addFilter(path_filter)
            file_formatter: logging.Formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(module_path)s:%(funcName)s:%(lineno)d] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)
            
            # 控制台处理器
            console_handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_handler.addFilter(path_filter)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - [%(module_path)s:%(funcName)s:%(lineno)d] - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            self._loggers[key] = logger
        
        return CommandLogger(self._loggers[key])
