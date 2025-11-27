"""
应用层 - 命令基类

定义命令模式的基础结构。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from src.core.container import DIContainer
from src.core.config import ConfigManager
from src.core.logger import LoggerManager
from src.core.protocols import LoggerProvider


@dataclass
class CommandContext:
    """命令上下文"""

    container: DIContainer
    config: ConfigManager
    logger_manager: LoggerManager


class BaseCommand(ABC):
    """命令基类"""

    def __init__(self, context: CommandContext):
        """
        初始化命令

        Args:
            context: 命令上下文
        """
        self.context: CommandContext = context
        self.config: ConfigManager = context.config
        self.container: DIContainer = context.container
        self.logger: LoggerProvider = context.logger_manager.get_logger(self.name)

    @property
    @abstractmethod
    def name(self) -> str:
        """命令名称"""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """命令描述"""
        pass

    @abstractmethod
    def execute(self) -> int:
        """
        执行命令

        Returns:
            int: 退出码 (0=成功, 非0=失败)
        """
        pass

    def cleanup(self) -> None:
        """清理资源（可选覆盖）"""
        pass
