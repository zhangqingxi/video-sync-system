"""
应用层 - 命令注册器

管理所有命令的注册和调度。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Type
from src.application.commands.base import BaseCommand, CommandContext
from src.core.exceptions import ValidationError


class CommandRegistry:
    """命令注册器"""
    
    def __init__(self) -> None:
        self._commands: dict[str, Type[BaseCommand]] = {}
    
    def register(self, command_class: Type[BaseCommand]) -> None:
        """
        注册命令
        
        Args:
            command_class: 命令类
        """
        # 临时实例化以获取名称
        temp_instance: BaseCommand = command_class.__new__(command_class)
        command_name: str = temp_instance.name
        self._commands[command_name] = command_class
    
    def get_command(self, name: str, context: CommandContext) -> BaseCommand:
        """
        获取命令实例
        
        Args:
            name: 命令名称
            context: 命令上下文
            
        Returns:
            BaseCommand: 命令实例
            
        Raises:
            ValidationError: 命令不存在时抛出
        """
        command_class: Type[BaseCommand] | None = self._commands.get(name)
        if not command_class:
            raise ValidationError(f"Command '{name}' not found")
        
        return command_class(context)
    
    def list_commands(self) -> list[str]:
        """
        列出所有已注册命令
        
        Returns:
            list[str]: 命令名称列表
        """
        return list(self._commands.keys())
    
    def has_command(self, name: str) -> bool:
        """
        检查命令是否存在
        
        Args:
            name: 命令名称
            
        Returns:
            bool: 是否存在
        """
        return name in self._commands
