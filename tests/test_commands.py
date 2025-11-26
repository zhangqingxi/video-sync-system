"""
测试 - 命令测试示例

演示如何测试命令。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path

from src.core.container import DIContainer
from src.core.config import ConfigManager
from src.core.logger import LoggerManager
from src.application.commands import CommandContext, CommandRegistry, S3OriginCheckCommand


class TestCommandRegistry(unittest.TestCase):
    """命令注册器测试"""
    
    def test_register_command(self) -> None:
        """测试命令注册"""
        registry = CommandRegistry()
        registry.register(S3OriginCheckCommand)
        
        # 验证命令已注册
        self.assertTrue(registry.has_command('s3_origin_check'))
    
    def test_list_commands(self) -> None:
        """测试列出命令"""
        registry = CommandRegistry()
        registry.register(S3OriginCheckCommand)
        
        commands = registry.list_commands()
        self.assertIn('s3_origin_check', commands)
    
    def test_get_command(self) -> None:
        """测试获取命令实例"""
        registry = CommandRegistry()
        registry.register(S3OriginCheckCommand)
        
        # 创建mock上下文
        container = Mock(spec=DIContainer)
        config = Mock(spec=ConfigManager)
        logger_manager = Mock(spec=LoggerManager)
        logger_manager.get_logger.return_value = Mock()
        
        context = CommandContext(
            container=container,
            config=config,
            logger_manager=logger_manager
        )
        
        # 获取命令
        command = registry.get_command('s3_origin_check', context)
        self.assertIsInstance(command, S3OriginCheckCommand)


class TestS3OriginCheckCommand(unittest.TestCase):
    """S3 Origin检查命令测试"""
    
    def setUp(self) -> None:
        """测试前准备"""
        # 创建mock依赖
        self.container = Mock(spec=DIContainer)
        self.config = Mock(spec=ConfigManager)
        self.logger_manager = Mock(spec=LoggerManager)
        self.logger = Mock()
        self.logger_manager.get_logger.return_value = self.logger
        
        # 配置mock config
        self.config.s3_storage = Mock()
        self.config.constants = Mock()
        self.config.threads = Mock()
        self.config.threads.check_threads = 2
        
        # 创建上下文
        self.context = CommandContext(
            container=self.container,
            config=self.config,
            logger_manager=self.logger_manager
        )
    
    def test_command_name(self) -> None:
        """测试命令名称"""
        command = S3OriginCheckCommand(self.context)
        self.assertEqual(command.name, 's3_origin_check')
    
    def test_command_description(self) -> None:
        """测试命令描述"""
        command = S3OriginCheckCommand(self.context)
        self.assertIsNotNone(command.description)


if __name__ == '__main__':
    unittest.main()
