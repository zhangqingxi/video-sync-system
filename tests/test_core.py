"""
测试 - 核心层测试示例

演示如何测试核心组件。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import unittest
from pathlib import Path
from src.core.container import DIContainer
from src.core.config import ConfigManager


class TestDIContainer(unittest.TestCase):
    """依赖注入容器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.container = DIContainer()
    
    def test_register_and_resolve_singleton(self):
        """测试单例注册和解析"""
        # 注册单例服务
        class TestService:
            def __init__(self):
                self.value = 42
        
        self.container.register(TestService, lambda: TestService(), singleton=True)
        
        # 解析服务
        service1 = self.container.resolve(TestService)
        service2 = self.container.resolve(TestService)
        
        # 验证是同一实例
        self.assertIs(service1, service2)
        self.assertEqual(service1.value, 42)
    
    def test_register_and_resolve_transient(self):
        """测试瞬时服务注册和解析"""
        class TestService:
            pass
        
        self.container.register(TestService, lambda: TestService(), singleton=False)
        
        # 解析服务
        service1 = self.container.resolve(TestService)
        service2 = self.container.resolve(TestService)
        
        # 验证不是同一实例
        self.assertIsNot(service1, service2)


class TestConfigManager(unittest.TestCase):
    """配置管理器测试"""
    
    def test_load_example_config(self):
        """测试加载示例配置"""
        config_path = Path('config/config.example.yaml')
        if not config_path.exists():
            self.skipTest("配置文件不存在")
        
        config = ConfigManager(config_path)
        
        # 验证配置加载
        self.assertIsNotNone(config.database)
        self.assertIsNotNone(config.api)
        self.assertIsNotNone(config.threads)
    
    def test_validation(self):
        """测试配置验证"""
        config_path = Path('config/config.example.yaml')
        if not config_path.exists():
            self.skipTest("配置文件不存在")
        
        config = ConfigManager(config_path)
        errors = config.validate()
        
        # 示例配置可能有验证错误（因为是模板）
        # 这里只验证validate方法能正常工作
        self.assertIsInstance(errors, list)


if __name__ == '__main__':
    unittest.main()
