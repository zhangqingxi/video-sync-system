"""
核心层 - 依赖注入容器

提供服务注册和解析功能。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from dataclasses import dataclass
from typing import Any, Callable, TypeVar
from src.core.exceptions import ConfigurationError

T = TypeVar('T')

@dataclass
class ServiceDescriptor:
    """服务描述符"""
    factory: Callable[[], Any]
    singleton: bool = False
    instance: Any = None


class DIContainer:
    """依赖注入容器"""
    
    def __init__(self):
        self._services: dict[type, ServiceDescriptor] = {}
    
    def register(
        self,
        interface: type[T],
        factory: Callable[[], T],
        singleton: bool = False
    ) -> None:
        """
        注册服务
        
        Args:
            interface: 服务接口类型
            factory: 服务工厂函数
            singleton: 是否为单例
        """
        self._services[interface]: ServiceDescriptor = ServiceDescriptor(
            factory=factory,
            singleton=singleton
        )
    
    def resolve(self, interface: type[T]) -> T:
        """
        解析服务
        
        Args:
            interface: 服务接口类型
            
        Returns:
            T: 服务实例
            
        Raises:
            ConfigurationError: 服务未注册时抛出
        """
        descriptor: ServiceDescriptor | None = self._services.get(interface)
        if not descriptor:
            raise ConfigurationError(f"Service {interface.__name__} not registered")
        
        if descriptor.singleton:
            if descriptor.instance is None:
                descriptor.instance = descriptor.factory()
            return descriptor.instance
        
        return descriptor.factory()
    
    def is_registered(self, interface: type) -> bool:
        """
        检查服务是否已注册
        
        Args:
            interface: 服务接口类型
            
        Returns:
            bool: 是否已注册
        """
        return interface in self._services
