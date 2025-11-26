"""
核心层 - 配置管理器

使用YAML格式配置文件，支持配置验证。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import yaml
from src.core.exceptions import ConfigurationError


@dataclass
class DatabaseConfig:
    """数据库配置"""
    host: str
    port: int
    user: str
    password: str
    database: str
    video_table_name: str
    tag_table_name: str
    video_tag_table_name: str
    charset: str = 'utf8mb4'
    min_connections: int = 1
    max_connections: int = 10
    connect_timeout: int = 10
    read_timeout: int = 30
    write_timeout: int = 30


@dataclass
class StorageConfig:
    """存储配置"""
    access_key: str
    secret_key: str
    region: str
    bucket: str
    encryption_key: str
    request_timeout: int = 60


@dataclass
class APIEndpoints:
    """API端点配置"""
    video_list: str
    video_detail: str
    login: str


@dataclass
class APIHeaders:
    """API请求头配置"""
    user_agent: str
    host: str
    origin: str
    referer: str

@dataclass
class APIRetry:
    """API请求重试配置"""
    max_attempts: int
    backoff_factor: int

@dataclass
class APIConfig:
    """API配置"""
    base_url: str
    domain: str
    username: str
    password: str
    endpoints: APIEndpoints
    headers: APIHeaders
    retry: APIRetry
    page_size: int = 50
    timeout: int = 30
    max_retries: int = 3


@dataclass
class ThreadConfig:
    """线程配置"""
    check_threads: int = 4
    sync_threads: int = 4


@dataclass
class TimingConfig:
    """时间配置"""
    request_delay: float = 0.5
    page_delay: float = 1.0


@dataclass
class ConstantsConfig:
    """常量配置"""
    video_prefix: str = 'video_data'
    origin_filename: str = 'origin.m3u8'
    index_filename: str = 'index.m3u8'
    cover_filename: str = 'cover.jpg'


class ConfigManager:
    """配置管理器"""
    
    def __init__(self, config_path: Path):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        self._config_path: Path = config_path
        self._raw_config: dict[str, Any] = {}
        self._load()
    
    def _load(self) -> None:
        """加载配置文件"""
        if not self._config_path.exists():
            raise ConfigurationError(f"配置文件不存在: {self._config_path}")
        
        try:
            with open(self._config_path, 'r', encoding='utf-8') as f:
                self._raw_config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            raise ConfigurationError(f"配置文件格式错误: {e}")
    
    def validate(self) -> list[str]:
        """
        验证配置完整性
        
        Returns:
            list[str]: 错误信息列表，空列表表示验证通过
        """
        errors: list[str] = []
        
        # 检查必需的配置段
        required_sections: dict[str, Any] = {
            'database': ['host', 'port', 'user', 'password', 'database'],
            'api': ['base_url', 'username', 'password'],
            'storage': {
                's3': ['access_key', 'secret_key', 'region', 'bucket', 'encryption_key'],
                'oss': ['access_key', 'secret_key', 'region', 'bucket', 'encryption_key']
            }
        }
        
        for section, fields in required_sections.items():
            if section not in self._raw_config:
                errors.append(f"缺少配置段: {section}")
                continue
            
            if isinstance(fields, dict):
                # 嵌套配置
                for subsection, subfields in fields.items():
                    if subsection not in self._raw_config[section]:
                        errors.append(f"缺少配置段: {section}.{subsection}")
                        continue
                    
                    for field in subfields:
                        if field not in self._raw_config[section][subsection]:
                            errors.append(f"缺少配置项: {section}.{subsection}.{field}")
            else:
                # 普通配置
                for field in fields:
                    if field not in self._raw_config[section]:
                        errors.append(f"缺少配置项: {section}.{field}")
        
        return errors
    
    @property
    def database(self) -> DatabaseConfig:
        """获取数据库配置"""
        db_config: dict[str, Any] = self._raw_config.get('database', {})
        return DatabaseConfig(**db_config)
    
    @property
    def s3_storage(self) -> StorageConfig:
        """获取S3存储配置"""
        s3_config: dict[str, Any] = self._raw_config.get('storage', {}).get('s3', {})
        return StorageConfig(**s3_config)
    
    @property
    def oss_storage(self) -> StorageConfig:
        """获取OSS存储配置"""
        oss_config: dict[str, Any] = self._raw_config.get('storage', {}).get('oss', {})
        return StorageConfig(**oss_config)
    
    @property
    def api(self) -> APIConfig:
        """获取API配置"""
        api_config: dict[str, Any] = self._raw_config.get('api', {})
        
        # 提取并构建端点配置
        endpoints_data: dict[str, Any] = api_config.get('endpoints', {})
        endpoints: APIEndpoints = APIEndpoints(
            video_list=endpoints_data.get('video_list', '/video/index'),
            video_detail=endpoints_data.get('video_detail', '/video/get-video'),
            login=endpoints_data.get('login', '/login/check-login')
        )
        
        # 提取并构建请求头配置
        headers_data: dict[str, Any] = api_config.get('headers', {})
        headers: APIHeaders = APIHeaders(
            user_agent=headers_data.get('user_agent', 'Mozilla/5.0'),
            host=headers_data.get('host', api_config.get('domain', '')),
            origin=headers_data.get('origin', f"https://{api_config.get('domain', '')}"),
            referer=headers_data.get('referer', f"https://{api_config.get('domain', '')}")
        )
        
        # 提取并构建重试配置
        retry_data: dict[str, Any] = api_config.get('retry', {})
        retry: APIRetry = APIRetry(
            max_attempts=retry_data.get('max_attempts', 3),
            backoff_factor=retry_data.get('backoff_factor', 2)
        )
        
        return APIConfig(
            base_url=api_config.get('base_url', ''),
            domain=api_config.get('domain', ''),
            username=api_config.get('username', ''),
            password=api_config.get('password', ''),
            endpoints=endpoints,
            headers=headers,
            retry=retry,
            page_size=api_config.get('page_size', 50),
            timeout=api_config.get('timeout', 30),
            max_retries=retry_data.get('max_attempts', 3)
        )
    
    @property
    def threads(self) -> ThreadConfig:
        """获取线程配置"""
        thread_config: dict[str, Any] = self._raw_config.get('threads', {})
        return ThreadConfig(
            check_threads=thread_config.get('check', 4),
            sync_threads=thread_config.get('sync', 4)
        )
    
    @property
    def timing(self) -> TimingConfig:
        """获取时间配置"""
        timing_config: dict[str, Any] = self._raw_config.get('timing', {})
        return TimingConfig(**timing_config)
    
    @property
    def constants(self) -> ConstantsConfig:
        """获取常量配置"""
        constants_config: dict[str, Any] = self._raw_config.get('constants', {})
        return ConstantsConfig(**constants_config)
    
    @property
    def site_domains(self) -> list[str]:
        """获取站点域名列表"""
        site_config: dict[str, Any] = self._raw_config.get('site', {})
        domains: Any = site_config.get('domains', [])
        return list(domains) if domains else []
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """
        获取配置项
        
        Args:
            section: 配置段名称
            key: 配置项名称
            default: 默认值
            
        Returns:
            Any: 配置值
        """
        return self._raw_config.get(section, {}).get(key, default)

    @property
    def raw_config(self) -> dict[str, Any]:
        """获取原始配置数据"""
        return self._raw_config
