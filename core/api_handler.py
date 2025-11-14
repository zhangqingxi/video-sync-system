"""
第三方API请求处理器

封装所有对第三方API的请求操作，包括用户认证、视频列表获取、视频详情查询等。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import json
import logging
import math
from configparser import ConfigParser, SectionProxy
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3.util.retry import Retry

# ============================================================================
# 模块级日志记录器
# ============================================================================

logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# API处理器类
# ============================================================================

class ApiHandler:
    """
    第三方API请求处理器
    
    负责处理与外部API的所有交互，包括身份验证、数据获取、错误处理等。
    
    Main Features:
        - 用户登录和Token管理
        - 视频列表分页获取
        - 视频详情信息获取
        - 自动重试和错误处理
        - 连接池优化
    
    Attributes:
        base_url: API基础URL
        video_list_url: 视频列表接口完整URL
        video_detail_url: 视频详情接口完整URL
        login_url: 登录接口完整URL
        page_size: 每页返回的记录数量
        username: 登录用户名
        password: 登录密码
        domain: 登录域名
        token: 当前会话Token
        session: HTTP会话对象
        connection_timeout: 连接超时时间（秒）
        read_timeout: 读取超时时间（秒）
        verify_ssl: 是否验证SSL证书
        
    Example:
        >>> from core import ApiHandler, load_config
        >>> config = load_config()
        >>> api = ApiHandler(config=config)
        >>> token = api.login()
        >>> if token:
        ...     videos = api.fetch_video_page(page_number=1)
    """
    
    def __init__(self, config: ConfigParser) -> None:
        """
        初始化API处理器
        
        Args:
            config: 配置对象，包含API相关配置信息
            
        Raises:
            KeyError: 当配置中缺少必要的API配置项时抛出
        """
        # 读取API配置
        api_config: SectionProxy = config['api']
        
        # URL配置
        self.base_url: str = api_config.get('base_url', fallback='')
        self.video_list_url: str = self.base_url + api_config.get('video_list_endpoint', fallback='')
        self.video_detail_url: str = self.base_url + api_config.get('video_detail_endpoint', fallback='')
        self.login_url: str = self.base_url + api_config.get('login_endpoint', fallback='')
        
        # 分页配置
        self.page_size: int = api_config.getint('page_size', fallback=20)
        
        # 认证配置
        self.username: str = api_config.get('username', fallback='')
        self.password: str = api_config.get('password', fallback='')
        self.domain: str = api_config.get('domain', fallback='')
        self.token: str | None = None
        
        # 请求头配置
        self.referer: str = api_config.get('referer', fallback='')
        self.origin: str = api_config.get('origin', fallback='')
        self.user_agent: str = api_config.get('user_agent', fallback='Mozilla/5.0')
        
        # 超时配置
        self.connection_timeout: int = api_config.getint('connection_timeout', fallback=30)
        self.read_timeout: int = api_config.getint('read_timeout', fallback=300)
        
        # SSL配置
        self.verify_ssl: bool = api_config.getboolean('verify_ssl', fallback=True)
        
        # 初始化HTTP会话
        self.session: Session = self._setup_session()
        
        logger.info("API处理器初始化完成")
    
    def _setup_session(self) -> Session:
        """
        配置HTTP会话
        
        设置连接池、重试策略和基础请求头。
        
        Returns:
            Session: 配置好的requests会话对象
            
        Note:
            - 连接池大小: 20
            - 重试次数: 3次（手动控制）
            - 重试状态码: 429, 500, 502, 503, 504
        """
        session: Session = requests.Session()
        
        # 配置重试策略
        retry_strategy: Retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        # 配置HTTP适配器
        adapter: HTTPAdapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=retry_strategy
        )
        
        session.mount(prefix='http://', adapter=adapter)
        session.mount(prefix='https://', adapter=adapter)
        
        logger.debug("HTTP会话配置完成")
        return session
    
    def _default_headers(self, with_token: bool = False) -> dict[str, str]:
        """
        生成默认请求头
        
        Args:
            with_token: 是否在请求头中包含认证Token
            
        Returns:
            dict[str, str]: 请求头字典
        """
        headers: dict[str, str] = {
            'Content-Type': 'application/json'
        }
        
        # 添加可选请求头
        if self.user_agent:
            headers['User-Agent'] = self.user_agent
        if self.referer:
            headers['Referer'] = self.referer
        if self.origin:
            headers['Origin'] = self.origin
        if with_token and self.token:
            headers['zq-os-token'] = self.token
        
        return headers
    
    def set_token(self, token: str) -> None:
        """
        设置当前会话使用的API Token
        
        Args:
            token: API访问令牌
            
        Example:
            >>> api.set_token(token="your_token_here")
        """
        self.token = token
        logger.debug(f"Token已更新: {token[:20]}...")
    
    def login(self) -> str | None:
        """
        执行登录操作获取新Token
        
        使用配置中的用户名、密码和域名进行登录认证。
        
        Returns:
            str | None: 成功时返回新Token，失败时返回None
            
        Raises:
            requests.RequestException: 当网络请求失败时抛出
            
        Example:
            >>> token = api.login()
            >>> if token:
            ...     print("登录成功")
        """
        logger.info("开始登录以获取新Token...")
        
        # 构建登录请求载荷
        payload: dict[str, str] = {
            "user_name": self.username,
            "password": self.password,
            "domain": self.domain
        }
        
        headers: dict[str, str] = self._default_headers(with_token=False)
        
        try:
            response: requests.Response = self.session.post(
                url=self.login_url,
                data=json.dumps(payload),
                headers=headers,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=self.verify_ssl
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            
            # 解析响应数据
            if data.get('code') == 0 and 'data' in data and 'token' in data['data']:
                new_token: str = data['data']['token']
                self.set_token(token=new_token)
                logger.info("登录成功，Token已更新")
                return new_token
            else:
                error_msg: str = data.get('msg', '未知错误')
                logger.error(f"登录失败: {error_msg}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"登录请求失败: {e}")
            return None
    
    def fetch_video_page(self, page_number: int) -> dict[str, Any] | None:
        """
        根据页码获取视频列表
        
        Args:
            page_number: 要获取的页码（从1开始）
            
        Returns:
            dict[str, Any] | None: 成功时返回包含视频列表的字典，失败时返回None
            
        Response Structure:
            {
                "code": 0,
                "data": [
                    {
                        "id": "video_id",
                        "title": "视频标题",
                        "cover": "封面URL",
                        ...
                    }
                ]
            }
            
        Raises:
            requests.RequestException: 当网络请求失败时抛出
            
        Example:
            >>> result = api.fetch_video_page(page_number=1)
            >>> if result and result.get('code') == 0:
            ...     videos = result.get('data', [])
        """
        # 检查Token
        if not self.token:
            logger.error("Token未设置，无法获取视频列表")
            return {"code": 402, "data": []}
        
        # 构建请求载荷
        payload: dict[str, int] = {
            "page": page_number,
            "page_size": self.page_size
        }
        
        headers: dict[str, str] = self._default_headers(with_token=True)
        
        try:
            logger.info(f"正在请求第 {page_number} 页的视频列表...")
            
            response: requests.Response = self.session.post(
                url=self.video_list_url,
                data=json.dumps(payload),
                headers=headers,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=self.verify_ssl
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            
            # 处理响应
            if data.get('code') == 0:
                total_items: int = data.get('data', {}).get('total', 0)
                total_pages: int = math.ceil(total_items / self.page_size)
                logger.info(f"请求成功。总记录数: {total_items}, 总页数: {total_pages}")
                return {'code': 0, 'data': data['data']['list']}
            elif data.get('code') == 402:
                logger.warning("Token已过期")
                return {"code": 402, "data": []}
            else:
                error_msg: str = data.get('msg', '未知错误')
                logger.error(f"API返回错误: {error_msg}")
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"请求视频列表失败 (页码: {page_number}): {e}")
            return None
    
    def fetch_video_details(self, douban_id: str) -> dict[str, Any] | None:
        """
        根据douban_id获取视频详细信息
        
        Args:
            douban_id: 视频的豆瓣唯一标识
            
        Returns:
            dict[str, Any] | None: 成功时返回视频详情，失败时返回None
            
        Response Structure:
            {
                "code": 0,
                "data": {
                    "id": "video_id",
                    "title": "视频标题",
                    "video_list": ["m3u8_url1", "m3u8_url2"],
                    "cover": "封面URL",
                    "desc": "视频描述",
                    ...
                }
            }
            
        Raises:
            requests.RequestException: 当网络请求失败时抛出
            
        Example:
            >>> details = api.fetch_video_details(douban_id="12345")
            >>> if details and details.get('code') == 0:
            ...     video_data = details.get('data')
        """
        # 检查Token
        if not self.token:
            logger.error("Token未设置，无法获取视频详情")
            return {"code": 402, "data": []}
        
        # 构建请求载荷
        payload: dict[str, str] = {
            "id": douban_id,
            "lang_code": "en"
        }
        
        headers: dict[str, str] = self._default_headers(with_token=True)
        
        try:
            logger.info(f"正在获取视频详情 (douban_id: {douban_id})...")
            
            response: requests.Response = self.session.post(
                url=self.video_detail_url,
                data=json.dumps(payload),
                headers=headers,
                timeout=(self.connection_timeout, self.read_timeout),
                verify=self.verify_ssl
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()
            
            # 处理响应
            if data.get('code') == 0 and 'data' in data and data['data'].get('list'):
                logger.debug(f"视频详情获取成功 (douban_id: {douban_id})")
                return {'code': 0, 'data': data['data']['list'][0]}
            elif data.get('code') == 402:
                logger.warning("Token已过期")
                return {"code": 402, "data": []}
            else:
                error_msg: str = data.get('msg', '未知错误')
                logger.error(f"获取视频详情失败 (douban_id: {douban_id}): {error_msg}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"请求视频详情失败 (douban_id: {douban_id}): {e}")
            return None
    
    def close(self) -> None:
        """
        关闭HTTP会话，释放资源
        
        Example:
            >>> api.close()
        """
        if self.session:
            self.session.close()
            logger.debug("API处理器会话已关闭")
