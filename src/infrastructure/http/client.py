"""
基础设施层 - HTTP客户端

提供带重试机制的HTTP客户端。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from src.core.config import APIConfig
from src.core.exceptions import APIError
from src.core.protocols import LoggerProvider
from src.core.state import StateManager


class HTTPClient:
    """HTTP客户端（带重试）"""

    def __init__(
        self, api_config: APIConfig, state_manager: StateManager, logger: LoggerProvider
    ) -> None:
        """
        初始化HTTP客户端

        Args:
            api_config: API配置
            state_manager: 状态管理器
            logger: 日志提供者
        """
        self.config: APIConfig = api_config
        self.state: StateManager = state_manager
        self.logger: LoggerProvider = logger
        self._session: requests.Session = self._create_session()

        # 从 state 中读取 token
        self._token: str | None = self.state.api_token or None
        if self._token:
            self.logger.info("使用缓存的API Token")

    def _create_session(self) -> requests.Session:
        """创建带重试配置的Session"""
        session: requests.Session = requests.Session()
        # 配置重试策略
        retry_strategy: Retry = Retry(
            total=self.config.retry.max_attempts,
            backoff_factor=self.config.retry.backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"],
        )

        adapter: HTTPAdapter = HTTPAdapter(
            pool_connections=20, pool_maxsize=20, max_retries=retry_strategy
        )
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _default_headers(self, with_token: bool = False) -> dict[str, str]:
        """
        生成默认请求头

        Args:
            with_token: 是否在请求头中包含认证Token

        Returns:
            dict[str, str]: 请求头字典
        """
        headers: dict[str, str] = {"Content-Type": "application/json"}

        # 添加可选请求头
        if self.config.headers.user_agent:
            headers["User-Agent"] = self.config.headers.user_agent
        if self.config.headers.referer:
            headers["Referer"] = self.config.headers.referer
        if self.config.headers.origin:
            headers["Origin"] = self.config.headers.origin
        if with_token and self._token:
            headers["zq-os-token"] = self._token

        return headers

    def login(self) -> str:
        """
        执行登录并获取Token

        Returns:
            str: API Token

        Raises:
            APIError: 登录失败时抛出
        """
        self.logger.info("开始登录以获取新Token...")

        payload: dict[str, str] = {
            "user_name": self.config.username,
            "password": self.config.password,
            "domain": self.config.domain,
        }

        headers: dict[str, str] = self._default_headers(with_token=False)
        url: str = f"{self.config.base_url}{self.config.endpoints.login}"

        try:
            response: requests.Response = self._session.post(
                url=url,
                json=payload,
                headers=headers,
                timeout=self.config.timeout,
            )
            response.raise_for_status()
            data: dict[str, Any] = response.json()

            # 解析响应数据（code=0 表示成功）
            if data.get("code") == 0 and "data" in data and "token" in data["data"]:
                self._token = data["data"]["token"]
                # 保存 token 到 state
                self.state.update_api_token(self._token)
                self.logger.info("登录成功，Token已更新并保存到状态文件")
                return self._token
            else:
                error_msg: str = data.get("msg", "未知错误")
                self.logger.error(f"登录失败: {error_msg}")
                raise APIError(f"Login failed: {error_msg}")

        except requests.RequestException as e:
            self.logger.error(f"登录请求失败: {e}")
            raise APIError(f"Login request failed: {e}")

    def post_with_auth(
        self, endpoint: str, payload: dict | None = None
    ) -> dict[str, Any]:
        """
        发送带Token认证的POST请求

        Args:
            endpoint: API端点
            payload: 请求数据

        Returns:
            dict: JSON响应

        Raises:
            APIError: 请求失败时抛出
            TokenExpiredError: Token过期时抛出
        """
        # 检查并获取Token
        if not self._token:
            self.logger.info("Token未设置，开始登录...")
            self.login()

        headers: dict[str, str] = self._default_headers(with_token=True)
        url: str = f"{self.config.base_url}{endpoint}"

        try:
            response: requests.Response = self._session.post(
                url=url,
                json=payload,
                headers=headers,
                timeout=self.config.timeout,
            )
            response.raise_for_status()
            result: dict[str, Any] = response.json()

            # 检查Token是否过期（code=402）
            if result.get("code") == 402:
                self.logger.warning("Token已过期，重新登录...")
                self.login()
                # 重试请求
                return self.post_with_auth(endpoint=endpoint, payload=payload)

            # 检查响应是否成功（code=0）
            if result.get("code") == 0:
                return result
            else:
                error_msg: str = result.get("msg", "未知错误")
                self.logger.error(f"API返回错误: {error_msg}")
                raise APIError(f"API error: {error_msg}")

        except requests.RequestException as e:
            self.logger.error(f"认证POST请求失败 ({url}): {e}")
            raise APIError(f"Authenticated POST request failed: {e}")

    def close(self) -> None:
        """关闭Session"""
        if self._session:
            self._session.close()
            self.logger.warning("API处理器会话已关闭")
