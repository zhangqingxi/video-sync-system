"""
站点数据同步处理器

负责与目标站点进行数据同步、清理等操作，提供统一的站点交互接口。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import logging
from configparser import ConfigParser
from typing import Any
from urllib.parse import urljoin

import requests
import json
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3.util.retry import Retry

# ============================================================================
# 模块级日志记录器
# ============================================================================

logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# 站点处理器类
# ============================================================================

class SiteHandler:
    """
    站点数据同步处理器
    
    提供视频数据同步到站点、站点数据清理等功能，支持多域名配置和会话管理。
    
    Main Features:
        - 批量视频数据同步
        - 多域名支持
        - 站点数据清理
        - 自动重试机制
        - 失败记录追踪
    
    Attributes:
        api_token: API认证令牌
        sync_endpoint: 同步接口端点
        clean_endpoint: 清理接口端点
        request_timeout: 请求超时时间（秒）
        domains: 目标站点域名列表
        session: HTTP会话对象
        
    Example:
        >>> from core import SiteHandler, load_config
        >>> config = load_config()
        >>> site = SiteHandler(config=config)
        >>> result = site.sync_videos_to_site(videos=[...])
        >>> site.close()
    """
    
    def __init__(self, config: ConfigParser) -> None:
        """
        初始化站点处理器
        
        Args:
            config: 配置对象，需包含站点域名、认证信息等
            
        Raises:
            KeyError: 当配置中缺少必要的站点配置项时抛出
        """
        # 读取配置
        self.api_token: str = config.get('site', 'api_token')
        self.sync_endpoint: str = config.get('site', 'sync_endpoint', fallback='/api/sync')
        self.clean_endpoint: str = config.get('site', 'clean_endpoint', fallback='/api/clean')
        self.request_timeout: int = config.getint('site', 'timeout', fallback=30)
        
        # 解析域名列表
        domains_str: str = config.get('site', 'domains', fallback='')
        self.domains: list[str] = [
            domain.strip()
            for domain in domains_str.split(',')
            if domain.strip()
        ]
        
        # 初始化HTTP会话
        self.session: Session = self._init_session()
        
        logger.info(f"站点处理器初始化完成，目标域名数量: {len(self.domains)}")
    
    def _init_session(self) -> Session:
        """
        初始化HTTP会话
        
        配置连接池、重试策略和认证头。
        
        Returns:
            Session: 配置好的requests会话对象
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
            pool_connections=10,
            pool_maxsize=10,
            max_retries=retry_strategy
        )
        
        session.mount(prefix='http://', adapter=adapter)
        session.mount(prefix='https://', adapter=adapter)
        
        # 设置默认请求头
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.api_token}"
        })
        
        logger.debug("HTTP会话配置完成")
        return session
    
    def _extract_video_ids(self, batch_videos: list[dict[str, Any]]) -> list[str]:
        """
        从视频数据中提取ID列表
        
        安全地处理可能的键缺失情况，确保返回有效ID列表。
        
        Args:
            batch_videos: 视频数据列表，每个元素为包含vod_douban_id的字典
            
        Returns:
            list[str]: 提取的视频ID列表（字符串类型）
            
        Example:
            >>> videos = [{"vod_douban_id": 123}, {"vod_douban_id": 456}]
            >>> ids = handler._extract_video_ids(batch_videos=videos)
            >>> print(ids)  # ['123', '456']
        """
        video_ids: list[str] = []
        
        for video in batch_videos:
            # 确保ID存在且转换为字符串
            video_id: Any = video.get('vod_douban_id')
            if video_id is not None:
                video_ids.append(str(video_id))
        
        logger.debug(f"提取视频ID数量: {len(video_ids)}")
        return video_ids
    
    def sync_videos_to_site(
        self,
        videos: list[dict[str, Any]],
        domain: str | None = None
    ) -> dict[str, set[str]]:
        """
        同步视频数据到目标站点
        
        支持单个域名或配置中的所有域名同步，返回按域名分组的同步失败记录。
        
        Args:
            videos: 待同步的视频数据列表
            domain: 可选，指定单个域名进行同步；为None时同步到所有配置的域名
            
        Returns:
            dict[str, set[str]]: 按域名分组的同步失败ID集合
            
        Example:
            >>> videos = [{"vod_douban_id": 123, "vod_name": "测试视频"}]
            >>> failed = site.sync_videos_to_site(videos=videos)
            >>> for domain, failed_ids in failed.items():
            ...     print(f"{domain}: {len(failed_ids)} 个失败")
        """
        failed: dict[str, set[str]] = {}
        target_domains: list[str] = [domain] if domain else self.domains
        
        if not target_domains:
            logger.warning("未配置目标站点域名，同步终止")
            return failed
        
        # 提取所有视频ID用于失败记录
        all_video_ids: list[str] = self._extract_video_ids(batch_videos=videos)
        video_id_set: set[str] = set(all_video_ids)
        
        # 遍历每个域名进行同步
        for target_domain in target_domains:
            try:
                sync_url: str = urljoin(base=target_domain, url=self.sync_endpoint)
                logger.info(f"开始同步 {len(videos)} 条数据到 {target_domain}")
                
                response: requests.Response = self.session.post(
                    url=sync_url,
                    json={"videos_data": json.dumps(videos, default=str)},
                    timeout=self.request_timeout,
                    verify=False  # 注意：生产环境建议改为True并配置CA证书
                )
                
                response.raise_for_status()
                
                # API直接返回失败的视频ID列表
                failed_ids: list = response.json()
                
                if failed_ids:
                    logger.warning(f"{target_domain} 同步部分失败，失败数量: {len(failed_ids)}")
                    # 将列表转换为字符串集合
                    failed[target_domain] = set(str(vid) for vid in failed_ids)
                else:
                    logger.info(f"{target_domain} 同步成功，处理 {len(videos)} 条数据")
                    failed[target_domain] = set()  # 成功则失败集合为空
                
            except requests.exceptions.RequestException as e:
                logger.error(f"{target_domain} 同步请求失败: {str(e)}")
                failed[target_domain] = video_id_set.copy()  # 请求失败记录所有ID
            except Exception as e:
                logger.error(f"{target_domain} 同步异常: {str(e)}")
                failed[target_domain] = video_id_set.copy()  # 其他异常记录所有ID
        
        return failed
    
    def clean_to_site(self) -> dict[str, bool]:
        """
        清理站点数据
        
        对配置中的所有域名执行数据清理操作。
        
        Returns:
            dict[str, bool]: 按域名分组的清理结果（True为成功，False为失败）
            
        Example:
            >>> results = site.clean_to_site()
            >>> for domain, success in results.items():
            ...     status = "成功" if success else "失败"
            ...     print(f"{domain}: 清理{status}")
        """
        clean_results: dict[str, bool] = {}
        
        if not self.domains:
            logger.warning("未配置站点域名，跳过站点清理")
            return clean_results
        
        # 遍历每个域名执行清理
        for domain in self.domains:
            try:
                clean_url: str = urljoin(base=domain, url=self.clean_endpoint)
                logger.info(f"开始清理 {domain} 站点数据")
                
                response: requests.Response = self.session.post(
                    url=clean_url,
                    json={},
                    timeout=self.request_timeout,
                    verify=False  # 注意：生产环境建议改为True
                )
                
                if response.status_code == 200:
                    logger.info(f"{domain} 数据清理成功")
                    clean_results[domain] = True
                else:
                    logger.error(f"{domain} 数据清理失败，状态码: {response.status_code}")
                    clean_results[domain] = False
                    
            except Exception as e:
                logger.error(f"{domain} 清理操作异常: {str(e)}")
                clean_results[domain] = False
        
        return clean_results
    
    def close(self) -> None:
        """
        关闭HTTP会话释放资源
        
        Example:
            >>> site.close()
        """
        if self.session:
            self.session.close()
            logger.debug("站点处理器会话已关闭")