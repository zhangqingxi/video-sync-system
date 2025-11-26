"""
领域层 - 站点服务

处理站点同步逻辑。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import json
import requests
from urllib.parse import urljoin
from typing import Any
from src.core.protocols import LoggerProvider

class SiteService:
    """站点同步服务"""
    
    def __init__(
        self,
        domains: list[str],
        api_token: str,
        sync_endpoint: str,
        logger: LoggerProvider
    ):
        """
        初始化站点服务
        
        Args:
            domains: 站点域名列表
            api_token: API认证Token
            sync_endpoint: 同步端点
            logger: 日志提供者
        """
        self.domains: list[str] = domains
        self.api_token: str = api_token
        self.sync_endpoint: str = sync_endpoint
        self.logger: LoggerProvider = logger
    
    def sync_to_sites(self, videos: list[dict[str, Any]]) -> dict[str, list[int]]:
        """
        同步视频数据到所有配置的站点
        
        Args:
            videos: 视频数据列表
            
        Returns:
            dict[str, list[int]]: 每个域名的失败ID列表 {domain: [failed_ids]}
        """
        if not videos:
            return {}
        
        all_video_ids: set[int] = set(v.get('vod_douban_id') for v in videos)
        failed_by_domain: dict[str, list[int]] = {}
        
        for domain in self.domains:
            try:
                sync_url: str = urljoin(base=domain, url=self.sync_endpoint)
                self.logger.info(f"开始同步 {len(videos)} 条数据到 {domain}")
                
                response: requests.Response = requests.post(
                    url=sync_url,
                    json={"videos_data": json.dumps(videos, default=str)},
                    headers={
                        'Authorization': f'Bearer {self.api_token}',
                        'Content-Type': 'application/json'
                    },
                    timeout=30,
                    verify=False
                )
                
                response.raise_for_status()
                
                failed_ids: list = response.json()
                
                if failed_ids:
                    self.logger.warning(f"{domain} 同步部分失败，失败数量: {len(failed_ids)}")
                    failed_by_domain[domain] = [int(video_id) for video_id in failed_ids]
                else:
                    self.logger.info(f"{domain} 同步成功，处理 {len(videos)} 条数据")
                    failed_by_domain[domain] = []
                    
            except Exception as e:
                self.logger.error(f"{domain} 同步请求失败: {e}")
                # 请求失败，所有ID都标记为失败
                failed_by_domain[domain] = list(all_video_ids)
        
        return failed_by_domain
