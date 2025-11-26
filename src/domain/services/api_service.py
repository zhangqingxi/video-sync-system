"""
领域层 - API服务

处理第三方API交互。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import time
from typing import Any
from src.core.config import APIConfig, TimingConfig
from src.core.exceptions import APIError
from src.core.protocols import LoggerProvider
from src.core.state import StateManager
from src.infrastructure.http import HTTPClient
from src.domain.models import Video


class APIService:
    """API业务服务"""
    
    def __init__(
        self,
        http_client: HTTPClient,
        api_config: APIConfig,
        timing_config: TimingConfig,
        state_manager: StateManager,
        logger: LoggerProvider
    ) -> None:
        """
        初始化API服务
        
        Args:
            http_client: HTTP客户端
            api_config: API配置
            timing_config: 时间配置
            state_manager: 状态管理器
            logger: 日志提供者
        """
        self._http: HTTPClient = http_client
        self._api_config: APIConfig = api_config
        self._timing: TimingConfig = timing_config
        self._state: StateManager = state_manager
        self._logger: LoggerProvider = logger
    
    def fetch_video_list(self, page: int = 1) -> list[Video]:
        """
        获取视频列表（仅基础信息）
        
        Args:
            page: 页码
            
        Returns:
            list[Video]: 视频列表（仅包含基础信息）
        """
        try:
            self._logger.info(f"正在请求第 {page} 页的视频列表...")
            
            # 请求延迟
            time.sleep(self._timing.request_delay)
            
            # 请求 list 接口
            params: dict[str, Any] = {
                'page': page,
                'page_size': self._api_config.page_size
            }

            response: dict[str, Any] = self._http.post_with_auth(
                endpoint=self._api_config.endpoints.video_list,
                payload=params
            )

            # 解析列表响应
            data: dict[str, Any] = response.get('data', {})
            items: list[Any] = data.get('list', [])
            total: int = data.get('total', 0)
            
            if not items:
                self._logger.info(f"第 {page} 页无数据")
                return []
            
            self._logger.info(f"第 {page} 页获取到 {len(items)} 条基础数据（总记录数: {total}）")
            
            # 转换为Video对象
            videos: list[Video] = []
            for item in items:
                try:
                    video = Video.from_api_list(data=item)
                    videos.append(video)
                except Exception as e:
                    self._logger.error(f"解析视频数据失败: {e}")
                    continue
            
            return videos
        
        except Exception as e:
            self._logger.error(f"获取视频列表失败: {e}")
            raise APIError(f"Failed to fetch video list: {e}")
    
    def fetch_video_detail(self, video_id: str) -> dict[str, Any] | None:
        """
        获取视频详情
        
        Args:
            video_id: 视频ID（douban_id）
            
        Returns:
            dict: 视频详情，失败返回None
        """
        try:
            # 请求延迟
            time.sleep(self._timing.request_delay)
            
            # 发送请求（POST方式）
            params: dict[str, Any] = {
                'id': video_id,
            }
            
            response: dict[str, Any] = self._http.post_with_auth(
                endpoint=self._api_config.endpoints.video_detail,
                payload=params
            )
            
            # 检查响应（code=0 表示成功）
            if response.get('code') != 0:
                error_msg = response.get('msg', '未知错误')
                self._logger.warning(f"获取视频详情失败: {error_msg}")
                return None
            
            # 返回 data.list[0]
            data: dict[str, Any] = response.get('data', {})
            detail_list: list[dict[str, Any]] = data.get('list', [])
            
            if detail_list and len(detail_list) > 0:
                return detail_list[0]
            else:
                self._logger.warning(f"视频详情list为空: ID={video_id}")
                return None
        
        except Exception as e:
            self._logger.error(f"请求视频详情异常: ID={video_id}, Error={e}")
            return None
    
    def update_progress(self, page: int) -> None:
        """
        更新抓取进度
        
        Args:
            page: 当前页码
        """
        self._state.update_last_page(page=page)
        self._logger.debug(f"更新进度: page={page}")
