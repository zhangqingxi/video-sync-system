"""
应用层 - Site Fix命令

同步失败的视频数据到配置的站点。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.database import VideoRepositoryImpl
from src.domain.services import SiteService


class SiteFixCommand(BaseCommand):
    """站点同步修复命令"""
    
    @property
    def name(self) -> str:
        return "site_fix"
    
    @property
    def description(self) -> str:
        return "同步失败的视频数据到站点（基于state维护的失败记录）"
    
    def execute(self) -> int:
        """执行同步"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始站点数据修复同步")
            self.logger.info("=" * 60)
            
            # 获取依赖
            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(interface=VideoRepositoryImpl)
            
            # 获取站点配置
            site_config: dict[str, Any] = self.config.raw_config.get('site', {})
            api_token: str = site_config.get('api_token', '')
            sync_endpoint: str = site_config.get('endpoints', {}).get('sync', '/api/sync')
            
            # 从 state 获取失败ID（按域名分组）
            failed_ids_by_domain: dict[str, list[int]] = state_manager.get_site_failed_ids()
            
            if not failed_ids_by_domain:
                self.logger.info("没有需要修复的视频")
                return 0
            
            # 只处理 state 中维护的域名
            for domain, failed_ids in failed_ids_by_domain.items():
                if not failed_ids:
                    self.logger.info(f"\n[{domain}] 没有需要修复的视频")
                    continue
                
                self.logger.info(f"\n[{domain}] 开始修复 {len(failed_ids)} 个视频")
                
                # 从数据库获取视频数据
                videos_data: list[dict[str, Any]] = []
                for video_id in failed_ids:
                    video_data = video_repo.get_by_id(video_id=video_id)
                    if video_data:
                        videos_data.append(video_data)
                    else:
                        self.logger.warning(f"[{domain}] 未找到视频: ID={video_id}")
                
                if not videos_data:
                    self.logger.warning(f"[{domain}] 没有可用的视频数据")
                    continue
                
                self.logger.info(f"[{domain}] 从数据库获取到 {len(videos_data)} 条视频数据")
                
                site_service: SiteService = SiteService(
                    domains=[domain],  # 当前域名
                    api_token=api_token,
                    sync_endpoint=sync_endpoint,
                    logger=self.logger
                )
                
                # 批量同步到该域名
                failed_by_domain: dict[str, list[int]] = site_service.sync_to_sites(videos=videos_data)
                
                # 更新失败列表
                new_failed_ids: list[int] = failed_by_domain.get(domain, [])
                
                # 先清空该域名的旧失败列表
                state_manager.clear_site_failed_ids_for_domain(domain=domain)
                
                # 添加新的失败ID
                for video_id in new_failed_ids:
                    state_manager.add_site_failed_id(domain=domain, video_id=video_id)
                
                success_count: int = len(videos_data) - len(new_failed_ids)
                self.logger.info(f"[{domain}] 修复完成: 成功={success_count}, 仍失败={len(new_failed_ids)}")
            
            self.logger.info("=" * 60)
            self.logger.info("所有域名修复完成")
            self.logger.info("=" * 60)
            
            return 0
        
        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1
