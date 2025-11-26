"""
应用层 - 视频标签修复命令

修复缺失的视频标签数据。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.database import VideoRepositoryImpl, TagRepositoryImpl, DatabasePool
from src.infrastructure.http import HTTPClient
from src.domain.services import APIService


class VideoTagFixCommand(BaseCommand):
    """视频标签修复命令"""
    
    @property
    def name(self) -> str:
        return "video_tag_fix"
    
    @property
    def description(self) -> str:
        return "修复缺失的视频标签数据"
    
    def execute(self) -> int:
        """执行修复"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始视频标签修复")
            self.logger.info("=" * 60)
            
            # 获取依赖
            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(interface=VideoRepositoryImpl)
            db_pool = self.container.resolve(interface=DatabasePool)
            
            # 创建标签仓储
            tag_repo: TagRepositoryImpl = TagRepositoryImpl(
                db_pool=db_pool,
                logger=self.logger
            )
            
            # 初始化HTTP客户端和API服务
            http_client: HTTPClient = HTTPClient(
                api_config=self.config.api,
                state_manager=state_manager,
                logger=self.logger
            )
            
            api_service: APIService = APIService(
                http_client=http_client,
                api_config=self.config.api,
                timing_config=self.config.timing,
                state_manager=state_manager,
                logger=self.logger
            )
            
            # 获取失败ID列表
            failed_ids: list[int] = state_manager.get_failed_video_tag_ids()
            
            if not failed_ids:
                self.logger.info("没有需要修复的视频")
                http_client.close()
                return 0
            
            self.logger.info(f"待修复视频数量: {len(failed_ids)}")
            
            # 使用线程池并发修复
            thread_count: int = self.config.threads.sync_threads
            success_count: int = 0
            failed_count: int = 0
            video_id: int
            
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures: dict[Future[bool], int] = {}
                for video_id in failed_ids:
                    future: Future[bool] = executor.submit(
                        self._fix_video,
                        video_id=video_id,
                        api_service=api_service,
                        tag_repo=tag_repo,
                        state_manager=state_manager
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        success: bool = future.result()
                        
                        if success:
                            success_count += 1
                            state_manager.remove_failed_video_tag_id(video_id=video_id)
                            self.logger.info(f"修复成功: ID={video_id}")
                        else:
                            failed_count += 1
                            self.logger.warning(f"修复失败: ID={video_id}")
                        
                        if (success_count + failed_count) % 10 == 0:
                            total: int = success_count + failed_count
                            self.logger.info(f"修复进度: {total}/{len(failed_ids)}")
                    
                    except Exception as e:
                        self.logger.error(f"处理失败: ID={video_id}, Error={e}")
                        failed_count += 1
            
            self.logger.info("=" * 60)
            self.logger.info(f"修复完成: 成功={success_count}, 失败={failed_count}")
            self.logger.info("=" * 60)
            
            http_client.close()
            return 0
        
        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1
    
    def _fix_video(
        self,
        video_id: int,
        api_service: APIService,
        tag_repo: TagRepositoryImpl,
        state_manager: StateManager
    ) -> bool:
        """
        修复单个视频的标签
        
        Args:
            video_id: 视频ID
            api_service: API服务
            tag_repo: 标签仓储
            state_manager: 状态管理器
            
        Returns:
            bool: 是否成功
        """
        try:
            # 步骤1：从API获取视频详情（包含tags）
            detail_data: dict[str, Any] | None = api_service.fetch_video_detail(video_id=str(video_id))
            
            if not detail_data:
                self.logger.warning(f"无法获取视频详情: ID={video_id}")
                return False
            
            tags: list[str] = detail_data.get('tags', [])
            
            if not tags:
                self.logger.warning(f"视频无标签数据: ID={video_id}")
                return True
            
            # 步骤2：更新本地数据库标签
            # 先删除旧标签
            tag_repo.delete_video_tags(douban_id=video_id)
            
            # 插入新标签
            for tag_name in tags:
                tag_id: int = tag_repo.ensure_tag_exists(tag_name=tag_name)
                tag_repo.insert_video_tag(
                    douban_id=video_id,
                    tag_id=tag_id,
                    type_id=16
                )
            
            self.logger.info(f"已更新标签: ID={video_id}, 标签数={len(tags)}")
            
            # 步骤3：将视频ID添加到所有域名的 site_failed_synced_ids
            domains: list[str] = self.config.site_domains
            if domains:
                for domain in domains:
                    state_manager.add_site_failed_id(domain=domain, video_id=video_id)
                self.logger.info(f"已添加到站点同步队列: ID={video_id}, 域名数={len(domains)}")
            else:
                self.logger.warning(f"未配置站点域名，跳过添加到同步队列: ID={video_id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"修复视频标签失败: ID={video_id}, Error={e}")
            return False
