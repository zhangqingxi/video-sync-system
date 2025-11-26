"""
应用层 - S3 Cover检查命令

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.database import VideoRepositoryImpl
from src.infrastructure.storage import StorageFactory
from src.core.protocols import StorageProvider


class S3CoverCheckCommand(BaseCommand):
    """S3 Cover资源检查命令"""
    
    @property
    def name(self) -> str:
        return "s3_cover_check"
    
    @property
    def description(self) -> str:
        return "检查S3上的cover图片资源"
    
    def execute(self) -> int:
        """执行检查"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始S3 Cover资源检查")
            self.logger.info("=" * 60)
            
            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(interface=VideoRepositoryImpl)
            
            s3_adapter: StorageProvider = StorageFactory.create_s3(
                config=self.config.s3_storage,
                constants=self.config.constants,
                logger=self.logger
            )
            
            last_checked_id: int = state_manager.get_last_checked_s3_cover()
            self.logger.info(f"上次检查ID: {last_checked_id}")
            
            videos: list[dict[str, Any]] = video_repo.get_videos_after_id(last_id=last_checked_id, limit=100)
            if not videos:
                self.logger.info("没有需要检查的视频")
                return 0
            
            self.logger.info(f"待检查视频数量: {len(videos)}")
            
            thread_count: int = self.config.threads.check_threads
            failed_count: int = 0
            checked_count: int = 0
            video_id: int
            
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures: dict[Future[bool], int] = {}
                for video_data in videos:
                    video_id = video_data['vod_douban_id']
                    future: Future[bool] = executor.submit(self._check_video, s3_adapter, video_id)
                    futures[future] = video_id
                
                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        exists: bool = future.result()
                        checked_count += 1
                        
                        if not exists:
                            failed_count += 1
                            state_manager.add_s3_failed_cover_id(video_id=video_id)
                            self.logger.warning(f"Cover不存在: ID={video_id}")
                        else:
                            self.logger.info(f"Cover存在: ID={video_id}")
                        
                        if checked_count % 10 == 0:
                            self.logger.info(f"检查进度: {checked_count}/{len(videos)}")

                    except Exception as e:
                        self.logger.error(f"检查失败: ID={video_id}, Error={e}")
            
            if videos:
                max_id: int = max(video_data['vod_douban_id'] for video_data in videos)
                state_manager.update_last_checked_s3_cover(video_id=max_id)
                self.logger.info(f"更新检查进度: last_id={max_id}")
            
            self.logger.info("=" * 60)
            self.logger.info(f"检查完成: 总数={checked_count}, 失败={failed_count}")
            self.logger.info("=" * 60)
            
            return 0
        
        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1
    
    def _check_video(self, s3_adapter: StorageProvider, video_id: int) -> bool:
        """
        检查单个视频的cover资源
        
        Args:
            s3_adapter: S3适配器
            video_id: 视频ID
            
        Returns:
            bool: 资源是否存在
        """
        try:
            cover_key: str = s3_adapter.generate_key(
                resource_id=video_id,
                resource_origin='type_16',
                resource_filename='cover.jpg',
            )
            
            return s3_adapter.check_exists(key=cover_key)
        except Exception as e:
            self.logger.error(f"检查视频失败: ID={video_id}, Error={e}")
            return False
