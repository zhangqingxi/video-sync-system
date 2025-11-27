"""
应用层 - 视频标签检查命令

检查视频是否有标签数据。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.database import (
    VideoRepositoryImpl,
    TagRepositoryImpl,
    DatabasePool,
)


class VideoTagCheckCommand(BaseCommand):
    """视频标签检查命令"""

    @property
    def name(self) -> str:
        return "video_tag_check"

    @property
    def description(self) -> str:
        return "检查视频标签数据是否存在"

    def execute(self) -> int:
        """执行检查"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始视频标签检查")
            self.logger.info("=" * 60)

            # 获取依赖
            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(
                interface=VideoRepositoryImpl
            )
            db_pool: DatabasePool = self.container.resolve(interface=DatabasePool)

            # 创建标签仓储
            tag_repo: TagRepositoryImpl = TagRepositoryImpl(
                db_pool=db_pool, logger=self.logger
            )

            # 获取上次检查的ID
            last_checked_id: int = state_manager.get_last_checked_video_tag()
            self.logger.info(f"上次检查ID: {last_checked_id}")

            # 获取待检查的视频列表
            videos: list[dict[str, Any]] = video_repo.get_videos_after_id(
                last_id=last_checked_id, limit=10000
            )
            if not videos:
                self.logger.info("没有需要检查的视频")
                return 0

            self.logger.info(f"待检查视频数量: {len(videos)}")

            # 使用线程池并发检查
            thread_count: int = self.config.threads.check_threads
            failed_count: int = 0
            checked_count: int = 0
            video_id: int

            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures: dict[Future[bool], int] = {}
                for video_data in videos:
                    video_id = video_data["vod_douban_id"]
                    future: Future[bool] = executor.submit(
                        self._check_video, tag_repo, video_id
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        has_tags: bool = future.result()
                        checked_count += 1

                        if not has_tags:
                            failed_count += 1
                            state_manager.add_failed_video_tag_id(video_id=video_id)
                            self.logger.warning(f"标签缺失: ID={video_id}")
                        else:
                            self.logger.info(f"标签存在: ID={video_id}")

                        if checked_count % 10 == 0:
                            self.logger.info(f"检查进度: {checked_count}/{len(videos)}")

                    except Exception as e:
                        self.logger.error(f"检查失败: ID={video_id}, Error={e}")

            # 批次完成后，更新为这批视频的最大ID
            if videos:
                max_id: int = max(video_data["vod_douban_id"] for video_data in videos)
                state_manager.update_last_checked_video_tag(video_id=max_id)
                self.logger.info(f"更新检查进度: last_id={max_id}")

            self.logger.info("=" * 60)
            self.logger.info(f"检查完成: 总数={checked_count}, 标签缺失={failed_count}")
            self.logger.info("=" * 60)

            return 0

        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1

    def _check_video(self, tag_repo: TagRepositoryImpl, video_id: int) -> bool:
        """
        检查单个视频的标签

        Args:
            tag_repo: 标签仓储
            video_id: 视频ID

        Returns:
            bool: 是否有标签
        """
        try:
            tag_count: int = tag_repo.get_video_tag_count(douban_id=video_id)
            return tag_count > 0
        except Exception as e:
            self.logger.error(f"检查视频标签失败: ID={video_id}, Error={e}")
            return False
