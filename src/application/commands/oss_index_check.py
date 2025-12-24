"""
应用层 - OSS Index检查命令

检查OSS上的index m3u8资源。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import re
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.database import VideoRepositoryImpl
from src.infrastructure.storage import StorageFactory
from src.core.protocols import StorageProvider


class OSSIndexCheckCommand(BaseCommand):
    """OSS Index资源检查命令"""

    @property
    def name(self) -> str:
        return "oss_index_check"

    @property
    def description(self) -> str:
        return "检查OSS上的index m3u8资源是否存在"

    def execute(self) -> int:
        """执行检查"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始OSS Index资源检查")
            self.logger.info("=" * 60)

            # 获取依赖
            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(
                interface=VideoRepositoryImpl
            )

            # 创建OSS存储适配器
            oss_adapter: StorageProvider = StorageFactory.create_oss(
                config=self.config.oss_storage,
                constants=self.config.constants,
                logger=self.logger,
            )

            # 获取上次检查的ID
            last_checked_id: int = state_manager.get_last_checked_oss_index()
            self.logger.info(f"上次检查ID: {last_checked_id}")

            # 获取待检查的视频列表
            videos: list[dict[str, Any]] = video_repo.get_videos_after_id(
                last_id=last_checked_id, limit=100
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
                futures: dict[Future[dict[int, list[int]]], int] = {}
                for video_data in videos:
                    video_id = video_data.get('vod_douban_id', 0)
                    video_episodes: int = video_data.get('vod_total', 0)
                    video_origin: str = 'type_' + str(video_data.get('type_id', 0))

                    future: Future[dict[int, list[int]]] = executor.submit(
                        self._check_video_index, oss_adapter, video_id, video_episodes, video_origin
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        failed_episodes: dict[int, list[int]] = future.result()
                        checked_count += 1
                        if failed_episodes:
                            # 有失败的集数，添加到状态
                            for vid, episodes in failed_episodes.items():
                                failed_count += 1
                                state_manager.add_oss_failed_index_episodes(
                                    video_id=vid, episodes=episodes
                                )
                                self.logger.warning(
                                    f"Index资源不存在: ID={vid}, 集数={episodes}"
                                )
                        else:
                            self.logger.info(f"Index资源存在: ID={video_id}")

                        if checked_count % 10 == 0:
                            self.logger.info(f"检查进度: {checked_count}/{len(videos)}")

                    except Exception as e:
                        self.logger.error(f"检查失败: ID={video_id}, Error={e}")

            # 批次完成后，更新为这批视频的最大ID
            if videos:
                max_id: int = max(video_data["vod_douban_id"] for video_data in videos)
                state_manager.update_last_checked_oss_index(video_id=max_id)
                self.logger.info(f"更新检查进度: last_id={max_id}")

            self.logger.info("=" * 60)
            self.logger.info(f"检查完成: 总数={checked_count}, 失败={failed_count}")
            self.logger.info("=" * 60)

            return 0

        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1

    def _check_video_index(
        self, oss_adapter: StorageProvider, video_id: int, video_episodes: int, video_origin: str
    ) -> dict[int, list[int]]:
        """
        检查单个视频的index资源（主入口）

        Args:
            oss_adapter: OSS适配器
            video_id: 视频ID
            video_episodes: 视频集数
            video_origin: 视频来源

        Returns:
            dict[int, list[int]]: 失败的视频ID和对应的集数列表
        """
        try:
            failed_episodes: list[int] = []

            # 检查每一个剧集
            for episode in range(1, video_episodes + 1):
                if not self._check_video(oss_adapter=oss_adapter, video_id=video_id, video_episode=episode, video_origin=video_origin):
                    failed_episodes.append(episode)

            # 如果有失败的集数，返回 {video_id: [failed_episodes]}
            if failed_episodes:
                return {video_id: failed_episodes}
            return {}

        except Exception as e:
            self.logger.error(f"检查视频Index失败: ID={video_id}, Error={e}")
            # 异常时，将所有集数标记为失败
            return {video_id: list(range(1, video_episodes + 1))}

    def _check_video(
        self, oss_adapter: StorageProvider, video_id: int, video_episode: int, video_origin: str
    ) -> bool:
        """
        检查单个视频的单集index资源

        Args:
            oss_adapter: OSS适配器
            video_id: 视频ID
            video_episode: 集数
            video_origin: 视频来源

        Returns:
            bool: 资源是否存在
        """
        try:
            # 1. 检查index.m3u8文件
            index_key: str = oss_adapter.generate_key(
                resource_id=video_id,
                resource_origin=video_origin,
                resource_filename="index.m3u8",
                resource_episode=video_episode,
            )

            if not oss_adapter.check_exists(key=index_key):
                self.logger.warning(
                    f"Index文件不存在: ID={video_id}, EP={video_episode}"
                )
                return False

            # 2. 获取index.m3u8内容并检查TS文件
            try:
                # 下载并解析index.m3u8
                content: bytes | None = oss_adapter.download_file(key=index_key)
                if not content:
                    self.logger.warning(
                        f"Index文件内容为空: ID={video_id}, EP={video_episode}"
                    )
                    return False

                m3u8_content: str = content.decode("utf-8")
                
                # 解析TS文件名（应该是相对路径，如 0001.ts, 0002.ts）
                ts_files: list[str] = re.findall(r"(\d{4}\.ts)", m3u8_content)
                if not ts_files:
                    self.logger.warning(
                        f"未找到TS文件: ID={video_id}, EP={video_episode}"
                    )
                    return False

                # 3. 检查每个TS文件
                for ts_filename in ts_files:
                    ts_key: str = oss_adapter.generate_key(
                        resource_id=video_id,
                        resource_origin=video_origin,
                        resource_filename=ts_filename,
                        resource_episode=video_episode,
                    )

                    if not oss_adapter.check_exists(key=ts_key):
                        self.logger.warning(
                            f"TS文件不存在: ID={video_id}, EP={video_episode}, TS={ts_filename}"
                        )
                        return False

                return True

            except Exception as e:
                self.logger.error(
                    f"检查TS文件失败: ID={video_id}, EP={video_episode}, Error={e}"
                )
                return False

        except Exception as e:
            self.logger.error(
                f"检查单集失败: ID={video_id}, EP={video_episode}, Error={e}"
            )
            return False
