"""
应用层 - OSS Index修复命令

修复OSS上的index m3u8资源（下载origin.m3u8 -> 解析TS -> 上传TS -> 生成index.m3u8）。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import re
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.storage import StorageFactory
from src.infrastructure.database import VideoRepositoryImpl
from src.core.protocols import StorageProvider


class OSSIndexFixCommand(BaseCommand):
    """OSS index资源修复命令"""

    @property
    def name(self) -> str:
        return "oss_index_fix"

    @property
    def description(self) -> str:
        return "修复OSS index m3u8资源 (Origin -> TS -> Index)"

    def execute(self) -> int:
        """执行修复"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始OSS index资源修复")
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

            # 获取失败ID列表
            failed_data: dict[str, list[int]] = state_manager.get_oss_failed_index_episodes()
            if not failed_data:
                self.logger.info("没有需要修复的资源")
                return 0

            # 获取失败的视频ID列表
            video_ids: list = list(failed_data.keys())
            videos: list[dict[str, Any]] = video_repo.get_videos_batch(video_ids=video_ids)
            self.logger.info(f"待修复视频数量: {len(videos)}")

            # 使用线程池并发修复
            thread_count: int = self.config.threads.sync_threads
            success_count: int = 0
            failed_count: int = 0
            video_id: int

            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures: dict[Future[dict[int, list[int]]], int] = {}
                for video_data in videos:
                    video_id = video_data.get('vod_douban_id', 0)
                    video_episodes: list[int] = failed_data.get(str(video_id), [])
                    video_origin: str = 'type_' + str(video_data.get('type_id', 0))
                    future: Future[dict[int, list[int]]] = executor.submit(
                        self._fix_video_index, video_id, video_episodes, video_origin, oss_adapter
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        result: dict[int, list[int]] = future.result()
                        
                        if not result:
                            # 所有集数都修复成功，移除该视频ID
                            success_count += 1
                            state_manager.remove_oss_failed_index_episodes(video_id=video_id)
                            self.logger.info(f"修复成功: ID={video_id}")
                        else:
                            # 还有失败的集数，更新失败列表
                            failed_count += 1
                            for vid, episodes in result.items():
                                state_manager.remove_oss_failed_index_episodes(
                                    video_id=vid, episodes=episodes
                                )
                                self.logger.warning(
                                    f"修复部分失败: ID={vid}, 失败集数={episodes}"
                                )

                        if (success_count + failed_count) % 10 == 0:
                            total: int = success_count + failed_count
                            self.logger.info(f"修复进度: {total}/{len(failed_data)}")

                    except Exception as e:
                        self.logger.error(f"处理失败: ID={video_id}, Error={e}")
                        failed_count += 1

            self.logger.info("=" * 60)
            self.logger.info(f"修复完成: 成功={success_count}, 失败={failed_count}")
            self.logger.info("=" * 60)

            return 0
        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1

    def _fix_video_index(
        self, video_id: int, video_episodes: list[int], video_origin: str, oss_adapter: StorageProvider
    ) -> dict[int, list[int]]:
        """
        修复单个视频的index资源

        Args:
            video_id: 视频ID
            video_episodes: 需要修复的集数列表
            video_origin: 视频原始文件夹
            oss_adapter: OSS适配器

        Returns:
            dict[int, list[int]]: 仍然失败的集数 {video_id: [video_episodes]}
        """
        try:
            failed_episodes: list[int] = []

            # 只处理失败的集数
            for episode in video_episodes:
                if self._process_episode(
                    video_id=video_id,
                    video_episode=episode,
                    video_origin=video_origin,
                    oss_adapter=oss_adapter
                ):
                    # 修复成功
                    self.logger.info(f"集数修复成功: ID={video_id}, EP={episode}")
                else:
                    # 修复失败，添加到失败列表
                    failed_episodes.append(episode)
                    self.logger.warning(f"集数修复失败: ID={video_id}, EP={episode}")
            
            # 返回仍然失败的集数
            if failed_episodes:
                return {video_id: failed_episodes}
            return {}

        except Exception as e:
            self.logger.error(f"修复视频Index失败: ID={video_id}, Error={e}")
            # 异常时，返回所有集数为失败
            return {video_id: video_episodes}

    def _process_episode(
        self,
        video_id: int,
        video_episode: int,
        video_origin: str,
        oss_adapter: StorageProvider
    ) -> bool:
        """
        修复视频集数缺失

        Args:
            oss_adapter: OSS适配器
            video_id: 视频ID
            video_episode: 集数
            video_origin: 视频来源

        Returns:
            bool: 资源是否存在
        """
        try:
            # 1. 下载 origin.m3u8
            origin_key: str = oss_adapter.generate_key(
                resource_id=video_id,
                resource_origin=video_origin,
                resource_filename="origin.m3u8",
                resource_episode=video_episode,
            )

            content: bytes | None = oss_adapter.download_file(origin_key)
            if not content:
                if video_episode == 1:
                    self.logger.warning(
                        f"Origin文件不存在: ID={video_id}, Key={origin_key}"
                    )
                return False

            # 源内容    
            m3u8_content: str = content.decode("utf-8")

            # 目标内容
            index_content: str = m3u8_content

            # 2. 解析TS链接
            ts_urls: list[str] = re.findall(r"(https?://[^\s]+)", m3u8_content)
            if not ts_urls:
                self.logger.warning(f"未找到TS链接: ID={video_id}, EP={video_episode}")
                return False
            
            # 3. 下载并上传TS
            for index, ts_url in enumerate(ts_urls):
                ts_filename: str = f"{(index+1):04d}.ts"

                # 生成TS的OSS Key
                ts_key: str = oss_adapter.generate_key(
                    resource_id=video_id,
                    resource_origin="type_16",
                    resource_filename=ts_filename,
                    resource_episode=video_episode,
                )

                # 替换源内容中的TS链接
                index_content = index_content.replace(ts_url, ts_filename)

                # 上传
                if not oss_adapter.upload_from_url(resource_url=ts_url, resource_type="index", resource_key=ts_key):
                    self.logger.error(f"TS上传失败: {ts_url}, ID={video_id}, EP={video_episode}")
                    return False

            # 4. 最后上传index.m3u8
            index_key: str = oss_adapter.generate_key(
                resource_id=video_id,
                resource_origin="type_16",
                resource_filename="index.m3u8",
                resource_episode=video_episode,
            )
            
            # 上传index.m3u8
            content_type: str = "application/vnd.apple.mpegurl"
            if not oss_adapter.upload_file(content=index_content.encode("utf-8"), key=index_key, content_type=content_type):
                self.logger.error(f"Index.m3u8上传失败: ID={video_id}, EP={video_episode}")
                return False

            return True
        except Exception as e:
            self.logger.error(f"处理分集失败: ID={video_id}, EP={video_episode}, Error={e}")
            return False
