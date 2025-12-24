"""
应用层 - S3 Fix命令

同步失败的S3资源。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.storage import StorageFactory
from src.infrastructure.http import HTTPClient
from src.domain.services import APIService
from src.infrastructure.database import VideoRepositoryImpl
from src.core.protocols import StorageProvider


class S3OriginFixCommand(BaseCommand):
    """S3 origin资源修复命令"""

    @property
    def name(self) -> str:
        return "s3_origin_fix"

    @property
    def description(self) -> str:
        return "同步失败的S3 origin m3u8资源"

    def execute(self) -> int:
        """执行同步"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始S3 origin资源修复")
            self.logger.info("=" * 60)

            # 获取依赖
            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(
                interface=VideoRepositoryImpl
            )

            # 创建S3存储适配器
            s3_adapter: StorageProvider = StorageFactory.create_s3(
                config=self.config.s3_storage,
                constants=self.config.constants,
                logger=self.logger,
            )

            # 初始化API服务
            http_client: HTTPClient = HTTPClient(
                api_config=self.config.api,
                state_manager=state_manager,
                logger=self.logger,
            )

            api_service: APIService = APIService(
                http_client=http_client,
                api_config=self.config.api,
                timing_config=self.config.timing,
                state_manager=state_manager,
                logger=self.logger,
            )

            # 获取失败ID列表
            failed_data: dict[str, list[int]] = state_manager.get_s3_failed_origin_episodes()

            if not failed_data:
                self.logger.info("没有需要修复的资源")
                http_client.close()
                return 0

            # 获取失败的视频ID列表
            video_ids: list = list(failed_data.keys())
            videos: list[dict[str, Any]] = video_repo.get_videos_batch(video_ids=video_ids)
            self.logger.info(f"待修复视频数量: {len(videos)}")

            # 使用线程池并发同步
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
                        self._fix_video_origin, video_id, video_episodes, video_origin, s3_adapter, api_service
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        result: dict[int, list[int]] = future.result()
                        
                        if not result:
                            # 所有集数都修复成功，移除该视频ID
                            success_count += 1
                            state_manager.remove_s3_failed_origin_episodes(video_id=video_id)
                            self.logger.info(f"修复成功: ID={video_id}")
                        else:
                            # 还有失败的集数，更新失败列表
                            failed_count += 1
                            for vid, episodes in result.items():
                                state_manager.remove_s3_failed_origin_episodes(
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

            http_client.close()
            return 0

        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1

    def _fix_video_origin(
        self, 
        video_id: int, 
        video_episodes: list[int], 
        video_origin: str, 
        s3_adapter: StorageProvider,
        api_service: APIService
    ) -> dict[int, list[int]]:
        """
        修复单个视频的Origin资源

        Args:
            video_id: 视频ID
            video_episodes: 需要修复的集数列表
            video_origin: 视频原始文件夹
            s3_adapter: S3适配器
            api_service: API服务

        Returns:
            dict[int, list[int]]: 仍然失败的集数 {video_id: [video_episodes]}
        """
        try:
            failed_episodes: list[int] = []

            # 1. 从API获取最新详情
            detail_data: dict[str, Any] | None = api_service.fetch_video_detail(
                video_id=str(video_id)
            )

            video_list: list[str] = detail_data.get("video_list", []) 

            if not detail_data or len(video_list) == 0:
                raise Exception(f"无法获取视频详情")

            # 只处理失败的集数
            for episode in video_episodes:
                if self._process_episode(
                    video_id=video_id,
                    video_episode=episode,
                    video_episode_url=video_list[episode - 1],
                    video_origin=video_origin,
                    s3_adapter=s3_adapter
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
            self.logger.error(f"修复视频Origin失败: ID={video_id}, Error={e}")
            # 异常时，返回所有集数为失败
            return {video_id: video_episodes}

    def _process_episode(
        self,
        video_id: int,
        video_episode: int,
        video_episode_url: str,
        video_origin: str,
        s3_adapter: StorageProvider
    ) -> bool:
        """
        修复视频集数缺失

        Args:
            s3_adapter: S3适配器
            video_id: 视频ID
            video_episode: 集数
            video_episode_url: 剧集URL
            video_origin: 视频来源

        Returns:
            bool: 资源是否存在
        """
        try:
            # 1. 生成 origin.m3u8 Key  
            key: str = s3_adapter.generate_key(
                resource_id=video_id,
                resource_origin=video_origin,
                resource_filename="origin.m3u8",
                resource_episode=video_episode,
            )

            if not s3_adapter.upload_from_url(
                resource_url=video_episode_url,
                resource_type='origin',
                resource_key=key,
            ):
                raise Exception(f"Origin.m3u8上传失败")

            return True
        except Exception as e:
            self.logger.error(f"处理分集失败: ID={video_id}, EP={video_episode}, Error={e}")
            return False
