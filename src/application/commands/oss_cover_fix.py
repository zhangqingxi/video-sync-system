"""
应用层 - OSS Cover修复命令

修复OSS上的cover图片资源。

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
from src.infrastructure.http import HTTPClient
from src.core.protocols import StorageProvider


class OSSCoverFixCommand(BaseCommand):
    """OSS Cover资源修复命令"""

    @property
    def name(self) -> str:
        return "oss_cover_fix"

    @property
    def description(self) -> str:
        return "修复OSS上的cover图片资源"

    def execute(self) -> int:
        """执行修复"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始OSS Cover资源修复")
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

            # 初始化API服务
            http_client: HTTPClient = HTTPClient(
                api_config=self.config.api,
                state_manager=state_manager,
                logger=self.logger,
            )

            # 获取失败ID列表
            failed_ids: list[int] = state_manager.get_oss_failed_cover_ids()

            if not failed_ids:
                self.logger.info("没有需要修复的资源")
                http_client.close()
                return 0

            self.logger.info(f"待修复资源数量: {len(failed_ids)}")

            # 使用线程池并发修复
            thread_count: int = self.config.threads.sync_threads
            success_count: int = 0
            failed_count: int = 0
            video_id: int

            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures: dict[Future[bool], int] = {}
                for video_id in failed_ids:
                    future: Future[bool] = executor.submit(
                        self._fix_video_cover, video_id, video_repo, oss_adapter
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        success: bool = future.result()

                        if success:
                            success_count += 1
                            state_manager.remove_oss_failed_cover_id(video_id=video_id)
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

    def _fix_video_cover(
        self,
        video_id: int,
        video_repo: VideoRepositoryImpl,
        oss_adapter: StorageProvider,
    ) -> bool:
        """
        修复单个视频的cover资源

        Args:
            oss_adapter: OSS适配器
            video_id: 视频ID
            video_repo: 视频仓库

        Returns:
            bool: 是否修复成功
        """
        try:
            # 从数据库获取视频信息
            video_data: dict[str, Any] | None = video_repo.get_by_id(video_id=video_id)
            if not video_data:
                self.logger.warning(f"数据库中未找到视频: ID={video_id}")
                return False

            # 获取视频封面
            cover_url: str = video_data.get("vod_pic", "")
            if not cover_url:
                self.logger.warning(f"视频没有封面: ID={video_id}")
                return False

            # 生成cover key
            cover_key: str = oss_adapter.generate_key(
                resource_id=video_id,
                resource_origin="type_16",
                resource_filename="cover.jpg",
            )

            # 上传到OSS
            if not oss_adapter.upload_from_url(
                resource_url=cover_url, resource_type="cover", resource_key=cover_key
            ):
                self.logger.error(f"上传封面失败: ID={video_id}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"修复视频Cover失败: ID={video_id}, Error={e}")
            return False
