"""
应用层 - S3 Index修复命令

修复S3上的index m3u8资源（下载origin.m3u8 -> 解析TS -> 上传TS -> 生成index.m3u8）。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import re
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.storage import StorageFactory
from src.infrastructure.http import HTTPClient
from src.core.protocols import StorageProvider

class S3IndexFixCommand(BaseCommand):
    """S3 index资源修复命令"""

    @property
    def name(self) -> str:
        return "s3_index_fix"

    @property
    def description(self) -> str:
        return "修复S3 index m3u8资源 (Origin -> TS -> Index)"

    def execute(self) -> int:
        """执行修复"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始S3 index资源修复")
            self.logger.info("=" * 60)

            # 获取依赖
            state_manager: StateManager = self.container.resolve(interface=StateManager)

            # 创建S3存储适配器
            s3_adapter: StorageProvider = StorageFactory.create_s3(
                config=self.config.s3_storage,
                constants=self.config.constants,
                logger=self.logger
            )

            # 初始化HTTP客户端 (用于下载TS)
            http_client: HTTPClient = HTTPClient(
                api_config=self.config.api,
                state_manager=state_manager,
                logger=self.logger
            )

            # 获取失败ID列表
            failed_ids: list[int] = state_manager.get_s3_failed_index_ids()

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
                        self._fix_video_index,
                        video_id,
                        s3_adapter,
                        http_client
                    )
                    futures[future] = video_id

                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        success: bool = future.result()

                        if success:
                            success_count += 1
                            state_manager.remove_s3_failed_index_id(video_id=video_id)
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

    def _fix_video_index(
            self,
            video_id: int,
            s3_adapter: StorageProvider,
            http_client: HTTPClient
    ) -> bool:
        """
        修复单个视频的index资源

        Args:
            video_id: 视频ID
            s3_adapter: S3适配器
            http_client: HTTP客户端

        Returns:
            bool: 是否成功
        """
        try:
            # 尝试处理第1集
            return self._process_episode(video_id, 1, s3_adapter, http_client)

        except Exception as e:
            self.logger.error(f"修复视频Index失败: ID={video_id}, Error={e}")
            return False

    def _process_episode(
            self,
            video_id: int,
            episode: int,
            s3_adapter: StorageProvider,
            http_client: HTTPClient
    ) -> bool:
        """处理单集"""
        try:
            # 1. 下载 origin.m3u8
            origin_key: str = s3_adapter.generate_key(
                resource_id=video_id,
                resource_origin='type_16',
                resource_filename='origin.m3u8',
                resource_episode=episode
            )

            content: bytes | None = s3_adapter.download_file(origin_key)
            if not content:
                if episode == 1:
                    self.logger.warning(f"Origin文件不存在: ID={video_id}, Key={origin_key}")
                return False

            m3u8_content: str = content.decode('utf-8')

            # 2. 解析TS链接
            ts_urls: list[str] = re.findall(r'(https?://[^\s]+)', m3u8_content)
            if not ts_urls:
                self.logger.warning(f"未找到TS链接: ID={video_id}")
                return False

            # 3. 下载并上传TS
            new_m3u8_lines: list[str] = []
            ts_count: int = 0
        
            lines: list[str] = m3u8_content.splitlines()
            for line in lines:
                if line.startswith('#'):
                    new_m3u8_lines.append(line)
                    continue

                if line.strip() == '':
                    continue

                # 是URL行
                ts_url: str = line.strip()
                ts_filename: str = f"{ts_count:04d}.ts"

                # 生成TS的S3 Key
                ts_key: str = s3_adapter.generate_key(
                    resource_id=video_id,
                    resource_origin='type_16',
                    resource_filename=ts_filename,
                    resource_episode=episode
                )

                # 下载 TS
                if not s3_adapter.upload_from_url(resource_url=ts_url, resource_type='index', resource_key=ts_key):
                    self.logger.error(f"TS上传失败: {ts_url}")
                    return False

                new_m3u8_lines.append(ts_filename)
                ts_count += 1

            # 4. 上传 index.m3u8
            index_content: str = '\n'.join(new_m3u8_lines)
            index_key: str = s3_adapter.generate_key(
                resource_id=video_id,
                resource_origin='type_16',
                resource_filename='index.m3u8',
                resource_episode=episode
            )

            if not s3_adapter.upload_file(key=index_key, content=index_content.encode('utf-8'), content_type='application/vnd.apple.mpegurl'):
                self.logger.error(f"Index m3u8上传失败: ID={video_id}")
                return False

            return True

        except Exception as e:
            self.logger.error(f"处理分集失败: ID={video_id}, EP={episode}, Error={e}")
            return False
