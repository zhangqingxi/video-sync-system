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
            
            # 创建S3存储适配器
            s3_adapter: StorageProvider = StorageFactory.create_s3(
                config=self.config.s3_storage,
                constants=self.config.constants,
                logger=self.logger
            )
            
            # 初始化API服务
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
            failed_ids: list[int] = state_manager.get_s3_failed_origin_ids()
            
            if not failed_ids:
                self.logger.info("没有需要修复的资源")
                http_client.close()
                return 0
            
            self.logger.info(f"待修复资源数量: {len(failed_ids)}")
            
            # 使用线程池并发同步
            thread_count: int = self.config.threads.sync_threads
            success_count: int = 0
            failed_count: int = 0
            video_id: int
            
            with ThreadPoolExecutor(max_workers=thread_count) as executor:
                futures: dict[Future[bool], int] = {}
                for video_id in failed_ids:
                    future: Future[bool] = executor.submit(
                        self._sync_video,
                        video_id,
                        api_service,
                        s3_adapter
                    )
                    futures[future] = video_id
                
                for future in as_completed(futures):
                    video_id = futures[future]
                    try:
                        success: bool = future.result()
                        
                        if success:
                            success_count += 1
                            # 从失败列表移除
                            state_manager.remove_s3_failed_origin_id(video_id=video_id)
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
    
    def _sync_video(
        self,
        video_id: int,
        api_service: APIService,
        s3_adapter: StorageProvider
    ) -> bool:
        """
        同步单个视频
        
        Args:
            video_id: 视频ID
            api_service: API服务
            s3_adapter: S3适配器
            
        Returns:
            bool: 是否成功
        """
        try:
            # 1. 从API获取最新详情
            detail_data: dict[str, Any] = api_service.fetch_video_detail(str(video_id))
            
            if not detail_data:
                self.logger.warning(f"无法获取视频详情，跳过: ID={video_id}")
                return False

            # 2. 提取数据
            title: str = detail_data.get('title', '')
            cover: str = detail_data.get('cover', '')
            video_list: list[str] = detail_data.get('video_list', [])
            
            if not video_list:
                self.logger.warning(f"视频播放列表为空: ID={video_id}")
                return False
            
            # 3. 调用S3适配器同步
            return s3_adapter.process_single_video_sync(
                resource_id=video_id,
                resource_title=title,
                resource_type='origin',
                video_list=video_list,
                cover=cover
            )
            
        except Exception as e:
            self.logger.error(f"同步视频失败: ID={video_id}, Error={e}")
            return False
