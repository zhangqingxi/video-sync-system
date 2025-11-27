"""
应用层 - Scraper命令

从第三方API抓取视频数据，插入数据库，上传OSS，同步到站点。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import time
from typing import Any
from src.application.commands.base import BaseCommand
from src.core.state import StateManager
from src.infrastructure.database import VideoRepositoryImpl
from src.infrastructure.http import HTTPClient
from src.infrastructure.storage import OSSAdapter
from src.domain.services import APIService
from src.domain.models import Video


class ScraperCommand(BaseCommand):
    """API数据抓取命令"""

    @property
    def name(self) -> str:
        return "scraper"

    @property
    def description(self) -> str:
        return "从第三方API抓取视频数据"

    def execute(self) -> int:
        """执行抓取"""
        try:
            self.logger.info("=" * 60)
            self.logger.info("开始API数据抓取")
            self.logger.info("=" * 60)

            state_manager: StateManager = self.container.resolve(interface=StateManager)
            video_repo: VideoRepositoryImpl = self.container.resolve(
                interface=VideoRepositoryImpl
            )
            oss_adapter: OSSAdapter = self.container.resolve(interface=OSSAdapter)

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

            last_page: int = state_manager.last_page
            current_page: int = last_page - 1 if last_page > 0 else 1
            self.logger.info(f"从第 {current_page} 页开始抓取")

            site_config: dict = self.config.raw_config.get("site", {})
            domains: list[str] = site_config.get("domains", [])
            if isinstance(domains, str):
                domains = [d.strip() for d in domains.split(",") if d.strip()]
            api_token: str = site_config.get("api_token", "")
            sync_endpoint: str = site_config.get("endpoints", {}).get(
                "sync", "/api/sync"
            )

            total_inserted: int = 0
            total_skipped: int = 0
            total_oss_success: int = 0
            total_oss_failed: int = 0
            total_site_success: int = 0
            total_site_failed: int = 0

            while True:
                try:
                    # 1. 获取视频列表（仅基础信息）
                    videos: list[Video] = api_service.fetch_video_list(
                        page=current_page
                    )

                    if not videos:
                        self.logger.warning(f"第 {current_page} 页无数据")
                        break

                    processed_videos: list[dict] = (
                        []
                    )  # 存储处理成功的视频数据(vod_dict)用于站点同步

                    for video in videos:
                        # 2. 检查视频是否存在
                        if video_repo.exists(video_id=int(video.id)):
                            self.logger.warning(f"视频已存在，跳过: ID={video.id}")
                            total_skipped += 1
                            continue

                        # 3. 获取视频详情（仅当视频不存在时）
                        self.logger.info(
                            f"获取视频详情: '{video.title}' (ID: {video.id})"
                        )
                        detail_data: dict[str, Any] | None = (
                            api_service.fetch_video_detail(video_id=video.id)
                        )
                        if not detail_data:
                            self.logger.warning(
                                f"无法获取视频详情，跳过: ID={video.id}"
                            )
                            continue

                        video.update_from_detail(detail_data=detail_data)

                        # 4. 转换为数据库格式
                        vod_dict: dict[str, Any] = video.to_vod_dict()

                        # 5. 插入数据库
                        if video_repo.insert(vod_data=vod_dict):
                            total_inserted += 1
                            processed_videos.append(vod_dict)
                            self.logger.info(
                                f"插入成功: ID={video.id}, Title={video.title}"
                            )

                            # 6. 插入标签数据
                            try:
                                from src.infrastructure.database import (
                                    TagRepositoryImpl,
                                    DatabasePool,
                                )

                                db_pool: DatabasePool = self.container.resolve(
                                    interface=DatabasePool
                                )
                                tag_repo: TagRepositoryImpl = TagRepositoryImpl(
                                    db_pool=db_pool, logger=self.logger
                                )

                                tags: list[str] = video.tags if video.tags else []
                                if tags:
                                    self.logger.info(
                                        f"开始插入标签: ID={video.id}, 标签数={len(tags)}"
                                    )

                                    # 先删除旧标签（如果有的话）
                                    tag_repo.delete_video_tags(douban_id=int(video.id))

                                    # 插入新标签
                                    for tag_name in tags:
                                        tag_id: int = tag_repo.ensure_tag_exists(
                                            tag_name=tag_name
                                        )
                                        tag_repo.insert_video_tag(
                                            douban_id=int(video.id),
                                            tag_id=tag_id,
                                            type_id=16,
                                        )

                                    self.logger.info(
                                        f"标签插入成功: ID={video.id}, 标签数={len(tags)}"
                                    )
                                else:
                                    self.logger.warning(
                                        f"视频无标签数据: ID={video.id}"
                                    )

                            except Exception as e:
                                self.logger.error(
                                    f"标签插入失败: ID={video.id}, Error={e}"
                                )
                                state_manager.add_failed_video_tag_id(
                                    video_id=int(video.id)
                                )

                            # 7. 上传OSS
                            try:
                                self.logger.info(f"开始上传OSS: ID={video.id}")
                                uploaded: bool = oss_adapter.process_single_video_sync(
                                    resource_id=int(video.id),
                                    resource_title=video.title,
                                    resource_type="origin",
                                    video_list=video.video_list,
                                    cover=video.cover,
                                )

                                if uploaded:
                                    total_oss_success += 1
                                else:
                                    total_oss_failed += 1
                                    state_manager.add_oss_failed_origin_id(
                                        video_id=int(video.id)
                                    )

                            except Exception as e:
                                self.logger.error(
                                    f"OSS上传异常: ID={video.id}, Error={e}"
                                )
                                total_oss_failed += 1
                                state_manager.add_oss_failed_origin_id(
                                    video_id=int(video.id)
                                )
                        else:
                            self.logger.error(f"插入失败: ID={video.id}")

                        time.sleep(self.config.timing.request_delay)

                    # 8. 同步到站点
                    if processed_videos and domains:
                        try:
                            from src.domain.services import SiteService

                            self.logger.info(
                                f"开始同步 {len(processed_videos)} 个视频到站点"
                            )

                            site_service: SiteService = SiteService(
                                domains=domains,
                                api_token=api_token,
                                sync_endpoint=sync_endpoint,
                                logger=self.logger,
                            )

                            failed_by_domain: dict[str, list[int]] = (
                                site_service.sync_to_sites(videos=processed_videos)
                            )

                            # 按域名记录失败ID
                            for domain, failed_ids in failed_by_domain.items():
                                for video_id in failed_ids:
                                    state_manager.add_site_failed_id(
                                        domain=domain, video_id=video_id
                                    )
                                    total_site_failed += 1

                                success_count: int = len(processed_videos) - len(
                                    failed_ids
                                )
                                total_site_success += success_count

                        except Exception as e:
                            self.logger.error(f"站点同步失败: {e}")
                            # 出错时，为所有域名添加所有视频ID为失败
                            for domain in domains:
                                for video_data in processed_videos:
                                    vid: int | None = video_data.get("vod_douban_id")
                                    if vid:
                                        state_manager.add_site_failed_id(
                                            domain=domain, video_id=vid
                                        )
                                        total_site_failed += 1
                    elif processed_videos:
                        self.logger.warning("未配置站点域名，跳过站点同步")
                    else:
                        self.logger.info("本页没有需要同步的新视频")

                    api_service.update_progress(page=current_page)

                    self.logger.info(
                        f"第 {current_page} 页完成: "
                        f"新增={len(videos)}, 总新增={total_inserted}, 总跳过={total_skipped}, "
                        f"OSS成功={total_oss_success}, OSS失败={total_oss_failed}, "
                        f"站点成功={total_site_success}, 站点失败={total_site_failed}"
                    )

                    current_page += 1
                    time.sleep(self.config.timing.page_delay)

                except KeyboardInterrupt:
                    self.logger.warning("收到中断信号，停止抓取")
                    break
                except Exception as e:
                    self.logger.error(f"处理第 {current_page} 页时出错: {e}")
                    current_page += 1
                    continue

            self.logger.info("=" * 60)
            self.logger.info(
                f"抓取完成: 总新增={total_inserted}, 总跳过={total_skipped}, "
                f"OSS成功={total_oss_success}, OSS失败={total_oss_failed}, "
                f"站点成功={total_site_success}, 站点失败={total_site_failed}, "
                f"最后页码={current_page-1}"
            )
            self.logger.info("=" * 60)

            http_client.close()

            return 0

        except Exception as e:
            self.logger.error(f"命令执行失败: {e}", exc_info=True)
            return 1
