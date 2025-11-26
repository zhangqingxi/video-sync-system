"""
基础设施层 - 视频数据仓库

提供视频数据的数据库操作。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from src.core.config import DatabaseConfig
from src.core.protocols import LoggerProvider, VideoRepository
from src.infrastructure.database.pool import DatabasePool


class VideoRepositoryImpl(VideoRepository):
    """视频数据仓库实现"""
    
    def __init__(self, db_pool: DatabasePool, config: DatabaseConfig, logger: LoggerProvider):
        """
        初始化视频仓库
        
        Args:
            db_pool: 数据库连接池
            config: 数据库配置
            logger: 日志提供者
        """
        self._db_pool: DatabasePool = db_pool
        self._table: str = config.video_table_name
        self._logger: LoggerProvider = logger
    
    def get_by_id(self, video_id: int) -> dict[str, Any] | None:
        """
        根据ID获取视频
        
        Args:
            video_id: 视频ID
            
        Returns:
            dict | None: 视频信息字典，不存在则返回None
        """
        query: str = f"SELECT * FROM `{self._table}` WHERE vod_douban_id = %s LIMIT 1"
        results: list[dict[str, Any]] = self._db_pool.execute_query(query=query, params=(video_id,))
        return results[0] if results else None
    
    def get_videos_after_id(self, last_id: int, limit: int = 100) -> list[dict[str, Any]]:
        """
        获取ID之后的视频列表
        
        Args:
            last_id: 起始ID
            limit: 返回数量限制
            
        Returns:
            list[dict]: 视频列表
        """
        query: str = f"""
            SELECT * FROM `{self._table}` 
            WHERE vod_douban_id > %s 
            ORDER BY vod_douban_id ASC 
            LIMIT %s
        """
        return self._db_pool.execute_query(query=query, params=(last_id, limit))
    
    def insert(self, vod_data: dict[str, Any]) -> bool:
        """
        插入视频数据（vod_*格式）
        
        Args:
            vod_data: vod_*格式的视频数据（来自Video.to_vod_dict()）
            
        Returns:
            bool: 是否成功
        """
        try:
            sql: str = f"""
                INSERT INTO `{self._table}` (
                    type_id, type_id_1, vod_name, vod_sub, vod_blurb, vod_content,
                    vod_total, vod_pic, vod_pic_thumb, vod_pic_slide, vod_lang, vod_year,
                    vod_class, vod_play_from, vod_play_url, vod_time, vod_time_add,
                    vod_down_url, vod_letter, vod_color, vod_pic_screenshot, vod_actor,
                    vod_writer, vod_behind, vod_remarks, vod_pubdate, vod_serial,
                    vod_status, vod_tag, vod_douban_id, vod_points, vod_points_play,
                    vod_points_down, vod_trysee, vod_hits
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
            """
            
            params: tuple = (
                vod_data['type_id'], vod_data['type_id_1'], vod_data['vod_name'],
                vod_data['vod_sub'], vod_data['vod_blurb'], vod_data['vod_content'],
                vod_data['vod_total'], vod_data['vod_pic'], vod_data['vod_pic_thumb'],
                vod_data['vod_pic_slide'], vod_data['vod_lang'], vod_data['vod_year'],
                vod_data['vod_class'], vod_data['vod_play_from'], vod_data['vod_play_url'],
                vod_data['vod_time'], vod_data['vod_time_add'], vod_data['vod_down_url'],
                vod_data['vod_letter'], vod_data['vod_color'], vod_data['vod_pic_screenshot'],
                vod_data['vod_actor'], vod_data['vod_writer'], vod_data['vod_behind'],
                vod_data['vod_remarks'], vod_data['vod_pubdate'], vod_data['vod_serial'],
                vod_data['vod_status'], vod_data['vod_tag'], vod_data['vod_douban_id'],
                vod_data['vod_points'], vod_data['vod_points_play'], vod_data['vod_points_down'],
                vod_data['vod_trysee'], vod_data['vod_hits']
            )
            
            self._db_pool.execute_update(query=sql, params=params)
            self._logger.info(f"视频插入成功: '{vod_data['vod_name']}' (ID: {vod_data['vod_douban_id']})")
            return True
        except Exception as e:
            self._logger.error(f"视频插入失败: {e}")
            return False

    def exists(self, video_id: int) -> bool:
        """
        检查视频是否存在
        
        Args:
            video_id: 视频ID
            
        Returns:
            bool: 是否存在
        """
        query: str = f"SELECT COUNT(*) as count FROM `{self._table}` WHERE vod_douban_id = %s"
        results: list[dict[str, Any]] = self._db_pool.execute_query(query=query, params=(video_id,))
        return results[0]['count'] > 0 if results else False
    
    def get_all_video_ids(self) -> list[int]:
        """
        获取所有视频ID列表
        
        Returns:
            list[int]: ID列表
        """
        query: str = f"SELECT vod_douban_id FROM `{self._table}` ORDER BY vod_douban_id ASC"
        results: list[dict[str, Any]] = self._db_pool.execute_query(query)
        return [row['vod_douban_id'] for row in results]
    
    def get_videos_batch(self, video_ids: list) -> list[dict[str, Any]]:
        """
        批量获取视频（按ID列表）
        
        Args:
            video_ids: 视频ID列表
            
        Returns:
            list[dict]: 视频列表
        """
        if not video_ids:
            return []
            
        placeholders: str = ','.join(['%s'] * len(video_ids))
        
        query: str = f"""
            SELECT * FROM `{self._table}` 
            WHERE vod_douban_id IN ({placeholders})
            ORDER BY vod_douban_id ASC
        """
        return self._db_pool.execute_query(query=query, params=tuple(video_ids))
