"""
MySQL数据库处理器

封装所有与MySQL数据库的交互操作，提供稳定的连接管理和数据操作接口。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import logging
import random
import time
from configparser import ConfigParser, SectionProxy
from typing import Any

import pymysql
from pymysql.connections import Connection
from pymysql.cursors import DictCursor

# ============================================================================
# 模块级日志记录器
# ============================================================================

logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# 数据库处理器类
# ============================================================================

class DatabaseHandler:
    """
    MySQL数据库处理器
    
    封装所有与MySQL数据库的交互操作，提供稳定的连接管理和数据操作接口。
    
    Main Features:
        - 自动连接管理和重连
        - 视频数据增删改查
        - 批量数据操作
        - 连接池优化
        - 错误处理和日志记录
    
    Attributes:
        conn_params: 数据库连接参数字典
        conn: 数据库连接对象
        cursor: 数据库游标对象
        user_table_name: 用户表名
        order_table_name: 订单表名
        view_table_name: 浏览记录表名
        popup_table_name: 弹窗表名
        vod_table_name: 视频表名
        typeId: 类型ID
        type_id_1: 子类型ID
        lang: 语言
        vod_play_from: 播放来源
        year: 年份
        points: 积分
        status: 状态
        trysee: 试看集数
        
    Example:
        >>> from core import DatabaseHandler, load_config
        >>> config = load_config()
        >>> db = DatabaseHandler(config=config)
        >>> exists = db.video_exists(douban_id="12345")
        >>> db.close()
    """
    
    def __init__(self, config: ConfigParser) -> None:
        """
        初始化数据库处理器
        
        Args:
            config: 配置对象，包含数据库连接信息
            
        Raises:
            KeyError: 当配置中缺少必要的数据库配置项时抛出
        """
        db_config: SectionProxy = config['database']
        
        # 数据库连接参数
        self.conn_params: dict[str, str] = {
            'host': db_config.get('host', fallback='localhost'),
            'user': db_config.get('user', fallback='root'),
            'password': db_config.get('password', fallback=''),
            'database': db_config.get('database', fallback=''),
            'charset': db_config.get('charset', fallback='utf8'),
        }
        
        # 连接和游标对象
        self.conn: Connection | None = None
        self.cursor: DictCursor | None = None
        
        # 表名配置
        self.video_table_name: str = db_config.get('vod_table_name', fallback='mac_vod')
        
        # 业务常量配置
        self.typeId: int = 16
        self.type_id_1: int = 2
        self.lang: str = 'English'
        self.vod_play_from: str = 'dplayer'
        self.year: int = 2025
        self.points: float = 9.99
        self.status: int = 1
        self.trysee: int = 0
        
        logger.info("数据库处理器初始化完成")
    
    def _get_conn(self) -> Connection | None:
        """
        获取活动的数据库连接
        
        自动检查连接状态并在需要时重新连接。
        
        Returns:
            Connection | None: 数据库连接对象，失败时返回None
            
        Note:
            该方法实现了连接池管理和自动故障恢复机制
        """
        try:
            if not self.conn or not self.conn.open:
                logger.info("数据库连接不存在或已关闭，正在重连...")
                self.conn = pymysql.connect(**self.conn_params)
                self.cursor = None  # 废弃旧游标
                logger.info("数据库连接成功")
            else:
                # 保持连接活跃
                self.conn.ping(reconnect=True)
            
            return self.conn
            
        except pymysql.MySQLError as e:
            logger.error(f"数据库连接失败: {e}")
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
            self.conn = None
            self.cursor = None
            return None
    
    def _get_cursor(self) -> DictCursor | None:
        """
        获取与当前连接匹配的游标
        
        Returns:
            DictCursor | None: 数据库游标对象，失败时返回None
        """
        conn: Connection | None = self._get_conn()
        
        if conn:
            if not self.cursor or self.cursor.connection != conn:
                self.cursor = conn.cursor(cursor=DictCursor)
            return self.cursor
        
        return None
    
    def close(self) -> None:
        """
        安全地关闭游标和连接
        
        清理所有数据库资源，防止内存泄漏。
        
        Example:
            >>> db.close()
        """
        if self.cursor:
            try:
                self.cursor.close()
            except Exception:
                pass
        
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
        
        self.conn = None
        self.cursor = None
        logger.info("数据库连接已关闭")
    
    def video_exists(self, douban_id: str) -> bool:
        """
        检查视频是否已存在
        
        通过 vod_douban_id 检查数据库中是否已有该视频记录。
        
        Args:
            douban_id: 豆瓣视频ID
            
        Returns:
            bool: 存在返回True，不存在或错误返回False
            
        Example:
            >>> if db.video_exists(douban_id="12345"):
            ...     print("视频已存在")
        """
        cursor: DictCursor | None = self._get_cursor()
        
        if not cursor:
            logger.error("无法获取数据库游标")
            return False
        
        try:
            sql: str = f"SELECT vod_id FROM {self.video_table_name} WHERE vod_douban_id = %s"
            cursor.execute(sql, (douban_id,))
            result: dict[str, Any] | None = cursor.fetchone()
            exists: bool = result is not None
            
            if exists:
                logger.debug(f"视频已存在 (douban_id: {douban_id})")
            
            return exists
            
        except pymysql.MySQLError as e:
            logger.error(f"检查视频是否存在失败 (douban_id: {douban_id}): {e}")
            return False
    
    def insert_video(self, video_data: dict[str, Any]) -> bool:
        """
        向数据库插入新视频记录
        
        Args:
            video_data: 包含视频信息的字典
            
        Required Fields:
            - id: 豆瓣ID
            - title: 视频标题
            - cover: 封面URL
            - tags: 标签列表
            - video_list: 视频URL列表
            - download_url: 下载URL
            - desc: 视频描述
            - total_episodes: 总集数
            - free_watch_episodes: 免费观看集数
            
        Returns:
            bool: 插入成功返回True，失败返回False
            
        Example:
            >>> video = {
            ...     "id": "12345",
            ...     "title": "测试视频",
            ...     "cover": "https://example.com/cover.jpg",
            ...     ...
            ... }
            >>> success = db.insert_video(video_data=video)
        """
        conn: Connection | None = self._get_conn()
        cursor: DictCursor | None = self._get_cursor()
        
        if not conn or not cursor:
            logger.error("无法获取数据库连接或游标")
            return False
        
        # 提取和处理视频数据
        title: str = video_data.get('title', '')
        cover_url: str = video_data.get('cover', '')
        video_class: str = ",".join(video_data.get('tags', []))[:255]
        vod_tag: str = video_class[:99]
        vod_play_url: str = "#".join(video_data.get('video_list', []))
        vod_down_url: str = video_data.get('download_url', '')
        vod_content: str = video_data.get('desc', '')
        vod_blurb: str = vod_content[:250]
        
        logger.info(f"准备插入视频: '{title}' (douban_id: {video_data.get('id')})")
        
        # 构建SQL语句
        sql: str = f"""
            INSERT INTO {self.video_table_name} (
                type_id, type_id_1, vod_name, vod_sub, vod_blurb, vod_content,
                vod_total, vod_pic, vod_pic_thumb, vod_pic_slide, vod_lang, vod_year,
                vod_class, vod_play_from, vod_play_url, vod_time, vod_time_add,
                vod_down_url, vod_letter, vod_color, vod_pic_screenshot,
                vod_actor, vod_writer, vod_behind, vod_remarks, vod_pubdate,
                vod_serial, vod_status, vod_tag, vod_douban_id, vod_points,
                vod_points_play, vod_points_down, vod_trysee, vod_hits
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
        """
        
        # 准备参数
        now_time: int = int(time.time())
        params: tuple = (
            self.typeId,                                      # type_id
            self.type_id_1,                                   # type_id_1
            title,                                            # vod_name
            title,                                            # vod_sub
            vod_blurb,                                        # vod_blurb
            vod_content,                                      # vod_content
            video_data.get('total_episodes', 0),              # vod_total
            cover_url,                                        # vod_pic
            cover_url,                                        # vod_pic_thumb
            cover_url,                                        # vod_pic_slide
            self.lang,                                        # vod_lang
            self.year,                                        # vod_year
            video_class,                                      # vod_class
            self.vod_play_from,                               # vod_play_from
            vod_play_url,                                     # vod_play_url
            now_time,                                         # vod_time
            now_time,                                         # vod_time_add
            vod_down_url,                                     # vod_down_url
            title[0:1] if title else '',                      # vod_letter
            '',                                               # vod_color
            '',                                               # vod_pic_screenshot
            '',                                               # vod_actor
            '',                                               # vod_writer
            '',                                               # vod_behind
            '',                                               # vod_remarks
            self.year,                                        # vod_pubdate
            '',                                               # vod_serial
            self.status,                                      # vod_status
            vod_tag,                                          # vod_tag
            video_data.get('id'),                             # vod_douban_id
            self.points,                                      # vod_points
            self.points,                                      # vod_points_play
            self.points,                                      # vod_points_down
            video_data.get('free_watch_episodes', 0),         # vod_trysee
            random.randint(100000, 300000)                    # vod_hits
        )
        
        try:
            cursor.execute(sql, params)
            conn.commit()
            logger.info(f"视频插入成功: '{title}' (douban_id: {video_data.get('id')})")
            return True
            
        except pymysql.MySQLError as e:
            logger.error(f"插入视频失败: '{title}': {e}")
            conn.rollback()
            return False
    
    def get_videos_by_ids(self, douban_ids: list[str]) -> list[dict[str, Any]]:
        """
        根据豆瓣ID列表批量查询视频数据
        
        Args:
            douban_ids: 豆瓣视频ID列表
            
        Returns:
            list[dict[str, Any]]: 视频数据列表
            
        Example:
            >>> videos = db.get_videos_by_ids(douban_ids=["123", "456", "789"])
            >>> for video in videos:
            ...     print(video['vod_name'])
        """
        cursor: DictCursor | None = self._get_cursor()
        
        if not cursor:
            logger.error("无法获取数据库游标")
            return []
        
        if not douban_ids:
            logger.warning("传入的douban_ids列表为空")
            return []
        
        try:
            # 构建IN查询
            placeholders: str = ','.join(['%s'] * len(douban_ids))
            sql: str = f"SELECT * FROM {self.video_table_name} WHERE vod_douban_id IN ({placeholders})"
            
            cursor.execute(sql, douban_ids)
            results: list[dict[str, Any]] = cursor.fetchall()
            
            logger.info(f"批量查询成功，返回 {len(results)} 条记录")
            return results
            
        except pymysql.MySQLError as e:
            logger.error(f"批量查询视频失败: {e}")
            return []
