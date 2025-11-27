"""
基础设施层 - 标签仓储实现

管理标签数据的CRUD操作。使用DatabasePool进行连接管理。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from src.infrastructure.database.pool import DatabasePool
from src.core.protocols import LoggerProvider


class TagRepositoryImpl:
    """标签仓储实现"""

    def __init__(self, db_pool: DatabasePool, logger: LoggerProvider) -> None:
        """
        初始化标签仓储

        Args:
            db_pool: 数据库连接池
            logger: 日志器
        """
        self.db_pool: DatabasePool = db_pool
        self.logger: LoggerProvider = logger
        self.tag_table: str = db_pool.config.tag_table_name
        self.video_tag_table: str = db_pool.config.video_tag_table_name

    def ensure_tag_exists(self, tag_name: str) -> int:
        """
        确保标签存在，如果不存在则创建

        Args:
            tag_name: 标签名称

        Returns:
            int: 标签ID
        """
        try:
            # 先尝试查找
            query: str = f"""
                SELECT tag_id FROM {self.tag_table} WHERE tag_name = %s
            """
            results: list[dict[str, Any]] = self.db_pool.execute_query(
                query=query, params=(tag_name,)
            )

            if results:
                return int(results[0]["tag_id"])

            # 不存在则插入
            insert_query: str = f"""
                INSERT INTO {self.tag_table} (tag_name, tag_recommend) VALUES (%s, 0)
            """
            self.db_pool.execute_update(query=insert_query, params=(tag_name,))

            # 再次查询获取ID
            results = self.db_pool.execute_query(query=query, params=(tag_name,))
            return int(results[0]["tag_id"])

        except Exception as e:
            self.logger.error(f"确保标签存在失败: tag_name={tag_name}, error={e}")
            raise

    def insert_video_tag(self, douban_id: int, tag_id: int, type_id: int) -> bool:
        """
        插入视频标签关联（使用INSERT IGNORE避免重复）

        Args:
            douban_id: 豆瓣ID
            tag_id: 标签ID
            type_id: 类型ID

        Returns:
            bool: 是否成功
        """
        try:
            query: str = f"""
                INSERT IGNORE INTO {self.video_tag_table} (douban_id, tag_id, type_id) VALUES (%s, %s, %s)
            """
            self.db_pool.execute_update(
                query=query, params=(douban_id, tag_id, type_id)
            )
            return True

        except Exception as e:
            self.logger.error(
                f"插入视频标签失败: douban_id={douban_id}, tag_id={tag_id}, error={e}"
            )
            return False

    def get_video_tag_count(self, douban_id: int) -> int:
        """
        获取视频的标签数量

        Args:
            douban_id: 豆瓣ID

        Returns:
            int: 标签数量
        """
        try:
            query: str = f"""
                SELECT COUNT(*) as countFROM {self.video_tag_table} WHERE douban_id = %s
            """
            results: list[dict[str, Any]] = self.db_pool.execute_query(
                query=query, params=(douban_id,)
            )
            return results[0]["count"] if results else 0

        except Exception as e:
            self.logger.error(f"获取视频标签数量失败: douban_id={douban_id}, error={e}")
            return 0

    def delete_video_tags(self, douban_id: int) -> bool:
        """
        删除视频的所有标签

        Args:
            douban_id: 豆瓣ID

        Returns:
            bool: 是否成功
        """
        try:
            query: str = f"""
                DELETE FROM {self.video_tag_table} WHERE douban_id = %s
            """
            self.db_pool.execute_update(query=query, params=(douban_id,))
            return True

        except Exception as e:
            self.logger.error(f"删除视频标签失败: douban_id={douban_id}, error={e}")
            return False
