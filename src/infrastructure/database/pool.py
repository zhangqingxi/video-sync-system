"""
基础设施层 - 数据库连接池

提供MySQL连接池管理。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import mysql.connector
from mysql.connector import pooling
from typing import Any
from src.core.config import DatabaseConfig
from src.core.exceptions import DatabaseError
from src.core.protocols import LoggerProvider

class DatabasePool:
    """数据库连接池"""
    
    def __init__(self, config: DatabaseConfig, logger: LoggerProvider):
        """
        初始化数据库连接池
        
        Args:
            config: 数据库配置
            logger: 日志提供者
        """
        self.config: DatabaseConfig = config
        self.logger: LoggerProvider = logger
        self._pool: pooling.MySQLConnectionPool | None = None
        self._initialize_pool()
    
    def _initialize_pool(self) -> None:
        """初始化连接池"""
        try:
            self._pool = pooling.MySQLConnectionPool(
                pool_name="video_sync_pool",
                pool_size=self.config.max_connections,
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database,
                charset=self.config.charset,
                connect_timeout=self.config.connect_timeout,
                autocommit=True
            )
            self.logger.info(f"数据库连接池初始化成功 (max_connections={self.config.max_connections})")
        except mysql.connector.Error as e:
            raise DatabaseError(f"Failed to initialize database pool: {e}")
    
    def get_connection(self):
        """
        从连接池获取连接
        
        Returns:
            连接对象
            
        Raises:
            DatabaseError: 获取连接失败时抛出
        """
        try:
            return self._pool.get_connection()
        except mysql.connector.Error as e:
            raise DatabaseError(f"Failed to get connection from pool: {e}")
    
    def execute_query(self, query: str, params: tuple | None = None) -> list[dict[str, Any]]:
        """
        执行查询并返回结果
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            list[dict]: 查询结果（字典列表）
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            results: list[dict[str, Any]] = cursor.fetchall()
            return results
        except mysql.connector.Error as e:
            self.logger.error(f"Query failed: {e}")
            raise DatabaseError(f"Query execution failed: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
    
    def execute_update(self, query: str, params: tuple | None = None) -> int:
        """
        执行更新操作
        
        Args:
            query: SQL语句
            params: 参数
            
        Returns:
            int: 受影响的行数
        """
        conn = None
        cursor = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute(query, params or ())
            affected_rows: int = cursor.rowcount
            conn.commit()
            return affected_rows
        except mysql.connector.Error as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Update failed: {e}")
            raise DatabaseError(f"Update execution failed: {e}")
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
