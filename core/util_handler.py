"""
通用工具函数模块

提供项目范围内的通用工具函数，包括配置管理、状态管理和系统检查。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import json
import logging
import shutil
from configparser import ConfigParser
from pathlib import Path
from typing import Any

# ============================================================================
# 常量定义
# ============================================================================

# 项目根目录路径
BASE_DIR: Path = Path(__file__).resolve().parent.parent

# 模块级日志记录器
logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# 配置管理函数
# ============================================================================

def load_config() -> ConfigParser:
    """
    加载并解析项目配置文件
    
    从项目根目录读取 config.ini 文件并返回配置解析器对象。
    
    Returns:
        ConfigParser: 配置解析器对象，包含所有配置项
        
    Raises:
        FileNotFoundError: 当配置文件不存在时抛出
        configparser.Error: 当配置文件格式错误时抛出
        
    Example:
        >>> config = load_config()
        >>> db_host = config.get('database', 'host')
    
    Note:
        配置文件必须使用UTF-8编码
    """
    config: ConfigParser = ConfigParser()
    config_path: Path = BASE_DIR / 'config.ini'
    
    if not config_path.exists():
        error_msg: str = f"配置文件不存在: {config_path}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    try:
        config.read(config_path, encoding='utf-8')
        logger.info(f"配置文件加载成功: {config_path}")
        return config
    except Exception as e:
        logger.error(f"配置文件解析失败: {e}")
        raise


# ============================================================================
# 状态管理函数
# ============================================================================

def load_state() -> dict[str, Any]:
    """
    加载项目状态文件
    
    从 state.json 读取项目运行状态，如果文件不存在则创建默认状态。
    
    Returns:
        dict[str, Any]: 包含项目状态信息的字典
        
    State Structure:
        {
            "last_synced_page": int,        # 最后同步的页码
            "api_token": str | None,        # API访问令牌
            "failed_synced_ids": list[int], # 同步失败的视频ID列表
            "failed_site": dict[str, set],  # 站点同步失败记录
            "filed_detail_ids": list[int]   # 详情获取失败的ID列表
        }
        
    Raises:
        json.JSONDecodeError: 当JSON文件格式错误时抛出
        IOError: 当文件读取失败时抛出
        
    Example:
        >>> state = load_state()
        >>> current_page = state.get('last_synced_page', 0)
    """
    config: ConfigParser = load_config()
    state_file_path: Path = BASE_DIR / config.get('project_state', 'state_file')
    
    # 文件不存在时初始化默认状态
    if not state_file_path.exists():
        logger.warning(f"状态文件不存在，创建默认状态: {state_file_path}")
        initial_state: dict[str, any] = {
            "last_synced_page": 0,
            "api_token": None,
            "failed_synced_ids": [],
            "failed_site": {},
            "filed_detail_ids": []
        }
        save_state(data=initial_state)
        return initial_state
    
    try:
        with open(state_file_path, mode='r', encoding='utf-8') as f:
            state_data: dict[str, Any] = json.load(f)
            logger.debug(f"状态文件加载成功: {state_file_path}")
            return state_data
    except json.JSONDecodeError as e:
        logger.error(f"状态文件JSON格式错误: {e}")
        raise
    except Exception as e:
        logger.error(f"状态文件读取失败: {e}")
        raise


def save_state(data: dict[str, Any]) -> None:
    """
    保存项目状态到文件
    
    将状态数据序列化为JSON并保存到 state.json 文件。
    
    Args:
        data: 需要保存的状态数据字典
        
    Raises:
        json.JSONEncodeError: 当数据无法序列化为JSON时抛出
        IOError: 当文件写入失败时抛出
        
    Example:
        >>> state = load_state()
        >>> state['last_synced_page'] = 10
        >>> save_state(data=state)
    
    Note:
        保存时会格式化JSON输出，缩进为4个空格
    """
    config: ConfigParser = load_config()
    state_file_path: Path = BASE_DIR / config.get('project_state', 'state_file')
    
    try:
        with open(state_file_path, mode='w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            logger.debug(f"状态已保存: {state_file_path}")
    except Exception as e:
        logger.error(f"状态文件保存失败: {e}")
        raise


# ============================================================================
# 系统检查函数
# ============================================================================

def is_ffmpeg_installed() -> bool:
    """
    检查系统是否安装了FFmpeg
    
    通过检查PATH环境变量中是否存在 ffmpeg 可执行文件来判断。
    
    Returns:
        bool: 已安装返回True，否则返回False
        
    Example:
        >>> if is_ffmpeg_installed():
        ...     print("FFmpeg 已安装")
        ... else:
        ...     print("请安装 FFmpeg")
    
    Note:
        该函数仅检查 ffmpeg 是否在PATH中，不验证版本或功能
    """
    is_installed: bool = shutil.which('ffmpeg') is not None
    
    if is_installed:
        logger.debug("FFmpeg 已安装")
    else:
        logger.warning("FFmpeg 未安装")
    
    return is_installed
