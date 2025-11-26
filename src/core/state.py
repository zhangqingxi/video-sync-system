"""
核心层 - 状态管理器

管理程序运行状态（state.json）。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from src.core.exceptions import ValidationError


@dataclass
class StateData:
    """状态数据结构"""
    # API相关
    last_page: int = 0
    api_token: str = ""
    
    # S3同步失败ID
    s3_failed_synced_origin_m3u8_ids: list[int] = field(default_factory=list)
    s3_failed_synced_index_m3u8_ids: list[int] = field(default_factory=list)
    s3_failed_synced_cover_ids: list[int] = field(default_factory=list)
    
    # OSS同步失败ID
    oss_failed_synced_origin_m3u8_ids: list[int] = field(default_factory=list)
    oss_failed_synced_index_m3u8_ids: list[int] = field(default_factory=list)
    oss_failed_synced_cover_ids: list[int] = field(default_factory=list)
    
    # 站点同步失败ID (按域名分组)
    site_failed_synced_ids: dict[str, list[int]] = field(default_factory=dict)
    
    # 资源检查相关
    last_checked_s3_origin: int = 0
    last_checked_s3_index: int = 0
    last_checked_oss_origin: int = 0
    last_checked_oss_index: int = 0
    last_checked_s3_cover: int = 0
    last_checked_oss_cover: int = 0
    
    # 视频标签检查相关
    last_checked_video_tag: int = 0
    failed_video_tag_ids: list[int] = field(default_factory=list)


class StateManager:
    """状态管理器"""
    
    def __init__(self, state_file: Path):
        """
        初始化状态管理器
        
        Args:
            state_file: 状态文件路径
        """
        self._state_file: Path = state_file
        self._state: StateData = StateData()
        self._load()
    
    def _load(self) -> None:
        """从文件加载状态"""
        if not self._state_file.exists():
            # 如果文件不存在，创建默认状态
            self._save()
            return
        
        try:
            with open(self._state_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 使用字典解包初始化StateData
            self._state = StateData(**data)
        except (json.JSONDecodeError, TypeError) as e:
            raise ValidationError(f"状态文件格式错误: {e}")
    
    def _save(self) -> None:
        """保存状态到文件"""
        with open(self._state_file, 'w', encoding='utf-8') as f:
            json.dump(asdict(self._state), f, ensure_ascii=False, indent=2)
    
    # ========== API状态 ==========
    
    @property
    def last_page(self) -> int:
        """获取最后处理的页码"""
        return self._state.last_page
    
    def update_last_page(self, page: int) -> None:
        """更新最后处理的页码"""
        self._state.last_page = page
        self._save()
    
    @property
    def api_token(self) -> str:
        """获取API Token"""
        return self._state.api_token
    
    def update_api_token(self, token: str) -> None:
        """更新API Token"""
        self._state.api_token = token
        self._save()
    
    # ========== S3失败ID管理 ==========
    
    def get_s3_failed_origin_ids(self) -> list[int]:
        """获取S3 origin失败ID列表"""
        return self._state.s3_failed_synced_origin_m3u8_ids.copy()
    
    def add_s3_failed_origin_id(self, video_id: int) -> None:
        """添加S3 origin失败ID"""
        if video_id not in self._state.s3_failed_synced_origin_m3u8_ids:
            self._state.s3_failed_synced_origin_m3u8_ids.append(video_id)
            self._save()
    
    def remove_s3_failed_origin_id(self, video_id: int) -> None:
        """移除S3 origin失败ID"""
        if video_id in self._state.s3_failed_synced_origin_m3u8_ids:
            self._state.s3_failed_synced_origin_m3u8_ids.remove(video_id)
            self._save()
    
    def get_s3_failed_index_ids(self) -> list[int]:
        """获取S3 index失败ID列表"""
        return self._state.s3_failed_synced_index_m3u8_ids.copy()
    
    def add_s3_failed_index_id(self, video_id: int) -> None:
        """添加S3 index失败ID"""
        if video_id not in self._state.s3_failed_synced_index_m3u8_ids:
            self._state.s3_failed_synced_index_m3u8_ids.append(video_id)
            self._save()

    def remove_s3_failed_index_id(self, video_id: int) -> None:
        """移除S3 index失败ID"""
        if video_id in self._state.s3_failed_synced_index_m3u8_ids:
            self._state.s3_failed_synced_index_m3u8_ids.remove(video_id)
            self._save()
    
    def get_s3_failed_cover_ids(self) -> list[int]:
        """获取S3 cover失败ID列表"""
        return self._state.s3_failed_synced_cover_ids.copy()

    def add_s3_failed_cover_id(self, video_id: int) -> None:
        """添加S3 cover失败ID"""
        if video_id not in self._state.s3_failed_synced_cover_ids:
            self._state.s3_failed_synced_cover_ids.append(video_id)
            self._save()

    def remove_s3_failed_cover_id(self, video_id: int) -> None:
        """移除S3 cover失败ID"""
        if video_id in self._state.s3_failed_synced_cover_ids:
            self._state.s3_failed_synced_cover_ids.remove(video_id)
            self._save()
    
    # ========== OSS失败ID管理 ==========
    
    def get_oss_failed_origin_ids(self) -> list[int]:
        """获取OSS origin失败ID列表"""
        return self._state.oss_failed_synced_origin_m3u8_ids.copy()
    
    def add_oss_failed_origin_id(self, video_id: int) -> None:
        """添加OSS origin失败ID"""
        if video_id not in self._state.oss_failed_synced_origin_m3u8_ids:
            self._state.oss_failed_synced_origin_m3u8_ids.append(video_id)
            self._save()

    def remove_oss_failed_origin_id(self, video_id: int) -> None:
        """移除OSS origin失败ID"""
        if video_id in self._state.oss_failed_synced_origin_m3u8_ids:
            self._state.oss_failed_synced_origin_m3u8_ids.remove(video_id)
            self._save()
    
    def get_oss_failed_index_ids(self) -> list[int]:
        """获取OSS index失败ID列表"""
        return self._state.oss_failed_synced_index_m3u8_ids.copy()
    
    def add_oss_failed_index_id(self, video_id: int) -> None:
        """添加OSS index失败ID"""
        if video_id not in self._state.oss_failed_synced_index_m3u8_ids:
            self._state.oss_failed_synced_index_m3u8_ids.append(video_id)
            self._save()

    def remove_oss_failed_index_id(self, video_id: int) -> None:
        """移除OSS index失败ID"""
        if video_id in self._state.oss_failed_synced_index_m3u8_ids:
            self._state.oss_failed_synced_index_m3u8_ids.remove(video_id)
            self._save()

    def get_oss_failed_cover_ids(self) -> list[int]:
        """获取OSS cover失败ID列表"""
        return self._state.oss_failed_synced_cover_ids.copy()

    def add_oss_failed_cover_id(self, video_id: int) -> None:
        """添加OSS cover失败ID"""
        if video_id not in self._state.oss_failed_synced_cover_ids:
            self._state.oss_failed_synced_cover_ids.append(video_id)
            self._save()

    def remove_oss_failed_cover_id(self, video_id: int) -> None:
        """移除OSS cover失败ID"""
        if video_id in self._state.oss_failed_synced_cover_ids:
            self._state.oss_failed_synced_cover_ids.remove(video_id)
            self._save()
    
    # ========== 站点失败ID管理 ==========
    
    def get_site_failed_ids(self) -> dict[str, list[int]]:
        """获取所有域名的站点同步失败ID"""
        return {domain: ids.copy() for domain, ids in self._state.site_failed_synced_ids.items()}
    
    def get_site_failed_ids_for_domain(self, domain: str) -> list[int]:
        """获取指定域名的站点同步失败ID"""
        return self._state.site_failed_synced_ids.get(domain, []).copy()
    
    def add_site_failed_id(self, domain: str, video_id: int) -> None:
        """添加站点同步失败ID"""
        if domain not in self._state.site_failed_synced_ids:
            self._state.site_failed_synced_ids[domain] = []
        
        if video_id not in self._state.site_failed_synced_ids[domain]:
            self._state.site_failed_synced_ids[domain].append(video_id)
            self._save()

    def remove_site_failed_id(self, domain: str, video_id: int) -> None:
        """移除站点同步失败ID"""
        if domain in self._state.site_failed_synced_ids:
            if video_id in self._state.site_failed_synced_ids[domain]:
                self._state.site_failed_synced_ids[domain].remove(video_id)
                # 如果该域名的列表为空，删除该键
                if not self._state.site_failed_synced_ids[domain]:
                    del self._state.site_failed_synced_ids[domain]
                self._save()
    
    def clear_site_failed_ids_for_domain(self, domain: str) -> None:
        """清空指定域名的失败ID列表"""
        if domain in self._state.site_failed_synced_ids:
            del self._state.site_failed_synced_ids[domain]
            self._save()

    # ==================== 视频标签检查相关 ====================
    
    def get_last_checked_video_tag(self) -> int:
        """获取上次检查的视频标签ID"""
        return self._state.last_checked_video_tag
    
    def update_last_checked_video_tag(self, video_id: int) -> None:
        """更新上次检查的视频标签ID"""
        self._state.last_checked_video_tag = video_id
        self._save()
    
    def get_failed_video_tag_ids(self) -> list[int]:
        """获取标签检查失败ID列表"""
        return self._state.failed_video_tag_ids.copy()
    
    def add_failed_video_tag_id(self, video_id: int) -> None:
        """添加标签检查失败ID"""
        if video_id not in self._state.failed_video_tag_ids:
            self._state.failed_video_tag_ids.append(video_id)
            self._save()
    
    def remove_failed_video_tag_id(self, video_id: int) -> None:
        """移除标签检查失败ID"""
        if video_id in self._state.failed_video_tag_ids:
            self._state.failed_video_tag_ids.remove(video_id)
            self._save()
    
    # ========== 检查进度管理 ==========
    
    def get_last_checked_s3_origin(self) -> int:
        """获取S3 origin最后检查ID"""
        return self._state.last_checked_s3_origin
    
    def update_last_checked_s3_origin(self, video_id: int) -> None:
        """更新S3 origin最后检查ID"""
        self._state.last_checked_s3_origin = video_id
        self._save()
    
    def get_last_checked_s3_index(self) -> int:
        """获取S3 index最后检查ID"""
        return self._state.last_checked_s3_index
    
    def update_last_checked_s3_index(self, video_id: int) -> None:
        """更新S3 index最后检查ID"""
        self._state.last_checked_s3_index = video_id
        self._save()
    
    def get_last_checked_oss_origin(self) -> int:
        """获取OSS origin最后检查ID"""
        return self._state.last_checked_oss_origin
    
    def update_last_checked_oss_origin(self, video_id: int) -> None:
        """更新OSS origin最后检查ID"""
        self._state.last_checked_oss_origin = video_id
        self._save()
    
    def get_last_checked_oss_index(self) -> int:
        """获取OSS index最后检查ID"""
        return self._state.last_checked_oss_index
    
    def update_last_checked_oss_index(self, video_id: int) -> None:
        """更新OSS index最后检查ID"""
        self._state.last_checked_oss_index = video_id
        self._save()

    def get_last_checked_s3_cover(self) -> int:
        """获取S3 cover最后检查ID"""
        return self._state.last_checked_s3_cover
    
    def update_last_checked_s3_cover(self, video_id: int) -> None:
        """更新S3 cover最后检查ID"""
        self._state.last_checked_s3_cover = video_id
        self._save()

    def get_last_checked_oss_cover(self) -> int:
        """获取OSS cover最后检查ID"""
        return self._state.last_checked_oss_cover
    
    def update_last_checked_oss_cover(self, video_id: int) -> None:
        """更新OSS cover最后检查ID"""
        self._state.last_checked_oss_cover = video_id
        self._save()
