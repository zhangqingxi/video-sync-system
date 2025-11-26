"""
领域层 - 视频模型

定义视频领域模型。

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from typing import Any
from dataclasses import dataclass, field

@dataclass
class Video:
    """视频领域模型"""
    # 基础字段（来自 list 接口）
    id: str  # douban_id
    title: str
    
    # Detail 接口返回的字段
    video_list: list[str] = field(default_factory=list)  # m3u8 URL列表
    download_url: str = ""
    cover: str = ""
    desc: str = ""  # 视频描述
    tags: list[str] = field(default_factory=list)  # 分类标签
    total_episodes: int = 0  # 总集数
    free_watch_episodes: int = 0  # 免费观看集数
    
    @classmethod
    def from_api_list(cls, data: dict[str, Any]) -> 'Video':
        """从API list接口数据创建Video对象（基础字段）"""
        return cls(
            id=str(data.get('id', '')),
            title=data.get('title', '')
        )
    
    def update_from_detail(self, detail_data: dict[str, Any]) -> None:
        """从API detail接口数据更新字段"""
        self.video_list = detail_data.get('video_list', [])
        self.download_url = detail_data.get('download_url', '')
        self.cover = detail_data.get('cover', '')
        self.desc = detail_data.get('desc', '') or detail_data.get('c_desc', '')
        self.tags = detail_data.get('tags', [])
        self.total_episodes = detail_data.get('total_episodes', 0)
        self.free_watch_episodes = detail_data.get('free_watch_episodes', 0)
    
    def to_dict(self) -> dict[str, Any]:
        """转换为字典（用于数据库插入）"""
        return {
            'id': self.id,
            'title': self.title,
            'video_list': self.video_list,
            'download_url': self.download_url,
            'cover': self.cover,
            'desc': self.desc,
            'tags': self.tags,
            'total_episodes': self.total_episodes,
            'free_watch_episodes': self.free_watch_episodes
        }
    
    def to_vod_dict(self) -> dict[str, Any]:
        """转换为数据库vod_*格式"""
        import time
        import random
        
        vod_play_url = "#".join(self.video_list) if self.video_list else ''
        vod_down_url = self.download_url if self.download_url else ''
        vod_class = ','.join(self.tags) if self.tags else ''
        vod_content = self.desc if self.desc else ''
        vod_blurb = vod_content[:250] if vod_content else ''
        
        now_time = int(time.time())
        
        return {
            'type_id': 16,
            'type_id_1': 2,
            'vod_name': self.title,
            'vod_sub': self.title,
            'vod_blurb': vod_blurb,
            'vod_content': vod_content,
            'vod_total': self.total_episodes,
            'vod_pic': self.cover,
            'vod_pic_thumb': self.cover,
            'vod_pic_slide': self.cover,
            'vod_lang': 'English',
            'vod_year': 2025,
            'vod_class': vod_class,
            'vod_play_from': 'dplayer',
            'vod_play_url': vod_play_url,
            'vod_time': now_time,
            'vod_time_add': now_time,
            'vod_down_url': vod_down_url,
            'vod_letter': self.title[0:1] if self.title else '',
            'vod_color': '',
            'vod_pic_screenshot': '',
            'vod_actor': '',
            'vod_writer': '',
            'vod_behind': '',
            'vod_remarks': '',
            'vod_pubdate': 2025,
            'vod_serial': '',
            'vod_status': 1,
            'vod_tag': vod_class,
            'vod_douban_id': int(self.id),
            'vod_points': 9.99,
            'vod_points_play': 9.99,
            'vod_points_down': 9.99,
            'vod_trysee': self.free_watch_episodes,
            'vod_hits': random.randint(100000, 300000)
        }
