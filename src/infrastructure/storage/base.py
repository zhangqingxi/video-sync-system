"""
基础设施层 - 存储基类

Author: Qasim
Version: 3.0
Python: 3.11+
"""

from abc import ABC, abstractmethod
import requests
from urllib.parse import urljoin
from src.core.config import StorageConfig, ConstantsConfig
from src.core.protocols import LoggerProvider
from src.utils.crypto import AESCrypto

class BaseStorageAdapter(ABC):
    """存储适配器基类"""
    
    def __init__(
        self,
        config: StorageConfig,
        constants: ConstantsConfig,
        logger: LoggerProvider
    ):
        self.config: StorageConfig = config
        self.constants: ConstantsConfig = constants
        self.logger: LoggerProvider = logger
    
    @abstractmethod
    def upload_file(self, key: str, content: bytes, content_type: str | None = None) -> bool:
        """上传文件"""
        pass
    
    @abstractmethod
    def check_exists(self, key: str) -> bool:
        """检查文件是否存在"""
        pass
    
    @abstractmethod
    def download_file(self, key: str) -> bytes | None:
        """下载文件"""
        pass
    
    def upload_from_url(self, resource_url: str, resource_type: str, resource_key: str) -> bool:
        """从URL下载并上传"""
        try:
            headers: dict[str, str] = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response: requests.Response = requests.get(url=resource_url, timeout=30, headers=headers, verify=False)
            response.raise_for_status()
            
            content: bytes = response.content
            if not content:
                raise Exception("下载内容为空")
            
            # 确定内容类型
            content_type: str | None = None
            if resource_type == 'cover':
                content_type = response.headers.get('Content-Type', 'image/jpeg')
            elif resource_type == 'origin':
                content_type = 'application/vnd.apple.mpegurl'
                content = self._keep_remote_ts_paths(content=content, base_url=resource_url)
            elif resource_type == 'index':
                content_type = 'application/vnd.apple.mpegurl'
            
            return self.upload_file(key=resource_key, content=content, content_type=content_type)
            
        except Exception as e:
            self.logger.error(f"从URL上传失败: url={resource_url}, type={resource_type}, key={resource_key}, error={e}")
            return False
    
    def generate_key(
        self,
        resource_id: int,
        resource_origin: str,
        resource_filename: str,
        resource_episode: int | None = None
    ) -> str:
        """
        生成存储键名（AES加密）
        
        Args:
            resource_id: 视频ID
            resource_origin: 视频来源
            resource_filename: 文件名（origin.m3u8, index.m3u8, cover.jpg等）
            resource_episode: 剧集编号
            
        Returns:
            str: 加密后的存储键名
        """
        # 使用当前存储器的加密key
        crypto: AESCrypto = AESCrypto(key=self.config.encryption_key)

        # 加密基础路径
        vod_str: str = f"{resource_origin}|{resource_id}"
        vod_encrypted: str = crypto.encrypt(plaintext=vod_str)
        vod_hex: str = vod_encrypted.replace('+', '-').replace('/', '_').replace('=', '')
        
        # 构建路径
        if resource_episode is not None:
            # 剧集路径：video_data/{id}/{encrypted}/{episode}/{encrypted_ep}/filename
            vod_ep_str: str = f"{resource_origin}|{resource_id}|{resource_episode}"
            vod_ep_encrypted: str = crypto.encrypt(plaintext=vod_ep_str)
            vod_ep_hex: str = vod_ep_encrypted.replace('+', '-').replace('/', '_').replace('=', '')
            return f"video_data/{resource_id}/{vod_hex}/{resource_episode}/{vod_ep_hex}/{resource_filename}"
        else:
            # 封面路径：video_data/{id}/{encrypted}/filename
            return f"video_data/{resource_id}/{vod_hex}/{resource_filename}"
    
    def process_single_video_sync(
        self,
        resource_id: int,
        resource_title: str,
        resource_type: str,
        video_list: list[str],
        cover: str
    ) -> bool:
        """同步单个视频资源"""
        try:
            self.logger.info(f"开始同步视频: {resource_title} (douban_id={resource_id}, type={resource_type})")

            if resource_type in ('origin', 'index'):
                # 上传所有剧集
                filename: str = f'{resource_type}.m3u8'
                for index, m3u8_url in enumerate(video_list, start=1):
                    if not m3u8_url:
                        self.logger.warning(f"跳过空的m3u8链接 (剧集 {index})")
                        continue
                    
                    key: str = self.generate_key(
                        resource_id=int(resource_id),
                        resource_origin='type_16',
                        resource_filename=filename,
                        resource_episode=index
                    )

                    if not self.upload_from_url(resource_url=m3u8_url, resource_type=resource_type, resource_key=key):
                        raise Exception(f"M3U8同步失败 (剧集 {index})")

            elif resource_type == 'cover':
                if not cover:
                    self.logger.warning(f"视频没有封面: douban_id={resource_id}")
                    return False
                
                # 上传封面
                cover_key: str = self.generate_key(
                    resource_id=int(resource_id),
                    resource_origin='type_16',
                    resource_filename='cover.jpg'
                )
                
                if not self.upload_from_url(resource_url=cover, resource_type='cover', resource_key=cover_key):
                    raise Exception("封面图片同步失败")
            else:
                raise ValueError(f"未知资源类型: {resource_type}")
            
            self.logger.info(f"视频同步完成: {resource_title} (douban_id={resource_id}, type={resource_type})")
            return True
            
        except Exception as e:
            self.logger.error(f"同步视频失败: {resource_title} (douban_id={resource_id}, type={resource_type}), error={e}")
            return False
    
    @staticmethod
    def _keep_remote_ts_paths(content: bytes, base_url: str) -> bytes:
        """转换m3u8中TS路径为绝对URL"""
        text: str = content.decode('utf-8')
        lines: list[str] = text.split('\n')
        modified_lines: list[str] = []
        
        for line in lines:
            stripped: str = line.strip()
            # 处理TS分片行
            if stripped and not stripped.startswith('#') and (stripped.endswith('.ts') or '/ts?' in stripped):
                if not stripped.startswith('http'):
                    absolute_url: str = urljoin(base=base_url, url=stripped)
                    modified_lines.append(absolute_url)
                else:
                    modified_lines.append(stripped)
            else:
                modified_lines.append(line)
        
        return '\n'.join(modified_lines).encode('utf-8')
