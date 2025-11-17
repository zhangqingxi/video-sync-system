"""
AWS S3对象存储处理器

基于boto3 SDK，提供S3对象存储操作和AES加密路径生成。

Author: Qasim
Version: 2.0
Python: 3.11+
Date: 2025-01-14
"""

import base64
import hashlib
import logging
from configparser import ConfigParser, SectionProxy
from urllib.parse import urljoin

import boto3
import requests
from botocore.config import Config
from botocore.exceptions import ClientError
from Crypto.Cipher import AES
from Crypto.Cipher._mode_cbc import CbcMode
from Crypto.Util.Padding import pad, unpad
from requests.adapters import HTTPAdapter
from requests.sessions import Session
from urllib3.util.retry import Retry

# ============================================================================
# 模块级日志记录器
# ============================================================================

logger: logging.Logger = logging.getLogger(__name__)


# ============================================================================
# S3处理器类
# ============================================================================

class S3Handler:
    """
    AWS S3对象存储处理器
    
    提供基于AWS S3的文件上传、路径加密、对象检测等功能。
    
    Main Features:
        - AES加密对象路径
        - m3u8文件上传
        - 图片URL上传
        - 对象存在性检测
        - 批量视频同步
    
    Attributes:
        config: 配置对象
        s3: S3客户端实例
        bucket_name: 存储桶名称
        region_name: AWS区域
        encryption_key: AES加密密钥
        aes_key: 处理后的AES密钥
        aes_iv: AES初始化向量
        request_timeout: 请求超时时间
        session: HTTP会话对象
        
    Example:
        >>> from core import S3Handler, load_config
        >>> config = load_config()
        >>> s3_handler = S3Handler(config=config)
        >>> success = s3_handler.upload_image_from_url(
        ...     image_url="https://example.com/image.jpg",
        ...     s3_key="path/to/image.jpg"
        ... )
        >>> s3_handler.close()
    """
    
    def __init__(self, config: ConfigParser) -> None:
        """
        初始化S3客户端及AES配置
        
        Args:
            config: 业务配置对象，需包含S3密钥、Region等信息
            
        Raises:
            KeyError: 当配置中缺少必要的S3配置项时抛出
        """
        s3_config: SectionProxy = config['aws_s3']
        try:
            access_key_id: str = s3_config['access_key_id'].strip()
            access_key_secret: str = s3_config['secret_access_key'].strip()
            self.region: str = s3_config['region'].strip()
            self.bucket_name: str = s3_config['bucket_name'].strip()
        except KeyError as e:
            raise KeyError(f"S3配置缺失必需项: {e}")
        
        # 验证必需配置项不为空
        if not access_key_id:
            raise ValueError("S3 access_key_id 不能为空")
        if not access_key_secret:
            raise ValueError("S3 secret_access_key 不能为空")
        if not self.region:
            raise ValueError("S3 region 不能为空")
        if not self.bucket_name:
            raise ValueError("S3 bucket_name 不能为空")

        # 可选：设置超时时间
        connect_timeout = s3_config.getint('connect_timeout', fallback=60)
        readwrite_timeout = s3_config.getint('readwrite_timeout', fallback=300)
        
        # 配置boto3客户端
        botocore_config: Config = Config(
            max_pool_connections=100,
            retries={'max_attempts': 3, 'mode': 'standard'},
            read_timeout=readwrite_timeout,
            connect_timeout=connect_timeout
        )
        
        # 初始化S3客户端
        self.s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=access_key_id,
            aws_secret_access_key=access_key_secret,
            region_name=self.region,
            config=botocore_config
        )
        
        # 初始化AES加密配置
        self.encryption_key: str = config.get('aws_s3', 'encryption_key', fallback='default_key_12345')
        key_bytes: bytes = self.encryption_key.encode('utf-8')
        self.aes_key: bytes = hashlib.sha256(key_bytes).digest()
        self.aes_iv: bytes = hashlib.md5(key_bytes).digest()[:16]
        
        # 超时配置
        self.request_timeout: int = config.getint('aws_s3', 'request_timeout', fallback=60)
        
        # 初始化HTTP会话
        self.session: Session = self._setup_session()
        
        logger.info("S3处理器初始化完成")
    
    def _setup_session(self) -> Session:
        """
        配置HTTP会话
        
        设置连接池、重试策略和基础请求头。
        
        Returns:
            Session: 配置好的requests会话对象
        """
        session: Session = requests.Session()
        
        # 配置重试策略
        retry_strategy: Retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )
        
        # 配置HTTP适配器
        adapter: HTTPAdapter = HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=retry_strategy,
        )
        
        session.mount(prefix='http://', adapter=adapter)
        session.mount(prefix='https://', adapter=adapter)
        
        logger.debug("HTTP会话配置完成")
        return session
    
    def generate_s3_key(
        self,
        title: str,
        douban_id: str,
        key: str,
        resource_type: str,
        episode: int | None = None
    ) -> str:
        """
        生成唯一S3对象Key（支持AES加密）
        
        Args:
            title: 视频标题
            douban_id: 豆瓣ID
            key: 目录前缀
            resource_type: 资源类型（m3u8/cover）
            episode: 剧集索引（仅m3u8时使用）
            
        Returns:
            str: 规范化的对象键名
            
        Raises:
            ValueError: 当参数不合法时抛出
            
        Example:
            >>> s3_key = handler.generate_s3_key(
            ...     title="测试视频",
            ...     douban_id="12345",
            ...     key="video_data",
            ...     resource_type="m3u8",
            ...     episode=1
            ... )
        """
        vod_str: str = f"{title}|{douban_id}"
        vod_hex: str = self._deterministic_aes_encrypt(plaintext=vod_str)
        
        if resource_type == 'm3u8':
            if episode is None:
                raise ValueError("m3u8类型必须提供episode参数")
            
            vod_ep_str: str = f"{title}|{douban_id}|{episode}"
            vod_ep_hex: str = self._deterministic_aes_encrypt(plaintext=vod_ep_str)
            return f"{key}/{douban_id}/{vod_hex}/{episode}/{vod_ep_hex}"
            
        elif resource_type == 'cover':
            return f"{key}/{douban_id}/{vod_hex}/cover.jpg"
        else:
            raise ValueError(f"不支持的资源类型: {resource_type}")
    
    def _deterministic_aes_encrypt(self, plaintext: str) -> str:
        """
        确定性AES加密（固定IV保证同一明文输出唯一）
        
        Args:
            plaintext: 明文字符串
            
        Returns:
            str: Base64编码的加密字符串
        """
        cipher: CbcMode = AES.new(key=self.aes_key, mode=AES.MODE_CBC, iv=self.aes_iv)
        padded: bytes = pad(data_to_pad=plaintext.encode('utf-8'), block_size=AES.block_size)
        encrypted: bytes = cipher.encrypt(padded)
        return base64.urlsafe_b64encode(encrypted).decode('utf-8').rstrip('=')
    
    def _deterministic_aes_decrypt(self, encrypted_text: str) -> str:
        """
        解密AES密文
        
        Args:
            encrypted_text: Base64编码的密文
            
        Returns:
            str: 解密后的明文
        """
        cipher: CbcMode = AES.new(key=self.aes_key, mode=AES.MODE_CBC, iv=self.aes_iv)
        encrypted_data: bytes = base64.urlsafe_b64decode(
            encrypted_text + '=' * (4 - len(encrypted_text) % 4)
        )
        plain_padded: bytes = cipher.decrypt(encrypted_data)
        return unpad(padded_data=plain_padded, block_size=AES.block_size).decode('utf-8')
    
    def check_s3_object_exists(self, s3_key: str) -> bool:
        """
        检查S3对象是否存在
        
        Args:
            s3_key: S3对象键名
            
        Returns:
            bool: 存在返回True，不存在返回False
            
        Example:
            >>> exists = handler.check_s3_object_exists(s3_key="path/to/file.jpg")
        """
        try:
            self.s3.head_object(Bucket=self.bucket_name, Key=s3_key)
            logger.debug(f"S3对象存在: {s3_key}")
            return True
        except ClientError as e:
            # 获取错误代码
            error_code: str = e.response.get('Error', {}).get('Code', '')
            
            if error_code == '404' or error_code == 'NoSuchKey':
                logger.debug(f"S3对象不存在: {s3_key}")
                return False
            else:
                # 其他错误（如权限问题）记录日志
                logger.error(f"检查S3对象时发生错误 ({error_code}): {e}")
                return False
        except Exception as e:
            # 捕获其他非预期异常
            logger.error(f"检查S3对象时发生未知错误: {e}")
            return False
    
    def upload_m3u8_stream(self, m3u8_url: str, s3_base_key: str) -> bool:
        """
        上传m3u8文件到S3（保留远程TS路径）
        
        Args:
            m3u8_url: m3u8在线地址
            s3_base_key: S3存储前缀路径
            
        Returns:
            bool: 上传成功返回True，失败返回False
            
        Example:
            >>> success = handler.upload_m3u8_stream(
            ...     m3u8_url="https://example.com/video.m3u8",
            ...     s3_base_key="video_data/12345/encrypted_path/1"
            ... )
        """
        try:
            headers: dict[str, str] = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info(f"开始下载m3u8: {m3u8_url}")
            response: requests.Response = self.session.get(
                url=m3u8_url,
                timeout=self.request_timeout,
                verify=False,
                headers=headers
            )
            response.raise_for_status()
            
            m3u8_content: str = response.text
            if not m3u8_content:
                raise Exception("下载的m3u8内容为空")
            
            # 转换TS路径为绝对URL
            modified_m3u8: str = self._keep_remote_ts_paths(
                m3u8_content=m3u8_content,
                base_url=m3u8_url
            )
            
            # 构建S3对象键名
            m3u8_s3_key: str = f"{s3_base_key.rstrip('/')}/origin.m3u8"
            
            # 上传到S3
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=m3u8_s3_key,
                Body=modified_m3u8.encode('utf-8'),
                ContentType='application/vnd.apple.mpegurl'
            )
            
            logger.info(f"m3u8上传成功: {m3u8_s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"上传m3u8到S3失败: {e}")
            return False
    
    def upload_image_from_url(self, image_url: str, s3_key: str) -> bool:
        """
        从URL下载图片并上传到S3
        
        Args:
            image_url: 网络图片URL
            s3_key: S3对象键名
            
        Returns:
            bool: 上传成功返回True，失败返回False
            
        Example:
            >>> success = handler.upload_image_from_url(
            ...     image_url="https://example.com/cover.jpg",
            ...     s3_key="video_data/12345/encrypted_path/cover.jpg"
            ... )
        """
        try:
            headers: dict[str, str] = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            logger.info(f"开始下载图片: {image_url}")
            response: requests.Response = self.session.get(
                url=image_url,
                timeout=30,
                verify=False,
                headers=headers
            )
            response.raise_for_status()
            
            # 获取内容类型
            content_type: str = response.headers.get('content-type', 'image/jpeg')
            
            # 上传到S3
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=response.content,
                ContentType=content_type
            )
            
            logger.info(f"图片上传成功: {s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"图片上传S3失败: {e}")
            return False
    
    def process_single_video_sync(
        self,
        douban_id: str,
        title: str,
        video_list: list[str],
        cover: str
    ) -> bool:
        """
        批量同步单个视频的所有剧集和封面到S3
        
        Args:
            douban_id: 豆瓣ID
            title: 视频标题
            video_list: m3u8链接列表
            cover: 封面图片链接
            
        Returns:
            bool: 成功返回True，失败返回False
            
        Example:
            >>> success = handler.process_single_video_sync(
            ...     douban_id=12345,
            ...     title="测试视频",
            ...     video_list=["https://example.com/ep1.m3u8", "https://example.com/ep2.m3u8"],
            ...     cover="https://example.com/cover.jpg"
            ... )
        """
        try:
            logger.info(f"开始同步视频到S3: {title} (douban_id: {douban_id})")
            
            # 上传所有剧集
            for index, m3u8_url in enumerate(video_list, start=1):
                if not m3u8_url:
                    logger.warning(f"跳过空的m3u8链接 (剧集 {index})")
                    continue
                
                s3_key: str = self.generate_s3_key(
                    title=title,
                    key='video_data',
                    douban_id=str(douban_id),
                    resource_type='m3u8',
                    episode=index
                )
                
                if not self.upload_m3u8_stream(m3u8_url=m3u8_url, s3_base_key=s3_key):
                    raise Exception(f"M3U8同步失败 (剧集 {index})")
            
            # 上传封面
            s3_key = self.generate_s3_key(
                title=title,
                key='video_data',
                douban_id=str(douban_id),
                resource_type='cover'
            )
            
            if not self.upload_image_from_url(image_url=cover, s3_key=s3_key):
                raise Exception("封面图片同步失败")
            
            logger.info(f"视频同步完成: {title} (douban_id: {douban_id})")
            return True
            
        except Exception as e:
            logger.error(f"同步视频失败: {e}")
            return False
    
    def process_single_video_episode_sync(
        self,
        douban_id: str,
        title: str,
        episode: int,
        episode_url: str,
        cover: str
    ) -> bool:
        """
        同步单个剧集和封面到S3
        
        Args:
            douban_id: 豆瓣ID
            title: 视频标题
            episode: 剧集编号
            episode_url: m3u8地址
            cover: 封面图片链接
            
        Returns:
            bool: 成功返回True，失败返回False
            
        Example:
            >>> success = handler.process_single_video_episode_sync(
            ...     douban_id=12345,
            ...     title="测试视频",
            ...     episode=1,
            ...     episode_url="https://example.com/ep1.m3u8",
            ...     cover="https://example.com/cover.jpg"
            ... )
        """
        try:
            logger.info(f"开始同步单集到S3: {title} 第{episode}集 (douban_id: {douban_id})")
            
            # 上传剧集
            s3_key: str = self.generate_s3_key(
                title=title,
                key='video_data_2',
                douban_id=str(douban_id),
                resource_type='m3u8',
                episode=episode
            )
            
            if not self.upload_m3u8_stream(m3u8_url=episode_url, s3_base_key=s3_key):
                raise Exception("M3U8同步失败")
            
            # 上传封面
            s3_key = self.generate_s3_key(
                title=title,
                key='video_data_2',
                douban_id=str(douban_id),
                resource_type='cover'
            )
            
            if not self.upload_image_from_url(image_url=cover, s3_key=s3_key):
                raise Exception("封面图片同步失败")
            
            logger.info(f"单集同步完成: {title} 第{episode}集 (douban_id: {douban_id})")
            return True
            
        except Exception as e:
            logger.error(f"同步剧集失败: {e}")
            return False
    
    def _keep_remote_ts_paths(self, m3u8_content: str, base_url: str) -> str:
        """
        保持TS分片的远程路径，转换为绝对URL
        
        Args:
            m3u8_content: m3u8文件内容
            base_url: 原始m3u8链接
            
        Returns:
            str: 修改后的m3u8内容
        """
        lines: list[str] = m3u8_content.split('\n')
        modified_lines: list[str] = []
        
        for line in lines:
            stripped: str = line.strip()
            
            # 检查是否为TS分片行
            if stripped and not stripped.startswith('#') and \
               (stripped.endswith('.ts') or '/ts?' in stripped):
                # 转换为绝对URL
                if not stripped.startswith('http'):
                    absolute_url: str = urljoin(base=base_url, url=stripped)
                    modified_lines.append(absolute_url)
                else:
                    modified_lines.append(stripped)
            else:
                modified_lines.append(line)
        
        return '\n'.join(modified_lines)
    
    def close(self) -> None:
        """
        释放资源
        
        Example:
            >>> handler.close()
        """
        if self.session:
            self.session.close()
        logger.debug("S3处理器资源已释放")
