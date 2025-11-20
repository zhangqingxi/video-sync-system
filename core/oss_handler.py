"""
阿里云OSS对象存储处理器

基于alibabacloud_oss_v2 SDK，提供OSS对象存储操作和AES加密路径生成。

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

import alibabacloud_oss_v2 as oss
import requests
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
# OSS处理器类
# ============================================================================

class OSSHandler:
    """
    阿里云OSS对象存储处理器
    
    提供基于阿里云OSS的文件上传、路径加密、对象检测等功能。
    
    Main Features:
        - AES加密对象路径
        - m3u8文件上传
        - 图片URL上传
        - 对象存在性检测
        - 批量视频同步
    
    Attributes:
        config: 配置对象
        region: OSS区域
        bucket_name: 存储桶名称
        client: OSS客户端实例
        encryption_key: AES加密密钥
        aes_key: 处理后的AES密钥
        aes_iv: AES初始化向量
        session: HTTP会话对象
        
    Example:
        >>> from core import OSSHandler, load_config
        >>> config = load_config()
        >>> oss_handler = OSSHandler(config=config)
        >>> success = oss_handler.upload_image_from_url(
        ...     image_url="https://example.com/image.jpg",
        ...     oss_key="path/to/image.jpg"
        ... )
        >>> oss_handler.close()
    """
    
    def __init__(self, config: ConfigParser) -> None:
        """
        初始化OSS客户端及AES配置
        
        Args:
            config: 业务配置对象，需包含OSS密钥、Region等信息
            
        Raises:
            KeyError: 当配置中缺少必要的OSS配置项时抛出
        """
        oss_config: SectionProxy = config['aliyun_oss']
        try:
            access_key_id: str = oss_config['access_key_id'].strip()
            access_key_secret: str = oss_config['secret_access_key'].strip()
            self.region: str = oss_config['region'].strip()
            self.bucket_name: str = oss_config['bucket_name'].strip()
            self.endpoint: str = oss_config['endpoint'].strip()
        except KeyError as e:
            raise KeyError(f"OSS配置缺失必需项: {e}")
        
        # 验证必需配置项不为空
        if not access_key_id:
            raise ValueError("OSS access_key_id 不能为空")
        if not access_key_secret:
            raise ValueError("OSS secret_access_key 不能为空")
        if not self.region:
            raise ValueError("OSS region 不能为空")
        if not self.bucket_name:
            raise ValueError("OSS bucket_name 不能为空")
        if not self.endpoint:
            raise ValueError("OSS endpoint 不能为空")
        
         # 创建凭证提供者
        try:
            credentials_provider: oss.credentials.CredentialsProvider = \
                oss.credentials.StaticCredentialsProvider(
                    access_key_id=access_key_id,
                    access_key_secret=access_key_secret
                )
            logger.debug("OSS凭证提供者创建成功")
        except Exception as e:
            logger.error(f"创建OSS凭证提供者失败: {e}")
            raise
        
        # 加载并配置OSS客户端
        try:
            cfg: oss.config.Config = oss.config.load_default()
            cfg.credentials_provider = credentials_provider
            cfg.region = self.region
            cfg.endpoint = self.endpoint
            
            # 可选：设置超时时间
            cfg.connect_timeout = oss_config.getint('connect_timeout', fallback=60)
            cfg.readwrite_timeout = oss_config.getint('readwrite_timeout', fallback=300)
            cfg.connect_timeout = oss_config.getint('connect_timeout', fallback=60)
            
            self.client: oss.Client = oss.Client(config=cfg)
            logger.debug("OSS客户端创建成功")
        except Exception as e:
            logger.error(f"创建OSS客户端失败: {e}")
            raise
        
        # 初始化AES加密配置
        self.encryption_key: str = oss_config.get('encryption_key', fallback='default_key_12345')
        key_bytes: bytes = self.encryption_key.encode('utf-8')
        self.aes_key: bytes = hashlib.sha256(key_bytes).digest()
        self.aes_iv: bytes = hashlib.md5(key_bytes).digest()[:16]
        
        # 初始化HTTP会话
        self.session: Session = self._setup_session()

        # 超时配置
        self.request_timeout: int = oss_config.getint('request_timeout', fallback=60)
        
        logger.info("OSS处理器初始化完成")
    
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
            max_retries=retry_strategy
        )
        
        session.mount(prefix='http://', adapter=adapter)
        session.mount(prefix='https://', adapter=adapter)
        
        logger.debug("HTTP会话配置完成")
        return session
    
    def generate_oss_key(
        self,
        title: str,
        douban_id: str,
        key: str,
        resource_type: str,
        episode: int | None = None
    ) -> str:
        """
        生成唯一OSS对象Key（支持AES加密）
        
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
            >>> oss_key = handler.generate_oss_key(
            ...     title="测试视频",
            ...     douban_id=12345,
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
    
    def check_oss_object_exists(self, oss_key: str) -> bool:
        """
        检查OSS对象是否存在
        
        Args:
            oss_key: OSS对象键名
            
        Returns:
            bool: 存在返回True，不存在返回False
            
        Example:
            >>> exists = handler.check_oss_object_exists(oss_key="path/to/file.jpg")
        """
        try:
            request: oss.HeadObjectRequest = oss.HeadObjectRequest(
                bucket=self.bucket_name,
                key=oss_key
            )
            self.client.head_object(request=request)
            logger.debug(f"OSS对象存在: {oss_key}")
            return True
        except Exception as e:
            logger.debug(f"OSS对象不存在: {oss_key}")
            return False
    
    def upload_m3u8_stream(self, m3u8_url: str, oss_base_key: str) -> bool:
        """
        上传m3u8文件到OSS（保留远程TS路径）
        
        Args:
            m3u8_url: m3u8在线地址
            oss_base_key: OSS存储前缀路径
            
        Returns:
            bool: 上传成功返回True，失败返回False
            
        Example:
            >>> success = handler.upload_m3u8_stream(
            ...     m3u8_url="https://example.com/video.m3u8",
            ...     oss_base_key="video_data/12345/encrypted_path/1"
            ... )
        """
        try:
            headers: dict[str, str] = {'User-Agent': 'Mozilla/5.0'}
            
            logger.info(f"开始下载m3u8: {m3u8_url}")
            response: requests.Response = self.session.get(
                url=m3u8_url,
                timeout=self.request_timeout,
                headers=headers,
                verify=False
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
            
            # 构建OSS对象键名
            oss_m3u8_key: str = f"{oss_base_key.rstrip('/')}/origin.m3u8"
            
            # 上传到OSS
            request: oss.PutObjectRequest = oss.PutObjectRequest(
                bucket=self.bucket_name,
                key=oss_m3u8_key,
                body=modified_m3u8.encode('utf-8')
            )
            result: oss.PutObjectResult = self.client.put_object(request=request)
            
            logger.info(f"m3u8上传成功: {oss_m3u8_key}, ETag: {result.etag}")
            return True
            
        except Exception as e:
            logger.error(f"上传m3u8到OSS失败: {e}")
            return False
    
    def upload_image_from_url(self, image_url: str, oss_key: str) -> bool:
        """
        从URL下载图片并上传到OSS
        
        Args:
            image_url: 网络图片URL
            oss_key: OSS对象键名
            
        Returns:
            bool: 上传成功返回True，失败返回False
            
        Example:
            >>> success = handler.upload_image_from_url(
            ...     image_url="https://example.com/cover.jpg",
            ...     oss_key="video_data/12345/encrypted_path/cover.jpg"
            ... )
        """
        try:
            headers: dict[str, str] = {'User-Agent': 'Mozilla/5.0'}
            
            logger.info(f"开始下载图片: {image_url}")
            response: requests.Response = self.session.get(
                url=image_url,
                timeout=self.request_timeout,
                headers=headers,
                verify=False
            )
            response.raise_for_status()
            
            # 上传到OSS
            request: oss.PutObjectRequest = oss.PutObjectRequest(
                bucket=self.bucket_name,
                key=oss_key,
                body=response.content
            )
            result: oss.PutObjectResult = self.client.put_object(request=request)
            
            logger.info(f"图片上传成功: {oss_key}, ETag: {result.etag}")
            return True
            
        except Exception as e:
            logger.error(f"图片上传OSS失败: {e}")
            return False
    
    def process_single_video_sync(
        self,
        douban_id: str,
        title: str,
        video_list: list[str],
        cover: str
    ) -> bool:
        """
        批量同步单个视频的所有剧集和封面到OSS
        
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
            logger.info(f"开始同步视频到OSS: {title} (douban_id: {douban_id})")
            
            # 上传所有剧集
            for index, m3u8_url in enumerate(video_list, start=1):
                if not m3u8_url:
                    logger.warning(f"跳过空的m3u8链接 (剧集 {index})")
                    continue
                
                oss_key: str = self.generate_oss_key(
                    title=title,
                    key='video_data',
                    douban_id=douban_id,
                    resource_type='m3u8',
                    episode=index
                )
                
                if not self.upload_m3u8_stream(m3u8_url=m3u8_url, oss_base_key=oss_key):
                    raise Exception(f"M3U8同步失败 (剧集 {index})")
            
            # 上传封面
            oss_key = self.generate_oss_key(
                title=title,
                key='video_data',
                douban_id=douban_id,
                resource_type='cover'
            )
            
            if not self.upload_image_from_url(image_url=cover, oss_key=oss_key):
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
        同步单个剧集和封面到OSS
        
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
            logger.info(f"开始同步单集到OSS: {title} 第{episode}集 (douban_id: {douban_id})")
            
            # 上传剧集
            oss_key: str = self.generate_oss_key(
                title=title,
                key='video_data_2',
                douban_id=douban_id,
                resource_type='m3u8',
                episode=episode
            )
            
            if not self.upload_m3u8_stream(m3u8_url=episode_url, oss_base_key=oss_key):
                raise Exception("M3U8同步失败")
            
            # 上传封面
            oss_key = self.generate_oss_key(
                title=title,
                key='video_data_2',
                douban_id=douban_id,
                resource_type='cover'
            )
            
            if not self.upload_image_from_url(image_url=cover, oss_key=oss_key):
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
        logger.debug("OSS处理器资源已释放")
